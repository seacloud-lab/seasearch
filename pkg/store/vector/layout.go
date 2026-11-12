package vector

import (
	"context"
	"fmt"
	"slices"

	"github.com/zincsearch/zincsearch/pkg/meta"
	vectorstore "github.com/zincsearch/zincsearch/pkg/vector"
)

// ReconcileLayout is explicit maintenance under the owner's metadata write lock,
// never a side effect of Get or Search. The caller marks dirty before entry and
// saves the entire Index even if this call fails after confirming some changes.
// This preserves the legacy contiguous growing-tail reconciliation, but does not
// invent metadata for gaps, missing sealed data, or unrecorded native snapshots.
func (f *fieldIndex) ReconcileLayout(ctx context.Context, ref *meta.VecIndex) error {
	err := f.check(ctx, ref)
	if err != nil {
		return err
	}
	if f.settings.TargetType != vectorstore.IVFPQ {
		return nil
	}
	stored, err := vectorstore.ListVecSegments(f.storeName)
	if err != nil {
		return err
	}
	for _, segRef := range ref.Segments {
		if _, ok := stored[int64(segRef.Id)]; !ok {
			if segRef.Id == ref.CurrentSegmentId && segRef.Status == vectorstore.StatusGrowing && segRef.Count == 0 {
				continue
			}
			return fmt.Errorf("%w: missing segment %d", ErrVecIndexCorruption, segRef.Id)
		}
	}
	var extra []int
	for id := range stored {
		if id < 0 || int64(int(id)) != id {
			return fmt.Errorf("%w: invalid segment %d", ErrVecIndexCorruption, id)
		}
		if int(id) >= len(ref.Segments) {
			extra = append(extra, int(id))
		}
	}
	slices.Sort(extra)
	// A missing empty current segment is legitimate only while it is the tail.
	if len(extra) > 0 {
		if _, ok := stored[int64(ref.CurrentSegmentId)]; !ok {
			return fmt.Errorf("%w: missing segment before unrecorded tail", ErrVecIndexCorruption)
		}
	}
	defer func() {
		ref.Count = 0
		for _, seg := range ref.Segments {
			ref.Count += seg.Count
		}
	}()
	for _, id := range extra {
		err := ctx.Err()
		if err != nil {
			return err
		}
		if id != len(ref.Segments) {
			return fmt.Errorf("%w: non-contiguous segment %d", ErrVecIndexCorruption, id)
		}
		// An orphan native index cannot safely be classified as a growing tail.
		exists, err := f.resources.storage.ExistsFile(fmt.Sprintf("%s/%04x/faiss", f.storeName, id))
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("%w: unrecorded sealed segment %d", ErrVecIndexCorruption, id)
		}
		segRef := &meta.VectorSegment{Id: id, Status: vectorstore.StatusGrowing}
		// Use a temporary runtime, not a cached call or metadata pointer.
		runtime, err := openIVFPQSegment(f, segRef)
		if err != nil {
			return err
		}
		call := &segmentCall{IVFPQSegment: runtime, ref: segRef, ctx: ctx}
		reader, err := call.openVecStoreReader()
		if err != nil {
			runtime.Close()
			return err
		}
		if reader == nil {
			runtime.Close()
			return fmt.Errorf("%w: empty unrecorded segment %d", ErrVecIndexCorruption, id)
		}
		count, countErr := reader.Count()
		reader.Close()
		runtime.Close()
		if countErr != nil {
			return countErr
		}
		segRef.Count = int64(count)
		ref.Segments = append(ref.Segments, segRef)
		ref.CurrentSegmentId = id
	}
	return ctx.Err()
}

package vector

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	vectorstore "github.com/zincsearch/zincsearch/pkg/vector"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"

	"golang.org/x/sync/errgroup"
)

type DocDistance struct {
	DocID    string
	Distance float32
}

type BatchResult struct {
	NeedBuildHNSW bool
}

// VectorIndex borrows metadata only until each call (and all its internal work)
// returns. The caller holds an owner Index reference throughout. Batch,
// ReconcileLayout, Seal and BuildHNSW require its metadata write lock and dirty
// marking before entry; all other operations require its metadata read lock.
// No operation persists Index metadata or acquires an Index reference.
// Store maintenance passes a non-cancelable context after its preflight checks;
// Seal/BuildHNSW and all cleanup must finish before the caller releases protection.
type VectorIndex interface {
	Batch(context.Context, *meta.VecIndex, [][]float32, []int64, []int64) (BatchResult, error)
	Search(context.Context, *meta.VecIndex, []float32, int64, int) (map[string]float32, error)
	SearchByIDs(context.Context, *meta.VecIndex, []float32, int64, []string) ([]DocDistance, error)
	ReconcileLayout(context.Context, *meta.VecIndex) error
	Seal(context.Context, *meta.VecIndex) error
	BuildHNSW(context.Context, *meta.VecIndex) error
	Recall(context.Context, *meta.VecIndex, int, int64, int) (float32, error)
	// Close performs best-effort cleanup after owner users have drained.
	// Writer owners log cleanup failures without propagating them.
	Close()
	config() fieldConfig
}

// fieldConfig is an immutable value copy, never a second layout or statistics
// authority. Changing it for an existing runtime requires a future rebuild API.
type fieldConfig struct {
	TargetType                      string
	Dims, M, NBits                  int
	NList                           int
	LayoutVersion                   float64
	StoreWithHash, StoreWithFloat16 bool
}

func configFromMeta(ref *meta.VecIndex) fieldConfig {
	return fieldConfig{
		TargetType:       ref.TargetType,
		Dims:             ref.Dims,
		M:                ref.M,
		NBits:            ref.NBits,
		NList:            ref.NList,
		LayoutVersion:    ref.LayoutVersion,
		StoreWithHash:    ref.StoreWithHash,
		StoreWithFloat16: ref.StoreWithFloat16,
	}
}

func validateLayout(ref *meta.VecIndex) error {
	if ref == nil || ref.Dims <= 0 {
		return fmt.Errorf("invalid vector configuration")
	}
	switch ref.TargetType {
	case vectorstore.Flat, vectorstore.IVFPQ, vectorstore.HNSW:
	default:
		return fmt.Errorf("invalid vector index type %q", ref.TargetType)
	}
	if len(ref.Segments) == 0 || ref.CurrentSegmentId < 0 || ref.CurrentSegmentId >= len(ref.Segments) {
		return fmt.Errorf("invalid vector segment layout")
	}
	for id, seg := range ref.Segments {
		if seg == nil || seg.Id != id || seg.Count < 0 || (seg.Status != vectorstore.StatusGrowing && seg.Status != vectorstore.StatusSealed) {
			return fmt.Errorf("invalid vector segment %d", id)
		}
	}
	if ref.TargetType != vectorstore.IVFPQ && (len(ref.Segments) != 1 || ref.CurrentSegmentId != 0) {
		return fmt.Errorf("invalid single-segment vector layout")
	}
	if ref.TargetType == vectorstore.IVFPQ && ref.Segments[ref.CurrentSegmentId].Status != vectorstore.StatusGrowing {
		return fmt.Errorf("current vector segment is not growing")
	}
	if ref.TargetType == vectorstore.IVFPQ && ref.CurrentSegmentId != len(ref.Segments)-1 {
		return fmt.Errorf("current vector segment is not the layout tail")
	}
	if ref.TargetType == vectorstore.Flat && ref.Segments[0].Status != vectorstore.StatusGrowing {
		return fmt.Errorf("flat vector segment is not growing")
	}
	return nil
}

type fieldIndex struct {
	name, storeName string
	settings        fieldConfig
	resources       *resources
	mutex           sync.Mutex // runtime creation/closing only; metadata is protected by store
	closed          bool
	segments        map[int]*IVFPQSegment
	hnsw            *HNSWSegment
}

func (f *fieldIndex) config() fieldConfig {
	return f.settings
}

func (f *fieldIndex) Name() string {
	return f.name
}

func (f *fieldIndex) StoreName() string {
	return f.storeName
}

func (f *fieldIndex) UseFloat16() bool {
	return f.settings.StoreWithFloat16
}

func (f *fieldIndex) check(ctx context.Context, ref *meta.VecIndex) error {
	err := ctx.Err()
	if err != nil {
		return err
	}
	f.mutex.Lock()
	closed := f.closed
	f.mutex.Unlock()
	if closed {
		return ErrClosed
	}
	err = validateLayout(ref)
	if err != nil {
		return err
	}
	if configFromMeta(ref) != f.settings {
		return fmt.Errorf("vector field %s configuration changed", f.name)
	}
	return f.resources.ensure()
}

// call is never cached: only seg (which has no metadata reference) is cached.
func (f *fieldIndex) segment(ctx context.Context, ref *meta.VectorSegment) (*segmentCall, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.closed {
		return nil, ErrClosed
	}
	seg := f.segments[ref.Id]
	if seg == nil {
		var err error
		seg, err = openIVFPQSegment(f, ref)
		if err != nil {
			return nil, err
		}
		f.segments[ref.Id] = seg
	}
	return &segmentCall{IVFPQSegment: seg, ref: ref, ctx: ctx}, nil
}

func (f *fieldIndex) Batch(ctx context.Context, ref *meta.VecIndex, vectors [][]float32, addIDs, deleteIDs []int64) (BatchResult, error) {
	var result BatchResult
	err := f.check(ctx, ref)
	if err != nil {
		return result, err
	}
	if len(vectors) != len(addIDs) {
		return result, fmt.Errorf("vector and ID counts differ")
	}
	for _, vec := range vectors {
		if len(vec) != f.settings.Dims {
			return result, fmt.Errorf("wrong vector dimensions: %d", len(vec))
		}
	}
	if f.hnsw != nil {
		_, err := f.hnsw.Batch(ctx, addIDs, vectors, deleteIDs)
		// Finish confirmed statistics even after cancellation/partial failure.
		count, countErr := f.hnsw.Count(context.WithoutCancel(ctx))
		if countErr == nil {
			ref.Count = count
			ref.Segments[0].Count = count
		}
		need, needErr := f.hnsw.NeedRebuildHNSW(ctx)
		result.NeedBuildHNSW = need
		return result, errors.Join(err, countErr, needErr)
	}
	defer func() {
		ref.Count = 0
		for _, seg := range ref.Segments {
			ref.Count += seg.Count
		}
	}()
	if len(deleteIDs) > 0 {
		for _, segRef := range ref.Segments {
			err := ctx.Err()
			if err != nil {
				return result, err
			}
			seg, err := f.segment(ctx, segRef)
			if err != nil {
				return result, err
			}
			ids, err := seg.GetExistsIds(deleteIDs)
			if err != nil {
				return result, err
			}
			if len(ids) > 0 {
				err = seg.Batch(&segBatch{deleteIds: ids})
				if err != nil {
					return result, err
				}
			}
		}
	}
	if len(addIDs) > 0 {
		err := ctx.Err()
		if err != nil {
			return result, err
		}
		segRef := ref.Segments[ref.CurrentSegmentId]
		seg, err := f.segment(ctx, segRef)
		if err != nil {
			return result, err
		}
		err = seg.Batch(&segBatch{
			addIds:     addIDs,
			addVectors: vectors,
		})
		if err != nil {
			return result, err
		}
		if f.settings.TargetType == vectorstore.IVFPQ && segRef.Count >= config.Global.VectorConfig.IvfPqThreshold {
			id := len(ref.Segments)
			ref.Segments = append(ref.Segments, &meta.VectorSegment{Id: id, Status: vectorstore.StatusGrowing})
			ref.CurrentSegmentId = id
		}
	}
	return result, nil
}

type segBatch struct {
	addIds, deleteIds []int64
	addVectors        [][]float32
}

func (f *fieldIndex) Search(ctx context.Context, ref *meta.VecIndex, query []float32, k int64, nprobe int) (map[string]float32, error) {
	err := f.checkQuery(ctx, ref, query, k)
	if err != nil {
		return nil, err
	}
	if f.hnsw != nil {
		ids, distances, err := f.hnsw.Search(ctx, query, k)
		if err != nil {
			return nil, err
		}
		result := make(map[string]float32, len(ids))
		for i, id := range ids {
			result[base62.Encode(id)] = distances[i]
		}
		return result, ctx.Err()
	}
	// Each worker owns its result slot; merge only after all workers end their
	// metadata borrow, even on failure. No goroutine escapes this operation.
	results := make([]map[string]float32, len(ref.Segments))
	var workers errgroup.Group
	workers.SetLimit(max(1, config.Global.Shard.GoroutineNum))
	for i, segRef := range ref.Segments {
		workers.Go(func() error {
			err := ctx.Err()
			if err != nil {
				return err
			}
			seg, err := f.segment(ctx, segRef)
			if err != nil {
				return err
			}
			results[i], err = seg.Search(query, k, nprobe)
			return err
		})
	}
	err = workers.Wait()
	if err != nil {
		return nil, err
	}
	merged := make(map[string]float32)
	for _, result := range results {
		for id, distance := range result {
			if old, ok := merged[id]; !ok || distance < old {
				merged[id] = distance
			}
		}
	}
	items := make([]DocDistance, 0, len(merged))
	for id, distance := range merged {
		items = append(items, DocDistance{
			DocID:    id,
			Distance: distance,
		})
	}
	sortDistances(items)
	result := make(map[string]float32)
	for _, item := range items[:min(len(items), int(k))] {
		result[item.DocID] = item.Distance
	}
	return result, ctx.Err()
}

func (f *fieldIndex) checkQuery(ctx context.Context, ref *meta.VecIndex, query []float32, k int64) error {
	err := f.check(ctx, ref)
	if err != nil {
		return err
	}
	if len(query) != f.settings.Dims || k <= 0 {
		return fmt.Errorf("invalid vector query dimensions or k")
	}
	return nil
}

func sortDistances(items []DocDistance) {
	slices.SortFunc(items, func(a, b DocDistance) int {
		return cmp.Or(cmp.Compare(a.Distance, b.Distance), cmp.Compare(a.DocID, b.DocID))
	})
}

func (f *fieldIndex) SearchByIDs(ctx context.Context, ref *meta.VecIndex, query []float32, k int64, ids []string) ([]DocDistance, error) {
	err := f.checkQuery(ctx, ref, query, k)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	var items []DocDistance
	if f.hnsw != nil {
		filter := make([]int64, len(ids))
		for i, id := range ids {
			filter[i] = base62.Decode(id)
		}
		matched, distances, err := f.hnsw.SearchByIDs(ctx, query, k, filter)
		if err != nil {
			return nil, err
		}
		for i, id := range matched {
			items = append(items, DocDistance{
				DocID:    base62.Encode(id),
				Distance: distances[i],
			})
		}
	} else {
		for _, segRef := range ref.Segments {
			err := ctx.Err()
			if err != nil {
				return nil, err
			}
			seg, err := f.segment(ctx, segRef)
			if err != nil {
				return nil, err
			}
			result, err := seg.SearchByIDs(query, k, ids)
			if err != nil {
				return nil, err
			}
			items = append(items, result...)
		}
	}
	// Old copies may remain in multiple segments after a partial write. Dedup
	// before applying k so one document cannot consume multiple candidate slots.
	merged := make(map[string]float32, len(items))
	for _, item := range items {
		if old, ok := merged[item.DocID]; !ok || item.Distance < old {
			merged[item.DocID] = item.Distance
		}
	}
	items = items[:0]
	for id, distance := range merged {
		items = append(items, DocDistance{DocID: id, Distance: distance})
	}
	sortDistances(items)
	return items[:min(len(items), int(k))], ctx.Err()
}

func (f *fieldIndex) Seal(ctx context.Context, ref *meta.VecIndex) error {
	err := f.check(ctx, ref)
	if err != nil {
		return err
	}
	if f.settings.TargetType != vectorstore.IVFPQ {
		return fmt.Errorf("seal requires IVFPQ")
	}
	for _, segRef := range ref.Segments {
		err := ctx.Err()
		if err != nil {
			return err
		}
		if segRef.Id == ref.CurrentSegmentId || segRef.Status != vectorstore.StatusGrowing || segRef.Count < config.Global.VectorConfig.IvfPqThreshold {
			continue
		}
		seg, err := f.segment(ctx, segRef)
		if err != nil {
			return err
		}
		err = seg.Seal()
		if err != nil {
			return fmt.Errorf("seal segment %d: %w", segRef.Id, err)
		}
	}
	return ctx.Err()
}

func (f *fieldIndex) BuildHNSW(ctx context.Context, ref *meta.VecIndex) error {
	err := f.check(ctx, ref)
	if err != nil {
		return err
	}
	if f.hnsw == nil {
		return fmt.Errorf("build requires HNSW")
	}
	// One build drains the state observed under the caller's exclusive metadata
	// lock; no concurrent store Batch can generate more work during this call.
	need, err := f.hnsw.NeedRebuildHNSW(ctx)
	if err != nil {
		return err
	}
	if need {
		return f.hnsw.BuildHNSW(ctx)
	}
	return ctx.Err()
}

func (f *fieldIndex) Recall(ctx context.Context, ref *meta.VecIndex, count int, k int64, nprobe int) (float32, error) {
	err := f.check(ctx, ref)
	if err != nil {
		return 0, err
	}
	if f.hnsw != nil {
		return 0, fmt.Errorf("recall is not supported for HNSW")
	}
	if count <= 0 || k <= 0 {
		return 0, fmt.Errorf("invalid recall count or k")
	}
	var total float32
	for _, segRef := range ref.Segments {
		err := ctx.Err()
		if err != nil {
			return 0, err
		}
		seg, err := f.segment(ctx, segRef)
		if err != nil {
			return 0, err
		}
		value, err := seg.Recall(count, k, nprobe)
		if err != nil {
			return 0, err
		}
		total += value
	}
	return total / float32(len(ref.Segments)), ctx.Err()
}

func (f *fieldIndex) Close() {
	f.mutex.Lock()
	if f.closed {
		f.mutex.Unlock()
		return
	}
	f.closed = true
	segments, hnsw := f.segments, f.hnsw
	f.segments, f.hnsw = nil, nil
	f.mutex.Unlock()

	for _, seg := range segments {
		seg.Close()
	}
	if hnsw != nil {
		hnsw.Close()
	}
}

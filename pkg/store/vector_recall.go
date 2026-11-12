package store

import (
	"context"
	"fmt"

	"github.com/zincsearch/zincsearch/pkg/vector"
)

// VectorRecall borrows an owner Index reference and holds mu's read lock through
// sampling, native execution and Reader cleanup. It never reconciles or saves
// layout. IVFPQ preserves the unweighted mean of per-segment recall (growing
// segments count as 1); Flat needs no storage resources, and HNSW is unsupported.
func (index *Index) VectorRecall(ctx context.Context, field string, queryCount int, k int64, nprobe int) (float32, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if queryCount <= 0 || k <= 0 || nprobe <= 0 {
		return 0, fmt.Errorf("recall query count, k and nprobe must be positive")
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	ref, err := index.vectorMetadataLocked(field)
	if err != nil {
		return 0, err
	}
	// Get validates the current layout without opening storage or native objects,
	// including for Flat's resource-free shortcut.
	runtime, err := index.vector.Get(field, ref)
	if err != nil {
		return 0, fmt.Errorf("get vector field %s on index %s: %w", field, index.name, err)
	}
	switch ref.TargetType {
	case vector.Flat:
		return 1, ctx.Err()
	case vector.HNSW:
		return 0, fmt.Errorf("recall is not supported for HNSW")
	default:
		value, err := runtime.Recall(ctx, ref, queryCount, k, nprobe)
		if err != nil {
			return 0, fmt.Errorf("recall vector field %s on index %s: %w", field, index.name, err)
		}
		return value, ctx.Err()
	}
}

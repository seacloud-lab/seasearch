package store

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/zincsearch/zincsearch/pkg/vector"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
)

// prepareVectors runs before any text worker. Get never repairs metadata;
// reconciliation is explicitly serialized here. Only a successful check of all
// fields is remembered for this Index lifetime, independently of dirty/save
// success. Partial failures retry all fields on the next call. The enclosing
// operation must save even if preparation partly fails.
// The caller must hold index.mutex's write lock; this helper does not acquire it.
func (index *Index) prepareVectors() error {
	if index.vectorLayoutChecked {
		return nil
	}
	fields := make([]string, 0, len(index.ref.VecIndexes))
	for field := range index.ref.VecIndexes {
		fields = append(fields, field)
	}
	slices.Sort(fields)
	for _, field := range fields {
		runtime, err := index.vector.Get(field, index.ref.VecIndexes[field])
		if err != nil {
			return fmt.Errorf("prepare vector field %s: %w", field, err)
		}
		index.dirty = true
		err = runtime.ReconcileLayout(context.Background(), index.ref.VecIndexes[field])
		if err != nil {
			return fmt.Errorf("reconcile vector field %s: %w", field, err)
		}
	}
	index.vectorLayoutChecked = true
	return nil
}

// batchVectors consumes one confirmed target batch, never combines windows
// (delete/add ordering matters for repeated IDs). Different text workers may
// address the same field, so only the parent calls this after all workers finish.
// All fields are attempted, even if one fails; no component persists metadata.
// The caller must hold index.mutex's write lock; this helper does not acquire it.
func (index *Index) batchVectors(actions []*vector.VecAction) error {
	byField := make(map[string][]*vector.VecAction)
	for _, action := range actions {
		if action == nil {
			return fmt.Errorf("nil vector action")
		}
		byField[action.Field] = append(byField[action.Field], action)
	}
	fields := make([]string, 0, len(byField))
	for field := range byField {
		fields = append(fields, field)
	}
	slices.Sort(fields)
	var errs []error
	for _, field := range fields {
		ref := index.ref.VecIndexes[field]
		if ref == nil {
			errs = append(errs, fmt.Errorf("vector field %s has no metadata", field))
			continue
		}
		var vectors [][]float32
		var addIDs, deleteIDs []int64
		var prepareErr error
		for _, action := range byField[field] {
			if action.Index != index.name || base62.Encode(base62.Decode(action.DocId)) != action.DocId {
				prepareErr = fmt.Errorf("invalid vector action index or document ID %q", action.DocId)
				break
			}
			id := base62.Decode(action.DocId)
			switch action.Action {
			case vector.Update:
				deleteIDs = append(deleteIDs, id)
				fallthrough
			case vector.Insert:
				addIDs = append(addIDs, id)
				vectors = append(vectors, action.Vector)
			case vector.Delete:
				deleteIDs = append(deleteIDs, id)
			default:
				prepareErr = fmt.Errorf("invalid vector action %d", action.Action)
			}
		}
		if prepareErr != nil {
			errs = append(errs, fmt.Errorf("vector field %s: %w", field, prepareErr))
			continue
		}
		runtime, err := index.vector.Get(field, ref)
		if err != nil {
			errs = append(errs, fmt.Errorf("get vector field %s: %w", field, err))
			continue
		}
		index.dirty = true // Batch may modify layout/statistics before returning an error.
		result, err := runtime.Batch(context.Background(), ref, vectors, addIDs, deleteIDs)
		if result.NeedBuildHNSW {
			// Partial Batch failure may still leave rebuild work. Submission never
			// waits for the worker; disabled/stopped scheduling does not fail writes.
			_ = vectorMaintenance.submit(maintenanceHint{index: index.name, field: field, kind: maintenanceBuildHNSW})
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("write vector field %s: %w", field, err))
		}
	}
	return errors.Join(errs...)
}

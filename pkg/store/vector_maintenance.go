package store

import (
	"context"
	"errors"
	"fmt"

	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/vector"
)

// SealVector checks the current field and seals eligible non-current IVFPQ
// segments. The caller holds an owner Index reference until this call returns.
// Cancellation is checked before work starts (including after waiting for mu);
// once started, maintenance and saving run synchronously without cancellation.
// Missing fields, other algorithms and fields needing no work are successful
// no-ops. Errors do not imply that no persistent changes have taken effect.
func (index *Index) SealVector(ctx context.Context, field string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	ref, err := index.maintenanceMetadataLocked(field, vector.IVFPQ)
	if err != nil || ref == nil {
		return err
	}
	runtime, err := index.vector.Get(field, ref)
	if err != nil {
		return fmt.Errorf("get vector field %s on index %s: %w", field, index.name, err)
	}
	workCtx := context.WithoutCancel(ctx)
	index.dirty = true // Reconciliation and seal can partly succeed before failure.
	err = runtime.ReconcileLayout(workCtx, ref)
	if err == nil {
		err = runtime.Seal(workCtx, ref)
	}
	if err != nil {
		err = fmt.Errorf("seal vector field %s on index %s: %w", field, index.name, err)
	}
	return errors.Join(err, index.saveLocked())
}

// BuildHNSW checks the current field and rebuilds its snapshot when needed. It
// follows SealVector's reference, cancellation, no-op and partial-failure rules.
func (index *Index) BuildHNSW(ctx context.Context, field string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	ref, err := index.maintenanceMetadataLocked(field, vector.HNSW)
	if err != nil || ref == nil {
		return err
	}
	runtime, err := index.vector.Get(field, ref)
	if err != nil {
		return fmt.Errorf("get vector field %s on index %s: %w", field, index.name, err)
	}
	index.dirty = true // Conservatively save the authoritative state after work.
	err = runtime.BuildHNSW(context.WithoutCancel(ctx), ref)
	if err != nil {
		err = fmt.Errorf("build HNSW field %s on index %s: %w", field, index.name, err)
	}
	return errors.Join(err, index.saveLocked())
}

// maintenanceMetadataLocked distinguishes an obsolete hint from corrupt current
// metadata. It requires mu's write lock and never opens runtime resources.
func (index *Index) maintenanceMetadataLocked(field, algorithm string) (*meta.VecIndex, error) {
	if index.readOnly {
		return nil, zincerrors.ErrIndexServerMismatch
	}
	if index.ref.Mappings == nil {
		return nil, fmt.Errorf("index %s has no mappings", index.name)
	}
	prop, ok := index.ref.Mappings.GetProperty(field)
	if !ok || prop.Type != "vector" {
		return nil, nil
	}
	ref, err := index.vectorMetadataLocked(field)
	if err != nil {
		return nil, err
	}
	if ref.TargetType != prop.VecIndexType {
		return nil, fmt.Errorf("vector field %s algorithm disagrees with mapping on index %s", field, index.name)
	}
	switch ref.TargetType {
	case vector.Flat, vector.IVFPQ, vector.HNSW:
	default:
		return nil, fmt.Errorf("invalid vector index type %q on index %s", ref.TargetType, index.name)
	}
	if ref.TargetType != algorithm {
		return nil, nil
	}
	return ref, nil
}

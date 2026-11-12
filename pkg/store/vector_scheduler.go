package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/config"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/vector"
)

// ErrVectorMaintenanceStopped means hints cannot be accepted before Start or
// after Stop. It does not affect synchronous Index maintenance methods.
var ErrVectorMaintenanceStopped = errors.New("vector maintenance is not running")

type maintenanceKind string

const (
	maintenanceSeal      maintenanceKind = "seal"
	maintenanceBuildHNSW maintenanceKind = "build_hnsw"
)

// Hints describe current-state checks, never a particular Index incarnation.
type maintenanceHint struct {
	index string
	field string
	kind  maintenanceKind
}

type vectorMaintenanceScheduler struct {
	// control serializes Start/Stop, including waiting for the previous workers.
	// Producers and workers only use mu, never control.
	control sync.Mutex
	mu      sync.Mutex
	running bool
	pending map[maintenanceHint]struct{}
	items   []maintenanceHint
	wake    chan struct{}
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

var vectorMaintenance vectorMaintenanceScheduler

// StartVectorMaintenance starts one worker and an hourly IVFPQ scan. The first
// scan runs after one hour; HNSW has no periodic compensation. Repeated starts
// are harmless. Do not start alongside legacy core vector background work on
// the same storage paths. Application startup integration is deliberately separate.
func StartVectorMaintenance() {
	s := &vectorMaintenance
	s.control.Lock()
	defer s.control.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.running = true
	s.pending = make(map[maintenanceHint]struct{})
	s.items = nil
	s.wake = make(chan struct{}, 1)
	s.cancel = cancel
	s.wg.Add(2)
	go s.work(ctx)
	go s.scan(ctx)
}

// StopVectorMaintenance rejects new hints, discards queued hints and waits for
// both loops and any started maintenance to finish. Open/metadata-lock waits and
// started native work are not forcibly interrupted. Call before Store.Stop and
// before closing metadata or storage dependencies. Repeated stops are harmless.
func StopVectorMaintenance() {
	s := &vectorMaintenance
	s.control.Lock()
	defer s.control.Unlock()
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	s.cancel()
	s.items = nil
	s.pending = nil
	s.mu.Unlock()
	s.wg.Wait()
}

// RequestVectorSeal queues a current-state IVFPQ check without loading an Index.
// Success means accepted (or already pending), not completed or even necessary.
// Ownership, field validity and maintenance conditions are checked by the worker.
func RequestVectorSeal(indexName, field string) error {
	if indexName == "" || field == "" {
		return fmt.Errorf("index and vector field must not be empty")
	}
	return vectorMaintenance.submit(maintenanceHint{index: indexName, field: field, kind: maintenanceSeal})
}

// submit never waits for the worker, so callers may hold Index.mu. Only pending
// hints are deduplicated; a hint taken by the worker may immediately queue again.
func (s *vectorMaintenanceScheduler) submit(hint maintenanceHint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return ErrVectorMaintenanceStopped
	}
	if _, exists := s.pending[hint]; exists {
		return nil
	}
	s.pending[hint] = struct{}{}
	s.items = append(s.items, hint)
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
}

func (s *vectorMaintenanceScheduler) take() (maintenanceHint, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running || len(s.items) == 0 {
		return maintenanceHint{}, false
	}
	hint := s.items[0]
	s.items[0] = maintenanceHint{}
	s.items = s.items[1:]
	if len(s.items) == 0 {
		s.items = nil
	}
	delete(s.pending, hint)
	return hint, true
}

func (s *vectorMaintenanceScheduler) work(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		}
		for ctx.Err() == nil {
			hint, ok := s.take()
			if !ok {
				break
			}
			if err := executeMaintenanceHint(ctx, hint); err != nil {
				if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
					continue
				}
				log.Error().Err(err).Str("index", hint.index).Str("field", hint.field).
					Str("task", string(hint.kind)).Msg("vector maintenance failed")
			}
		}
	}
}

func executeMaintenanceHint(ctx context.Context, hint maintenanceHint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	index, err := Open(hint.index)
	if errors.Is(err, ErrIndexNotFound) || errors.Is(err, zincerrors.ErrIndexServerMismatch) {
		return nil
	}
	if err != nil {
		return err
	}
	defer index.Close()
	switch hint.kind {
	case maintenanceSeal:
		return index.SealVector(ctx, hint.field)
	case maintenanceBuildHNSW:
		return index.BuildHNSW(ctx, hint.field)
	default:
		return fmt.Errorf("unknown vector maintenance task %q", hint.kind)
	}
}

func (s *vectorMaintenanceScheduler) scan(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.scanIVFPQ(ctx); err != nil && ctx.Err() == nil {
				log.Error().Err(err).Msg("failed to scan IVFPQ maintenance")
			}
		}
	}
}

// Scan persisted metadata only, never runtime resources. Failed loads/seals are
// retried by a later scan, not a hot retry loop. HNSW failures require a later
// write to produce another hint; no cold HNSW resources are loaded by this scan.
func (s *vectorMaintenanceScheduler) scanIVFPQ(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	indexes, err := metadata.Index.List(0, 0)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !cluster.AssignCheck(index.Name) {
			continue
		}
		for field, ref := range index.VecIndexes {
			if err := ctx.Err(); err != nil {
				return err
			}
			if ref == nil || ref.TargetType != vector.IVFPQ {
				continue
			}
			for _, segment := range ref.Segments {
				if segment != nil && segment.Id != ref.CurrentSegmentId &&
					segment.Status == vector.StatusGrowing && segment.Count >= config.Global.VectorConfig.IvfPqThreshold {
					if err := s.submit(maintenanceHint{index: index.Name, field: field, kind: maintenanceSeal}); err != nil {
						return err
					}
					break
				}
			}
		}
	}
	return nil
}

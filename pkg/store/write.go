package store

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store/text"

	"golang.org/x/sync/errgroup"
)

// shardWrite is request-owned state for one first shard. Seconds accumulates
// across rounds; ReadyVectors and err are drained by the parent after each round.
type shardWrite struct {
	id         string
	operations []text.Operation
	next       int
	stopped    bool
	result     text.ShardWriteResult
	err        error
}

func newShardWrites(groups map[string][]text.Operation) []*shardWrite {
	work := make([]*shardWrite, 0, len(groups))
	for id, operations := range groups {
		work = append(work, &shardWrite{
			id:         id,
			operations: operations,
		})
	}
	slices.SortFunc(work, func(a, b *shardWrite) int {
		return cmp.Compare(a.id, b.id)
	})
	return work
}

// commit requires the caller to hold index.mutex's write lock through vector
// initialization, bounded text windows, vector work, layout publication and one
// final save. Text workers borrow metadata read-only. Between rounds the parent
// alone mutates vector metadata. A shard that fails stops its later windows,
// without abandoning other shards or cleanup.
func (index *Index) commit(groups map[string][]text.Operation) error {
	batchSize := config.Global.BatchSize
	if batchSize <= 0 {
		return errors.Join(fmt.Errorf("batch size must be positive"), index.save())
	}
	work := newShardWrites(groups)
	if len(work) > 0 {
		err := index.prepareVectors()
		if err != nil {
			return errors.Join(err, index.save())
		}
	}
	var errs []error
	for {
		active, err := index.executeWriteRound(work, batchSize)
		if !active {
			break
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	err := index.applyWriteResults(work)
	if err != nil {
		errs = append(errs, err)
	}
	err = index.save()
	if err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// executeWriteRound runs at most one window per active first shard. All
// text workers must exit before the parent mutates vector metadata. Vector
// payloads are drained here, rather than retained for the entire bulk.
// The caller must hold index.mutex's write lock until all workers finish.
func (index *Index) executeWriteRound(work []*shardWrite, batchSize int) (bool, error) {
	var workers errgroup.Group
	workers.SetLimit(max(1, config.Global.Shard.GoroutineNum))
	active := false
	for _, item := range work {
		if item.stopped || item.next >= len(item.operations) {
			continue
		}
		active = true
		workers.Go(func() error {
			item.err = index.executeTextWindow(item, batchSize)
			return nil // All workers finish, even on partial failure.
		})
	}
	_ = workers.Wait()
	if !active {
		return false, nil
	}

	var errs []error
	for _, item := range work {
		if item.err != nil {
			errs = append(errs, item.err)
			item.stopped, item.err = true, nil
		}
		// A text error must not discard earlier confirmed target batches.
		for _, actions := range item.result.ReadyVectors {
			err := index.batchVectors(actions)
			if err != nil {
				errs = append(errs, fmt.Errorf("first shard %s: %w", item.id, err))
				item.stopped = true
				break // Do not apply later target batches out of order after failure.
			}
		}
		item.result.ReadyVectors = nil
	}
	return true, errors.Join(errs...)
}

// executeTextWindow runs in a worker while the parent holds index.mutex's write lock.
// It only mutates this shard's request state and borrows shared metadata read-only.
func (index *Index) executeTextWindow(item *shardWrite, batchSize int) error {
	end := min(item.next+batchSize, len(item.operations))
	batches, err := text.PrepareBatches(index.name, index.ref.Mappings, index.analyzers, index.ref.VecIndexes, item.operations[item.next:end])
	item.next = end
	if err != nil {
		return fmt.Errorf("prepare first shard %s: %w", item.id, err)
	}
	err = index.text.GetShardByShardKey(item.id).ExecuteBatches(batches, &item.result)
	if err != nil {
		return fmt.Errorf("write first shard %s: %w", item.id, err)
	}
	return nil
}

// applyWriteResults publishes accumulated statistics and shard expansion
// after all rounds, including partial failures. The caller saves metadata separately.
// The caller must hold index.mutex's write lock; this helper does not acquire it.
func (index *Index) applyWriteResults(work []*shardWrite) error {
	var errs []error
	for _, item := range work {
		if len(item.result.Seconds) == 0 {
			continue
		}
		index.dirty = true
		ref := index.ref.Shards[item.id]
		for id, result := range item.result.Seconds {
			stats := &ref.Shards[id].Stats
			if result.CountValid {
				stats.DocNum = result.DocNum
			}
			if result.SizeValid {
				stats.StorageSize = result.StorageSize
			}
			for _, timestamp := range result.Timestamps {
				includeTimestamp(stats, timestamp)
			}
		}
		latest := ref.Shards[len(ref.Shards)-1]
		ref.Stats.DocTimeMin = latest.Stats.DocTimeMin
		ref.Stats.DocTimeMax = latest.Stats.DocTimeMax
		// Preserve the legacy size threshold and one expansion per commit. The
		// next request can use the new shard only after this save attempt ends.
		if result := item.result.Seconds[latest.ID]; result != nil && result.SizeValid && latest.Stats.StorageSize > uint64(config.Global.Shard.MaxSize) {
			err := index.text.GetShardByShardKey(item.id).AppendShard(ref)
			if err != nil {
				errs = append(errs, err)
			}
		}
		ref.Stats.DocNum, ref.Stats.StorageSize = 0, 0
		for _, second := range ref.Shards {
			ref.Stats.DocNum += second.Stats.DocNum
			ref.Stats.StorageSize += second.Stats.StorageSize
		}
	}
	if index.dirty {
		index.ref.Stats.DocNum, index.ref.Stats.StorageSize = 0, 0
		for _, shard := range index.ref.Shards {
			index.ref.Stats.DocNum += shard.Stats.DocNum
			index.ref.Stats.StorageSize += shard.Stats.StorageSize
		}
	}
	return errors.Join(errs...)
}

func includeTimestamp(stats *meta.IndexStat, timestamp int64) {
	if stats.DocTimeMin == 0 || timestamp < stats.DocTimeMin {
		stats.DocTimeMin = timestamp
	}
	if stats.DocTimeMax == 0 || timestamp > stats.DocTimeMax {
		stats.DocTimeMax = timestamp
	}
}

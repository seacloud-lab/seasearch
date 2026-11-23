package text

import (
	"errors"
	"fmt"
	"slices"

	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/vector"
)

// ShardWriteResult contains only operation-owned values. Parallel first-shard
// workers never mutate shared metadata. The store applies results after waiting
// for every worker, including workers that failed after a partial commit.
type ShardWriteResult struct {
	Seconds map[int64]*SecondWriteResult
	// ReadyVectors preserves target-batch order. Only batches whose primary
	// text write AND old-copy cleanup succeeded are eligible; vector-only batches
	// are immediately eligible. Failed/uncertain or unstarted batches never enter
	// this list. Statistics errors do not undo a confirmed batch's eligibility.
	// The store drains this request-owned list after each bounded window.
	ReadyVectors [][]*vector.VecAction
}

type SecondWriteResult struct {
	DocNum      uint64
	CountValid  bool
	StorageSize uint64
	SizeValid   bool
	Timestamps  []int64
}

// ExecuteBatches executes one prepared window sequentially within a first shard.
// The caller holds the owner Index reference and metadata write lock. Writers
// remain cached; all statistics Readers are closed before returning.
func (s *IndexShard) ExecuteBatches(batches map[int64]*PreparedBatch, result *ShardWriteResult) error {
	ids := make([]int64, 0, len(batches))
	for id := range batches {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var writeErr error
	for _, id := range ids {
		batch := batches[id]
		if batch.TextOperations == 0 {
			if len(batch.Vectors) > 0 {
				result.ReadyVectors = append(result.ReadyVectors, batch.Vectors)
			}
			continue
		}
		target := id
		if target < 0 {
			target = s.GetLatestShardID()
		}
		writer, err := s.GetWriter(target)
		if err != nil {
			writeErr = err
			break
		}
		second := result.second(target)
		// Include attempted timestamps conservatively: an error from Batch does
		// not promise rollback. Wider time bounds are safe; narrower bounds could
		// hide a partially committed document from time-filtered queries.
		second.Timestamps = append(second.Timestamps, batch.Timestamps...)
		err = writer.Batch(batch.Batch)
		if err != nil {
			writeErr = fmt.Errorf("write shard %s/%d: %w", s.name, target, err)
			break
		}
		if id == ShardIDNeedUpdate && batch.DeleteOperations > 0 {
			for old := range target {
				writer, err := s.GetWriter(old)
				if err != nil {
					writeErr = err
					break
				}
				result.second(old)
				err = writer.Batch(batch.DeleteOthers)
				if err != nil {
					writeErr = fmt.Errorf("delete old copies in shard %s/%d: %w", s.name, old, err)
					break
				}
			}
			if writeErr != nil {
				break
			}
		}
		if len(batch.Vectors) > 0 {
			result.ReadyVectors = append(result.ReadyVectors, batch.Vectors)
		}
	}
	// Refresh every attempted shard even when Batch or a later old-copy deletion
	// fails. Never replace an existing count with zero when reading it failed.
	var errs []error
	if writeErr != nil {
		errs = append(errs, writeErr)
	}
	for id, second := range result.Seconds {
		writer, err := s.GetWriter(id)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		_, second.StorageSize = writer.DirectoryStats()
		second.SizeValid = true
		reader, err := writer.Reader()
		if err != nil {
			second.CountValid = false
			errs = append(errs, fmt.Errorf("read shard %s/%d statistics: %w", s.name, id, err))
			continue
		}
		count, countErr := reader.Count()
		second.CountValid = countErr == nil
		if countErr == nil {
			second.DocNum = count
		}
		reader.Close()
		if countErr != nil {
			errs = append(errs, fmt.Errorf("read shard %s/%d statistics: %w", s.name, id, countErr))
		}
	}
	return errors.Join(errs...)
}

func (r *ShardWriteResult) second(id int64) *SecondWriteResult {
	if r.Seconds == nil {
		r.Seconds = make(map[int64]*SecondWriteResult)
	}
	if r.Seconds[id] == nil {
		r.Seconds[id] = new(SecondWriteResult)
	}
	return r.Seconds[id]
}

// AppendShard publishes a new empty runtime shard and its borrowed metadata
// together. The store must hold its Index metadata mutex exclusively and mark
// metadata dirty first.
// No Writer is opened and no persistence is performed here.
func (s *IndexShard) AppendShard(ref *meta.IndexShard) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return ErrClosed
	}
	if s.options.ReadOnly {
		return ErrReadOnly
	}
	if ref == nil || ref.ID != s.id || ref.ShardNum != int64(len(s.shards)) || len(ref.Shards) != len(s.shards) {
		return fmt.Errorf("inconsistent layout for shard %s", s.name)
	}
	id := int64(len(s.shards))
	ref.Shards = append(ref.Shards, &meta.IndexSecondShard{ID: id})
	ref.ShardNum++
	// First-shard time bounds track the active second shard in the legacy format.
	ref.Stats.DocTimeMin = 0
	ref.Stats.DocTimeMax = 0
	s.shards = append(s.shards, &IndexSecondShard{id: id})
	return nil
}

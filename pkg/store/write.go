package store

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
	"golang.org/x/sync/errgroup"
)

// DocOperation preserves compatibility with bulk callers during their migration.
// The new write path never calls the embedded legacy Index.
type DocOperation = core.DocOperation

// CreateDocument inserts a document, or replaces an existing document when update
// is true. The caller holds one Index reference through this synchronous call.
func (index *Index) CreateDocument(docID string, doc map[string]interface{}, update bool) error {
	return index.CreateDocuments([]DocOperation{{DocId: docID, Doc: doc, Update: update}})
}

func (index *Index) CreateDocuments(operations []DocOperation) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := index.checkWriteLocked(); err != nil {
		return err
	}
	groups := make(map[string][]text.Operation)
	for _, operation := range operations {
		shard := index.text.GetShardByDocID(operation.DocId)
		id := text.ShardIDNeedLatest
		update := operation.Update
		if update {
			var err error
			id, _, err = shard.FindDocument(context.Background(), index.ref.Shards[shard.GetID()], operation.DocId, false)
			if errors.Is(err, zincerrors.ErrorIDNotFound) {
				update = false
			} else if err != nil {
				return errors.Join(err, index.saveLocked())
			}
		}
		op, err := index.prepareDocumentLocked(operation.DocId, operation.Doc, update, id)
		if err != nil {
			return errors.Join(err, index.saveLocked())
		}
		groups[shard.GetID()] = append(groups[shard.GetID()], op)
	}
	return index.commitLocked(groups)
}

// UpdateDocument preserves replace/upsert semantics: insert controls whether an
// absent document can be created. It does not merge the old source with doc.
func (index *Index) UpdateDocument(docID string, doc map[string]interface{}, insert bool) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := index.checkWriteLocked(); err != nil {
		return err
	}
	shard := index.text.GetShardByDocID(docID)
	id, _, err := shard.FindDocument(context.Background(), index.ref.Shards[shard.GetID()], docID, false)
	update := true
	if err != nil {
		if !insert || !errors.Is(err, zincerrors.ErrorIDNotFound) {
			return err
		}
		update = false
	}
	op, err := index.prepareDocumentLocked(docID, doc, update, id)
	if err != nil {
		return errors.Join(err, index.saveLocked())
	}
	return index.commitLocked(map[string][]text.Operation{shard.GetID(): {op}})
}

func (index *Index) DeleteDocument(docID string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := index.checkWriteLocked(); err != nil {
		return err
	}
	return index.deleteDocumentsLocked([]string{docID}, false)
}

// DeleteDocuments ignores missing text documents but still deletes their vectors,
// preserving the legacy bulk cleanup of vector remnants after partial failures.
func (index *Index) DeleteDocuments(docIDs []string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := index.checkWriteLocked(); err != nil {
		return err
	}
	return index.deleteDocumentsLocked(docIDs, true)
}

func (index *Index) deleteDocumentsLocked(docIDs []string, ignoreMissing bool) error {
	groups := make(map[string][]text.Operation)
	for _, docID := range docIDs {
		if len(index.ref.VecIndexes) > 0 && base62.Encode(base62.Decode(docID)) != docID {
			return fmt.Errorf("document ID (%v) is not a valid base62 string", docID)
		}
		shard := index.text.GetShardByDocID(docID)
		id, _, err := shard.FindDocument(context.Background(), index.ref.Shards[shard.GetID()], docID, false)
		action := meta.ActionTypeDelete
		if err != nil {
			if !errors.Is(err, zincerrors.ErrorIDNotFound) {
				return err
			}
			if len(index.ref.VecIndexes) > 0 {
				action, id = meta.ActionTypeDeleteVecOnly, text.ShardIDNeedLatest
			} else if ignoreMissing {
				continue
			} else {
				return err
			}
		}
		groups[shard.GetID()] = append(groups[shard.GetID()], text.Operation{DocID: docID, Action: action, ShardID: id})
	}
	return index.commitLocked(groups)
}

func (index *Index) GetDocument(docID string) (*meta.Hit, error) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	shard := index.text.GetShardByDocID(docID)
	_, hit, err := shard.FindDocument(context.Background(), index.ref.Shards[shard.GetID()], docID, true)
	return hit, err
}

func (index *Index) checkWriteLocked() error {
	if index.readOnly {
		return text.ErrReadOnly
	}
	return nil
}

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
		work = append(work, &shardWrite{id: id, operations: operations})
	}
	sort.Slice(work, func(i, j int) bool { return work[i].id < work[j].id })
	return work
}

// commitLocked holds mu through vector initialization, bounded text windows,
// vector work, layout publication and one final save. Text workers borrow metadata
// read-only. Between rounds the parent alone mutates vector metadata. A shard that
// fails stops its later windows, without abandoning other shards or cleanup.
func (index *Index) commitLocked(groups map[string][]text.Operation) error {
	batchSize := config.Global.BatchSize
	if batchSize <= 0 {
		return errors.Join(fmt.Errorf("batch size must be positive"), index.saveLocked())
	}
	work := newShardWrites(groups)
	if len(work) > 0 {
		if err := index.prepareVectorsLocked(); err != nil {
			return errors.Join(err, index.saveLocked())
		}
	}
	var errs []error
	for {
		active, err := index.executeWriteRoundLocked(work, batchSize)
		if !active {
			break
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	if err := index.applyWriteResultsLocked(work); err != nil {
		errs = append(errs, err)
	}
	if err := index.saveLocked(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// executeWriteRoundLocked runs at most one window per active first shard. All
// text workers must exit before the parent mutates vector metadata. Vector
// payloads are drained here, rather than retained for the entire bulk.
func (index *Index) executeWriteRoundLocked(work []*shardWrite, batchSize int) (bool, error) {
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
			if err := index.batchVectorsLocked(actions); err != nil {
				errs = append(errs, fmt.Errorf("first shard %s: %w", item.id, err))
				item.stopped = true
				break // Do not apply later target batches out of order after failure.
			}
		}
		item.result.ReadyVectors = nil
	}
	return true, errors.Join(errs...)
}

// executeTextWindow runs in a worker while the parent holds mu's write lock.
// It only mutates this shard's request state and borrows shared metadata read-only.
func (index *Index) executeTextWindow(item *shardWrite, batchSize int) error {
	end := min(item.next+batchSize, len(item.operations))
	batches, err := text.PrepareBatches(index.name, index.ref.Mappings, index.analyzers, index.ref.VecIndexes, item.operations[item.next:end])
	item.next = end
	if err != nil {
		return fmt.Errorf("prepare first shard %s: %w", item.id, err)
	}
	if err := index.text.GetShardByShardKey(item.id).ExecuteBatches(batches, &item.result); err != nil {
		return fmt.Errorf("write first shard %s: %w", item.id, err)
	}
	return nil
}

// applyWriteResultsLocked publishes accumulated statistics and shard expansion
// after all rounds, including partial failures. The caller saves metadata separately.
func (index *Index) applyWriteResultsLocked(work []*shardWrite) error {
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
			if err := index.text.GetShardByShardKey(item.id).AppendShard(ref); err != nil {
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

package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/zincsearch/zincsearch/pkg/core"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
)

// DocOperation preserves compatibility with bulk callers during their migration.
// The new write path never calls the embedded legacy Index.
type DocOperation = core.DocOperation

// CreateDocument inserts a document, or replaces an existing document when update
// is true. The caller holds one Index reference through this synchronous call.
func (index *Index) CreateDocument(docID string, doc map[string]any, update bool) error {
	return index.CreateDocuments([]DocOperation{{DocId: docID, Doc: doc, Update: update}})
}

func (index *Index) CreateDocuments(operations []DocOperation) error {
	index.mutex.Lock()
	defer index.mutex.Unlock()

	err := index.checkWritable()
	if err != nil {
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
				return errors.Join(err, index.save())
			}
		}
		op, err := index.prepareDocument(operation.DocId, operation.Doc, update, id)
		if err != nil {
			return errors.Join(err, index.save())
		}
		groups[shard.GetID()] = append(groups[shard.GetID()], op)
	}
	return index.commit(groups)
}

// UpdateDocument preserves replace/upsert semantics: insert controls whether an
// absent document can be created. It does not merge the old source with doc.
func (index *Index) UpdateDocument(docID string, doc map[string]any, insert bool) error {
	index.mutex.Lock()
	defer index.mutex.Unlock()

	err := index.checkWritable()
	if err != nil {
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
	op, err := index.prepareDocument(docID, doc, update, id)
	if err != nil {
		return errors.Join(err, index.save())
	}
	return index.commit(map[string][]text.Operation{shard.GetID(): {op}})
}

func (index *Index) DeleteDocument(docID string) error {
	index.mutex.Lock()
	defer index.mutex.Unlock()

	err := index.checkWritable()
	if err != nil {
		return err
	}
	return index.deleteDocuments([]string{docID}, false)
}

// DeleteDocuments ignores missing text documents but still deletes their vectors,
// preserving the legacy bulk cleanup of vector remnants after partial failures.
func (index *Index) DeleteDocuments(docIDs []string) error {
	index.mutex.Lock()
	defer index.mutex.Unlock()

	err := index.checkWritable()
	if err != nil {
		return err
	}
	return index.deleteDocuments(docIDs, true)
}

// deleteDocuments requires index.mutex's write lock; it does not acquire it.
func (index *Index) deleteDocuments(docIDs []string, ignoreMissing bool) error {
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
		groups[shard.GetID()] = append(groups[shard.GetID()], text.Operation{
			DocID:   docID,
			Action:  action,
			ShardID: id,
		})
	}
	return index.commit(groups)
}

func (index *Index) GetDocument(docID string) (*meta.Hit, error) {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

	shard := index.text.GetShardByDocID(docID)
	_, hit, err := shard.FindDocument(context.Background(), index.ref.Shards[shard.GetID()], docID, true)
	return hit, err
}

// checkWritable reads immutable state and does not require index.mutex.
func (index *Index) checkWritable() error {
	if index.readOnly {
		return text.ErrReadOnly
	}
	return nil
}

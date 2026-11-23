package text

import (
	"fmt"

	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/vector"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	blugeindex "github.com/blugelabs/bluge/index"
)

const (
	ShardIDNeedLatest int64 = -1
	ShardIDNeedUpdate int64 = -2
)

// Operation transfers request-owned data to batch preparation. ShardID is a
// second-shard ID or a routing sentinel, within one already selected first shard.
// Delete operations need no Document.
type Operation struct {
	DocID    string
	Action   string
	ShardID  int64
	Document *Document
}

// PreparedBatch owns request-level batches, vectors and successful-write metadata
// inputs. It opens no Writers and retains no mutable metadata; Bluge fields may
// reference the shared immutable analyzers.
// The executor resolves routing sentinels and applies DeleteOthers to old shards
// only when an operation targets all shards (ShardIDNeedUpdate).
type PreparedBatch struct {
	Batch        *blugeindex.Batch
	DeleteOthers *blugeindex.Batch
	Vectors      []*vector.VecAction
	Timestamps   []int64

	// Counts avoid opening Writers for merged no-ops and vector-only changes.
	TextOperations   int
	DeleteOperations int
}

type mergedOperation struct {
	first string
	last  Operation
}

// PrepareBatches preserves legacy merging within each target shard: the first
// action determines insert versus update semantics, the last action and payload
// determine the final effect. Call separately for each configured batch-size
// window, as in the original synchronous commit path. The caller protects all
// metadata reads for this call and keeps the returned batches within its Index
// reference lifetime. Input documents are read-only for the duration of the call.
func PrepareBatches(indexName string, mappings *meta.Mappings, analyzers map[string]*analysis.Analyzer, vectorFields map[string]*meta.VecIndex, operations []Operation) (map[int64]*PreparedBatch, error) {
	groups := make(map[int64]map[string]*mergedOperation)
	order := make([]*mergedOperation, 0, len(operations))
	for _, op := range operations {
		switch op.Action {
		case meta.ActionTypeInsert, meta.ActionTypeUpdate, meta.ActionTypeDelete, meta.ActionTypeDeleteVecOnly:
		default:
			return nil, fmt.Errorf("invalid document action %q", op.Action)
		}
		if op.ShardID < ShardIDNeedUpdate {
			return nil, fmt.Errorf("invalid second shard ID %d", op.ShardID)
		}
		group := groups[op.ShardID]
		if group == nil {
			group = make(map[string]*mergedOperation)
			groups[op.ShardID] = group
		}
		merged := group[op.DocID]
		if merged == nil {
			merged = &mergedOperation{first: op.Action}
			group[op.DocID] = merged
			order = append(order, merged)
		}
		merged.last = op
	}
	result := make(map[int64]*PreparedBatch, len(groups))
	for _, merged := range order {
		op := merged.last
		prepared := result[op.ShardID]
		if prepared == nil {
			prepared = &PreparedBatch{
				Batch:        blugeindex.NewBatch(),
				DeleteOthers: blugeindex.NewBatch(),
			}
			result[op.ShardID] = prepared
		}
		action := mergedAction(merged.first, op.Action)
		if action == vector.None {
			continue
		}
		var vectors map[string][]float32
		switch {
		case op.Action == meta.ActionTypeDeleteVecOnly:
			// No text changes, including when earlier actions had text payloads.
		case action == vector.Delete:
			id := bluge.Identifier(op.DocID)
			prepared.Batch.Delete(id)
			prepared.DeleteOthers.Delete(id)
			prepared.TextOperations++
			prepared.DeleteOperations++
		default:
			bdoc, values, err := BuildDocument(indexName, op.DocID, op.Document, mappings, analyzers)
			if err != nil {
				return nil, err
			}
			vectors = values
			if action == vector.Insert {
				prepared.Batch.Insert(bdoc)
			} else {
				prepared.Batch.Update(bdoc.ID(), bdoc)
				prepared.DeleteOthers.Delete(bdoc.ID())
				prepared.DeleteOperations++
			}
			prepared.TextOperations++
			prepared.Timestamps = append(prepared.Timestamps, op.Document.Timestamp)
		}
		if action == vector.Delete {
			for field := range vectorFields {
				prepared.Vectors = append(prepared.Vectors, &vector.VecAction{
					DocId:  op.DocID,
					Action: action,
					Index:  indexName,
					Field:  field,
				})
			}
		} else {
			for field, value := range vectors {
				prepared.Vectors = append(prepared.Vectors, &vector.VecAction{
					DocId:  op.DocID,
					Action: action,
					Index:  indexName,
					Field:  field,
					Vector: value,
				})
			}
		}
	}
	return result, nil
}

func mergedAction(first, last string) vector.Action {
	switch last {
	case meta.ActionTypeInsert, meta.ActionTypeUpdate:
		if first == meta.ActionTypeInsert {
			return vector.Insert
		}
		return vector.Update
	case meta.ActionTypeDelete:
		if first == meta.ActionTypeInsert {
			return vector.None
		}
		return vector.Delete
	case meta.ActionTypeDeleteVecOnly:
		return vector.Delete
	default:
		return vector.None
	}
}

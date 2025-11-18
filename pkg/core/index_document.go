/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package core

import (
	"fmt"
	"strings"
	"time"

	blugeindex "github.com/blugelabs/bluge/index"
	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"golang.org/x/sync/errgroup"

	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

type DocOperation struct {
	Update bool
	DocId  string
	Doc    map[string]interface{}
}

// metaDataUpdateCloser
// used for update metadata without WAL
var metaDataUpdateCloser = z.NewCloser(1)
var metaDataUpdateCh = make(chan *Index, 100)

func InitAsyncMetaDataUpdate() {
	if config.Global.EnableWal {
		return
	}
	go updateMetadataProcess()
}

func updateMetadataProcess() {
	defer metaDataUpdateCloser.Done()
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	for {
		select {
		case <-metaDataUpdateCloser.HasBeenClosed():
			return
		case <-ticker.C:
		OUTER:
			for {
				select {
				case index := <-metaDataUpdateCh:
					if index != nil {
						if err := index.UpdateMetadata(); err != nil {
							log.Error().Err(err).Msgf("update index %s metadata error", index.GetName())
						}
					}
				// there is no index need to update, we just out
				default:
					break OUTER
				}
			}
		}
	}
}

func CloseAsyncMetaDataUpdate() {
	if config.Global.EnableWal {
		return
	}
	metaDataUpdateCloser.SignalAndWait()
}

// CreateDocument inserts or updates a document in the zinc index
func (index *Index) CreateDocument(docID string, doc map[string]interface{}, update bool) error {
	// metrics
	IncrMetricStatsByIndex(index.GetName(), "wal_request")

	shard := index.GetShardByDocID(docID)
	secondShardID := ShardIDNeedLatest
	if update {
		var err error
		secondShardID, err = shard.FindShardByDocID(docID)
		if err != nil {
			if errors.Is(err, errors.ErrorIDNotFound) {
				update = false
			} else {
				return err
			}
		}
	}
	// When creating a new document in a vector index, the document ID must be
	// a valid, canonical base62-encoded integer.
	if len(index.GetVecIndexes()) > 0 {
		// Decode + re-encode docID; if the result differs, docID is not a
		// valid canonical base62 representation (invalid chars, wrong case,
		// leading zeros, etc.).
		if base62.Encode(base62.Decode(docID)) != docID {
			return fmt.Errorf("document ID (%v) is not a valid base62 string", docID)
		}
	}
	if config.Global.EnableWal {
		// check WAL
		if err := shard.OpenWAL(); err != nil {
			return err
		}
		data, err := shard.CheckDocument(docID, doc, update, secondShardID)
		if err != nil {
			return err
		}
		return shard.wal.Write(data)
	}
	op, err := shard.CheckDocumentOperation(docID, doc, update, secondShardID)
	if err != nil {
		return err
	}
	return CommitOperations(shard, []map[string]interface{}{op})
}

func (index *Index) CreateDocuments(ops []DocOperation) error {
	if len(ops) == 0 {
		return nil
	}
	// shardId -> docs
	shardMap := make(map[string][]map[string]interface{})

	for _, op := range ops {
		shard := index.GetShardByDocID(op.DocId)
		secondShardID := ShardIDNeedLatest
		if op.Update {
			var err error
			secondShardID, err = shard.FindShardByDocID(op.DocId)
			if err != nil {
				if errors.Is(err, errors.ErrorIDNotFound) {
					op.Update = false
				} else {
					return err
				}
			}
		}
		// When creating a new document in a vector index, the document ID must
		// be a valid, canonical base62-encoded integer.
		if len(index.GetVecIndexes()) > 0 {
			// Decode + re-encode docID; if the result differs, docID is not a
			// valid canonical base62 representation (invalid chars, wrong
			// case, leading zeros, etc.).
			if base62.Encode(base62.Decode(op.DocId)) != op.DocId {
				return fmt.Errorf("document ID (%v) is not a valid base62 string", op.DocId)
			}
		}
		doc, err := shard.CheckDocumentOperation(op.DocId, op.Doc, op.Update, secondShardID)
		if err != nil {
			return err
		}
		if list, ok := shardMap[shard.GetID()]; ok {
			shardMap[shard.GetID()] = append(list, doc)
		} else {
			shardMap[shard.GetID()] = []map[string]interface{}{doc}
		}
	}

	// each shard is immutable and has independent writer,
	// so we commit concurrently
	eg := errgroup.Group{}
	eg.SetLimit(config.Global.Shard.GoroutineNum)
	for shardId, docs := range shardMap {
		shardId := shardId
		docs := docs
		eg.Go(func() error {
			shard := index.GetShardByShardKey(shardId)
			return CommitOperations(shard, docs)
		})
	}
	return eg.Wait()
}

// GetDocument get a document in the zinc index
func (index *Index) GetDocument(docID string) (*meta.Hit, error) {
	// check WAL
	shard := index.GetShardByDocID(docID)
	if err := shard.OpenWAL(); err != nil {
		return nil, err
	}
	return shard.FindDocumentByDocID(docID)
}

// UpdateDocument updates a document in the zinc index
func (index *Index) UpdateDocument(docID string, doc map[string]interface{}, insert bool) error {
	// metrics
	IncrMetricStatsByIndex(index.GetName(), "wal_request")

	// check WAL
	shard := index.GetShardByDocID(docID)
	update := true
	secondShardID, err := shard.FindShardByDocID(docID)
	if err != nil {
		if insert && err == errors.ErrorIDNotFound {
			update = false
		} else {
			return err
		}
	}
	if config.Global.EnableWal {
		if err := shard.OpenWAL(); err != nil {
			return err
		}
		data, err := shard.CheckDocument(docID, doc, update, secondShardID)
		if err != nil {
			return err
		}

		return shard.wal.Write(data)
	}
	op, err := shard.CheckDocumentOperation(docID, doc, update, secondShardID)
	if err != nil {
		return err
	}
	return CommitOperations(shard, []map[string]interface{}{op})
}

// DeleteDocument deletes a document in the zinc index
func (index *Index) DeleteDocument(docID string) error {
	// metrics
	IncrMetricStatsByIndex(index.GetName(), "wal_request")

	shard := index.GetShardByDocID(docID)
	secondShardID, err := shard.FindShardByDocID(docID)
	deleteVecOnly := false
	if err != nil {
		if errors.Is(err, errors.ErrorIDNotFound) {
			if len(index.GetVecIndexes()) > 0 {
				deleteVecOnly = true
			} else {
				// there is no vec index, we just ignore.
				return errors.ErrorIDNotFound
			}
		} else {
			return err
		}
	}
	action := meta.ActionTypeDelete
	if deleteVecOnly {
		action = meta.ActionTypeDeleteVecOnly
	}

	data := map[string]interface{}{
		meta.IDFieldName:     docID,
		meta.ActionFieldName: action,
		meta.ShardFieldName:  secondShardID,
	}
	if config.Global.EnableWal {
		jstr, err := json.Marshal(data)
		if err != nil {
			return err
		}
		// check WAL
		if err := shard.OpenWAL(); err != nil {
			return err
		}
		return shard.wal.Write(jstr)
	}
	return CommitOperations(shard, []map[string]interface{}{data})
}

func (index *Index) DeleteDocuments(docIDs []string) error {
	// shardId -> doc
	shardMap := make(map[string][]map[string]interface{})

	containsVecIndex := len(index.GetVecIndexes()) > 0
	for _, docID := range docIDs {
		shard := index.GetShardByDocID(docID)
		secondShardID, err := shard.FindShardByDocID(docID)
		action := meta.ActionTypeDelete
		if err != nil {
			if errors.Is(err, errors.ErrorIDNotFound) {
				if containsVecIndex {
					action = meta.ActionTypeDeleteVecOnly
					secondShardID = ShardIDNeedLatest
				} else {
					continue
				}
			} else {
				return fmt.Errorf("index %s find id %s err: %w", index.GetName(), docID, err)
			}
		}
		data := map[string]interface{}{
			meta.IDFieldName:     docID,
			meta.ActionFieldName: action,
			meta.ShardFieldName:  secondShardID,
		}
		if list, ok := shardMap[shard.GetID()]; ok {
			shardMap[shard.GetID()] = append(list, data)
		} else {
			shardMap[shard.GetID()] = []map[string]interface{}{data}
		}
	}

	eg := errgroup.Group{}
	eg.SetLimit(config.Global.Shard.GoroutineNum)

	for shardKey, list := range shardMap {
		shardKey := shardKey
		list := list
		eg.Go(func() error {
			shard := index.GetShardByShardKey(shardKey)
			return CommitOperations(shard, list)
		})
	}
	return eg.Wait()
}

// isDateProperty returns true if the given value matches the default date format.
func isDateProperty(value string) (string, bool) {
	layout := detectTimeLayout(value)
	if layout == "" {
		return "", false
	}
	_, err := time.Parse(layout, value)
	return layout, err == nil
}

// detectTimeLayout tries to figure out the correct layout of the input date.
func detectTimeLayout(value string) string {
	layout := ""
	switch {
	case len(value) == 19 && strings.Index(value, " ") == 10:
		layout = "2006-01-02 15:04:05"
	case len(value) == 19 && strings.Index(value, "T") == 10:
		layout = "2006-01-02T15:04:05"
	case len(value) == 25 && strings.Index(value, "T") == 10:
		layout = time.RFC3339
	case len(value) == 29 && strings.Index(value, "T") == 10 && strings.Index(value, ".") == 19:
		layout = "2006-01-02T15:04:05.999Z07:00"
	}

	return layout
}

func CommitOperations(shard *IndexShard, opList []map[string]interface{}) error {
	docsList := make([]walMergeDocs, 0)

	for i := 0; i < len(opList); i += config.Global.BatchSize {
		end := i + config.Global.BatchSize
		if end > len(opList) {
			end = len(opList)
		}
		docs := make(walMergeDocs)
		for _, doc := range opList[i:end] {
			docs.AddDocument(doc)
		}
		docsList = append(docsList, docs)
	}

	for _, docs := range docsList {
		batch := blugeindex.NewBatch()
		err := docs.WriteTo(shard, batch, false)
		if err != nil {
			return err
		}
	}

	if err := shard.CheckShards(); err != nil {
		log.Error().Err(err).Str("index", shard.GetIndexName()).Str("shard", shard.GetID()).Msg("index.CheckShards()")
		return err
	}

	shard.root.UpdateMetadataByShard(shard.GetID())

	metaDataUpdateCh <- shard.root
	return nil
}

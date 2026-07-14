package core

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/flex"

	"github.com/blugelabs/bluge"
	"github.com/rs/zerolog/log"
	usearch "github.com/unum-cloud/usearch/golang"
)

type HNSWSegment struct {
	store      string
	dimensions int

	loaded atomic.Bool
	mutex  sync.RWMutex

	// writer persists vector state in Bluge using three record families:
	// 1. "log/{logID}" for changes that have not yet been indexed by HNSW.
	// 2. "doc/{docID}" for the materialized vector state, which is the source
	//    for the HNSW index.
	// 3. singleton metadata docs such as "latest_log_id" and "hnsw_log_id".
	// New writes are appended to the "log/" family. The background worker will
	// periodically apply those logs to the "doc/" family and rebuild the HNSW
	// index.
	writer *bluge.Writer

	// hnswLogID is the log id of the last applied log to HNSW index.
	hnswLogID   int64
	latestLogID int64

	// cache is an in-memory state which applied from the "log/" family.
	// Vectors are stored contiguously in a single slice, therefore we can
	// search them using the usearch.ExactSearch function.
	cache struct {
		docIDs     []int64
		docVectors []float32
		docIDIndex map[int64]int
	}
	// docTS maps doc id to the latest log id (from the "log/" family).
	// Note: when a document is deleted, it will be removed from the cache and
	// added to docTS.
	docTS map[int64]int64

	index *usearch.Index
}

func newHNSWSegment(store string, dimensions int) *HNSWSegment {
	var seg HNSWSegment
	seg.store = store
	seg.dimensions = dimensions
	seg.cache.docIDIndex = make(map[int64]int)
	seg.docTS = make(map[int64]int64)
	return &seg
}

func (seg *HNSWSegment) Close() {
	seg.mutex.Lock()
	defer seg.mutex.Unlock()
	if seg.index != nil {
		seg.index.Close()
		seg.index = nil
	}
	if seg.writer != nil {
		seg.writer.Close()
		seg.writer = nil
	}
}

func (seg *HNSWSegment) load() error {
	if seg.loaded.Load() {
		return nil
	}

	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	if seg.loaded.Load() {
		return nil
	}

	name := filepath.Join(vector.VecPrefix, seg.store, "0000", "stored_vec")
	var cfg bluge.Config
	switch config.Global.StorageType {
	case "disk":
		cfg = directory.GetDiskConfig(config.Global.DataPath, name)
	case "s3":
		cfg = directory.GetS3Config(config.Global.DataPath, name)
	case "oss":
		cfg = directory.GetOssConfig(config.Global.DataPath, name)
	default:
		return fmt.Errorf("invalid storage type: %s", config.Global.StorageType)
	}

	if seg.writer == nil {
		writer, err := bluge.OpenWriter(cfg)
		if err != nil {
			return fmt.Errorf("failed to open bluge writer: %w", err)
		}
		seg.writer = writer
	}

	reader, err := seg.writer.Reader()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}
	defer reader.Close()

	err = seg.loadLogIDs(reader)
	if err != nil {
		return fmt.Errorf("failed to load log ids: %w", err)
	}
	err = seg.loadLogs(context.Background(), reader)
	if err != nil {
		return fmt.Errorf("failed to load logs: %w", err)
	}
	err = seg.loadHNSW()
	if err != nil {
		return fmt.Errorf("failed to load hnsw: %w", err)
	}

	seg.loaded.Store(true)

	return nil
}

func (seg *HNSWSegment) loadLogIDs(reader *bluge.Reader) error {
	id, err := seg.readLogID(reader, "hnsw_log_id")
	if err != nil {
		return fmt.Errorf("failed to read hnsw log id: %w", err)
	}
	seg.hnswLogID = id

	id, err = seg.readLogID(reader, "latest_log_id")
	if err != nil {
		return fmt.Errorf("failed to read latest log id: %w", err)
	}
	seg.latestLogID = id

	return nil
}

func (seg *HNSWSegment) loadLogs(ctx context.Context, reader *bluge.Reader) error {
	logs := make(map[int64]HNSWLogRow)

	it := seg.readHNSWLogRows(ctx, reader)
	for row := range it.Range() {
		r, ok := logs[row.DocID]
		if !ok || row.ID > r.ID {
			logs[row.DocID] = row
		}
	}
	if it.Error() != nil {
		return it.Error()
	}

	for _, row := range logs {
		if row.DocDel {
			seg.deleteCache(row.DocID)
		} else {
			seg.updateCache(row.DocID, row.Vector)
		}
		seg.docTS[row.DocID] = row.ID
	}

	return nil
}

func (seg *HNSWSegment) loadHNSW() error {
	name := path.Join(seg.store, "0000", "hnsw")
	exist, err := vecIdxManager.storage.ExistsFile(name)
	if err != nil {
		return fmt.Errorf("failed to check if file exists: %w", err)
	}
	if !exist {
		return nil
	}
	name, closer, err := vecIdxManager.storage.LoadFile(path.Join(seg.store, "0000", "hnsw"))
	if err != nil {
		return fmt.Errorf("failed to load file: %w", err)
	}
	defer closer.Close()

	index, err := seg.newIndex()
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	err = index.Load(name)
	if err != nil {
		index.Close()
		return fmt.Errorf("failed to load hnsw: %w", err)
	}
	if seg.index != nil {
		seg.index.Close()
	}
	seg.index = index

	return nil
}

func (seg *HNSWSegment) Batch(addIDs []int64, vectors [][]float32, delIDs []int64) (int, error) {
	err := seg.load()
	if err != nil {
		return 0, fmt.Errorf("failed to load: %w", err)
	}

	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	if len(addIDs) != len(vectors) {
		return 0, fmt.Errorf("invalid parameters: size of docIDs and vectors not equal")
	}
	for _, vector := range vectors {
		if len(vector) != seg.dimensions {
			return 0, fmt.Errorf("wrong dimension for vector: %v", len(vector))
		}
	}

	latestLogID := seg.latestLogID
	docTS := make(map[int64]int64)
	batch := bluge.NewBatch()

	for _, docID := range delIDs {
		latestLogID++
		docTS[docID] = latestLogID

		doc := bluge.NewDocument(fmt.Sprintf("log/%v", latestLogID))
		doc.AddField(bluge.NewStoredOnlyField("doc_id",
			[]byte(strconv.FormatInt(docID, 10)),
		))
		doc.AddField(bluge.NewStoredOnlyField("doc_del",
			[]byte("true"),
		))
		batch.Update(doc.ID(), doc)
	}
	for i, docID := range addIDs {
		latestLogID++
		docTS[docID] = latestLogID

		doc := bluge.NewDocument(fmt.Sprintf("log/%v", latestLogID))
		doc.AddField(bluge.NewStoredOnlyField("doc_id",
			[]byte(strconv.FormatInt(docID, 10)),
		))
		doc.AddField(bluge.NewStoredOnlyField("vector",
			zutils.VectorToBytes(vectors[i]),
		))
		batch.Update(doc.ID(), doc)
	}

	doc := bluge.NewDocument("latest_log_id")
	doc.AddField(bluge.NewStoredOnlyField("log_id",
		[]byte(strconv.FormatInt(latestLogID, 10)),
	))
	batch.Update(doc.ID(), doc)

	err = seg.writer.Batch(batch)
	if err != nil {
		return 0, fmt.Errorf("failed to write bluge batch: %w", err)
	}
	count, err := seg.countDelta(addIDs, delIDs)
	if err != nil {
		log.Warn().Err(err).Msg("failed to count hnsw batch delta")
	}

	seg.latestLogID = latestLogID
	for _, docID := range delIDs {
		seg.deleteCache(docID)
	}
	for i, docID := range addIDs {
		seg.updateCache(docID, vectors[i])
	}
	maps.Copy(seg.docTS, docTS)

	return count, nil
}

func (seg *HNSWSegment) countDelta(addIDs []int64, delIDs []int64) (int, error) {
	docIDs := make(map[int64]int)
	for _, docID := range delIDs {
		docIDs[docID] = -1 // -1 for deletion
	}
	for _, docID := range addIDs {
		docIDs[docID] = +1 // +1 for addition
	}

	var delta int
	for docID, count := range docIDs {
		// When a docID is present in the docIDIndex, it means that the
		// document is currently added or updated. Therefore, we only consider
		// deletions to the delta size.
		_, ok := seg.cache.docIDIndex[docID]
		if ok {
			if count < 0 {
				delta += count
			}
			continue
		}

		// Else, when a docID is present in the docTS, it means that the
		// document has been deleted in the current view. Therefore, we only
		// consider additions to the delta size.
		_, ok = seg.docTS[docID]
		if ok {
			if count > 0 {
				delta += count
			}
			continue
		}

		// Else, when the index is nil, it means that there is no HNSW snapshot
		// available. In this case, we treat unknown documents as absent.
		// Therefore, we only consider additions to the delta size.
		if seg.index == nil {
			if count > 0 {
				delta += count
			}
			continue
		}

		// Finally, we check the HNSW index to determine if the document is
		// present or not. If the document is present in the index, we only
		// consider deletions to the delta size. If the document is absent in
		// the index, we only consider additions to the delta size.
		ok, err := seg.index.Contains(usearch.Key(docID))
		if err != nil {
			return 0, fmt.Errorf("failed to check if index contains docID: %w", err)
		}
		if ok && count < 0 {
			delta += count
		} else if !ok && count > 0 {
			delta += count
		}
	}

	return delta, nil
}

func (seg *HNSWSegment) NeedRebuildHNSW() bool {
	err := seg.load()
	if err != nil {
		return false
	}

	seg.mutex.RLock()
	defer seg.mutex.RUnlock()
	unapplied := seg.latestLogID - seg.hnswLogID
	return unapplied > config.Global.VectorConfig.HNSWMaxLogs
}

func (seg *HNSWSegment) BuildHNSW(ctx context.Context) error {
	err := seg.load()
	if err != nil {
		return fmt.Errorf("failed to load: %w", err)
	}
	logID, err := seg.applyLogs(ctx)
	if err != nil {
		return fmt.Errorf("failed to apply logs: %w", err)
	}
	index, err := seg.buildHNSW(ctx)
	if err != nil {
		return fmt.Errorf("failed to build hnsw: %w", err)
	}
	err = seg.cleanLogs(ctx, logID, index)
	if err != nil {
		return fmt.Errorf("failed to clean logs: %w", err)
	}
	return nil
}

func (seg *HNSWSegment) applyLogs(ctx context.Context) (int64, error) {
	seg.mutex.RLock()
	writer := seg.writer
	reader, err := writer.Reader()
	latestLogID := seg.latestLogID
	seg.mutex.RUnlock()
	if err != nil {
		return 0, fmt.Errorf("failed to get reader: %w", err)
	}
	defer reader.Close()

	batch := bluge.NewBatch()
	logs := make(map[int64]HNSWLogRow)

	it := seg.readHNSWLogRows(ctx, reader)
	for row := range it.Range() {
		if row.ID > latestLogID {
			continue
		}
		r, ok := logs[row.DocID]
		if !ok || row.ID > r.ID {
			logs[row.DocID] = row
		}
	}
	for _, row := range logs {
		doc := bluge.NewDocument(fmt.Sprintf("doc/%v", row.DocID))
		if row.DocDel {
			batch.Delete(doc.ID())
		} else {
			doc.AddField(bluge.NewStoredOnlyField("vector",
				zutils.VectorToBytes(row.Vector),
			))
			batch.Update(doc.ID(), doc)
		}
	}
	if it.Error() != nil {
		return 0, it.Error()
	}

	err = writer.Batch(batch)
	if err != nil {
		return 0, fmt.Errorf("failed to write bluge batch: %w", err)
	}

	return latestLogID, nil
}

func (seg *HNSWSegment) buildHNSW(ctx context.Context) (*usearch.Index, error) {
	seg.mutex.Lock()
	reader, err := seg.writer.Reader()
	seg.mutex.Unlock()
	if err != nil {
		return nil, fmt.Errorf("failed to get reader: %w", err)
	}
	defer reader.Close()

	closeIndex := true
	index, err := seg.newIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %w", err)
	}
	defer func() {
		if closeIndex {
			index.Close()
		}
	}()

	size, err := seg.countHNSWDocRows(ctx, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to count hnsw doc rows: %w", err)
	}
	err = index.Reserve(uint(size))
	if err != nil {
		return nil, fmt.Errorf("failed to reserve index: %w", err)
	}

	// Read all document vectors stored in Bluge (under "doc/") and add them to
	// the HNSW index.
	// Note: The in-memory cache only tracks log entries that have not yet been
	// applied to that materialized "doc/" state, so the full applied vector
	// set must be read from "doc/" here.
	it := seg.readHNSWDocRows(ctx, reader)
	for row := range it.Range() {
		err = index.Add(usearch.Key(row.ID), row.Vector)
		if err != nil {
			return nil, fmt.Errorf("failed to add index: %w", err)
		}
	}
	if it.Error() != nil {
		return nil, it.Error()
	}

	file, err := os.CreateTemp(vecIdxManager.tmpDir, "hnsw.*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	file.Close()
	defer os.RemoveAll(file.Name())
	err = index.Save(file.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to save index: %w", err)
	}
	err = vecIdxManager.storage.SaveFile(file.Name(), path.Join(seg.store, "0000", "hnsw"))
	if err != nil {
		return nil, fmt.Errorf("failed to save index to storage: %w", err)
	}

	closeIndex = false
	return index, nil
}

func (seg *HNSWSegment) cleanLogs(ctx context.Context, logID int64, index *usearch.Index) error {
	defer func() {
		if index != nil {
			index.Close()
		}
	}()

	seg.mutex.Lock()
	writer := seg.writer
	reader, err := writer.Reader()
	seg.mutex.Unlock()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}
	defer reader.Close()

	batch := bluge.NewBatch()
	docs := make(map[int64]struct{})

	it := seg.readHNSWLogRows(ctx, reader)
	for row := range it.Range() {
		if row.ID > logID {
			continue
		}
		doc := bluge.NewDocument(fmt.Sprintf("log/%v", row.ID))
		batch.Delete(doc.ID())
		docs[row.DocID] = struct{}{}
	}
	if it.Error() != nil {
		return it.Error()
	}

	doc := bluge.NewDocument("hnsw_log_id")
	doc.AddField(bluge.NewStoredOnlyField("log_id",
		[]byte(strconv.FormatInt(logID, 10)),
	))
	batch.Update(doc.ID(), doc)

	err = writer.Batch(batch)
	if err != nil {
		return fmt.Errorf("failed to write bluge batch: %w", err)
	}

	seg.mutex.Lock()
	seg.hnswLogID = logID
	for docID := range docs {
		// Only delete documents with timestamps that are not newer than the
		// last applied HNSW log ID. This ensures we do not delete documents
		// that were added after the HNSW index was last updated.
		if seg.docTS[docID] <= logID {
			seg.deleteCache(docID)
			delete(seg.docTS, docID)
		}
	}
	if seg.index != nil {
		seg.index.Close()
	}
	seg.index = index
	index = nil
	seg.mutex.Unlock()

	return nil
}

func (seg *HNSWSegment) Search(query []float32, k int64) ([]int64, []float32, error) {
	err := seg.load()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load: %w", err)
	}

	seg.mutex.RLock()
	defer seg.mutex.RUnlock()

	if len(query) != seg.dimensions {
		return nil, nil, fmt.Errorf("wrong dimension for vector: %v", len(query))
	}
	k = max(k, 0)

	ids1, distances1, err := seg.searchCache(query, k)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to search cache: %w", err)
	}
	ids2, distances2, err := seg.searchHNSW(query, k)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to search hnsw: %w", err)
	}

	var i, j int
	var docIDs []int64
	var distances []float32
	for {
		if i == len(ids1) {
			docIDs = append(docIDs, ids2[j:]...)
			distances = append(distances, distances2[j:]...)
			break
		}
		if j == len(ids2) {
			docIDs = append(docIDs, ids1[i:]...)
			distances = append(distances, distances1[i:]...)
			break
		}
		if distances1[i] < distances2[j] {
			docIDs = append(docIDs, ids1[i])
			distances = append(distances, distances1[i])
			i++
		} else {
			docIDs = append(docIDs, ids2[j])
			distances = append(distances, distances2[j])
			j++
		}
	}
	if len(docIDs) > int(k) {
		docIDs = docIDs[:k]
		distances = distances[:k]
	}

	return docIDs, distances, nil
}

func (seg *HNSWSegment) searchCache(query []float32, k int64) ([]int64, []float32, error) {
	docIDs, distances, err := ExactSearch(seg.cache.docIDs, seg.cache.docVectors, query, seg.dimensions, int(k))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to exact search: %w", err)
	}
	return docIDs, distances, nil
}

func (seg *HNSWSegment) searchHNSW(query []float32, k int64) ([]int64, []float32, error) {
	if seg.index == nil {
		return nil, nil, nil
	}

	// The limit is set to k + len(seg.docTS) to ensure that we can retrieve
	// enough results from the HNSW index, even after filtering out any
	// documents that have been updated or deleted in unapplied logs.
	limit := uint(k + int64(len(seg.docTS)))
	keys, dts, err := seg.index.Search(query, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to filtered search: %w", err)
	}

	var docIDs []int64
	var distances []float32
	for i, key := range keys {
		if _, ok := seg.docTS[int64(key)]; ok {
			continue
		}
		docIDs = append(docIDs, int64(key))
		distances = append(distances, dts[i])
	}

	return docIDs, distances, nil
}

func (seg *HNSWSegment) SearchByIDs(query []float32, k int64, filter []int64) ([]int64, []float32, error) {
	err := seg.load()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load: %w", err)
	}

	seg.mutex.RLock()
	defer seg.mutex.RUnlock()

	if len(query) != seg.dimensions {
		return nil, nil, fmt.Errorf("wrong dimension for vector: %v", len(query))
	}
	k = max(k, 0)

	ids1, distances1, err := seg.searchCacheByIDs(query, k, filter)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to search cache: %w", err)
	}
	ids2, distances2, err := seg.searchDocsByIDs(query, k, filter)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to search docs by IDs: %w", err)
	}

	var i, j int
	var docIDs []int64
	var distances []float32
	for {
		if i == len(ids1) {
			docIDs = append(docIDs, ids2[j:]...)
			distances = append(distances, distances2[j:]...)
			break
		}
		if j == len(ids2) {
			docIDs = append(docIDs, ids1[i:]...)
			distances = append(distances, distances1[i:]...)
			break
		}
		if distances1[i] < distances2[j] {
			docIDs = append(docIDs, ids1[i])
			distances = append(distances, distances1[i])
			i++
		} else {
			docIDs = append(docIDs, ids2[j])
			distances = append(distances, distances2[j])
			j++
		}
	}
	if len(docIDs) > int(k) {
		docIDs = docIDs[:k]
		distances = distances[:k]
	}

	return docIDs, distances, nil
}

func (seg *HNSWSegment) searchCacheByIDs(query []float32, k int64, filter []int64) ([]int64, []float32, error) {
	var (
		dim = seg.dimensions
		ids = make([]int64, 0, len(filter))
		vec = make([]float32, 0, len(filter)*dim)
	)

	for _, id := range filter {
		if i, ok := seg.cache.docIDIndex[id]; ok {
			ids = append(ids, id)
			vec = append(vec, seg.cache.docVectors[i*dim:(i+1)*dim]...)
		}
	}

	docIDs, distances, err := ExactSearch(ids, vec, query, dim, int(k))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to exact search: %w", err)
	}
	return docIDs, distances, nil
}

func (seg *HNSWSegment) searchDocsByIDs(query []float32, k int64, filter []int64) ([]int64, []float32, error) {
	reader, err := seg.writer.Reader()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get reader: %w", err)
	}
	defer reader.Close()

	ids := make([]int64, 0, len(filter))
	vectors := make([]float32, 0, len(filter)*seg.dimensions)
	it := seg.readHNSWDocRowsByIDs(context.Background(), reader, filter)
	for doc := range it.Range() {
		if _, ok := seg.docTS[doc.ID]; ok {
			continue
		}
		ids = append(ids, doc.ID)
		vectors = append(vectors, doc.Vector...)
	}

	docIDs, distances, err := ExactSearch(ids, vectors, query, seg.dimensions, int(k))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to exact search: %w", err)
	}

	return docIDs, distances, nil
}

func (seg *HNSWSegment) updateCache(docID int64, vector []float32) {
	if i, ok := seg.cache.docIDIndex[docID]; ok {
		d := seg.dimensions
		copy(seg.cache.docVectors[i*d:(i+1)*d], vector)
	} else {
		seg.cache.docIDs = append(seg.cache.docIDs, docID)
		seg.cache.docVectors = append(seg.cache.docVectors, vector...)
		seg.cache.docIDIndex[docID] = len(seg.cache.docIDs) - 1
	}
}

func (seg *HNSWSegment) deleteCache(docID int64) {
	i, ok := seg.cache.docIDIndex[docID]
	if !ok {
		return
	}
	j := len(seg.cache.docIDs) - 1
	d := seg.dimensions

	// i is the index of the docID to be deleted, j is the last index.
	// We delete the doc by moving the last doc to the position of the deleted
	// doc, and then truncating the slice.

	if i != j {
		seg.cache.docIDs[i] = seg.cache.docIDs[j]
		copy(seg.cache.docVectors[i*d:(i+1)*d], seg.cache.docVectors[j*d:(j+1)*d])
		seg.cache.docIDIndex[seg.cache.docIDs[i]] = i
	}

	seg.cache.docIDs = seg.cache.docIDs[:j]
	seg.cache.docVectors = seg.cache.docVectors[:j*d]
	delete(seg.cache.docIDIndex, docID)
}

func (seg *HNSWSegment) newIndex() (*usearch.Index, error) {
	conf := usearch.IndexConfig{
		Quantization: usearch.I8,
		Metric:       usearch.L2sq,
		Dimensions:   uint(seg.dimensions),
	}
	return usearch.NewIndex(conf)
}

func (seg *HNSWSegment) readLogID(reader *bluge.Reader, id string) (int64, error) {
	var data []byte
	query := bluge.NewTermQuery(id).SetField("_id")
	request := bluge.NewTopNSearch(1, query)
	it := search.SearchAndVisitStoredFields(context.Background(), reader, request)
	for doc := range it.Range() {
		for field, value := range doc {
			if field == "log_id" {
				data = value
				break
			}
		}
	}
	if it.Error() != nil {
		return 0, it.Error()
	}
	if data == nil {
		return 0, nil
	}
	logID, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, err
	}
	return logID, nil
}

type HNSWLogRow struct {
	ID     int64
	DocID  int64
	DocDel bool
	Vector []float32
}

func (seg *HNSWSegment) readHNSWLogRows(ctx context.Context, reader *bluge.Reader) flex.Iter[HNSWLogRow] {
	return flex.NewIter(func(yield func(HNSWLogRow) bool) error {
		query := bluge.NewPrefixQuery("log/").SetField("_id")
		request := bluge.NewAllMatches(query)
		it := search.SearchAndVisitStoredFields(ctx, reader, request)
		for doc := range it.Range() {
			var row HNSWLogRow
			for field, value := range doc {
				switch field {
				case "_id":
					id, err := strconv.ParseInt(string(value)[4:], 10, 64)
					if err != nil {
						return fmt.Errorf("failed to parse doc id %v: %w", string(value), err)
					}
					row.ID = id
				case "doc_id":
					id, err := strconv.ParseInt(string(value), 10, 64)
					if err != nil {
						return fmt.Errorf("failed to parse doc id %v: %w", string(value), err)
					}
					row.DocID = id
				case "doc_del":
					if string(value) == "true" {
						row.DocDel = true
					}
				case "vector":
					row.Vector = zutils.BytesToVector(value)
				}
			}
			if !yield(row) {
				return nil
			}
		}
		if it.Error() != nil {
			return it.Error()
		}
		return nil
	})
}

type HNSWDocRow struct {
	ID     int64
	Vector []float32
}

func (seg *HNSWSegment) readHNSWDocRows(ctx context.Context, reader *bluge.Reader) flex.Iter[HNSWDocRow] {
	return flex.NewIter(func(yield func(HNSWDocRow) bool) error {
		query := bluge.NewPrefixQuery("doc/").SetField("_id")
		request := bluge.NewAllMatches(query)
		it := search.SearchAndVisitStoredFields(ctx, reader, request)
		for doc := range it.Range() {
			var row HNSWDocRow
			for field, value := range doc {
				switch field {
				case "_id":
					id, err := strconv.ParseInt(string(value)[4:], 10, 64)
					if err != nil {
						return fmt.Errorf("failed to parse doc id %v: %w", string(value), err)
					}
					row.ID = id
				case "vector":
					row.Vector = zutils.BytesToVector(value)
				}
			}
			if !yield(row) {
				return nil
			}
		}
		if it.Error() != nil {
			return it.Error()
		}
		return nil
	})
}

func (seg *HNSWSegment) readHNSWDocRowsByIDs(ctx context.Context, reader *bluge.Reader, ids []int64) flex.Iter[HNSWDocRow] {
	return flex.NewIter(func(yield func(HNSWDocRow) bool) error {
		query := bluge.NewBooleanQuery()
		for _, id := range ids {
			term := "doc/" + strconv.FormatInt(id, 10)
			query.AddShould(bluge.NewTermQuery(term).SetField("_id"))
		}
		request := bluge.NewAllMatches(query)
		it := search.SearchAndVisitStoredFields(ctx, reader, request)
		for doc := range it.Range() {
			var row HNSWDocRow
			for field, value := range doc {
				switch field {
				case "_id":
					id, err := strconv.ParseInt(string(value)[4:], 10, 64)
					if err != nil {
						return fmt.Errorf("failed to parse doc id %v: %w", string(value), err)
					}
					row.ID = id
				case "vector":
					row.Vector = zutils.BytesToVector(value)
				}
			}
			if !yield(row) {
				return nil
			}
		}
		if it.Error() != nil {
			return it.Error()
		}
		return nil
	})
}

func (seg *HNSWSegment) countHNSWDocRows(ctx context.Context, reader *bluge.Reader) (int, error) {
	query := bluge.NewPrefixQuery("doc/").SetField("_id")
	request := bluge.NewTopNSearch(0, query).WithStandardAggregations()
	dmi, err := reader.Search(ctx, request)
	if err != nil {
		return 0, err
	}
	size := dmi.Aggregations().Count()
	return int(size), nil
}

func ExactSearch(ids []int64, vectors, query []float32, dimensions, k int) ([]int64, []float32, error) {
	count := uint(len(vectors) / dimensions)
	stride := uint(dimensions * 4)
	limit := uint(min(k, int(count)))
	if limit == 0 {
		return nil, nil, nil
	}

	keys, distances, err := usearch.ExactSearch(
		vectors, query, count, 1,
		stride, stride, uint(dimensions),
		usearch.L2sq, limit, 0,
	)
	if err != nil {
		return nil, nil, err
	}

	var docIDs []int64
	for _, key := range keys {
		docID := ids[key]
		docIDs = append(docIDs, docID)
	}

	return docIDs, distances, nil
}

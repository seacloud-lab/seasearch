package core

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
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
	usearch "github.com/unum-cloud/usearch/golang"
)

type HNSWSegment struct {
	store      string
	dimensions int

	loaded atomic.Bool
	mutex  sync.RWMutex

	writer *bluge.Writer

	hnswLogID   int64
	latestLogID int64

	cache struct {
		docIDs     []int64
		docVectors []float32
		docIDIndex map[int64]int
	}
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

func (seg *HNSWSegment) Batch(addIDs []int64, vectors [][]float32, delIDs []int64) error {
	err := seg.load()
	if err != nil {
		return fmt.Errorf("failed to load: %w", err)
	}

	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	if len(addIDs) != len(vectors) {
		return fmt.Errorf("invalid parameters: size of docIDs and vectors not equal")
	}
	for _, vector := range vectors {
		if len(vector) != seg.dimensions {
			return fmt.Errorf("wrong dimension for vector: %v", len(vector))
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
		return fmt.Errorf("failed to write bluge batch: %w", err)
	}

	seg.latestLogID = latestLogID
	for _, docID := range delIDs {
		seg.deleteCache(docID)
	}
	for i, docID := range addIDs {
		seg.updateCache(docID, vectors[i])
	}
	maps.Copy(seg.docTS, docTS)

	return nil
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
	defer index.Close()

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
	seg.index, index = index, seg.index
	for docID := range docs {
		if seg.docTS[docID] <= logID {
			seg.deleteCache(docID)
			delete(seg.docTS, docID)
		}
	}
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
	count := uint(len(seg.cache.docIDs))
	stride := uint(seg.dimensions * 4)
	limit := uint(min(k, int64(count)))
	if limit == 0 {
		return nil, nil, nil
	}

	keys, distances, err := usearch.ExactSearch(
		seg.cache.docVectors, query, count, 1,
		stride, stride, uint(seg.dimensions),
		usearch.L2sq, limit, 0,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to exact search: %w", err)
	}

	var docIDs []int64
	for _, key := range keys {
		docID := seg.cache.docIDs[key]
		docIDs = append(docIDs, docID)
	}

	return docIDs, distances, nil
}

func (seg *HNSWSegment) searchHNSW(query []float32, k int64) ([]int64, []float32, error) {
	if seg.index == nil {
		return nil, nil, nil
	}

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

func (seg *HNSWSegment) SearchByIDs(query []float32, k int64, docIDs []int64) ([]int64, []float32, error) {
	err := seg.load()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load: %w", err)
	}

	seg.mutex.RLock()
	defer seg.mutex.RUnlock()

	if len(query) != seg.dimensions {
		return nil, nil, fmt.Errorf("wrong dimension for vector: %v", len(query))
	}
	if k <= 0 || len(docIDs) == 0 {
		return nil, nil, nil
	}

	type searchResult struct {
		id  int64
		dis float32
	}

	pending := make(map[int64]struct{}, len(docIDs))
	for _, docID := range docIDs {
		pending[docID] = struct{}{}
	}

	results := make([]searchResult, 0, len(pending))
	for docID := range pending {
		i, ok := seg.cache.docIDIndex[docID]
		if !ok {
			continue
		}
		d := seg.dimensions
		vec := seg.cache.docVectors[i*d : (i+1)*d]
		dis, err := L2Distance(query, vec)
		if err != nil {
			return nil, nil, err
		}
		results = append(results, searchResult{id: docID, dis: dis})
		delete(pending, docID)
	}

	if len(pending) > 0 {
		reader, err := seg.writer.Reader()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get reader: %w", err)
		}
		defer reader.Close()

		it := seg.readHNSWDocRows(context.Background(), reader)
		for row := range it.Range() {
			if _, ok := pending[row.ID]; !ok {
				continue
			}
			if _, ok := seg.docTS[row.ID]; ok {
				continue
			}
			dis, err := L2Distance(query, row.Vector)
			if err != nil {
				return nil, nil, err
			}
			results = append(results, searchResult{id: row.ID, dis: dis})
		}
		if it.Error() != nil {
			return nil, nil, it.Error()
		}
	}

	slices.SortFunc(results, func(a, b searchResult) int {
		return cmp.Compare(a.dis, b.dis)
	})
	if len(results) > int(k) {
		results = results[:int(k)]
	}

	ids := make([]int64, 0, len(results))
	distances := make([]float32, 0, len(results))
	for _, result := range results {
		ids = append(ids, result.id)
		distances = append(distances, result.dis)
	}

	return ids, distances, nil
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
	it := search.Search(context.Background(), reader, request)
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
		it := search.Search(ctx, reader, request)
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
		it := search.Search(ctx, reader, request)
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

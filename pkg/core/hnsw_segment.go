package core

import (
	"context"
	"fmt"
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
	usearch "github.com/unum-cloud/usearch/golang"
)

type HNSWSegment struct {
	store      string
	dimensions int

	writer *bluge.Writer

	mutex sync.RWMutex

	loaded atomic.Bool

	hnswLogID   int64
	latestLogID int64

	cache struct {
		docIDs     []int64
		docVectors []float32
		docIDIndex map[int64]int
	}
	shadowDocIDs map[int64]struct{}

	index *usearch.Index
}

func newHNSWSegment(path string, dimensions int) *HNSWSegment {
	var seg HNSWSegment
	seg.store = path
	seg.dimensions = dimensions
	seg.shadowDocIDs = make(map[int64]struct{})
	seg.cache.docIDIndex = make(map[int64]int)
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

	batch := bluge.NewBatch()
	latestLogID := seg.latestLogID

	for _, docID := range delIDs {
		latestLogID++
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
		seg.shadowDocIDs[docID] = struct{}{}
		seg.deleteCache(docID)
	}
	for i, docID := range addIDs {
		seg.shadowDocIDs[docID] = struct{}{}
		seg.updateCache(docID, vectors[i])
	}

	return nil
}

func (seg *HNSWSegment) NeedRebuildHNSW() bool {
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
	logID, err := seg.applyLogs()
	if err != nil {
		return fmt.Errorf("failed to apply logs: %w", err)
	}
	err = seg.buildHNSW()
	if err != nil {
		return fmt.Errorf("failed to build hnsw: %w", err)
	}
	err = seg.cleanLogs(logID)
	if err != nil {
		return fmt.Errorf("failed to clean logs: %w", err)
	}
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

	count := uint(len(seg.cache.docIDs))
	stride := uint(seg.dimensions * 4)
	k = min(max(k, 0), int64(count))
	if k == 0 {
		return nil, nil, nil
	}

	keys, distances, err := usearch.ExactSearch(
		seg.cache.docVectors, query, count, 1,
		stride, stride, uint(seg.dimensions),
		usearch.L2sq, uint(k), 0,
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

	writer, err := bluge.OpenWriter(cfg)
	if err != nil {
		return fmt.Errorf("failed to open bluge reader: %w", err)
	}
	seg.writer = writer

	reader, err := writer.Reader()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}
	defer reader.Close()

	err = seg.loadLogIDs(reader)
	if err != nil {
		return fmt.Errorf("failed to load log ids: %w", err)
	}
	err = seg.loadLogs(reader)
	if err != nil {
		return fmt.Errorf("failed to load logs: %w", err)
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

func (seg *HNSWSegment) loadLogs(reader *bluge.Reader) error {
	logs := make(map[int64]HNSWLogRow)

	it := seg.readHNSWLogRows(reader)
	for row := range it.Range() {
		r, ok := logs[row.DocID]
		if !ok || row.ID > r.ID {
			logs[row.DocID] = row
		}
	}
	for _, row := range logs {
		seg.shadowDocIDs[row.DocID] = struct{}{}
		if row.DocDel {
			seg.deleteCache(row.DocID)
		} else {
			seg.updateCache(row.DocID, row.Vector)
		}
	}
	if it.Error() != nil {
		return it.Error()
	}

	return nil
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

func (seg *HNSWSegment) applyLogs() (int64, error) {
	seg.mutex.Lock()
	writer := seg.writer
	reader, err := writer.Reader()
	latestLogID := seg.latestLogID
	seg.mutex.Unlock()
	if err != nil {
		return 0, fmt.Errorf("failed to get reader: %w", err)
	}
	defer reader.Close()

	batch := bluge.NewBatch()
	logs := make(map[int64]HNSWLogRow)

	it := seg.readHNSWLogRows(reader)
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

func (seg *HNSWSegment) buildHNSW() error {
	seg.mutex.Lock()
	reader, err := seg.writer.Reader()
	seg.mutex.Unlock()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}
	defer reader.Close()

	index, err := seg.newIndex()
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer index.Close()

	size, err := seg.countHNSWDocRows(reader)
	if err != nil {
		return fmt.Errorf("failed to count hnsw doc rows: %w", err)
	}
	err = index.Reserve(uint(size))
	if err != nil {
		return fmt.Errorf("failed to reserve index: %w", err)
	}

	it := seg.readHNSWDocRows(reader)
	for row := range it.Range() {
		err = index.Add(usearch.Key(row.ID), row.Vector)
		if err != nil {
			return fmt.Errorf("failed to add index: %w", err)
		}
	}
	if it.Error() != nil {
		return it.Error()
	}

	name := filepath.Join(vecIdxManager.tmpDir, seg.store)
	defer os.RemoveAll(name)
	err = index.Save(name)
	if err != nil {
		return fmt.Errorf("failed to save index: %w", err)
	}
	err = vecIdxManager.storage.SaveFile(name, path.Join(seg.store, "0000", "hnsw"))
	if err != nil {
		return fmt.Errorf("failed to save index to storage: %w", err)
	}

	return nil
}

func (seg *HNSWSegment) cleanLogs(logID int64) error {
	seg.mutex.Lock()
	writer := seg.writer
	reader, err := writer.Reader()
	seg.mutex.Unlock()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}
	defer reader.Close()

	batch := bluge.NewBatch()

	it := seg.readHNSWLogRows(reader)
	for row := range it.Range() {
		if row.ID > logID {
			continue
		}
		doc := bluge.NewDocument(fmt.Sprintf("log/%v", row.ID))
		batch.Delete(doc.ID())
	}
	if it.Error() != nil {
		return it.Error()
	}

	err = writer.Batch(batch)
	if err != nil {
		return fmt.Errorf("failed to write bluge batch: %w", err)
	}

	return nil
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

func (seg *HNSWSegment) readHNSWLogRows(reader *bluge.Reader) flex.Iter[HNSWLogRow] {
	return flex.NewIter(func(yield func(HNSWLogRow) bool) error {
		query := bluge.NewPrefixQuery("log/").SetField("_id")
		request := bluge.NewAllMatches(query)
		it := search.Search(context.Background(), reader, request)
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

func (seg *HNSWSegment) readHNSWDocRows(reader *bluge.Reader) flex.Iter[HNSWDocRow] {
	return flex.NewIter(func(yield func(HNSWDocRow) bool) error {
		query := bluge.NewPrefixQuery("doc/").SetField("_id")
		request := bluge.NewAllMatches(query)
		it := search.Search(context.Background(), reader, request)
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

func (seg *HNSWSegment) countHNSWDocRows(reader *bluge.Reader) (int, error) {
	query := bluge.NewPrefixQuery("doc/").SetField("_id")
	request := bluge.NewTopNSearch(0, query)
	dmi, err := reader.Search(context.Background(), request)
	if err != nil {
		return 0, err
	}
	size := dmi.Aggregations().Count()
	return int(size), nil
}

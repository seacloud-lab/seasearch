package core

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/zutils"

	"github.com/blugelabs/bluge"
	usearch "github.com/unum-cloud/usearch/golang"
)

type HNSWSegment struct {
	mutex sync.RWMutex

	path       string
	dimensions int

	loaded atomic.Bool

	currentLogID int64

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
	seg.path = path
	seg.dimensions = dimensions
	seg.shadowDocIDs = make(map[int64]struct{})
	seg.cache.docIDIndex = make(map[int64]int)
	return &seg
}

func (seg *HNSWSegment) Close() {
	if seg.index != nil {
		seg.index.Close()
		seg.index = nil
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

	var batch = bluge.NewBatch()
	for _, docID := range delIDs {
		seg.currentLogID++
		doc := bluge.NewDocument(fmt.Sprintf("log/%v", seg.currentLogID))
		doc.AddField(bluge.NewStoredOnlyField("doc_id",
			[]byte(strconv.FormatInt(docID, 10)),
		))
		doc.AddField(bluge.NewStoredOnlyField("doc_del",
			[]byte("true"),
		))
		batch.Update(doc.ID(), doc)
	}
	for i, docID := range addIDs {
		seg.currentLogID++
		doc := bluge.NewDocument(fmt.Sprintf("log/%v", seg.currentLogID))
		doc.AddField(bluge.NewStoredOnlyField("doc_id",
			[]byte(strconv.FormatInt(docID, 10)),
		))
		doc.AddField(bluge.NewStoredOnlyField("vector",
			zutils.VectorToBytes(vectors[i]),
		))
		batch.Update(doc.ID(), doc)
	}
	doc := bluge.NewDocument("current_log_id")
	doc.AddField(bluge.NewStoredOnlyField("log_id",
		[]byte(strconv.FormatInt(seg.currentLogID, 10)),
	))
	batch.Update(doc.ID(), doc)

	name := filepath.Join(vector.VecPrefix, seg.path, "0000", "stored_vec")
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
		return fmt.Errorf("failed to open bluge writer: %w", err)
	}
	defer writer.Close()
	err = writer.Batch(batch)
	if err != nil {
		return fmt.Errorf("failed to write bluge batch: %w", err)
	}

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

	name := filepath.Join(vector.VecPrefix, seg.path, "0000", "stored_vec")
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

	reader, err := bluge.OpenReader(cfg)
	if err != nil {
		return fmt.Errorf("failed to open bluge reader: %w", err)
	}
	defer reader.Close()

	err = seg.loadCurrentLogID(reader)
	if err != nil {
		return fmt.Errorf("failed to load current log id: %w", err)
	}
	err = seg.loadLogs(reader)
	if err != nil {
		return fmt.Errorf("failed to load logs: %w", err)
	}
	seg.loaded.Store(true)

	return nil
}

func (seg *HNSWSegment) loadCurrentLogID(reader *bluge.Reader) error {
	query := bluge.NewTermQuery("current_log_id").SetField("_id")
	request := bluge.NewTopNSearch(1, query)

	var data []byte
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
		return it.Error()
	}

	if data == nil {
		return nil
	}
	logID, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return err
	}
	seg.currentLogID = logID

	return nil
}

func (seg *HNSWSegment) loadLogs(reader *bluge.Reader) error {
	docTS := make(map[int64]int64)

	query := bluge.NewPrefixQuery("log/").SetField("_id")
	request := bluge.NewAllMatches(query)
	it := search.Search(context.Background(), reader, request)
	for doc := range it.Range() {
		var (
			logID  int64
			docID  int64
			docDel bool
			vector []float32
		)
		for field, value := range doc {
			switch field {
			case "_id":
				id, err := strconv.ParseInt(string(value)[4:], 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse doc id %v: %w", string(value), err)
				}
				logID = id
			case "doc_id":
				id, err := strconv.ParseInt(string(value), 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse doc id %v: %w", string(value), err)
				}
				docID = id
			case "doc_del":
				if string(value) == "true" {
					docDel = true
				}
			case "vector":
				vector = zutils.BytesToVector(value)
			}
		}

		seg.shadowDocIDs[docID] = struct{}{}

		if docTS[docID] > logID {
			continue
		}
		docTS[docID] = logID

		if docDel {
			seg.deleteCache(docID)
		} else {
			seg.updateCache(docID, vector)
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

	seg.cache.docIDs[i] = seg.cache.docIDs[j]
	seg.cache.docIDs = seg.cache.docIDs[:j]
	copy(seg.cache.docVectors[i*d:(i+1)*d], seg.cache.docVectors[j*d:(j+1)*d])
	seg.cache.docVectors = seg.cache.docVectors[:j*d]
	seg.cache.docIDIndex[seg.cache.docIDs[i]] = i
	delete(seg.cache.docIDIndex, docID)
}

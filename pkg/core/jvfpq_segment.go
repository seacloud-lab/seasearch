package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zincsearch/zincsearch/faiss_wrapper"
	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery/query"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"

	"github.com/DataIntelligenceCrew/go-faiss"
	"github.com/blugelabs/bluge"
	"github.com/rs/zerolog/log"
)

// we store original vectors with a fixed field.
const internalVecField = "vec"

type IVFPQSegment struct {
	baseIndex VectorIndex
	ref       *meta.VectorSegment
	index     faiss.Index
	// cache for search
	cachedVectorsCache []float32
	// vector idx -> docId
	ids []int64

	// for growing segment, the lock is used for synchronizing cache.
	// for sealed segment, the lock is used for avoiding concurrent read and write for faiss index.
	sync.RWMutex

	vecStoreWriter *bluge.Writer
	writerLock     sync.RWMutex
	writerRefCount int64
	closeTime      time.Time
}

func openIVFPQSegment(vecIndex VectorIndex, id int) (*IVFPQSegment, error) {
	ref := vecIndex.Meta().Segments[id]
	seg := &IVFPQSegment{
		ref:       ref,
		baseIndex: vecIndex,
	}
	// first load vector store
	err := seg.initVecStorePath()
	if err != nil {
		return nil, fmt.Errorf("open seg bluge store err: %w", err)
	}
	// load faiss index
	if ref.Status == vector.StatusSealed {
		err := seg.loadFaissIndexFile()
		if err != nil {
			return nil, fmt.Errorf("open seg faiss index err: %w", err)
		}
	}
	return seg, nil
}

func (s *IVFPQSegment) initVecStorePath() error {
	p := path.Join(config.Global.DataPath, path.Join(vector.VecPrefix, s.baseIndex.StoreName(), fmt.Sprintf("%04x", s.ref.Id), "stored_vec"))
	err := os.MkdirAll(p, 0777)
	if err != nil {
		return fmt.Errorf("init path err: error creating directory '%s': %w", p, err)
	}
	return nil
}

func (s *IVFPQSegment) openVecStoreReader() (*bluge.Reader, error) {
	dataPath := config.Global.DataPath
	name := path.Join(vector.VecPrefix, s.baseIndex.StoreName(), fmt.Sprintf("%04x", s.ref.Id), "stored_vec")
	var cfg bluge.Config
	switch config.Global.StorageType {
	case "disk":
		cfg = directory.GetDiskConfig(dataPath, name)
	case "s3":
		cfg = directory.GetS3Config(dataPath, name)
	case "oss":
		cfg = directory.GetOssConfig(dataPath, name)
	default:
		return nil, fmt.Errorf("invalid storage type: %s", config.Global.StorageType)
	}

	var err error
	var r *bluge.Reader
	for i := 0; i < 3; i++ {
		r, err = bluge.OpenReader(cfg)
		if err != nil {
			// it's a new empty index, without any document
			if errors.Is(err, directory.ErrPathNotExists) {
				return nil, nil
			}
			// When opening the reader, the writer may be writing and generating new snapshots concurrently.
			// The snapshots listed by the reader may have expired and been cleaned up at this time.
			// In this case, we should try to get the latest snapshot again.
			if strings.Contains(err.Error(), "unable to find a usable snapshot") {
				log.Warn().Msgf("failed to open reader for %s: %v, retrying", s.getName(), err)
				time.Sleep(time.Duration(zutils.RandInt(100, 300)) * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("open %s err: %w", s.getName(), err)
		}
		return r, nil
	}
	return r, err
}

func (s *IVFPQSegment) openVecStoreWriter() (*bluge.Writer, error) {
	s.writerLock.Lock()
	w := s.vecStoreWriter
	s.writerLock.Unlock()
	if w != nil {
		curRef := atomic.AddInt64(&s.writerRefCount, 1)
		log.Debug().Msgf("open vec segment %s, current ref count: %d", s.getName(), curRef)
		return w, nil
	}
	dataPath := config.Global.DataPath
	name := path.Join(vector.VecPrefix, s.baseIndex.StoreName(), fmt.Sprintf("%04x", s.ref.Id), "stored_vec")
	var cfg bluge.Config
	switch config.Global.StorageType {
	case "disk":
		cfg = directory.GetDiskConfig(dataPath, name)
	case "s3":
		cfg = directory.GetS3Config(dataPath, name)
	case "oss":
		cfg = directory.GetOssConfig(dataPath, name)
	default:
		return nil, fmt.Errorf("invalid storage type: %s", config.Global.StorageType)
	}

	writer, err := bluge.OpenWriter(cfg)
	if err != nil {
		return nil, err
	}
	s.writerLock.Lock()
	s.vecStoreWriter = writer
	s.writerLock.Unlock()
	curRef := atomic.AddInt64(&s.writerRefCount, 1)
	log.Debug().Msgf("open vec segment %s, current ref count: %d", s.getName(), curRef)
	return writer, nil
}

func (s *IVFPQSegment) closeVecStoreWriter() {
	s.writerLock.Lock()
	s.closeTime = time.Now()
	s.writerLock.Unlock()
	curRef := atomic.AddInt64(&s.writerRefCount, -1)
	log.Debug().Msgf("close vec segment %s, current ref count: %d", s.getName(), curRef)
}

func (s *IVFPQSegment) getName() string {
	return fmt.Sprintf("%s_%d", s.baseIndex.Name(), s.ref.Id)
}

func (s *IVFPQSegment) saveFaissIndex() error {
	f, err := os.CreateTemp(vecIdxManager.tmpDir, "temp_index")
	if err != nil {
		return err
	}
	_ = f.Close()
	defer func() {
		_ = os.Remove(f.Name())
	}()

	err = faiss.WriteIndex(s.index, f.Name())
	if err != nil {
		return err
	}
	err = vecIdxManager.storage.SaveFile(f.Name(), s.getFaissIndexPath())
	if err != nil {
		return err
	}
	return err
}

func (s *IVFPQSegment) loadFaissIndexFile() error {
	localFile, closer, err := vecIdxManager.storage.LoadFile(s.getFaissIndexPath())
	if err != nil {
		return err
	}
	defer func() {
		_ = closer.Close()
	}()
	s.index, err = faiss.ReadIndex(localFile, faiss.IOFlagReadOnly)
	return err
}

func (s *IVFPQSegment) getFaissIndexPath() string {
	return path.Join(s.baseIndex.StoreName(), fmt.Sprintf("%04x", s.ref.Id), "faiss")
}

func (s *IVFPQSegment) Batch(batch *segBatch) error {
	s.Lock()
	defer s.Unlock()
	if len(batch.deleteIds) > 0 {
		err := s.removeIDs(batch.deleteIds)
		if err != nil {
			return err
		}
	}
	if len(batch.addVectors) > 0 {
		err := s.addVectors(batch.addVectors, batch.addIds)
		if err != nil {
			return err
		}
	}
	return s.save()
}

// addVectors
// require Lock.
func (s *IVFPQSegment) addVectors(vectors [][]float32, ids []int64) error {
	// this should never have happened
	if s.ref.Status != vector.StatusGrowing {
		return fmt.Errorf("cannot add vectors to a non-Growing segment")
	}
	batch := bluge.NewBatch()
	for i, vec := range vectors {
		doc := bluge.NewDocument(base62.Encode(ids[i]))
		var bts []byte
		if s.baseIndex.UseFloat16() {
			bts = zutils.VectorToFloat16Bytes(vec)
		} else {
			bts = zutils.VectorToBytes(vec)
		}
		doc.AddField(bluge.NewStoredOnlyField(internalVecField, bts))
		batch.Insert(doc)
	}
	writer, err := s.openVecStoreWriter()
	if err != nil {
		return err
	}
	defer s.closeVecStoreWriter()

	err = writer.Batch(batch)
	if err != nil {
		return fmt.Errorf("add vectors to bluge err: %w", err)
	}
	return nil
}

// removeIDs
// require Lock.
func (s *IVFPQSegment) removeIDs(ids []int64) error {
	if s.ref.Status == vector.StatusSealed {
		selector, err := faiss.NewIDSelectorBatch(ids)
		if err != nil {
			return err
		}
		_, err = s.index.RemoveIDs(selector)
		if err != nil {
			return fmt.Errorf("remove seg faiss index err: %w", err)
		}
	}
	// remove vec_store
	batch := bluge.NewBatch()
	for _, id := range ids {
		batch.Delete(bluge.Identifier(base62.Encode(id)))
	}
	writer, err := s.openVecStoreWriter()
	if err != nil {
		return err
	}
	defer s.closeVecStoreWriter()
	err = writer.Batch(batch)
	if err != nil {
		return fmt.Errorf("remove seg bluge index err: %w", err)
	}
	return nil
}

// GetExistsIds
// Filter out the id that exists in this segment. It‘s lock free.
func (s *IVFPQSegment) GetExistsIds(ids []int64) ([]int64, error) {
	r, err := s.openVecStoreReader()
	if err != nil {
		return nil, err
	}
	if r == nil {
		return []int64{}, nil
	}
	defer func() {
		_ = r.Close()
	}()
	q := bluge.NewBooleanQuery()
	for _, id := range ids {
		sub := bluge.NewTermQuery(base62.Encode(id)).SetField("_id")
		q.AddShould(sub)
	}
	req := bluge.NewAllMatches(q)
	dmi, err := r.Search(context.Background(), req)
	if err != nil {
		return nil, err
	}
	result := make([]int64, 0)
	for next, err := dmi.Next(); err == nil && next != nil; next, err = dmi.Next() {
		var id string
		err = next.VisitStoredFields(func(f string, value []byte) bool {
			if f == "_id" {
				id = string(value)
				return false
			}
			return true
		})
		if err != nil {
			return nil, err
		}
		result = append(result, base62.Decode(id))
	}
	return result, nil
}

// save
// require Lock.
func (s *IVFPQSegment) save() error {
	if s.ref.Status != vector.StatusSealed {
		// the vectors have been updated, we clear the cache
		s.freeCache()

		// update count
		r, err := s.openVecStoreReader()
		if err != nil {
			return err
		}
		if r == nil {
			return nil
		}
		defer func() {
			_ = r.Close()
		}()
		c, err := r.Count()
		if err != nil {
			return err
		}
		atomic.StoreInt64(&s.ref.Count, int64(c))
		return nil
	}
	atomic.StoreInt64(&s.ref.Count, s.index.Ntotal())
	return s.saveFaissIndex()
}

func (s *IVFPQSegment) Free() {
	if s.index != nil {
		s.index.Delete()
	}
	s.writerLock.Lock()
	if s.vecStoreWriter != nil {
		_ = s.vecStoreWriter.Close()
	}
	s.writerLock.Unlock()

	s.Lock()
	s.freeCache()
	s.Unlock()
}

// freeCache
// require Lock
func (s *IVFPQSegment) freeCache() {
	s.ids = nil
	s.cachedVectorsCache = nil
}

// Search
// query vectors
func (s *IVFPQSegment) Search(vec []float32, k int64, nprobe int) (map[string]float32, error) {
	if s.ref.Status == vector.StatusGrowing {
		return s.searchFlat(vec, k)
	}
	return s.searchIvfPq(vec, k, nprobe)
}

func (s *IVFPQSegment) searchFlat(vec []float32, k int64) (map[string]float32, error) {
	s.Lock()
	ids, xb, err := s.getVecCaches()
	s.Unlock()

	if err != nil {
		return nil, err
	}
	if len(xb) == 0 || len(ids) == 0 {
		return make(map[string]float32), nil
	}

	return flatSearch(ids, xb, vec, int(k), s.baseIndex.Meta().Dims), nil
}

// getVecCaches
// require Lock
func (s *IVFPQSegment) getVecCaches() ([]int64, []float32, error) {
	var cacheVectors []float32
	var cacheIds []int64

	if s.cachedVectorsCache != nil && s.ids != nil {
		cacheVectors = s.cachedVectorsCache
		cacheIds = s.ids
	} else {
		var err error
		s.cachedVectorsCache, s.ids, err = s.getAllVectors()
		if err != nil {
			return nil, nil, err
		}
		cacheVectors = s.cachedVectorsCache
		cacheIds = s.ids
	}
	return cacheIds, cacheVectors, nil
}

func flatSearch(baseIds []int64, base []float32, query []float32, k, dim int) map[string]float32 {
	distances, ids := faiss_wrapper.Knn(query, base, 1, len(baseIds), k, dim)
	result := make(map[string]float32)
	for i, id := range ids {
		// it means there isn't enough K vectors.
		if id == -1 {
			break
		}
		result[base62.Encode(baseIds[id])] = distances[i]
	}
	return result
}

func (s *IVFPQSegment) searchIvfPq(vec []float32, k int64, nprobe int) (map[string]float32, error) {
	s.RLock()
	defer s.RUnlock()

	var ids []int64
	var err error
	ps, err := faiss.NewParameterSpace()
	if err != nil {
		return nil, err
	}
	defer ps.Delete()

	if err := ps.SetIndexParameter(s.index, "nprobe", float64(nprobe)); err != nil {
		return nil, err
	}

	var result = make(map[string]float32)
	_, ids, err = s.index.Search(vec, int64(k))
	if err != nil {
		return nil, err
	}

	docIds := make([]string, 0)
	for i := range ids {
		if ids[i] == -1 {
			continue
		}
		docIds = append(docIds, base62.Encode(ids[i]))
	}
	if len(docIds) == 0 {
		return result, nil
	}
	// for ivf_pq, we need to get vector and calculate the real distance
	q, err := query.TermsQuery(map[string]interface{}{
		"_id": docIds,
	}, nil)
	if err != nil {
		return nil, err
	}
	req := bluge.NewAllMatches(q)
	vectors, ids, err := s.getVectors(atomic.LoadInt64(&s.ref.Count), req)
	if err != nil {
		return nil, err
	}
	for i := range ids {
		realVector := vectors[i*s.baseIndex.Meta().Dims : (i+1)*s.baseIndex.Meta().Dims]
		dis, err := L2Distance(vec, realVector)
		if err != nil {
			return nil, err
		}
		result[base62.Encode(ids[i])] = dis
	}
	return result, nil
}

func L2Distance(slice1, slice2 []float32) (float32, error) {
	if len(slice1) != len(slice2) {
		return 0, fmt.Errorf("invalid vectors")
	}

	var sum float32
	for i := 0; i < len(slice1); i++ {
		diff := slice1[i] - slice2[i]
		sum += diff * diff
	}

	return sum, nil
}

func (s *IVFPQSegment) SearchByIDs(vec []float32, k int64, docIDs []string) ([]DocDistance, error) {
	zq, err := query.TermsQuery(map[string]any{"_id": docIDs}, nil)
	if err != nil {
		return nil, err
	}
	req := bluge.NewAllMatches(zq)
	vectors, ids, err := s.getVectors(-1, req)
	if err != nil {
		return nil, err
	}

	var distances []DocDistance
	for i := range ids {
		realVector := vectors[i*s.baseIndex.Meta().Dims : (i+1)*s.baseIndex.Meta().Dims]
		dis, err := L2Distance(vec, realVector)
		if err != nil {
			return nil, err
		}
		distances = append(distances, DocDistance{
			DocID:    base62.Encode(ids[i]),
			Distance: dis,
		})
	}
	return distances, nil
}

// Seal
// build an ivf_pq index, require Lock.
func (s *IVFPQSegment) Seal() error {
	s.Lock()
	defer s.Unlock()

	ids, vectors, err := s.getVecCaches()
	if err != nil {
		return err
	}
	if len(vectors) == 0 || len(ids) == 0 {
		// do nothing
		log.Warn().Msgf("try to seal ivf_pq index %s segment %d but there are no vectors", s.baseIndex.Name(), s.ref.Id)
		return nil
	}

	nlist := int(4 * math.Sqrt(float64(len(ids))))
	idx, err := faiss.IndexFactory(s.baseIndex.Meta().Dims, fmt.Sprintf("IVF%d,PQ%dx%d", nlist, s.baseIndex.Meta().M, s.baseIndex.Meta().NBits), faiss.MetricL2)
	if err != nil {
		return err
	}
	err = idx.Train(vectors)
	if err != nil {
		idx.Delete()
		return err
	}

	err = idx.AddWithIDs(vectors, ids)
	if err != nil {
		idx.Delete()
		return err
	}
	if s.index != nil {
		s.index.Delete()
	}
	s.index = idx
	err = s.saveFaissIndex()
	if err != nil {
		return err
	}
	s.ref.Status = vector.StatusSealed
	s.freeCache()
	return nil
}

// Recall
// calculate recall for whole segment, require RLock.
func (s *IVFPQSegment) Recall(count int, k int64, nprobe int) (float32, error) {
	s.RLock()
	defer s.RUnlock()

	if s.ref.Status == vector.StatusGrowing {
		return 1, nil
	}
	// get some really vectors as query vector
	xq, err := s.getQueryVectors(count)
	if err != nil {
		return 0, err
	}

	xb, idb, err := s.getAllVectors()
	if err != nil {
		return 0, err
	}

	correct := 0
	total := 0
	for _, q := range xq {
		results, err := s.searchIvfPq(q, k, nprobe)
		if err != nil {
			return 0, err
		}

		flatRes := flatSearch(idb, xb, q, int(k), s.baseIndex.Meta().Dims)
		total += len(flatRes)
		for id := range results {
			if _, ok := flatRes[id]; ok {
				correct++
			}
		}
	}

	recall := float32(correct) / float32(total)

	return recall, nil

}

// getQueryVectors
// get some vectors from vec_store for Recall
func (s *IVFPQSegment) getQueryVectors(n int) ([][]float32, error) {
	q := bluge.NewMatchAllQuery()
	req := bluge.NewTopNSearch(n, q)
	vectors, _, err := s.getVectors(int64(n), req)
	if err != nil {
		return nil, err
	}
	res := make([][]float32, 0, n)
	dim := s.baseIndex.Meta().Dims
	for i := 0; i < n; i++ {
		res = append(res, vectors[i*dim:(i+1)*dim])
	}
	return res, nil
}

// getAllVectors
// get all vectors from vec_store
func (s *IVFPQSegment) getAllVectors() ([]float32, []int64, error) {
	q := bluge.NewMatchAllQuery()
	req := bluge.NewAllMatches(q)
	return s.getVectors(atomic.LoadInt64(&s.ref.Count), req)
}

func (s *IVFPQSegment) getVectors(count int64, searchReq bluge.SearchRequest) ([]float32, []int64, error) {
	reader, err := s.openVecStoreReader()
	if err != nil {
		return nil, nil, err
	}
	if reader == nil {
		return []float32{}, []int64{}, nil
	}
	defer func() {
		_ = reader.Close()
	}()

	var (
		vectors []float32
		ids     []int64
	)
	if count > 0 {
		vectors = make([]float32, 0, int(count)*s.baseIndex.Meta().Dims)
		ids = make([]int64, 0, count)
	}

	dmi, err := reader.Search(context.Background(), searchReq)
	if err != nil {
		return nil, nil, err
	}
	for next, err := dmi.Next(); err == nil && next != nil; next, err = dmi.Next() {
		var id string
		var vec []float32
		err = next.VisitStoredFields(func(f string, value []byte) bool {
			if f == "_id" {
				id = string(value)
				return vec == nil
			}
			if f == internalVecField {
				if s.baseIndex.UseFloat16() {
					vec = zutils.Float16BytesToVector(value)
				} else {
					vec = zutils.BytesToVector(value)
				}
				return id == ""
			}
			return true
		})
		if err != nil {
			return vectors, ids, err
		}
		vectors = append(vectors, vec...)
		ids = append(ids, base62.Decode(id))
	}

	return vectors, ids, nil
}

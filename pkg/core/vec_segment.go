package core

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"sync"

	"github.com/DataIntelligenceCrew/go-faiss"
	"github.com/blugelabs/bluge"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/faiss_wrapper"
	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery/query"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
)

// we store original vectors with a fixed field.
const internalVecField = "vec"

type VecSegment struct {
	baseIndex      VectorIndex
	ref            *meta.VectorSegment
	vecStoreWriter *bluge.Writer
	index          faiss.Index
	// cache for search
	cachedVectorsCache []float32
	// vector idx -> docId
	ids       []int64
	cacheLock sync.Mutex // internal lock, only used for sync vectors caches.

	// used for sync segment write operations like: AddVectors, RemoveIDs, Sealed, Search;
	// Only caller controls the accessibility.
	sync.RWMutex
}

func openSegment(vecIndex VectorIndex, id int) (*VecSegment, error) {
	ref := vecIndex.Meta().Segments[id]
	seg := &VecSegment{
		ref:       ref,
		baseIndex: vecIndex,
	}
	// first load vector store
	err := seg.loadVectorStore()
	if err != nil {
		return nil, fmt.Errorf("open seg bluge store err: %w", err)
	}
	// load faiss index
	if ref.Status == vector.StatusSealed {
		err = seg.loadFaissIndexFile()
		if err != nil {
			return nil, fmt.Errorf("open seg faiss index err: %w", err)
		}
	}
	return seg, nil
}

func (s *VecSegment) saveFaissIndex() error {
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
	err = vecIdxManager.storage.SaveFile(f.Name(), s.getFaissVecStorePath())
	if err != nil {
		return err
	}
	return err
}

func (s *VecSegment) loadFaissIndexFile() error {
	localFile, closer, err := vecIdxManager.storage.LoadFile(s.getFaissVecStorePath())
	if err != nil {
		return err
	}
	defer func() {
		_ = closer.Close()
	}()
	s.index, err = faiss.ReadIndex(localFile, faiss.IOFlagReadOnly)
	return err
}

func (s *VecSegment) getFaissVecStorePath() string {
	return path.Join(s.baseIndex.Name(), fmt.Sprintf("%012x", s.ref.Id), "faiss")
}

func (s *VecSegment) loadVectorStore() error {
	dataPath := config.Global.DataPath
	name := path.Join(vector.VecPrefix, s.baseIndex.Name(), fmt.Sprintf("%012x", s.ref.Id), "stored_vec")
	var cfg bluge.Config
	switch config.Global.StorageType {
	case "disk":
		cfg = directory.GetDiskConfig(dataPath, name)
	case "s3":
		cfg = directory.GetS3Config(dataPath, name)
	case "oss":
		cfg = directory.GetOssConfig(dataPath, name)
	default:
		return fmt.Errorf("invalid storage type: %s", config.Global.StorageType)
	}
	var err error
	s.vecStoreWriter, err = bluge.OpenWriter(cfg)
	return err
}

// AddVectors
// require Lock.
func (s *VecSegment) AddVectors(vectors [][]float32, ids []int64) error {
	// this should never have happened
	if s.ref.Status != vector.StatusGrowing {
		return fmt.Errorf("cannot add vectors to a non-Growing segment")
	}
	batch := bluge.NewBatch()
	for i, vec := range vectors {
		doc := bluge.NewDocument(base62.Encode(ids[i]))
		doc.AddField(bluge.NewStoredOnlyField(internalVecField, zutils.VectorToBytes(vec)))
		batch.Insert(doc)
	}
	err := s.vecStoreWriter.Batch(batch)
	if err != nil {
		return fmt.Errorf("add vectors to bluge err: %w", err)
	}
	return nil
}

// RemoveIDs
// require Lock.
func (s *VecSegment) RemoveIDs(ids []int64) error {
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
	err := s.vecStoreWriter.Batch(batch)
	if err != nil {
		return fmt.Errorf("remove seg bluge index err: %w", err)
	}
	return nil
}

func (s *VecSegment) getExistsIds(ids []int64) ([]int64, error) {
	r, err := s.vecStoreWriter.Reader()
	defer func() {
		_ = r.Close()
	}()
	if err != nil {
		return nil, err
	}
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

// Save
// require Lock.
func (s *VecSegment) Save() error {
	if s.ref.Status != vector.StatusSealed {
		// the vectors have been updated, we clear the cache
		s.freeCache()

		// update count
		r, err := s.vecStoreWriter.Reader()
		if err != nil {
			return err
		}
		defer func() {
			_ = r.Close()
		}()
		c, err := r.Count()
		if err != nil {
			return err
		}
		s.ref.Count = int64(c)
		return nil
	}
	s.ref.Count = s.index.Ntotal()
	return s.saveFaissIndex()
}

func (s *VecSegment) Free() {
	_ = s.vecStoreWriter.Close()
	if s.index != nil {
		s.index.Delete()
	}

	s.freeCache()
}

func (s *VecSegment) freeCache() {
	s.cacheLock.Lock()
	s.ids = nil
	s.cachedVectorsCache = nil
	s.cacheLock.Unlock()
}

// Search
// query vectors, require RLock.
func (s *VecSegment) Search(vec []float32, k int64, nprobe int) (map[string]float32, error) {
	if s.ref.Status == vector.StatusGrowing {
		return s.searchFlat(vec, k)
	}
	return s.searchIvfPq(vec, k, nprobe)
}

func (s *VecSegment) searchFlat(vec []float32, k int64) (map[string]float32, error) {
	ids, xb, err := s.getVecCaches()
	if err != nil {
		return nil, err
	}
	if len(xb) == 0 || len(ids) == 0 {
		return make(map[string]float32), nil
	}

	return flatSearch(ids, xb, vec, int(k), s.baseIndex.Meta().Dims), nil
}

func (s *VecSegment) getVecCaches() ([]int64, []float32, error) {
	var cacheVectors []float32
	var cacheIds []int64

	s.cacheLock.Lock()
	if s.cachedVectorsCache != nil && s.ids != nil {
		cacheVectors = s.cachedVectorsCache
		cacheIds = s.ids
		s.cacheLock.Unlock()
	} else {
		var err error
		s.cachedVectorsCache, s.ids, err = s.getAllVectors()
		s.cacheLock.Unlock()
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

func (s *VecSegment) searchIvfPq(vec []float32, k int64, nprobe int) (map[string]float32, error) {
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
	vectors, ids, err := s.getVectors(s.ref.Count, req)
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
		return 0, fmt.Errorf("invliad vectors")
	}

	var sum float32
	for i := 0; i < len(slice1); i++ {
		diff := slice1[i] - slice2[i]
		sum += diff * diff
	}

	return sum, nil
}

// Sealed
// build an ivf_pq index, require Lock.
func (s *VecSegment) Sealed() error {
	ids, vectors, err := s.getVecCaches()
	if err != nil {
		return err
	}
	if len(vectors) == 0 || len(ids) == 0 {
		// do nothing
		log.Warn().Msgf("try to rebuild ivf_pq index %s segment %d but there are no vectors", s.baseIndex.Name(), s.ref.Id)
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
	s.ref.Count = idx.Ntotal()
	s.freeCache()
	return nil
}

// Recall
// calculate recall for whole segment, require RLock.
func (s *VecSegment) Recall(count int, k int64, nprobe int) (float32, error) {
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
		results, err := s.Search(q, k, nprobe)
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
func (s *VecSegment) getQueryVectors(n int) ([][]float32, error) {
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
func (s *VecSegment) getAllVectors() ([]float32, []int64, error) {
	q := bluge.NewMatchAllQuery()
	req := bluge.NewAllMatches(q)
	return s.getVectors(s.ref.Count, req)
}

func (s *VecSegment) getVectors(count int64, searchReq bluge.SearchRequest) ([]float32, []int64, error) {
	reader, err := s.vecStoreWriter.Reader()
	defer func() {
		_ = reader.Close()
	}()

	vectors := make([]float32, 0, int(count)*s.baseIndex.Meta().Dims)
	ids := make([]int64, 0, count)

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
				vec = zutils.BytesToVector(value)
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

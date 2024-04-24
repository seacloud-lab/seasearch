package core

import (
	"fmt"
	"math"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DataIntelligenceCrew/go-faiss"
	"github.com/blugelabs/bluge"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/faiss_wrapper"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery/query"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
)

type VectorIndex interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()
	// AddVectors
	// Caller needs to get Lock before call it.
	AddVectors(vectors [][]float32, ids []int64) error
	// RemoveIDs
	// Caller needs to get Lock before call it.
	RemoveIDs(ids []int64) (int, error)
	// Save
	// Caller needs to get Lock before call it.
	Save() error
	// Search
	// Caller needs to get RLock before call it.
	Search(vec []float32, k int64, nprobe int) (map[string]float32, error)
	Name() string
	Meta() *meta.VecIndex
	// Rebuild
	// Caller needs to get Lock before call it.
	Rebuild() error
	AddRef()
	ReduceRef()
	RefCount() int
	ATime() time.Time
	// Free
	// External caller needs to get Lock before call it.
	Free()
}

type baseIndex struct {
	// TODO: zincIndex may be closed when using
	zincIndex *Index
	name      string
	field     string
	ref       *meta.VecIndex
	refCount  int
	aTime     time.Time
	lock      sync.RWMutex
}

func (b *baseIndex) Meta() *meta.VecIndex {
	return b.ref
}

func (b *baseIndex) Name() string {
	return b.name
}

func (b *baseIndex) AddRef() {
	b.refCount++
	b.aTime = time.Now()
}

func (b *baseIndex) ReduceRef() {
	b.refCount--
}

func (b *baseIndex) RefCount() int {
	return b.refCount
}

func (b *baseIndex) ATime() time.Time {
	return b.aTime
}

func (b *baseIndex) getVectors() ([]float32, []int64, error) {
	readers, err := b.zincIndex.GetReaders(0, 0)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	if err != nil {
		return nil, nil, err
	}
	d := b.ref.Dims
	q := bluge.NewMatchAllQuery()
	req := bluge.NewAllMatches(q)
	return getVectors(b.field, d, b.ref.Count, req, readers...)
}

func MakeVecIndex(zincIndex *Index, field string, vecIndexMeta *meta.VecIndex) (VectorIndex, error) {
	if vecIndexMeta.TargetType == vector.Flat {
		return &FlatIndex{
			baseIndex: baseIndex{
				zincIndex: zincIndex,
				name:      path.Join(zincIndex.GetName(), field),
				field:     field,
				ref:       vecIndexMeta,
				refCount:  1,
				aTime:     time.Now(),
			},
		}, nil
	}

	ivfPqIndex := &IvfPqIndex{
		baseIndex: baseIndex{
			zincIndex: zincIndex,
			name:      path.Join(zincIndex.GetName(), field),
			field:     field,
			ref:       vecIndexMeta,
			refCount:  1,
			aTime:     time.Now(),
		},
	}
	// current type is ivf_pq, we need to load faiss index file
	if vecIndexMeta.Type == vector.IvfPQ {
		err := ivfPqIndex.loadIndexFile()
		if err != nil {
			return nil, err
		}
	}
	return ivfPqIndex, nil
}

// FlatIndex
// the vectors should have been saved into bluge and
// flat index doesn't save anything
type FlatIndex struct {
	baseIndex
	// vectors cache
	cachedVectorsCache []float32
	// vector idx -> docId
	ids []int64
}

func (f *FlatIndex) AddVectors(vectors [][]float32, ids []int64) error {
	if len(vectors) != len(ids) {
		return ErrInvalidVec
	}
	atomic.AddInt64(&f.ref.Count, int64(len(ids)))
	return nil
}

func (f *FlatIndex) RemoveIDs(ids []int64) (int, error) {
	atomic.AddInt64(&f.ref.Count, -int64(len(ids)))
	return len(ids), nil
}

// Save
// when vectors changed, we clear cache
func (f *FlatIndex) Save() error {
	f.Free()
	return f.zincIndex.SaveVecIndexMeta(f.field, f.ref)
}

func (f *FlatIndex) Free() {
	f.cachedVectorsCache = nil
	f.ids = nil
}

func (f *FlatIndex) Rebuild() error {
	// Flat index should never be called rebuild
	log.Warn().Msgf("Rebuild should not be called on flat index %s.", f.name)
	return nil
}

// Search
// flat index get all vectors from bluge, and use faiss to get top k
func (f *FlatIndex) Search(vec []float32, k int64, _ int) (map[string]float32, error) {
	if f.cachedVectorsCache == nil || f.ids == nil {
		var err error
		f.cachedVectorsCache, f.ids, err = f.getVectors()
		if err != nil {
			return nil, err
		}
	}
	cacheVectors := f.cachedVectorsCache
	cacheIds := f.ids

	if len(cacheVectors) == 0 || len(cacheIds) == 0 {
		return make(map[string]float32), nil
	}
	return flatSearch(cacheIds, cacheVectors, vec, int(k), f.ref.Dims), nil
}

func (f *FlatIndex) Lock() {
	f.lock.Lock()
}

func (f *FlatIndex) Unlock() {
	f.lock.Unlock()
}

func (f *FlatIndex) RLock() {
	f.lock.RLock()
}

func (f *FlatIndex) RUnlock() {
	f.lock.RUnlock()
}

// IvfPqIndex
// wrap of faiss ivf_pq
type IvfPqIndex struct {
	baseIndex
	index faiss.Index

	cachedVectorsCache []float32
	// vector idx -> docId
	ids []int64
}

func (v *IvfPqIndex) Free() {
	if v.index != nil {
		v.index.Delete()
	}
	v.cachedVectorsCache = nil
	v.ids = nil
}

func (v *IvfPqIndex) AddVectors(vectors [][]float32, ids []int64) error {
	if v.ref.Type == vector.Flat {
		atomic.AddInt64(&v.ref.Count, int64(len(ids)))
		return nil
	}
	d := v.index.D()
	finalVector := make([]float32, len(vectors)*d)
	if len(vectors) != len(ids) {
		log.Fatal().Err(ErrInvalidArguments).Msgf("len vectors is not equal to len ids")
		return ErrInvalidArguments
	}
	for k, vec := range vectors {
		if len(vec) != d {
			log.Fatal().Err(ErrInvalidVec).Msgf("vector dims is not equal to index setting")
			return ErrInvalidVec
		}
		for i, val := range vec {
			finalVector[k*d+i] = val
		}
	}
	err := v.index.AddWithIDs(finalVector, ids)
	return err
}

func (v *IvfPqIndex) RemoveIDs(ids []int64) (int, error) {
	if v.ref.Type == vector.Flat {
		atomic.AddInt64(&v.ref.Count, -int64(len(ids)))
		return len(ids), nil
	}
	selector, err := faiss.NewIDSelectorBatch(ids)
	if err != nil {
		return 0, err
	}
	return v.index.RemoveIDs(selector)
}

func (v *IvfPqIndex) Save() error {
	if v.ref.Type == vector.IvfPQ {
		v.ref.Count = v.index.Ntotal()
		v.ref.Stored = true
		err := save(v.index, v.zincIndex.GetName(), v.field)
		if err != nil {
			return err
		}
	} else {
		// for flat, we clear cache
		v.Free()
	}
	// update metaData
	return v.zincIndex.SaveVecIndexMeta(v.field, v.ref)
}

func save(idx faiss.Index, zincIndexName string, field string) error {
	f, err := os.CreateTemp(vecIdxManager.tmpDir, "temp_index")
	if err != nil {
		return err
	}
	_ = f.Close()
	defer func() {
		_ = os.Remove(f.Name())
	}()

	err = faiss.WriteIndex(idx, f.Name())
	if err != nil {
		return err
	}
	name := path.Join(zincIndexName, field)
	err = vecIdxManager.storage.SaveFile(f.Name(), name)
	if err != nil {
		return err
	}
	return err
}

// Search query vector index,
// return a map docId->distance
// should reorder by distance
func (v *IvfPqIndex) Search(vec []float32, k int64, nprobe int) (map[string]float32, error) {
	if v.ref.Type == vector.Flat {
		return v.searchFlat(vec, k)
	}
	return v.searchIvfPq(vec, k, nprobe)
}

func (v *IvfPqIndex) searchFlat(vec []float32, k int64) (map[string]float32, error) {
	if v.cachedVectorsCache == nil || v.ids == nil {
		var err error
		v.cachedVectorsCache, v.ids, err = v.getVectors()
		if err != nil {
			return nil, err
		}
	}
	cacheVectors := v.cachedVectorsCache
	cacheIds := v.ids

	if len(cacheVectors) == 0 || len(cacheIds) == 0 {
		return make(map[string]float32), nil
	}
	return flatSearch(cacheIds, cacheVectors, vec, int(k), v.ref.Dims), nil
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

func (v *IvfPqIndex) searchIvfPq(vec []float32, k int64, nprobe int) (map[string]float32, error) {
	var ids []int64
	var err error
	ps, err := faiss.NewParameterSpace()
	if err != nil {
		return nil, err
	}
	defer ps.Delete()

	if err := ps.SetIndexParameter(v.index, "nprobe", float64(nprobe)); err != nil {
		return nil, err
	}

	var result = make(map[string]float32)
	_, ids, err = v.index.Search(vec, k)
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
	readers, err := v.zincIndex.GetReaders(0, 0)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	if err != nil {
		return nil, err
	}
	req := bluge.NewAllMatches(q)

	vectors, ids, err := getVectors(v.field, v.ref.Dims, v.ref.Count, req, readers...)
	if err != nil {
		return nil, err
	}
	for i := range ids {
		realVector := vectors[i*v.ref.Dims : (i+1)*v.ref.Dims]
		dis, err := L2Distance(vec, realVector)
		if err != nil {
			return nil, err
		}
		result[base62.Encode(ids[i])] = dis
	}
	return result, nil
}

// Rebuild
// TODO: make sure no new vectors are being written when rebuilding
func (v *IvfPqIndex) Rebuild() error {
	var vectors []float32
	var ids []int64
	// if cached, we just use it
	if v.cachedVectorsCache != nil && v.ids != nil {
		vectors = v.cachedVectorsCache
		ids = v.ids
	} else {
		var err error
		vectors, ids, err = v.getVectors()
		if err != nil {
			return err
		}
	}

	if len(vectors) == 0 || len(ids) == 0 {
		// do nothing
		log.Warn().Msgf("try to rebuild ivf_pq index %s but there are no vectors", v.name)
		return nil
	}

	nlist := int(4 * math.Sqrt(float64(len(ids))))
	idx, err := faiss.IndexFactory(v.ref.Dims, fmt.Sprintf("IVF%d,PQ%dx%d", nlist, v.ref.M, v.ref.NBits), faiss.MetricL2)
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

	err = save(idx, v.zincIndex.GetName(), v.field)
	if err != nil {
		return err
	}
	v.ref.Type = vector.IvfPQ
	v.ref.NList = nlist
	v.ref.Count = idx.Ntotal()
	v.ref.Stored = true
	// free old index
	v.Free()
	v.index = idx

	return v.zincIndex.SaveVecIndexMeta(v.field, v.ref)
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

// loadIndexFile load faiss into memory
func (v *IvfPqIndex) loadIndexFile() error {
	vecIndexName := path.Join(v.zincIndex.GetName(), v.field)
	localFile, closer, err := vecIdxManager.storage.LoadFile(vecIndexName)
	if err != nil {
		return err
	}
	defer func() {
		_ = closer.Close()
	}()
	if v.index == nil {
		v.index, err = faiss.ReadIndex(localFile, faiss.IOFlagReadOnly)
	}
	return err
}

func (v *IvfPqIndex) Lock() {
	v.lock.Lock()
}

func (v *IvfPqIndex) Unlock() {
	v.lock.Unlock()
}

func (v *IvfPqIndex) RLock() {
	v.lock.RLock()
}

func (v *IvfPqIndex) RUnlock() {
	v.lock.RUnlock()
}

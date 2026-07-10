package core

import (
	"cmp"
	"container/heap"
	"context"
	"fmt"
	"path"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
	"golang.org/x/sync/errgroup"
)

type VectorIndex interface {
	Batch(addVectors [][]float32, addIds, deleteIds []int64) error
	Search(vec []float32, k int64, nprobe int) (map[string]float32, error)
	SearchByIDs(vec []float32, k int64, docIDs []string) ([]DocDistance, error)
	// PartialSearch is used for parallel search. The proxy assigns specific segments to each node,
	// and the local node performs the search based on the assigned segment IDs.
	PartialSearch(vec []float32, k int64, nprobe int, segments []int) (map[string]float32, error)
	Name() string
	StoreName() string
	Meta() *meta.VecIndex
	SealSeg() error
	AddRef()
	ReduceRef()
	RefCount() int
	ATime() time.Time
	Free()
	Recall(count int, k int64, nprobe int) (float32, error)
	UseFloat16() bool
	ListSegment() []IndexSegment
	BuildHNSW(context.Context) error
}

type IndexSegment interface {
	TryCloseIdleWriter(idleThreshold time.Duration) bool
}

type DocDistance struct {
	DocID    string
	Distance float32
}

type baseIndex struct {
	// TODO: zincIndex may be closed when using
	zincIndex *Index
	name      string
	storeName string
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

func (b *baseIndex) StoreName() string {
	return b.storeName
}

func (b *baseIndex) UseFloat16() bool {
	return b.ref.StoreWithFloat16
}

func MakeVecIndex(zincIndex *Index, field string, vecIndexMeta *meta.VecIndex) (VectorIndex, error) {
	var subPath string
	if vecIndexMeta.StoreWithHash {
		subPath = zutils.GetHashEncode(field)
	} else {
		subPath = field
	}

	switch vecIndexMeta.TargetType {
	case vector.Flat:
		return &FlatIndex{
			baseIndex: baseIndex{
				zincIndex: zincIndex,
				name:      path.Join(zincIndex.GetName(), field),
				storeName: path.Join(zincIndex.GetStoreName(), subPath),
				field:     field,
				ref:       vecIndexMeta,
				refCount:  1,
				aTime:     time.Now(),
			},
		}, nil

	case vector.IVFPQ:
		ivfPqIndex := &IvfPqIndex{
			baseIndex: baseIndex{
				zincIndex: zincIndex,
				name:      path.Join(zincIndex.GetName(), field),
				storeName: path.Join(zincIndex.GetStoreName(), subPath),
				field:     field,
				ref:       vecIndexMeta,
				refCount:  1,
				aTime:     time.Now(),
			},
		}
		err := ivfPqIndex.checkStoredSegments()
		if err != nil {
			return nil, err
		}
		ivfPqIndex.segments = make([]*IVFPQSegment, len(vecIndexMeta.Segments))
		return ivfPqIndex, nil

	case vector.HNSW:
		index := &HNSWIndex{
			baseIndex: baseIndex{
				zincIndex: zincIndex,
				name:      path.Join(zincIndex.GetName(), field),
				storeName: path.Join(zincIndex.GetStoreName(), subPath),
				field:     field,
				ref:       vecIndexMeta,
				refCount:  1,
				aTime:     time.Now(),
			},
		}
		index.segment = newHNSWSegment(index.StoreName(), vecIndexMeta.Dims)
		return index, nil

	default:
		return nil, fmt.Errorf("invalid index type %v", vecIndexMeta.TargetType)
	}
}

// FlatIndex stores vectors without building faiss index.
// Flat index has only one segment containing all vectors.
// Usually if you expect to have more than 100K vectors you should use IvfPQ index instead.
type FlatIndex struct {
	baseIndex
	seg *IVFPQSegment
}

func (f *FlatIndex) loadSegment() error {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.seg != nil {
		return nil
	}
	var err error
	// only one segment with id 0
	f.seg, err = openIVFPQSegment(f, 0)
	if err != nil {
		return err
	}
	return nil
}

func (f *FlatIndex) Batch(addVectors [][]float32, addIds, deleteIds []int64) error {
	err := f.loadSegment()
	if err != nil {
		return err
	}
	err = f.seg.Batch(&segBatch{
		addIds:     addIds,
		deleteIds:  deleteIds,
		addVectors: addVectors,
	})
	if err != nil {
		return err
	}
	atomic.StoreInt64(&f.ref.Count, atomic.LoadInt64(&f.seg.ref.Count))
	return nil
}

func (f *FlatIndex) Free() {
	if f.seg != nil {
		f.seg.Free()
	}
}

func (f *FlatIndex) SealSeg() error {
	// Flat index should never be called SealedSeg
	log.Warn().Msgf("SealSeg should not be called on flat index %s.", f.name)
	return nil
}

func (f *FlatIndex) BuildHNSW(ctx context.Context) error {
	log.Warn().Msgf("BuildHNSW should not be called on flat index %s.", f.name)
	return nil
}

// Search
// flat index get all vectors from vec_store, and use the function provided by faiss to get top k
func (f *FlatIndex) Search(vec []float32, k int64, _ int) (map[string]float32, error) {
	err := f.loadSegment()
	if err != nil {
		return nil, err
	}
	return f.seg.Search(vec, k, 0)
}

func (f *FlatIndex) SearchByIDs(vec []float32, k int64, docIDs []string) ([]DocDistance, error) {
	err := f.loadSegment()
	if err != nil {
		return nil, err
	}
	return f.seg.SearchByIDs(vec, k, docIDs)
}

func (f *FlatIndex) PartialSearch(vec []float32, k int64, nprobe int, segments []int) (map[string]float32, error) {
	log.Warn().Msgf("ParallelSearch should not be called on flat index %s.", f.name)
	return f.Search(vec, k, nprobe)
}

func (f *FlatIndex) Recall(count int, k int64, nprobe int) (float32, error) {
	log.Warn().Msgf("Recall should not be called on flat index %s.", f.name)
	return 1, nil
}

func (f *FlatIndex) ListSegment() []IndexSegment {
	f.lock.RLock()
	seg := f.seg
	f.lock.RUnlock()
	if seg == nil {
		return nil
	}
	res := make([]IndexSegment, 1)
	res[0] = seg
	return res
}

// IvfPqIndex has multiple segments, each contains at most 100K vectors.
// The growing segment (a.k.a. current segment) has no faiss index, while all sealed segments have faiss indexes built.
type IvfPqIndex struct {
	baseIndex
	segments []*IVFPQSegment
}

// checkStoredSegments
// check all stored segments and update metadata if there is any unrecorded segment.
func (v *IvfPqIndex) checkStoredSegments() error {
	ids, err := vector.ListVecSegments(v.name)
	if err != nil {
		return err
	}

	for _, segRef := range v.ref.Segments {
		if _, ok := ids[int64(segRef.Id)]; ok {
			continue
		} else {
			// current segment may haven't written anything, so it's ok.
			if segRef.Id == v.ref.CurrentSegmentId {
				continue
			}
			// segment exist in metadata but not in storage
			return ErrVecIndexCorruption
		}
	}

	// segment count matched
	if len(ids) == len(v.ref.Segments) {
		return nil
	}

	for id := v.ref.CurrentSegmentId + 1; id < len(ids); id++ {
		dataPath := config.Global.DataPath
		name := path.Join(vector.VecPrefix, v.name, fmt.Sprintf("%04x", id), "stored_vec")
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
		r, err := bluge.OpenReader(cfg)
		if err != nil {
			return fmt.Errorf("open vector index %s err: %w", name, err)
		}
		count, err := r.Count()
		_ = r.Close()
		if err != nil {
			return fmt.Errorf("get vec segment %s count err: %w", name, err)
		}
		v.ref.Segments = append(v.ref.Segments, &meta.VectorSegment{
			Id:     id,
			Status: vector.StatusGrowing,
			Count:  int64(count),
		})
	}

	return v.zincIndex.SaveVecIndexMeta(v.field, v.ref)
}

func (v *IvfPqIndex) loadCurrentSegment() error {
	if seg := v.segments[v.ref.CurrentSegmentId]; seg != nil {
		return nil
	}
	seg, err := openIVFPQSegment(v, v.ref.CurrentSegmentId)
	if err != nil {
		return err
	}
	v.segments[v.ref.CurrentSegmentId] = seg
	return nil
}

func (v *IvfPqIndex) loadAllSegments() error {
	ids := make([]int, len(v.ref.Segments))
	for i := range v.ref.Segments {
		ids[i] = i
	}
	_, err := v.loadSegments(ids)
	return err
}

func (v *IvfPqIndex) copySegments() ([]*IVFPQSegment, error) {
	err := v.loadAllSegments()
	if err != nil {
		return nil, err
	}
	res := make([]*IVFPQSegment, len(v.segments))
	copy(res, v.segments)
	return res, nil
}

func (v *IvfPqIndex) copySegmentsWithIds(ids []int) ([]*IVFPQSegment, error) {
	segments, err := v.loadSegments(ids)
	return segments, err
}

func (v *IvfPqIndex) loadSegments(ids []int) ([]*IVFPQSegment, error) {
	res := make([]*IVFPQSegment, 0, len(ids))
	for _, id := range ids {
		// already loaded
		if id > len(v.segments) || id < 0 {
			return nil, fmt.Errorf("invalid segment id: %d, current segments length is %d", id, len(v.segments))
		}
		seg := v.segments[id]
		if seg != nil {
			res = append(res, seg)
			continue
		}
		seg, err := openIVFPQSegment(v, id)
		if err != nil {
			return nil, err
		}
		v.segments[id] = seg
		res = append(res, seg)
	}
	return res, nil
}

// getCurSegment
// should be used with lock,
// otherwise other goroutine may has updated current seg id.
func (v *IvfPqIndex) getCurSegment() (*IVFPQSegment, error) {
	err := v.loadCurrentSegment()
	if err != nil {
		return nil, err
	}
	return v.segments[v.ref.CurrentSegmentId], nil
}

func (v *IvfPqIndex) Free() {
	v.lock.Lock()
	defer v.lock.Unlock()

	for _, seg := range v.segments {
		if seg != nil {
			seg.Free()
		}
	}
}

type segBatch struct {
	id         int
	addIds     []int64
	addVectors [][]float32
	deleteIds  []int64
}

func (v *IvfPqIndex) Batch(addVectors [][]float32, addIds, deleteIds []int64) error {
	err := v.processDel(deleteIds)
	if err != nil {
		return err
	}
	err = v.processAdd(addVectors, addIds)
	if err != nil {
		return err
	}

	// other goroutine may be creating a new segment, so we get RLock.
	v.lock.RLock()
	var total int64
	for _, segMeta := range v.ref.Segments {
		total += atomic.LoadInt64(&segMeta.Count)
	}
	atomic.StoreInt64(&v.ref.Count, total)
	v.lock.RUnlock()
	return nil
}

func (v *IvfPqIndex) processDel(deleteIds []int64) error {
	if len(deleteIds) == 0 {
		return nil
	}
	// lock and copy v.segments.
	// a new segment may be being added into v.segments by other goroutine,
	// but there will never be deletion happened in new segment for the current request,
	// so it's ok to use the replica of v.segments
	v.lock.Lock()
	segs, err := v.copySegments()
	v.lock.Unlock()
	if err != nil {
		return err
	}
	var delBatches map[int]*segBatch
	delBatches, err = getDelBatches(segs, deleteIds)
	if err != nil {
		return err
	}
	for _, batch := range delBatches {
		seg := segs[batch.id]
		err := seg.Batch(batch)
		if err != nil {
			return err
		}
	}
	return nil
}

func getDelBatches(segs []*IVFPQSegment, delIds []int64) (delBatch map[int]*segBatch, err error) {
	delBatch = make(map[int]*segBatch)
	for segId, seg := range segs {
		var ids []int64
		ids, err = seg.GetExistsIds(delIds)
		if err != nil {
			return
		}
		if len(ids) > 0 {
			delBatch[segId] = &segBatch{
				deleteIds: ids,
				id:        segId,
			}
		}
	}
	return
}

func (v *IvfPqIndex) processAdd(addVectors [][]float32, addIds []int64) error {
	// process add vectors with lock to avoid the current segment being updated concurrently.
	v.lock.Lock()
	defer v.lock.Unlock()

	curSeg, err := v.getCurSegment()
	if err != nil {
		return err
	}
	err = curSeg.Batch(&segBatch{
		addIds:     addIds,
		addVectors: addVectors,
	})
	if err != nil {
		return err
	}
	err = v.checkNewSeg(curSeg)
	if err != nil {
		return err
	}
	return nil
}

// checkNewSeg must be called with Lock
func (v *IvfPqIndex) checkNewSeg(curSeg *IVFPQSegment) error {
	if atomic.LoadInt64(&curSeg.ref.Count) >= config.Global.VectorConfig.IvfPqThreshold {
		// make new segment as current segment
		v.segments = append(v.segments, nil)
		v.ref.CurrentSegmentId++
		v.ref.Segments = append(v.ref.Segments, &meta.VectorSegment{Id: v.ref.CurrentSegmentId, Status: vector.StatusGrowing, Count: 0})
	}
	return nil
}

// Search query vector index,
// return a map docId->distance
// should reorder by distance
func (v *IvfPqIndex) Search(vec []float32, k int64, nprobe int) (map[string]float32, error) {
	v.lock.Lock()
	segs, err := v.copySegments()
	v.lock.Unlock()
	if err != nil {
		return nil, err
	}
	return searchSegments(vec, segs, k, nprobe)
}

func (v *IvfPqIndex) SearchByIDs(vec []float32, k int64, docIDs []string) ([]DocDistance, error) {
	v.lock.Lock()
	segs, err := v.copySegments()
	v.lock.Unlock()
	if err != nil {
		return nil, err
	}
	var distances []DocDistance
	for _, seg := range segs {
		result, err := seg.SearchByIDs(vec, k, docIDs)
		if err != nil {
			return nil, err
		}
		distances = append(distances, result...)
	}
	slices.SortFunc(distances, func(a, b DocDistance) int {
		return cmp.Compare(a.Distance, b.Distance)
	})
	if len(distances) > int(k) {
		distances = distances[:int(k)]
	}
	return distances, nil
}

func (v *IvfPqIndex) PartialSearch(vec []float32, k int64, nprobe int, segments []int) (map[string]float32, error) {
	v.lock.Lock()
	segs, err := v.copySegmentsWithIds(segments)
	v.lock.Unlock()
	if err != nil {
		return nil, err
	}

	return searchSegments(vec, segs, k, nprobe)
}

func searchSegments(vec []float32, segs []*IVFPQSegment, k int64, nprobe int) (map[string]float32, error) {
	searchHeap := &vecSearchHeap{}
	heap.Init(searchHeap)

	heapWait := sync.WaitGroup{}
	heapWait.Add(1)
	docCh := make(chan vecSearchRes, 10)
	go func() {
		defer heapWait.Done()
		for doc := range docCh {
			heap.Push(searchHeap, doc)
		}
	}()

	searchEg := errgroup.Group{}
	searchEg.SetLimit(config.Global.Shard.GoroutineNum)
	for _, seg := range segs {
		seg := seg
		searchEg.Go(func() error {
			mp, err := seg.Search(vec, k, nprobe)

			if err != nil {
				return err
			}
			for id, dis := range mp {
				docCh <- vecSearchRes{id: id, dis: dis}
			}
			return nil
		})
	}

	err := searchEg.Wait()
	close(docCh)
	heapWait.Wait()
	if err != nil {
		return nil, err
	}

	result := make(map[string]float32)
	size := k
	if searchHeap.Len() <= int(k) {
		size = int64(searchHeap.Len())
	}
	for i := int64(0); i < size; i++ {
		res := heap.Pop(searchHeap).(vecSearchRes)
		result[res.id] = res.dis
	}
	return result, nil
}

func (v *IvfPqIndex) ListSegment() []IndexSegment {
	v.lock.RLock()
	defer v.lock.RUnlock()

	res := make([]IndexSegment, 0, len(v.segments))
	for _, seg := range v.segments {
		if seg != nil {
			res = append(res, seg)
		}
	}
	return res
}

type vecSearchHeap struct {
	docs []vecSearchRes
}

type vecSearchRes struct {
	id  string
	dis float32
}

func (v *vecSearchHeap) Len() int {
	return len(v.docs)
}

func (v *vecSearchHeap) Less(i, j int) bool {
	return v.docs[i].dis < v.docs[j].dis
}

func (v *vecSearchHeap) Swap(i, j int) {
	v.docs[i], v.docs[j] = v.docs[j], v.docs[i]
}

func (v *vecSearchHeap) Push(x any) {
	v.docs = append(v.docs, x.(vecSearchRes))
}

func (v *vecSearchHeap) Pop() any {
	n := len(v.docs)
	doc := v.docs[n-1]
	v.docs = v.docs[:n-1]
	return doc
}

func (v *IvfPqIndex) SealSeg() error {
	var needSealSegIds []int
	v.lock.RLock()
	for _, segMeta := range v.ref.Segments {
		if segMeta.Status == vector.StatusGrowing &&
			segMeta.Id != v.ref.CurrentSegmentId &&
			atomic.LoadInt64(&segMeta.Count) >= config.Global.VectorConfig.IvfPqThreshold {
			needSealSegIds = append(needSealSegIds, segMeta.Id)
		}
	}
	v.lock.RUnlock()

	// nothing to process
	if len(needSealSegIds) == 0 {
		return nil
	}
	v.lock.Lock()
	segs, err := v.loadSegments(needSealSegIds)
	v.lock.Unlock()
	if err != nil {
		return err
	}
	for _, seg := range segs {
		err = seg.Seal()
		if err != nil {
			return fmt.Errorf("seal segemnt %d error: %w", seg.ref.Id, err)
		}
		err = v.zincIndex.SaveVecIndexMeta(v.field, v.ref)
		if err != nil {
			return fmt.Errorf("seal segemnt %d error: %w", seg.ref.Id, err)
		}
	}
	return nil
}

func (v *IvfPqIndex) BuildHNSW(ctx context.Context) error {
	log.Warn().Msgf("BuildHNSW should not be called on ivfpq index %s", v.name)
	return nil
}

func (v *IvfPqIndex) Recall(count int, k int64, nprobe int) (float32, error) {
	v.lock.Lock()
	segs, err := v.copySegments()
	v.lock.Unlock()
	if err != nil {
		return 0, err
	}
	total := float32(0)
	for _, seg := range segs {
		val, err := seg.Recall(count, k, nprobe)
		if err != nil {
			return 0, fmt.Errorf("recall segemnt %d error: %w", seg.ref.Id, err)
		}
		total += val
	}
	return total / float32(len(segs)), nil
}

type HNSWIndex struct {
	baseIndex

	segment *HNSWSegment
}

func (index *HNSWIndex) Free() {
	index.segment.Close()
}

func (index *HNSWIndex) Batch(vectors [][]float32, addIds, deleteIds []int64) error {
	count, err := index.segment.Batch(addIds, vectors, deleteIds)
	if err != nil {
		return fmt.Errorf("failed to batch segment: %w", err)
	}
	atomic.AddInt64(&index.ref.Count, int64(count))
	err = index.zincIndex.SaveVecIndexMeta(index.field, index.ref)
	if err != nil {
		return fmt.Errorf("failed to save vec index meta: %w", err)
	}
	if index.segment.NeedRebuildHNSW() {
		BuildHNSWIndex(index.zincIndex.GetName(), index.field)
	}
	return nil
}

func (index *HNSWIndex) BuildHNSW(ctx context.Context) error {
	if !index.segment.NeedRebuildHNSW() {
		return nil
	}
	err := index.segment.BuildHNSW(ctx)
	if err != nil {
		return fmt.Errorf("failed to build hnsw: %w", err)
	}
	return nil
}

func (index *HNSWIndex) Search(vec []float32, k int64, nprobe int) (map[string]float32, error) {
	ids, distances, err := index.segment.Search(vec, k)
	if err != nil {
		return nil, fmt.Errorf("failed to search segment: %w", err)
	}
	hash := make(map[string]float32)
	for i, id := range ids {
		hash[base62.Encode(id)] = distances[i]
	}
	return hash, nil
}

func (index *HNSWIndex) SearchByIDs(vec []float32, k int64, docIDs []string) ([]DocDistance, error) {
	ids := make([]int64, 0, len(docIDs))
	for _, docID := range docIDs {
		ids = append(ids, base62.Decode(docID))
	}

	matchedIDs, distances, err := index.segment.SearchByIDs(vec, k, ids)
	if err != nil {
		return nil, fmt.Errorf("failed to search segment by ids: %w", err)
	}

	result := make([]DocDistance, 0, len(matchedIDs))
	for i, id := range matchedIDs {
		result = append(result, DocDistance{
			DocID:    base62.Encode(id),
			Distance: distances[i],
		})
	}

	return result, nil
}

func (index *HNSWIndex) PartialSearch(vec []float32, k int64, nprobe int, segments []int) (map[string]float32, error) {
	return nil, fmt.Errorf("partial search is not supported for hnsw index")
}

func (index *HNSWIndex) Recall(count int, k int64, nprobe int) (float32, error) {
	return 0, fmt.Errorf("recall is not supported for hnsw index")
}

func (index *HNSWIndex) ListSegment() []IndexSegment {
	return nil
}

func (index *HNSWIndex) SealSeg() error {
	return fmt.Errorf("SealSeg should not be called on hnsw index %s", index.name)
}

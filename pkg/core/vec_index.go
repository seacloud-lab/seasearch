package core

import (
	"container/heap"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"golang.org/x/sync/errgroup"
)

type VectorIndex interface {
	Batch(addVectors [][]float32, addIds, deleteIds []int64) error
	Search(vec []float32, k int64, nprobe int) (map[string]float32, error)
	Name() string
	Meta() *meta.VecIndex
	SealedSeg() error
	AddRef()
	ReduceRef()
	RefCount() int
	ATime() time.Time
	Free()
	Recall(count int, k int64, nprobe int) (float32, error)
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
		segments: make(map[int]*VecSegment),
	}
	return ivfPqIndex, nil
}

// FlatIndex
// the vectors should have been saved into bluge and
// flat index doesn't save anything
type FlatIndex struct {
	baseIndex
	seg *VecSegment
}

func (f *FlatIndex) loadSegment() error {
	if f.seg != nil {
		return nil
	}
	var err error
	// only one segment with id 0
	f.seg, err = openSegment(f, 0)
	if err != nil {
		return err
	}
	return nil
}

func (f *FlatIndex) Batch(addVectors [][]float32, addIds, deleteIds []int64) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	err := f.loadSegment()
	if err != nil {
		return err
	}
	if len(deleteIds) > 0 {
		err := f.seg.RemoveIDs(deleteIds)
		if err != nil {
			return err
		}
	}
	if len(addVectors) > 0 && len(addIds) > 0 {
		f.seg.Lock()
		err := f.seg.AddVectors(addVectors, addIds)
		f.seg.Unlock()
		if err != nil {
			return err
		}
	}

	err = f.seg.Save()
	if err != nil {
		return err
	}
	f.ref.Count = f.seg.ref.Count
	return f.zincIndex.SaveVecIndexMeta(f.field, f.ref)
}

func (f *FlatIndex) Free() {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.seg != nil {
		f.seg.Free()
	}
}

func (f *FlatIndex) SealedSeg() error {
	// Flat index should never be called SealedSeg
	log.Warn().Msgf("SealedSeg should not be called on flat index %s.", f.name)
	return nil
}

// Search
// flat index get all vectors from bluge, and use faiss to get top k
func (f *FlatIndex) Search(vec []float32, k int64, _ int) (map[string]float32, error) {
	f.lock.Lock()
	err := f.loadSegment()
	f.lock.Unlock()
	if err != nil {
		return nil, err
	}

	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.seg.Search(vec, k, 0)
}

func (f *FlatIndex) Recall(count int, k int64, nprobe int) (float32, error) {
	log.Warn().Msgf("Recall should not be called on flat index %s.", f.name)
	return 1, nil
}

// IvfPqIndex
// wrap of faiss ivf_pq
type IvfPqIndex struct {
	baseIndex
	segments map[int]*VecSegment
}

func (v *IvfPqIndex) loadCurrentSegment() error {
	if _, ok := v.segments[v.ref.CurrentSegmentId]; ok {
		return nil
	}
	seg, err := openSegment(v, v.ref.CurrentSegmentId)
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
	return v.loadSegments(ids)
}

func (v *IvfPqIndex) loadSegments(ids []int) error {
	for _, id := range ids {
		// already loaded
		if _, ok := v.segments[id]; ok {
			continue
		}
		seg, err := openSegment(v, id)
		if err != nil {
			return err
		}
		v.segments[id] = seg
	}
	return nil
}

func (v *IvfPqIndex) getCurSegment() (*VecSegment, error) {
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
		seg.Lock()
		seg.Free()
		seg.Unlock()
	}
}

type segBatch struct {
	id         int
	addIds     []int64
	addVectors [][]float32
	deleteIds  []int64
}

func (v *IvfPqIndex) Batch(addVectors [][]float32, addIds, deleteIds []int64) error {
	v.lock.Lock()
	defer v.lock.Unlock()

	batches, err := v.getSegBatch(addVectors, addIds, deleteIds)
	if err != nil {
		return err
	}
	for _, batch := range batches {
		err = v.processSegmentBatch(batch)
		if err != nil {
			return err
		}
	}
	err = v.checkNewSeg()
	if err != nil {
		return err
	}

	var total int64
	for _, segMeta := range v.ref.Segments {
		total += segMeta.Count
	}
	v.ref.Count = total
	return v.zincIndex.SaveVecIndexMeta(v.field, v.ref)
}

func (v *IvfPqIndex) processSegmentBatch(batch *segBatch) error {
	seg := v.segments[batch.id]
	seg.Lock()
	defer seg.Unlock()
	if len(batch.deleteIds) > 0 {
		err := seg.RemoveIDs(batch.deleteIds)
		if err != nil {
			return err
		}
	}
	if len(batch.addVectors) > 0 {

		err := seg.AddVectors(batch.addVectors, batch.addIds)
		if err != nil {
			return err
		}
	}
	return seg.Save()
}

func (v *IvfPqIndex) getSegBatch(addVectors [][]float32, addIds, delIds []int64) (map[int]*segBatch, error) {
	batches := make(map[int]*segBatch)
	if len(delIds) > 0 {
		err := v.loadAllSegments()
		if err != nil {
			return nil, err
		}
		for segId, seg := range v.segments {
			ids, err := seg.getExistsIds(delIds)
			if err != nil {
				return nil, err
			}
			if len(ids) > 0 {
				batches[segId] = &segBatch{
					deleteIds: ids,
					id:        segId,
				}
			}
		}
	} else {
		// we load current seg
		err := v.loadCurrentSegment()
		if err != nil {
			return nil, err
		}
	}

	if len(addVectors) > 0 && len(addIds) > 0 {
		if seg, ok := batches[v.ref.CurrentSegmentId]; ok {
			seg.addVectors = addVectors
			seg.addIds = addIds
		} else {
			batches[v.ref.CurrentSegmentId] = &segBatch{
				addIds:     addIds,
				addVectors: addVectors,
				id:         v.ref.CurrentSegmentId,
			}
		}
	}
	return batches, nil
}

func (v *IvfPqIndex) checkNewSeg() error {
	seg, err := v.getCurSegment()
	if err != nil {
		return err
	}
	if atomic.LoadInt64(&seg.ref.Count) >= config.Global.VectorConfig.IvfPqThreshold {
		// make new segment as current segment
		v.ref.CurrentSegmentId++
		v.ref.Segments = append(v.ref.Segments, &meta.VectorSegment{Id: v.ref.CurrentSegmentId, Status: vector.StatusGrowing, Count: 0})
		err := v.zincIndex.SaveVecIndexMeta(v.field, v.ref)
		if err != nil {
			return err
		}
	}
	return nil
}

// Search query vector index,
// return a map docId->distance
// should reorder by distance
func (v *IvfPqIndex) Search(vec []float32, k int64, nprobe int) (map[string]float32, error) {
	v.lock.Lock()
	err := v.loadAllSegments()
	v.lock.Unlock()

	if err != nil {
		return nil, err
	}

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

	v.lock.RLock()
	defer v.lock.RUnlock()
	searchEg := errgroup.Group{}
	searchEg.SetLimit(config.Global.Shard.GoroutineNum)
	for _, seg := range v.segments {
		seg := seg
		searchEg.Go(func() error {
			seg.RLock()
			mp, err := seg.Search(vec, k, nprobe)
			seg.RUnlock()

			if err != nil {
				return err
			}
			for id, dis := range mp {
				docCh <- vecSearchRes{id: id, dis: dis}
			}
			return nil
		})
	}

	err = searchEg.Wait()
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
		res := searchHeap.Pop().(vecSearchRes)
		result[res.id] = res.dis
	}
	return result, nil
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

func (v *IvfPqIndex) SealedSeg() error {
	var needSealedSegIds []int
	v.lock.RLock()
	for _, segMeta := range v.ref.Segments {
		if segMeta.Status == vector.StatusGrowing &&
			segMeta.Id != v.ref.CurrentSegmentId &&
			segMeta.Count >= config.Global.VectorConfig.IvfPqThreshold {
			needSealedSegIds = append(needSealedSegIds, segMeta.Id)
		}
	}
	v.lock.RUnlock()

	// nothing to process
	if len(needSealedSegIds) == 0 {
		return nil
	}
	v.lock.Lock()
	err := v.loadSegments(needSealedSegIds)
	v.lock.Unlock()
	if err != nil {
		return err
	}
	for _, id := range needSealedSegIds {
		seg := v.segments[id]
		seg.Lock()
		err = seg.Sealed()
		seg.Unlock()

		if err != nil {
			return fmt.Errorf("sealed segemnt %d error: %w", id, err)
		}
		err = v.zincIndex.SaveVecIndexMeta(v.field, v.ref)
		if err != nil {
			return fmt.Errorf("sealed segemnt %d error: %w", id, err)
		}
	}
	return nil
}

func (v *IvfPqIndex) Recall(count int, k int64, nprobe int) (float32, error) {
	v.lock.Lock()
	err := v.loadAllSegments()
	v.lock.Unlock()
	if err != nil {
		return 0, err
	}
	v.lock.RLock()
	defer v.lock.RUnlock()

	total := float32(0)

	for _, seg := range v.segments {
		seg.RLock()
		val, err := seg.Recall(count, k, nprobe)
		seg.RUnlock()
		if err != nil {
			return 0, fmt.Errorf("recall segemnt %d error: %w", seg.ref.Id, err)
		}
		total += val
	}
	return total / float32(len(v.ref.Segments)), nil
}

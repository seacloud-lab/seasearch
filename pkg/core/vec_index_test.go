package core

import (
	"container/heap"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
)

func makeIvfPqForTest(t *testing.T) *IvfPqIndex {
	testIvfPqIndex := &Index{
		ref: &meta.Index{
			Name: testIdxName,
			VecIndexes: map[string]*meta.VecIndex{
				testFieldName: {
					TargetType:       vector.IvfPQ,
					NBits:            4,
					Dims:             4,
					M:                2,
					Count:            0,
					CurrentSegmentId: 0,
					Segments:         []*meta.VectorSegment{{Id: 0, Count: 0, Status: vector.StatusGrowing}},
				},
			},
		},
	}
	index, err := MakeVecIndex(testIvfPqIndex, testFieldName, testIvfPqIndex.ref.VecIndexes[testFieldName])

	assert.Nil(t, err)
	idx := index.(*IvfPqIndex)
	return idx
}

func makeFlatForTest(t *testing.T) *FlatIndex {
	testFlatIndex := &Index{
		ref: &meta.Index{
			Name: testIdxName,
			VecIndexes: map[string]*meta.VecIndex{
				testFieldName: {
					TargetType:       vector.Flat,
					Dims:             4,
					Count:            0,
					CurrentSegmentId: 0,
					Segments:         []*meta.VectorSegment{{Id: 0, Count: 0, Status: vector.StatusGrowing}},
				},
			},
		},
	}
	index, err := MakeVecIndex(testFlatIndex, testFieldName, testFlatIndex.ref.VecIndexes[testFieldName])

	assert.Nil(t, err)
	idx := index.(*FlatIndex)
	return idx
}

const testIdxName = "testIdx"
const testFieldName = "vec"

func TestBrandNewFlat(t *testing.T) {
	defer clean()
	idx := makeFlatForTest(t)

	err := idx.loadSegment()
	assert.Nil(t, err)
	assert.NotNil(t, idx.seg)
	info, err := os.Stat(path.Join(config.Global.DataPath, vector.VecPrefix, idx.name, fmt.Sprintf("%012x", 0), "stored_vec"))
	assert.Nil(t, err)
	assert.True(t, info.IsDir())

}

func clean() {
	// clear
	_ = os.RemoveAll(path.Join(config.Global.DataPath, vector.VecPrefix))
	_ = metadata.Index.Delete(testIdxName)
}

func TestAddFlatVectors(t *testing.T) {
	defer clean()
	idx := makeFlatForTest(t)

	ids, xq := createTestVecs(4, 10)
	err := idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	assert.EqualValues(t, 10, idx.ref.Count)
	assert.EqualValues(t, 10, idx.ref.Segments[0].Count)
}

func createTestVecs(d, count int) ([]int64, [][]float32) {
	xb := make([][]float32, count)
	ids := make([]int64, count)
	for i := 0; i < count; i++ {
		v := make([]float32, d)
		for j := 0; j < d; j++ {
			v[j] = rand.Float32()
		}
		xb[i] = v
		ids[i] = int64(i + 1)
	}
	return ids, xb
}

func TestAddAndRemoveFlatVectors(t *testing.T) {
	defer clean()

	idx := makeFlatForTest(t)

	ids, xq := createTestVecs(4, 10)
	err := idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	err = idx.Batch(nil, nil, []int64{1, 2, 3})
	assert.Nil(t, err)

	_, nowIds, err := idx.seg.getAllVectors()
	assert.Nil(t, err)
	assert.EqualValues(t, 7, len(nowIds))
	assert.EqualValues(t, 7, idx.ref.Count)
	assert.EqualValues(t, 7, idx.seg.ref.Count)

	assert.NotContains(t, nowIds, 1)
	assert.NotContains(t, nowIds, 2)
	assert.NotContains(t, nowIds, 3)

}

func TestSearchFlatVectors(t *testing.T) {
	defer clean()
	idx := makeFlatForTest(t)

	xq := make([][]float32, 4)
	xq[0] = []float32{0.1, 0.2, 0.3, 0.4}
	xq[1] = []float32{0.1, 0.3, 0.2, 0.3}
	xq[2] = []float32{0.1, 0.2, 0.6, 0.4}
	xq[3] = []float32{0.1, 0.2, 0.3, 0.9}
	ids := []int64{1, 2, 3, 4}
	err := idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	q := []float32{0.1, 0.1, 0.2, 0.3}
	res, err := idx.Search(q, 2, 0)
	assert.Nil(t, err)
	assert.EqualValues(t, 2, len(res))

	assert.Contains(t, res, base62.Encode(1))
	assert.Contains(t, res, base62.Encode(2))

	d1, _ := L2Distance(xq[0], q)
	assert.EqualValues(t, d1, res[base62.Encode(1)])

	d2, _ := L2Distance(xq[1], q)
	assert.EqualValues(t, d2, res[base62.Encode(2)])

}

func TestFreeFlat(t *testing.T) {
	defer clean()
	idx := makeFlatForTest(t)

	ids, xq := createTestVecs(4, 10)
	err := idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	q := []float32{0.1, 0.1, 0.2, 0.3}
	res, err := idx.Search(q, 2, 0)
	assert.Nil(t, err)
	assert.EqualValues(t, 2, len(res))

	assert.EqualValues(t, 10, len(idx.seg.ids))
	assert.EqualValues(t, 10*4, len(idx.seg.cachedVectorsCache))

	idx.Free()
	assert.Nil(t, idx.seg.ids)
	assert.Nil(t, idx.seg.cachedVectorsCache)
}

func TestBandNewIvfPq(t *testing.T) {
	defer clean()

	idx := makeIvfPqForTest(t)
	err := idx.loadAllSegments()
	assert.Nil(t, err)
	assert.EqualValues(t, 1, len(idx.segments))
	info, err := os.Stat(path.Join(config.Global.DataPath, vector.VecPrefix, idx.name, fmt.Sprintf("%012x", 0), "stored_vec"))
	assert.Nil(t, err)
	assert.True(t, info.IsDir())

}

func TestIvfPqAddAndRemove(t *testing.T) {
	defer func() {
		clean()
	}()
	idx := makeIvfPqForTest(t)

	ids, xq := createTestVecs(4, 10)
	err := idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	err = idx.Batch(nil, nil, []int64{1, 2, 3})
	assert.Nil(t, err)

	curSeg, err := idx.getCurSegment()
	assert.Nil(t, err)
	assert.EqualValues(t, 0, curSeg.ref.Id)
	assert.EqualValues(t, 1, len(idx.ref.Segments))
	assert.EqualValues(t, 0, idx.ref.CurrentSegmentId)

	_, nowIds, err := curSeg.getAllVectors()
	assert.Nil(t, err)
	assert.EqualValues(t, 7, len(nowIds))
	assert.EqualValues(t, 7, idx.ref.Count)
	assert.EqualValues(t, 7, curSeg.ref.Count)

	assert.NotContains(t, nowIds, 1)
	assert.NotContains(t, nowIds, 2)
	assert.NotContains(t, nowIds, 3)

}

func TestSealedIvfPq(t *testing.T) {
	defer clean()
	// we just use a test value
	config.Global.VectorConfig.IvfPqThreshold = 5000
	defer func() {
		config.Global.VectorConfig.IvfPqThreshold = 100000
	}()
	vecObjStore, err := vector.GetVectorStorage()
	assert.Nil(t, err)
	vecIdxManager = &VecIndexManager{
		storage: vecObjStore,
	}
	vecIdxManager.tmpDir = t.TempDir()
	defer func() {
		_ = os.RemoveAll(vecIdxManager.tmpDir)
	}()

	idx := makeIvfPqForTest(t)

	ids, xq := createTestVecs(4, 5100)
	err = idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	assert.EqualValues(t, 2, len(idx.ref.Segments))
	assert.EqualValues(t, 5100, idx.ref.Count)
	assert.EqualValues(t, 1, idx.ref.CurrentSegmentId)

	assert.EqualValues(t, 5100, idx.ref.Segments[0].Count)
	assert.EqualValues(t, 0, idx.ref.Segments[1].Count)
	assert.EqualValues(t, vector.StatusGrowing, idx.ref.Segments[0].Status)

	err = idx.SealedSeg()
	assert.Nil(t, err)

	assert.EqualValues(t, vector.StatusSealed, idx.ref.Segments[0].Status)
	assert.EqualValues(t, vector.StatusSealed, idx.segments[0].ref.Status)

	assert.EqualValues(t, 5100, idx.segments[0].ref.Count)

	assert.NotNil(t, idx.segments[0].index)
	assert.EqualValues(t, 5100, idx.segments[0].index.Ntotal())

	// check faiss file
	fileInfo, err := os.Stat(path.Join(config.Global.DataPath, vector.VecPrefix, idx.segments[0].getFaissVecStorePath(), "index.index"))
	assert.Nil(t, err)
	assert.False(t, fileInfo.IsDir())

	assert.Nil(t, idx.segments[0].ids)
	assert.Nil(t, idx.segments[0].cachedVectorsCache)
}

func TestAddAndRemoveWithSealedIvfPq(t *testing.T) {
	defer clean()
	// we just use a test value
	config.Global.VectorConfig.IvfPqThreshold = 5000
	defer func() {
		config.Global.VectorConfig.IvfPqThreshold = 100000
	}()
	vecObjStore, err := vector.GetVectorStorage()
	assert.Nil(t, err)
	vecIdxManager = &VecIndexManager{
		storage: vecObjStore,
	}
	vecIdxManager.tmpDir = t.TempDir()
	defer func() {
		_ = os.RemoveAll(vecIdxManager.tmpDir)
	}()

	idx := makeIvfPqForTest(t)
	ids, xq := createTestVecs(4, 5100)
	err = idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	err = idx.SealedSeg()
	assert.Nil(t, err)

	ids, xq = createTestVecs(4, 100)

	// make sure there is no repeat id
	for i := range ids {
		ids[i] = int64(5100 + i)
	}
	err = idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	assert.EqualValues(t, 100, idx.segments[1].ref.Count)
	assert.EqualValues(t, 100, idx.ref.Segments[1].Count)

	assert.EqualValues(t, 5100, idx.ref.Segments[0].Count)
	assert.EqualValues(t, 5100, idx.segments[0].ref.Count)

	assert.EqualValues(t, 5200, idx.ref.Count)

	err = idx.Batch(nil, nil, []int64{1, 2, 3, 5150, 5151})
	assert.Nil(t, err)

	assert.EqualValues(t, 5097, idx.segments[0].ref.Count)
	assert.EqualValues(t, 5097, idx.ref.Segments[0].Count)

	assert.EqualValues(t, 98, idx.segments[1].ref.Count)
	assert.EqualValues(t, 98, idx.ref.Segments[1].Count)

	reader1, err := idx.segments[1].vecStoreWriter.Reader()
	assert.Nil(t, err)
	seg1Count, err := reader1.Count()
	assert.Nil(t, err)
	assert.EqualValues(t, 98, seg1Count)
	assert.Nil(t, idx.segments[1].index)

	reader0, err := idx.segments[0].vecStoreWriter.Reader()
	assert.Nil(t, err)
	seg0Count, err := reader0.Count()
	assert.Nil(t, err)
	assert.EqualValues(t, 5097, seg0Count)
	assert.NotNil(t, idx.segments[0].index)
	assert.EqualValues(t, 5097, idx.segments[0].index.Ntotal())

	assert.EqualValues(t, 5195, idx.ref.Count)
}

func TestSearchIvfPq(t *testing.T) {
	defer clean()
	// we just use a test value
	config.Global.VectorConfig.IvfPqThreshold = 5000
	defer func() {
		config.Global.VectorConfig.IvfPqThreshold = 100000
	}()
	vecObjStore, err := vector.GetVectorStorage()
	assert.Nil(t, err)
	vecIdxManager = &VecIndexManager{
		storage: vecObjStore,
	}
	vecIdxManager.tmpDir = t.TempDir()
	defer func() {
		_ = os.RemoveAll(vecIdxManager.tmpDir)
	}()

	idx := makeIvfPqForTest(t)

	ids, xq := createTestVecs(4, 5100)
	err = idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	err = idx.SealedSeg()
	assert.Nil(t, err)

	ids, xq = createTestVecs(4, 100)

	// make sure there is no repeat id
	for i := range ids {
		ids[i] = int64(5100 + i)
	}
	err = idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	result, err := idx.Search([]float32{0.9, 0.2, 0.5, 0.6}, 10, 10)
	assert.Nil(t, err)
	assert.EqualValues(t, 10, len(result))

	for id := range result {
		idInt := base62.Decode(id)
		assert.True(t, 1 <= idInt && idInt <= 5200)
	}
}

func TestFreeIvfPq(t *testing.T) {
	defer clean()
	// we just use a test value
	config.Global.VectorConfig.IvfPqThreshold = 5000
	defer func() {
		config.Global.VectorConfig.IvfPqThreshold = 100000
	}()
	vecObjStore, err := vector.GetVectorStorage()
	assert.Nil(t, err)
	vecIdxManager = &VecIndexManager{
		storage: vecObjStore,
	}
	vecIdxManager.tmpDir = t.TempDir()
	defer func() {
		_ = os.RemoveAll(vecIdxManager.tmpDir)
	}()

	idx := makeIvfPqForTest(t)

	ids, xq := createTestVecs(4, 5100)
	err = idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	err = idx.SealedSeg()
	assert.Nil(t, err)

	ids, xq = createTestVecs(4, 100)
	// make sure there is no repeat id
	for i := range ids {
		ids[i] = int64(5100 + i)
	}
	err = idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	result, err := idx.Search([]float32{0.9, 0.2, 0.5, 0.6}, 10, 10)
	assert.Nil(t, err)
	assert.EqualValues(t, 10, len(result))

	assert.EqualValues(t, 100, len(idx.segments[1].ids))
	assert.EqualValues(t, 100*4, len(idx.segments[1].cachedVectorsCache))

	assert.Nil(t, idx.segments[0].ids)
	assert.Nil(t, idx.segments[0].cachedVectorsCache)

	idx.Free()

	assert.Nil(t, idx.segments[1].ids)
	assert.Nil(t, idx.segments[1].cachedVectorsCache)

}

func TestReOpenIvfPq(t *testing.T) {
	defer clean()
	// we just use a test value
	config.Global.VectorConfig.IvfPqThreshold = 5000
	defer func() {
		config.Global.VectorConfig.IvfPqThreshold = 100000
	}()
	vecObjStore, err := vector.GetVectorStorage()
	assert.Nil(t, err)
	vecIdxManager = &VecIndexManager{
		storage: vecObjStore,
	}
	vecIdxManager.tmpDir = t.TempDir()
	defer func() {
		_ = os.RemoveAll(vecIdxManager.tmpDir)
	}()

	idx := makeIvfPqForTest(t)

	ids, xq := createTestVecs(4, 5100)
	err = idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	err = idx.SealedSeg()
	assert.Nil(t, err)

	ids, xq = createTestVecs(4, 100)

	// make sure there is no repeat id
	for i := range ids {
		ids[i] = int64(5100 + i)
	}
	err = idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	ref := idx.ref
	// close idx
	idx.Free()
	// reopen
	idx = makeIvfPqForTest(t)
	idx.ref = ref

	result, err := idx.Search([]float32{0.9, 0.2, 0.5, 0.6}, 10, 10)
	assert.Nil(t, err)
	assert.EqualValues(t, 10, len(result))

	for id := range result {
		idInt := base62.Decode(id)
		assert.True(t, 1 <= idInt && idInt <= 5200)
	}
}

func TestRecall(t *testing.T) {
	defer clean()
	// we just use a test value
	config.Global.VectorConfig.IvfPqThreshold = 5000
	defer func() {
		config.Global.VectorConfig.IvfPqThreshold = 100000
	}()
	vecObjStore, err := vector.GetVectorStorage()
	assert.Nil(t, err)
	vecIdxManager = &VecIndexManager{
		storage: vecObjStore,
	}
	vecIdxManager.tmpDir = t.TempDir()
	defer func() {
		_ = os.RemoveAll(vecIdxManager.tmpDir)
	}()

	idx := makeIvfPqForTest(t)
	ids, xq := createTestVecs(4, 5100)
	err = idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	err = idx.SealedSeg()
	assert.Nil(t, err)

	ids, xq = createTestVecs(4, 100)

	// make sure there is no repeat id
	for i := range ids {
		ids[i] = int64(5100 + i)
	}
	err = idx.Batch(xq, ids, nil)
	assert.Nil(t, err)

	recall, err := idx.Recall(100, 10, 10)
	assert.Nil(t, err)
	t.Log(recall)

	seg0Recall, err := idx.segments[0].Recall(100, 10, 10)
	assert.Nil(t, err)

	seg1Recall, err := idx.segments[1].Recall(100, 10, 10)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, seg1Recall)

	assert.True(t, seg0Recall < recall && seg0Recall < 1)
}

func TestSearchHeap(t *testing.T) {
	list := &vecSearchHeap{}
	heap.Init(list)

	heap.Push(list, vecSearchRes{"A", 0.6})
	heap.Push(list, vecSearchRes{"B", 0.3})
	heap.Push(list, vecSearchRes{"C", 0.1})
	heap.Push(list, vecSearchRes{"D", 0.4})

	assert.EqualValues(t, 4, list.Len())
	itme1 := heap.Pop(list).(vecSearchRes)
	assert.Equal(t, "C", itme1.id)
	itme2 := heap.Pop(list).(vecSearchRes)
	assert.Equal(t, "B", itme2.id)
	itme3 := heap.Pop(list).(vecSearchRes)
	assert.Equal(t, "D", itme3.id)
	itme4 := heap.Pop(list).(vecSearchRes)
	assert.Equal(t, "A", itme4.id)

}

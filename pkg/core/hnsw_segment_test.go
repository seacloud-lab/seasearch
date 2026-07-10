package core

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

func makeHNSWSegmentForTest(t *testing.T) *HNSWSegment {
	t.Helper()
	initVecIdxManagerForTest(t)
	store := path.Join(testIdxName, "hnsw", t.Name())
	return newHNSWSegment(store, 2)
}

func TestHNSWSegment_BatchAndSearchCacheOnly(t *testing.T) {
	defer clean()

	seg := makeHNSWSegmentForTest(t)
	defer seg.Close()

	originalMaxLogs := config.Global.VectorConfig.HNSWMaxLogs
	config.Global.VectorConfig.HNSWMaxLogs = 2
	defer func() {
		config.Global.VectorConfig.HNSWMaxLogs = originalMaxLogs
	}()

	count, err := seg.Batch(
		[]int64{1, 2},
		[][]float32{zutils.MinuteVector(1), zutils.MinuteVector(5)},
		nil,
	)
	assert.Nil(t, err)
	assert.Equal(t, 2, count)
	assert.False(t, seg.NeedRebuildHNSW())

	count, err = seg.Batch(
		[]int64{3},
		[][]float32{zutils.MinuteVector(3)},
		nil,
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, count)
	assert.True(t, seg.NeedRebuildHNSW())

	ids, distances, err := seg.Search(zutils.MinuteVector(0), 2)
	assert.Nil(t, err)
	assert.EqualValues(t, []int64{1, 3}, ids)
	assert.Len(t, distances, 2)
	assert.EqualValues(t, 0, distances[0])
	assert.Greater(t, distances[1], float32(0))
}

func TestHNSWSegment_BuildHNSWAndSearch(t *testing.T) {
	defer clean()

	seg := makeHNSWSegmentForTest(t)
	defer seg.Close()

	count, err := seg.Batch(
		[]int64{1, 2, 3},
		[][]float32{zutils.MinuteVector(1), zutils.MinuteVector(5), zutils.MinuteVector(3)},
		nil,
	)
	assert.Nil(t, err)
	assert.Equal(t, 3, count)

	err = seg.BuildHNSW(context.Background())
	assert.Nil(t, err)
	assert.NotNil(t, seg.index)
	assert.EqualValues(t, seg.latestLogID, seg.hnswLogID)
	assert.Len(t, seg.cache.docIDs, 0)
	assert.Len(t, seg.docTS, 0)

	ids, _, err := seg.Search(zutils.MinuteVector(0), 2)
	assert.Nil(t, err)
	assert.EqualValues(t, []int64{1, 3}, ids)
}

func TestHNSWSegment_SearchFiltersDeletedInHNSW(t *testing.T) {
	defer clean()

	seg := makeHNSWSegmentForTest(t)
	defer seg.Close()

	count, err := seg.Batch(
		[]int64{1, 2},
		[][]float32{zutils.MinuteVector(0), zutils.MinuteVector(1)},
		nil,
	)
	assert.Nil(t, err)
	assert.Equal(t, 2, count)

	err = seg.BuildHNSW(context.Background())
	assert.Nil(t, err)

	count, err = seg.Batch(nil, nil, []int64{1})
	assert.Nil(t, err)
	assert.Equal(t, -1, count)

	ids, _, err := seg.Search(zutils.MinuteVector(0), 2)
	assert.Nil(t, err)
	assert.NotContains(t, ids, int64(1))
	assert.Contains(t, ids, int64(2))
}

func TestHNSWSegment_SearchByIDs_MergesCacheAndDocs(t *testing.T) {
	defer clean()

	seg := makeHNSWSegmentForTest(t)
	defer seg.Close()

	count, err := seg.Batch(
		[]int64{1, 2},
		[][]float32{zutils.MinuteVector(10), zutils.MinuteVector(20)},
		nil,
	)
	assert.Nil(t, err)
	assert.Equal(t, 2, count)

	err = seg.BuildHNSW(context.Background())
	assert.Nil(t, err)

	count, err = seg.Batch(
		[]int64{3},
		[][]float32{zutils.MinuteVector(5)},
		nil,
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, count)

	ids, _, err := seg.SearchByIDs(zutils.MinuteVector(0), 2, []int64{1, 3})
	assert.Nil(t, err)
	assert.EqualValues(t, []int64{3, 1}, ids)
	assert.NotContains(t, ids, int64(2))
}

func TestHNSWSegment_ReloadPersistence(t *testing.T) {
	defer clean()

	seg := makeHNSWSegmentForTest(t)

	count, err := seg.Batch(
		[]int64{1, 2},
		[][]float32{zutils.MinuteVector(10), zutils.MinuteVector(20)},
		nil,
	)
	assert.Nil(t, err)
	assert.Equal(t, 2, count)

	err = seg.BuildHNSW(context.Background())
	assert.Nil(t, err)

	count, err = seg.Batch(
		[]int64{3},
		[][]float32{zutils.MinuteVector(5)},
		nil,
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, count)

	store := seg.store
	seg.Close()

	seg2 := newHNSWSegment(store, 2)
	defer seg2.Close()

	ids, _, err := seg2.Search(zutils.MinuteVector(0), 2)
	assert.Nil(t, err)
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, int64(3))
	assert.Condition(t, func() bool {
		for _, id := range ids {
			if id == 1 || id == 2 {
				return true
			}
		}
		return false
	})
}

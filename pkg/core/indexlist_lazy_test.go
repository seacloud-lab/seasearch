package core

import (
	"crypto/md5"
	"encoding/hex"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/metadata"
)

func persistUnloadedIndex(t *testing.T, name string) *Index {
	t.Helper()
	IndexMgr.DropCache(name)
	index, err := NewIndex(name)
	require.NoError(t, err)
	data, err := index.MarshalJSON()
	require.NoError(t, err)
	require.NoError(t, metadata.Index.Set(name, data))
	return index
}

func TestDropCacheRemovesCachedIndex(t *testing.T) {
	const name = "TestDropCacheRemovesCachedIndex.index"
	mgr := newIndexManager()
	index := &Index{}
	mgr.cache[name] = index

	dropped, ok := mgr.DropCache(name)
	require.True(t, ok)
	require.Same(t, index, dropped)
	assert.Empty(t, mgr.GetCached())

	dropped, ok = mgr.DropCache(name)
	assert.False(t, ok)
	assert.Nil(t, dropped)
}

func TestLoadIndexLoadsOnlyRequestedIndex(t *testing.T) {
	nameA := "TestLoadIndexLoadsOnlyRequestedIndex.a"
	nameB := "TestLoadIndexLoadsOnlyRequestedIndex.b"
	persistUnloadedIndex(t, nameA)
	persistUnloadedIndex(t, nameB)
	t.Cleanup(func() {
		_ = IndexMgr.Delete(nameA)
		_ = IndexMgr.Delete(nameB)
	})

	index, err := IndexMgr.Get(nameA)
	require.NoError(t, err)
	assert.Equal(t, nameA, index.GetName())
	_, errA := IndexMgr.Get(nameA)
	_, errB := IndexMgr.Get(nameB)
	assert.Nil(t, errA)
	assert.Nil(t, errB)
}

func TestConcurrentLoadIndexReturnsCanonicalObject(t *testing.T) {
	name := "TestConcurrentLoadIndexReturnsCanonicalObject.index"
	persistUnloadedIndex(t, name)
	t.Cleanup(func() { _ = IndexMgr.Delete(name) })

	const workers = 16
	indexes := make([]*Index, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range indexes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			indexes[i], errs[i] = IndexMgr.Get(name)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range indexes {
		require.NoError(t, errs[i])
		assert.Same(t, indexes[0], indexes[i])
	}
}

func TestGetOrCreateLoadsExistingMetadata(t *testing.T) {
	name := "TestGetOrCreateLoadsExistingMetadata.index"
	stored := persistUnloadedIndex(t, name)
	stored.ref.Version = "preserved-version"
	for _, shard := range stored.ref.Shards {
		shard.NodeID = "preserved-node"
	}
	data, err := stored.MarshalJSON()
	require.NoError(t, err)
	require.NoError(t, metadata.Index.Set(name, data))
	t.Cleanup(func() { _ = IndexMgr.Delete(name) })

	loaded, existed, err := IndexMgr.GetOrCreate(name)
	require.NoError(t, err)
	assert.True(t, existed)
	assert.Equal(t, "preserved-version", loaded.ref.Version)
	for _, shard := range loaded.ref.Shards {
		assert.Equal(t, "preserved-node", shard.NodeID)
	}

	require.NoError(t, IndexMgr.Store(loaded))
	readBack, err := metadata.Index.Get(name)
	require.NoError(t, err)
	assert.Equal(t, "preserved-version", readBack.Version)
	for _, shard := range readBack.Shards {
		assert.Equal(t, "preserved-node", shard.NodeID)
	}
}

func TestUpdateIndexListEvictsUnassignedIndex(t *testing.T) {
	name := "TestUpdateIndexListEvictsUnassignedIndex.index"
	persistUnloadedIndex(t, name)
	t.Cleanup(func() { _ = IndexMgr.Delete(name) })

	cachedIndex, err := IndexMgr.Get(name)
	require.NoError(t, err)

	sum := md5.Sum([]byte(name))
	partition := hex.EncodeToString(sum[:])[:2]
	require.NoError(t, updateIndexList(map[string]struct{}{partition: {}}))

	for _, index := range IndexMgr.GetCached() {
		assert.NotEqual(t, name, index.GetName())
	}
	_, err = metadata.Index.Get(name)
	require.NoError(t, err)

	reloadedIndex, err := IndexMgr.Get(name)
	require.NoError(t, err)
	assert.NotSame(t, cachedIndex, reloadedIndex)
}

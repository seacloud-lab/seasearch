package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
)

func persistUnloadedIndex(t *testing.T, name string) *Index {
	t.Helper()
	index, err := createIndex(&meta.IndexSimple{Name: name})
	require.NoError(t, err)
	data, err := index.MarshalJSON()
	require.NoError(t, err)
	require.NoError(t, metadata.Index.Set(name, data))
	return index
}

func TestLoadIndexLoadsOnlyRequestedIndex(t *testing.T) {
	nameA := "TestLoadIndexLoadsOnlyRequestedIndex.a"
	nameB := "TestLoadIndexLoadsOnlyRequestedIndex.b"
	persistUnloadedIndex(t, nameA)
	persistUnloadedIndex(t, nameB)
	t.Cleanup(func() {
		_ = Delete(nameA)
		_ = Delete(nameB)
	})

	index, err := Open(nameA)
	require.NoError(t, err)
	assert.Equal(t, nameA, index.GetName())
	index.Close()
	cache.mutex.Lock()
	assert.NotNil(t, cache.items[nameA])
	assert.Nil(t, cache.items[nameB])
	cache.mutex.Unlock()
}

func TestConcurrentLoadIndexReturnsCanonicalObject(t *testing.T) {
	name := "TestConcurrentLoadIndexReturnsCanonicalObject.index"
	persistUnloadedIndex(t, name)
	t.Cleanup(func() { _ = Delete(name) })

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
			indexes[i], errs[i] = Open(name)
		}(i)
	}
	close(start)
	wg.Wait()
	defer func() {
		for _, index := range indexes {
			if index != nil {
				index.Close()
			}
		}
	}()

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
	t.Cleanup(func() { _ = Delete(name) })

	loaded, existed, err := OpenOrCreate(name)
	require.NoError(t, err)
	defer loaded.Close()
	assert.True(t, existed)
	assert.Equal(t, "preserved-version", loaded.ref.Version)
	for _, shard := range loaded.ref.Shards {
		assert.Equal(t, "preserved-node", shard.NodeID)
	}

	loaded.dirty = true
	require.NoError(t, writeIndex(loaded))
	readBack, err := metadata.Index.Get(name)
	require.NoError(t, err)
	assert.Equal(t, "preserved-version", readBack.Version)
	for _, shard := range readBack.Shards {
		assert.Equal(t, "preserved-node", shard.NodeID)
	}
}

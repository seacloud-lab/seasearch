package core

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/metadata"
)

func persistUnloadedIndex(t *testing.T, name string) *Index {
	t.Helper()
	index, err := NewIndex(name)
	require.NoError(t, err)
	data, err := index.MarshalJSON()
	require.NoError(t, err)
	require.NoError(t, metadata.Index.Set(name, data))
	ZINC_INDEX_LIST.Delete(name)
	return index
}

func TestLoadIndexLoadsOnlyRequestedIndex(t *testing.T) {
	nameA := "TestLoadIndexLoadsOnlyRequestedIndex.a"
	nameB := "TestLoadIndexLoadsOnlyRequestedIndex.b"
	persistUnloadedIndex(t, nameA)
	persistUnloadedIndex(t, nameB)
	t.Cleanup(func() {
		_ = DeleteIndex(nameA)
		_ = DeleteIndex(nameB)
	})

	index, err := LoadIndex(nameA)
	require.NoError(t, err)
	assert.Equal(t, nameA, index.GetName())
	_, loadedA := GetResidentIndex(nameA)
	_, loadedB := GetResidentIndex(nameB)
	assert.True(t, loadedA)
	assert.False(t, loadedB)
}

func TestConcurrentLoadIndexReturnsCanonicalObject(t *testing.T) {
	name := "TestConcurrentLoadIndexReturnsCanonicalObject.index"
	persistUnloadedIndex(t, name)
	t.Cleanup(func() { _ = DeleteIndex(name) })

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
			indexes[i], errs[i] = LoadIndex(name)
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
	t.Cleanup(func() { _ = DeleteIndex(name) })

	loaded, existed, err := GetOrCreateIndex(name)
	require.NoError(t, err)
	assert.True(t, existed)
	assert.Equal(t, "preserved-version", loaded.ref.Version)
	for _, shard := range loaded.ref.Shards {
		assert.Equal(t, "preserved-node", shard.NodeID)
	}

	require.NoError(t, StoreIndex(loaded))
	readBack, err := metadata.Index.Get(name)
	require.NoError(t, err)
	assert.Equal(t, "preserved-version", readBack.Version)
	for _, shard := range readBack.Shards {
		assert.Equal(t, "preserved-node", shard.NodeID)
	}
}

func TestGCLeavesResidentIndexLoaded(t *testing.T) {
	name := "TestGCLeavesResidentIndexLoaded.index"
	index, _, err := GetOrCreateIndex(name)
	require.NoError(t, err)
	t.Cleanup(func() { _ = DeleteIndex(name) })
	index.atime = 0

	require.NoError(t, ZINC_INDEX_LIST.GC())
	resident, ok := GetResidentIndex(name)
	assert.True(t, ok)
	assert.Same(t, index, resident)
}

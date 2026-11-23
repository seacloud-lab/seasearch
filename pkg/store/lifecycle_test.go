package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

func TestCreatePreservesExistingConfiguration(t *testing.T) {
	name := t.Name()
	t.Cleanup(func() { require.NoError(t, Delete(name)) })

	index, err := Create(&meta.IndexSimple{
		Name: name,
		Mappings: map[string]any{
			"properties": map[string]any{
				"category": map[string]any{
					"type": "keyword",
				},
			},
		},
	})
	require.NoError(t, err)
	defer index.Close()

	// Rejected creation must neither return a reference nor replace the mapping.
	duplicate, err := Create(&meta.IndexSimple{Name: name})
	require.ErrorIs(t, err, ErrIndexAlreadyExists)
	require.Nil(t, duplicate)

	property, ok := index.GetMappings().GetProperty("category")
	require.True(t, ok)
	require.Equal(t, "keyword", property.Type)
}

func TestEvictionAndReload(t *testing.T) {
	name := t.Name()
	t.Cleanup(func() { require.NoError(t, Delete(name)) })

	index, err := Create(&meta.IndexSimple{
		Name: name,
		Settings: &meta.IndexSettings{
			Analysis: &meta.IndexAnalysis{
				Analyzer: map[string]*meta.Analyzer{
					"exact": {
						Type: "keyword",
					},
				},
			},
		},
		Mappings: map[string]any{
			"properties": map[string]any{
				"title": map[string]any{
					"type":     "text",
					"analyzer": "exact",
				},
			},
		},
	})
	require.NoError(t, err)

	// Track references explicitly so an assertion failure cannot leak them and
	// delay cleanup. No object is used after its reference is released.
	firstHeld := true
	defer func() {
		if firstHeld {
			index.Close()
		}
	}()
	require.NoError(t, index.CreateDocument("1", map[string]any{
		"title": "Mixed CASE",
	}, false))

	other, err := Open(name)
	require.NoError(t, err)

	otherHeld := true
	defer func() {
		if otherHeld {
			other.Close()
		}
	}()
	require.Same(t, index, other)

	// Even an eligible cutoff cannot evict a referenced instance.
	cutoff := time.Now().Add(time.Hour)
	require.NoError(t, cache.cleanIndex(name, cutoff))
	cache.mutex.Lock()
	present := cache.items[name] != nil
	cache.mutex.Unlock()
	require.True(t, present, "referenced instance must not be evicted")

	// After both references are released, the same candidate becomes evictable.
	index.Close()
	firstHeld = false
	other.Close()
	otherHeld = false

	require.NoError(t, cache.cleanIndex(name, cutoff))
	cache.mutex.Lock()
	present = cache.items[name] != nil
	cache.mutex.Unlock()
	require.False(t, present)

	// Reload must allocate a new runtime and preserve persisted data and analysis.
	reloaded, err := Open(name)
	require.NoError(t, err)
	defer reloaded.Close()
	require.NotSame(t, index, reloaded)

	hit, err := reloaded.GetDocument("1")
	require.NoError(t, err)
	require.Equal(t, "Mixed CASE", hit.Source.(map[string]any)["title"])
	require.EqualValues(t, 1, reloaded.GetStats().DocNum)

	analyzer, err := reloaded.Analyzer("title", "")
	require.NoError(t, err)
	tokens := analyzer.Analyze([]byte("Mixed CASE"))
	require.Len(t, tokens, 1)
	require.Equal(t, "Mixed CASE", string(tokens[0].Term))
}

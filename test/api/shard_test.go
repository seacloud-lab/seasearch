package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

func TestDocumentUpdateAfterShardExpansion(t *testing.T) {
	// Force expansion with a tiny fixture; restore the global limit after cleanup.
	original := config.Global.Shard.MaxSize
	config.Global.Shard.MaxSize = 1
	t.Cleanup(func() { config.Global.Shard.MaxSize = original })

	name := indexResource(t)
	putDocument(t, name, "1", map[string]any{
		"title": "old",
	})
	putDocument(t, name, "2", map[string]any{
		"title": "other",
	})

	// Confirm expansion actually happened; otherwise this could pass without
	// exercising the old-shard update path.
	var index meta.Index
	apiJSON(t, "GET", "/api/index/"+name, nil, 200, &index)
	require.NotEmpty(t, index.Shards)
	for _, shard := range index.Shards {
		require.Greater(t, shard.ShardNum, int64(1))
	}

	// Updating an old-shard document must not introduce a second copy.
	apiJSON(t, "POST", "/api/"+name+"/_update/1", map[string]any{
		"title": "new",
	}, 200, nil)
	require.Equal(t, "new", getDocument(t, name, "1").Source["title"])

	result := searchDocuments(t, name, allDocuments())
	require.Equal(t, 2, result.Hits.Total.Value)
	require.ElementsMatch(t, []string{name + "/1", name + "/2"}, hitKeys(result))

	apiJSON(t, "DELETE", "/api/"+name+"/_doc/1", nil, 200, nil)
	require.Equal(t, []string{name + "/2"}, hitKeys(searchDocuments(t, name, allDocuments())))
}

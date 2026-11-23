package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

func vectorIndexConfig(kind string) map[string]any {
	return map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"embedding": map[string]any{
					"type":           "vector",
					"dims":           4,
					"vec_index_type": kind,
				},
				"category": map[string]any{
					"type": "keyword",
				},
			},
		},
	}
}

func vectorQuery(k int) map[string]any {
	return map[string]any{
		"query_field": "embedding",
		"vector":      []float32{0, 0, 0, 0},
		"k":           k,
		"nprobe":      2,
	}
}

func searchVectors(t *testing.T, target string, query any) *meta.SearchResponse {
	t.Helper()

	var result meta.SearchResponse
	apiJSON(t, "POST", "/api/"+target+"/_search/vector", query, 200, &result)
	return &result
}

func TestVectorDocumentLifecycle(t *testing.T) {
	name := indexResource(t)
	createIndex(t, name, vectorIndexConfig("flat"))
	putDocument(t, name, "1", map[string]any{
		"title":     "nearest",
		"category":  "a",
		"embedding": []float32{0, 0, 0, 0},
	})
	putDocument(t, name, "2", map[string]any{
		"title":     "second",
		"category":  "b",
		"embedding": []float32{1, 0, 0, 0},
	})
	putDocument(t, name, "3", map[string]any{
		"title":     "third",
		"category":  "a",
		"embedding": []float32{2, 0, 0, 0},
	})

	// Squared L2 distances 0, 1 and 4 become scores 1, 1/2 and 1/5.
	query := vectorQuery(3)
	query["_source"], query["return_fields"] = []string{"title"}, []string{"category"}
	result := searchVectors(t, name, query)
	require.Equal(t, 3, result.Hits.Total.Value)
	require.Equal(t, []string{name + "/1", name + "/2", name + "/3"}, hitKeys(result))
	for i, score := range []float64{1, .5, .2} {
		hit := result.Hits.Hits[i]
		require.InDelta(t, score, hit.Score, 1e-6)
		require.Len(t, hit.Source, 1)
		require.Contains(t, hit.Source, "title")
		require.Contains(t, hit.Fields, "category")
	}
	require.Equal(t, "nearest", hitSource(t, result.Hits.Hits[0])["title"])

	// Text filtering must restrict the native vector candidate set.
	query["filter_query"] = map[string]any{
		"term": map[string]any{
			"category": "a",
		},
	}
	filtered := searchVectors(t, name, query)
	require.Equal(t, []string{name + "/1", name + "/3"}, hitKeys(filtered))
	delete(query, "filter_query")

	// Updating a vector must remove its previous position in the ranking.
	putDocument(t, name, "1", map[string]any{
		"title":     "moved",
		"category":  "a",
		"embedding": []float32{3, 0, 0, 0},
	})
	require.Equal(t, []string{name + "/2", name + "/3", name + "/1"}, hitKeys(searchVectors(t, name, query)))

	// Omitting a vector keeps its prior value, although text source is replaced.
	apiJSON(t, "POST", "/api/"+name+"/_update/2", map[string]any{
		"title":    "renamed",
		"category": "b",
	}, 200, nil)
	result = searchVectors(t, name, query)
	require.Equal(t, name+"/2", hitKeys(result)[0])
	require.Equal(t, "renamed", hitSource(t, result.Hits.Hits[0])["title"])
	require.InDelta(t, .5, result.Hits.Hits[0].Score, 1e-6)

	apiJSON(t, "DELETE", "/api/"+name+"/_doc/2", nil, 200, nil)
	require.Equal(t, []string{name + "/3", name + "/1"}, hitKeys(searchVectors(t, name, query)))

	// Query-based deletion must clean vector data as well as text documents.
	apiJSON(t, "POST", "/es/"+name+"/_delete_by_query", map[string]any{
		"query": map[string]any{
			"match": map[string]any{
				"title": "third",
			},
		},
	}, 200, nil)
	require.Equal(t, []string{name + "/1"}, hitKeys(searchVectors(t, name, query)))
	require.Equal(t, []string{name + "/1"}, hitKeys(searchDocuments(t, name, allDocuments())))
}

func TestMultiIndexVectorSearch(t *testing.T) {
	a, b := indexResource(t), indexResource(t)
	for _, name := range []string{a, b} {
		createIndex(t, name, vectorIndexConfig("flat"))
	}

	// ID alone is not a global identity: backfill must also use the index name.
	putDocument(t, a, "1", map[string]any{
		"title":     "alpha",
		"embedding": []float32{2, 0, 0, 0},
	})
	putDocument(t, b, "1", map[string]any{
		"title":     "beta",
		"embedding": []float32{0, 0, 0, 0},
	})
	putDocument(t, a, "2", map[string]any{
		"title":     "extra",
		"embedding": []float32{3, 0, 0, 0},
	})

	result := searchVectors(t, a+","+b, vectorQuery(2))
	require.Equal(t, 2, result.Hits.Total.Value)
	require.Equal(t, []string{b + "/1", a + "/1"}, hitKeys(result))
	require.Equal(t, "beta", hitSource(t, result.Hits.Hits[0])["title"])
	require.Equal(t, "alpha", hitSource(t, result.Hits.Hits[1])["title"])
}

func TestVectorValidation(t *testing.T) {
	name := indexResource(t)
	createIndex(t, name, vectorIndexConfig("flat"))
	putDocument(t, name, "1", map[string]any{
		"embedding": []float32{1, 0, 0, 0},
	})

	for _, test := range []struct {
		name, field string
		value       any
	}{
		{"dimensions", "vector", []float32{1, 2}},
		{"k", "k", 0},
		{"nprobe", "nprobe", 0},
		{"field", "query_field", "missing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			query := vectorQuery(1)
			query[test.field] = test.value

			var response meta.HTTPResponseError
			apiJSON(t, "POST", "/api/"+name+"/_search/vector", query, 400, &response)
			require.NotEmpty(t, response.Error)
		})
	}

	// Validation failures leave both the vector and the text backfill usable.
	require.Equal(t, []string{name + "/1"}, hitKeys(searchVectors(t, name, vectorQuery(1))))

	var recall struct {
		Recall float32 `json:"recall"`
	}
	apiJSON(t, "POST", "/api/"+name+"/_recall", map[string]any{
		"field": "embedding",
	}, 200, &recall)
	require.EqualValues(t, 1, recall.Recall)
}

func TestVectorAlgorithmsSmoke(t *testing.T) {
	for _, kind := range []string{"hnsw", "ivf_pq"} {
		t.Run(kind, func(t *testing.T) {
			name := indexResource(t)
			config := vectorIndexConfig(kind)
			if kind == "ivf_pq" {
				field := config["mappings"].(map[string]any)["properties"].(map[string]any)["embedding"].(map[string]any)
				field["m"], field["nbits"] = 2, 4
			}

			createIndex(t, name, config)
			if kind == "ivf_pq" {
				// With no eligible segment this is a no-op hint. HTTP success
				// confirms acceptance only, never completion of a build.
				apiJSON(t, "POST", "/api/"+name+"/embedding/_seal", nil, 200, nil)
			}

			// Keep this smoke test independent of approximate ranking details.
			putDocument(t, name, "1", map[string]any{
				"title":     "first",
				"embedding": []float32{1, 0, 0, 0},
			})
			putDocument(t, name, "2", map[string]any{
				"title":     "second",
				"embedding": []float32{2, 0, 0, 0},
			})
			require.ElementsMatch(t, []string{name + "/1", name + "/2"}, hitKeys(searchVectors(t, name, vectorQuery(2))))

			putDocument(t, name, "1", map[string]any{
				"title":     "updated",
				"embedding": []float32{3, 0, 0, 0},
			})
			apiJSON(t, "DELETE", "/api/"+name+"/_doc/2", nil, 200, nil)

			result := searchVectors(t, name, vectorQuery(2))
			require.Equal(t, []string{name + "/1"}, hitKeys(result))
			require.Equal(t, "updated", hitSource(t, result.Hits.Hits[0])["title"])
		})
	}
}

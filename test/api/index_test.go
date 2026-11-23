package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexCreation(t *testing.T) {
	for _, route := range []string{"body", "target", "es"} {
		t.Run(route, func(t *testing.T) {
			name := indexResource(t)
			config := map[string]any{
				"name": name,
				"mappings": map[string]any{
					"properties": map[string]any{
						"category": map[string]any{
							"type": "keyword",
						},
					},
				},
			}

			uri := "/api/index"
			if route == "target" {
				uri += "/" + name
			}
			if route == "es" {
				uri = "/es/" + name
			}

			// Strict creation must not replace an existing index configuration.
			apiJSON(t, "PUT", uri, config, 200, nil)
			apiJSON(t, "HEAD", "/api/index/"+name, nil, 200, nil)
			apiJSON(t, "PUT", "/api/index/"+name, map[string]any{}, 400, nil)

			var mapping map[string]struct {
				Mappings struct {
					Properties map[string]struct {
						Type string `json:"type"`
					} `json:"properties"`
				} `json:"mappings"`
			}
			apiJSON(t, "GET", "/api/"+name+"/_mapping", nil, 200, &mapping)
			require.Equal(t, "keyword", mapping[name].Mappings.Properties["category"].Type)

			// Index deletion is idempotent, unlike missing-document deletion.
			apiJSON(t, "DELETE", "/api/index/"+name, nil, 200, nil)
			apiJSON(t, "HEAD", "/api/index/"+name, nil, 404, nil)
			apiJSON(t, "DELETE", "/api/index/"+name, nil, 200, nil)
		})
	}

	apiJSON(t, "PUT", "/api/index", map[string]any{
		"name": "",
	}, 400, nil)
}

func TestIndexDeleteAndRecreate(t *testing.T) {
	name := indexResource(t)
	config := vectorIndexConfig("flat")
	config["settings"] = map[string]any{
		"analysis": map[string]any{
			"analyzer": map[string]any{
				"old_analyzer": map[string]any{
					"type": "keyword",
				},
			},
		},
	}
	createIndex(t, name, config)
	putDocument(t, name, "1", map[string]any{
		"title":     "old",
		"embedding": []float32{0, 0, 0, 0},
	})
	require.Len(t, searchVectors(t, name, vectorQuery(2)).Hits.Hits, 1)

	apiJSON(t, "DELETE", "/api/index/"+name, nil, 200, nil)
	apiJSON(t, "HEAD", "/api/index/"+name, nil, 404, nil)

	// Reusing the name must create fresh metadata, analyzers and runtime resources.
	createIndex(t, name, map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"replacement": map[string]any{
					"type": "keyword",
				},
			},
		},
	})
	require.Zero(t, searchDocuments(t, name, allDocuments()).Hits.Total.Value)
	apiJSON(t, "POST", "/api/"+name+"/_analyze", map[string]any{
		"analyzer": "old_analyzer",
		"text":     "test",
	}, 400, nil)

	var mapping map[string]struct {
		Mappings struct {
			Properties map[string]any `json:"properties"`
		} `json:"mappings"`
	}
	apiJSON(t, "GET", "/api/"+name+"/_mapping", nil, 200, &mapping)
	require.NotContains(t, mapping[name].Mappings.Properties, "embedding")
	require.NotContains(t, mapping[name].Mappings.Properties, "title")

	// Reintroduce the vector field: old persistent vectors must not reappear.
	apiJSON(t, "PUT", "/api/"+name+"/_mapping", vectorIndexConfig("flat")["mappings"], 200, nil)
	putDocument(t, name, "2", map[string]any{
		"replacement": "new",
		"embedding":   []float32{1, 0, 0, 0},
	})
	require.Equal(t, []string{name + "/2"}, hitKeys(searchVectors(t, name, vectorQuery(10))))

	apiJSON(t, "DELETE", "/api/index/"+name, nil, 200, nil)
	apiJSON(t, "DELETE", "/api/index/"+name, nil, 200, nil)
}

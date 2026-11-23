package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTemplateCreationAndIsolation(t *testing.T) {
	templateName := resourceName()
	a, b := indexResource(t), indexResource(t)

	// Restrict the template to this fixture so other tests cannot inherit it.
	apiJSON(t, "PUT", "/es/_index_template/"+templateName, map[string]any{
		"index_patterns": []string{a, b},
		"template": map[string]any{
			"settings": map[string]any{
				"analysis": map[string]any{
					"analyzer": map[string]any{
						"exact": map[string]any{
							"type": "keyword",
						},
					},
				},
			},
			"mappings": map[string]any{
				"properties": map[string]any{
					"category": map[string]any{
						"type": "keyword",
					},
					"title": map[string]any{
						"type":     "text",
						"analyzer": "exact",
					},
				},
			},
		},
	}, 200, nil)
	t.Cleanup(func() { apiJSON(t, "DELETE", "/es/_index_template/"+templateName, nil, 200, nil) })

	var before map[string]any
	apiJSON(t, "GET", "/es/_index_template/"+templateName, nil, 200, &before)

	// Request settings override the same named template analyzer; template
	// mappings remain, while request-only mappings are local to this index.
	createIndex(t, a, map[string]any{
		"settings": map[string]any{
			"analysis": map[string]any{
				"analyzer": map[string]any{
					"exact": map[string]any{
						"type": "standard",
					},
				},
			},
		},
		"mappings": map[string]any{
			"properties": map[string]any{
				"local": map[string]any{
					"type": "keyword",
				},
			},
		},
	})
	putDocument(t, a, "1", map[string]any{
		"title":    "Mixed CASE",
		"category": "Exact Category",
		"local":    "only-a",
	})

	// b auto-creates from the original, unmodified template.
	putDocument(t, b, "1", map[string]any{
		"title":    "Mixed CASE",
		"category": "Exact Category",
	})

	query := map[string]any{
		"query": map[string]any{
			"match": map[string]any{
				"title": "mixed",
			},
		},
	}
	require.Equal(t, 1, searchDocuments(t, a, query).Hits.Total.Value)
	require.Zero(t, searchDocuments(t, b, query).Hits.Total.Value)

	query = map[string]any{
		"query": map[string]any{
			"term": map[string]any{
				"category": "Exact Category",
			},
		},
	}
	for _, name := range []string{a, b} {
		require.Equal(t, 1, searchDocuments(t, name, query).Hits.Total.Value)
	}

	// Creating either index must not mutate the persisted template.
	var after map[string]any
	apiJSON(t, "GET", "/es/_index_template/"+templateName, nil, 200, &after)
	require.Equal(t, before, after)

	var mapping map[string]struct {
		Mappings struct {
			Properties map[string]any `json:"properties"`
		} `json:"mappings"`
	}
	apiJSON(t, "GET", "/api/"+b+"/_mapping", nil, 200, &mapping)
	require.NotContains(t, mapping[b].Mappings.Properties, "local")
}

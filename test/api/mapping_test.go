package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMappingUpdates(t *testing.T) {
	name := indexResource(t)
	putDocument(t, name, "1", map[string]any{
		"title": "dynamic",
		"rank":  1,
	})

	type property struct {
		Type string `json:"type"`
	}
	readMapping := func() map[string]property {
		var result map[string]struct {
			Mappings struct {
				Properties map[string]property `json:"properties"`
			} `json:"mappings"`
		}
		apiJSON(t, "GET", "/api/"+name+"/_mapping", nil, 200, &result)
		return result[name].Mappings.Properties
	}

	// A new property augments the dynamically inferred mapping.
	require.Equal(t, "text", readMapping()["title"].Type)
	apiJSON(t, "PUT", "/api/"+name+"/_mapping", map[string]any{
		"properties": map[string]any{
			"category": map[string]any{
				"type": "keyword",
			},
		},
	}, 200, nil)

	before := readMapping()
	require.Equal(t, "keyword", before["category"].Type)

	// A conflicting type must leave all existing properties unchanged.
	apiJSON(t, "PUT", "/api/"+name+"/_mapping", map[string]any{
		"properties": map[string]any{
			"title": map[string]any{
				"type": "keyword",
			},
		},
	}, 400, nil)
	require.Equal(t, before, readMapping())

	// Verify the added keyword mapping through an actual exact-value query.
	putDocument(t, name, "2", map[string]any{
		"title":    "new",
		"category": "Exact Value",
	})
	result := searchDocuments(t, name, map[string]any{
		"query": map[string]any{
			"term": map[string]any{
				"category": "Exact Value",
			},
		},
	})
	require.Equal(t, []string{name + "/2"}, hitKeys(result))
	require.Equal(t, "dynamic", getDocument(t, name, "1").Source["title"])
}

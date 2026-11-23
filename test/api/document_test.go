package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

// /api and /es use the same storage but have separate route/middleware wiring.
func TestDocumentLifecycle(t *testing.T) {
	for _, prefix := range []string{"/api/", "/es/"} {
		t.Run(prefix, func(t *testing.T) {
			name := indexResource(t) // first write auto-creates the index
			uri := prefix + name

			// A generated ID must identify the same source through the read API.
			var created meta.HTTPResponseID
			apiJSON(t, "POST", uri+"/_doc", map[string]any{
				"title": "generated",
			}, 200, &created)
			require.NotEmpty(t, created.ID)
			require.Equal(t, "generated", getDocument(t, name, created.ID).Source["title"])

			// Replacing an existing ID removes fields absent from the new source.
			apiJSON(t, "PUT", uri+"/_doc/1", map[string]any{
				"title": "before",
				"old":   "remove",
			}, 200, nil)
			apiJSON(t, "PUT", uri+"/_doc/1", map[string]any{
				"title": "after",
				"keep":  "value",
			}, 200, nil)

			hit := getDocument(t, name, "1")
			require.Equal(t, "after", hit.Source["title"])
			require.NotContains(t, hit.Source, "old")
			require.Len(t, searchDocuments(t, name, allDocuments()).Hits.Hits, 2)

			// This project's _update replaces source; it is not ES-style merging.
			apiJSON(t, "POST", uri+"/_update/1", map[string]any{
				"title": "updated",
			}, 200, nil)
			hit = getDocument(t, name, "1")
			require.Equal(t, "updated", hit.Source["title"])
			require.NotContains(t, hit.Source, "keep")

			old := searchDocuments(t, name, map[string]any{
				"query": map[string]any{
					"match": map[string]any{
						"title": "after",
					},
				},
			})
			require.Zero(t, old.Hits.Total.Value)

			// A missing ID requires explicit upsert permission.
			apiJSON(t, "POST", uri+"/_update/2", map[string]any{
				"title": "missing",
			}, 500, nil)
			apiJSON(t, "POST", uri+"/_update/2?insert=true", map[string]any{
				"title": "upsert",
			}, 200, nil)
			require.Equal(t, "upsert", getDocument(t, name, "2").Source["title"])

			// Parsing errors must not modify an already stored document.
			apiRaw(t, "PUT", uri+"/_doc/1", "not-json", http.StatusBadRequest, nil)
			require.Equal(t, "updated", getDocument(t, name, "1").Source["title"])

			apiJSON(t, "DELETE", uri+"/_doc/1", nil, 200, nil)
			apiJSON(t, "GET", "/api/"+name+"/_doc/1", nil, 400, nil)
			apiJSON(t, "DELETE", uri+"/_doc/1", nil, 400, nil)

			result := searchDocuments(t, name, allDocuments())
			require.Equal(t, 2, result.Hits.Total.Value)
			require.ElementsMatch(t, []string{name + "/" + created.ID, name + "/2"}, hitKeys(result))
		})
	}
}

func TestESCreateRoutes(t *testing.T) {
	for _, method := range []string{"PUT", "POST"} {
		t.Run(method, func(t *testing.T) {
			name := indexResource(t)
			apiJSON(t, method, "/es/"+name+"/_create/1", map[string]any{
				"title": "first",
			}, 200, nil)

			// The compatibility route currently has replace/upsert semantics.
			apiJSON(t, method, "/es/"+name+"/_create/1", map[string]any{
				"title": "second",
			}, 200, nil)
			require.Equal(t, "second", getDocument(t, name, "1").Source["title"])
			require.Equal(t, 1, searchDocuments(t, name, allDocuments()).Hits.Total.Value)
		})
	}
}

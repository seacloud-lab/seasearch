package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

func TestMultiIndexSearch(t *testing.T) {
	a := indexResource(t)
	b := a + "-second"
	t.Cleanup(func() { apiJSON(t, "DELETE", "/api/index/"+b, nil, 200, nil) })

	// The same ID in different indexes must retain its own source and sort value.
	putDocument(t, a, "1", map[string]any{
		"title": "alpha",
		"rank":  2,
	})
	putDocument(t, b, "1", map[string]any{
		"title": "beta",
		"rank":  1,
	})

	query := map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
		"sort": []string{"rank"},
		"size": 10,
	}
	result := searchDocuments(t, a+","+b, query)
	require.Equal(t, 2, result.Hits.Total.Value)
	require.Equal(t, []string{b + "/1", a + "/1"}, hitKeys(result))
	require.Equal(t, "beta", hitSource(t, result.Hits.Hits[0])["title"])
	require.Equal(t, "alpha", hitSource(t, result.Hits.Hits[1])["title"])
	require.Equal(t, hitKeys(result), hitKeys(searchDocuments(t, a+"*", query)))

	alias := resourceName()
	apiJSON(t, "POST", "/es/_aliases", map[string]any{
		"actions": []any{
			map[string]any{
				"add": map[string]any{
					"indices": []string{a, b},
					"alias":   alias,
				},
			},
		},
	}, 200, nil)
	t.Cleanup(func() {
		apiJSON(t, "POST", "/es/_aliases", map[string]any{
			"actions": []any{
				map[string]any{
					"remove": map[string]any{
						"indices": []string{a, b},
						"alias":   alias,
					},
				},
			},
		}, 200, nil)
	})

	// Alias expansion must preserve global sorting, pagination and total hits.
	require.Equal(t, hitKeys(result), hitKeys(searchDocuments(t, alias, query)))

	query["from"], query["size"] = 1, 1
	page := searchDocuments(t, alias, query)
	require.Equal(t, 2, page.Hits.Total.Value)
	require.Equal(t, []string{a + "/1"}, hitKeys(page))

	// Each msearch response corresponds to the request at the same position.
	var multi struct {
		Responses []*meta.SearchResponse `json:"responses"`
	}
	apiRaw(t, "POST", "/es/_msearch", ndjson(t,
		map[string]any{
			"index": a,
		},
		allDocuments(),
		map[string]any{
			"index": b,
		},
		map[string]any{
			"query": map[string]any{
				"match": map[string]any{
					"title": "absent",
				},
			},
		},
	), 200, &multi)
	require.Len(t, multi.Responses, 2)
	require.Equal(t, []string{a + "/1"}, hitKeys(multi.Responses[0]))
	require.Zero(t, multi.Responses[1].Hits.Total.Value)

	// Removing one alias member must not remove the other member.
	apiJSON(t, "POST", "/es/_aliases", map[string]any{
		"actions": []any{
			map[string]any{
				"remove": map[string]any{
					"index": b,
					"alias": alias,
				},
			},
		},
	}, 200, nil)
	require.Equal(t, []string{a + "/1"}, hitKeys(searchDocuments(t, alias, allDocuments())))
}

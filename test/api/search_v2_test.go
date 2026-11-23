package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

func TestSearchV2(t *testing.T) {
	name := indexResource(t)
	apiRaw(t, "PUT", "/api/"+name+"/_doc/1", indexData, 200, nil)
	for _, test := range []struct {
		name, body string
		count      int
	}{
		{"all", `{"query":{"match_all":{}}}`, 1},
		{"no_match", `{"query":{"match":{"_all":"absent"}}}`, 0},
		{"match", `{"query":{"match":{"Athlete":"DEMTSCHENKO"}}}`, 1},
		{"wildcard", `{"query":{"wildcard":{"Athlete":"dem*"}}}`, 1},
		{"fuzzy", `{"query":{"fuzzy":{"Athlete":"demtschenk"}}}`, 1},
		{"term", `{"query":{"term":{"City":"turin"}}}`, 1},
		{"phrase", `{"query":{"match_phrase":{"Athlete":"DEMTSCHENKO"}}}`, 1},
		{"prefix", `{"query":{"prefix":{"Athlete":"dem"}}}`, 1},
		{"querystring", `{"query":{"query_string":{"query":"Athlete:DEMTSCHENKO"}}}`, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var result meta.SearchResponse
			apiRaw(t, "POST", "/es/"+name+"/_search", test.body, 200, &result)
			require.Equal(t, test.count, result.Hits.Total.Value)
			if test.count > 0 {
				require.Equal(t, []string{name + "/1"}, hitKeys(&result))
			} else {
				require.Empty(t, result.Hits.Hits)
			}
		})
	}
	t.Run("missing_index", func(t *testing.T) { apiRaw(t, "POST", "/es/"+resourceName()+"/_search", `{}`, 400, nil) })
	t.Run("without_target", func(t *testing.T) {
		var result meta.SearchResponse
		// Scope the query by an explicit index list so the test does not depend
		// on any other test's indexes, while still exercising the targetless route.
		apiJSON(t, "POST", "/es/_search", map[string]any{
			"index": []string{name},
			"query": map[string]any{
				"match_all": map[string]any{},
			},
		}, 200, &result)
		require.Equal(t, 1, result.Hits.Total.Value)
		require.Equal(t, []string{name + "/1"}, hitKeys(&result))
	})
}

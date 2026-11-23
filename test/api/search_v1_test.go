package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

func TestSearchV1(t *testing.T) {
	name := indexResource(t)
	apiRaw(t, "PUT", "/api/"+name+"/_doc/1", indexData, 200, nil)
	for _, test := range []struct {
		name, body string
		count      int
	}{
		{"all", `{"search_type":"alldocuments"}`, 1},
		{"no_match", `{"search_type":"match","query":{"term":"absent"}}`, 0},
		{"match", `{"search_type":"match","query":{"field":"Athlete","term":"DEMTSCHENKO"}}`, 1},
		{"wildcard", `{"search_type":"wildcard","query":{"field":"Athlete","term":"dem*"}}`, 1},
		{"fuzzy", `{"search_type":"fuzzy","query":{"field":"Athlete","term":"demtschenk"}}`, 1},
		{"term", `{"search_type":"term","query":{"field":"City","term":"turin"}}`, 1},
		{"matchall", `{"search_type":"matchall","query":{"term":"demtschenk"}}`, 1},
		{"phrase", `{"search_type":"matchphrase","query":{"field":"Athlete","term":"DEMTSCHENKO"}}`, 1},
		{"multiphrase", `{"search_type":"multiphrase","query":{"terms":[["demtschenko"],["albert"]]}}`, 1},
		{"prefix", `{"search_type":"prefix","query":{"field":"Athlete","term":"dem"}}`, 1},
		{"querystring", `{"search_type":"querystring","query":{"term":"Athlete:DEMTSCHENKO"}}`, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var result meta.SearchResponse
			apiRaw(t, "POST", "/api/"+name+"/_search", test.body, 200, &result)
			require.Equal(t, test.count, result.Hits.Total.Value)
			if test.count > 0 {
				require.Equal(t, []string{name + "/1"}, hitKeys(&result))
			} else {
				require.Empty(t, result.Hits.Hits)
			}
		})
	}
	t.Run("missing_index", func(t *testing.T) { apiRaw(t, "POST", "/api/"+resourceName()+"/_search", `{}`, 400, nil) })
	for _, test := range []struct{ name, body string }{
		{"terms", `{"search_type":"matchall","aggs":{"years":{"agg_type":"terms","field":"Year"}}}`},
		{"metrics", `{"search_type":"matchall","aggs":{"maximum":{"agg_type":"max","field":"Year"},"minimum":{"agg_type":"min","field":"Year"},"average":{"agg_type":"avg","field":"Year"}}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var result struct {
				Aggregations map[string]struct {
					Value   float64 `json:"value"`
					Buckets []struct {
						Key   float64 `json:"key"`
						Count int     `json:"doc_count"`
					} `json:"buckets"`
				} `json:"aggregations"`
			}
			apiRaw(t, "POST", "/api/"+name+"/_search", test.body, 200, &result)
			if test.name == "terms" {
				buckets := result.Aggregations["years"].Buckets
				require.Len(t, buckets, 1)
				require.EqualValues(t, 2006, buckets[0].Key)
				require.Equal(t, 1, buckets[0].Count)
			} else {
				for _, key := range []string{"maximum", "minimum", "average"} {
					require.EqualValues(t, 2006, result.Aggregations[key].Value)
				}
			}
		})
	}
}

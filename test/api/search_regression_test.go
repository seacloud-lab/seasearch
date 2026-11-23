package api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchResultsAndAggregations(t *testing.T) {
	name := indexResource(t)
	createIndex(t, name, map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"category": map[string]any{
					"type": "keyword",
				},
			},
		},
	})
	for i, category := range []string{"a", "b", "a", "b"} {
		putDocument(t, name, fmt.Sprint(i+1), map[string]any{
			"title":    "searchable",
			"rank":     i + 1,
			"category": category,
		})
	}

	// Filtering selects ranks 2–4; descending pagination returns only rank 3.
	result := searchDocuments(t, name, map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{
						"match": map[string]any{
							"title": "searchable",
						},
					},
				},
				"filter": []any{
					map[string]any{
						"range": map[string]any{
							"rank": map[string]any{
								"gte": 2,
							},
						},
					},
				},
			},
		},
		"sort":    []string{"-rank"},
		"from":    1,
		"size":    1,
		"_source": []string{"category"},
	})
	require.Equal(t, 3, result.Hits.Total.Value)
	require.Equal(t, []string{name + "/3"}, hitKeys(result))
	require.Equal(t, map[string]any{
		"category": "a",
	}, result.Hits.Hits[0].Source)

	// Aggregation values cover the full fixture, not the paginated hits above.
	var aggregation struct {
		Aggregations map[string]struct {
			Value   float64 `json:"value"`
			Buckets []struct {
				Key   string `json:"key"`
				Count int    `json:"doc_count"`
			} `json:"buckets"`
		} `json:"aggregations"`
	}
	apiJSON(t, "POST", "/es/"+name+"/_search", map[string]any{
		"size": 0,
		"query": map[string]any{
			"match_all": map[string]any{},
		},
		"aggs": map[string]any{
			"categories": map[string]any{
				"terms": map[string]any{
					"field": "category",
				},
			},
			"average": map[string]any{
				"avg": map[string]any{
					"field": "rank",
				},
			},
		},
	}, 200, &aggregation)
	require.InDelta(t, 2.5, aggregation.Aggregations["average"].Value, 1e-8)

	counts := map[string]int{}
	for _, bucket := range aggregation.Aggregations["categories"].Buckets {
		counts[bucket.Key] = bucket.Count
	}
	require.Equal(t, map[string]int{"a": 2, "b": 2}, counts)
}

package api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeleteByQueryWindow(t *testing.T) {
	name := indexResource(t)
	for i := 1; i <= 5; i++ {
		putDocument(t, name, fmt.Sprint(i), map[string]any{
			"rank":  i,
			"title": "selected",
		})
	}

	// All five documents match, but only the sorted window (IDs 2 and 3) is deleted.
	var response struct {
		Total    int      `json:"total"`
		Deleted  int      `json:"deleted"`
		Failures []string `json:"failures"`
	}
	apiJSON(t, "POST", "/es/"+name+"/_delete_by_query", map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
		"sort": []string{"rank"},
		"from": 1,
		"size": 2,
	}, 200, &response)
	require.Equal(t, 5, response.Total)
	require.Equal(t, 2, response.Deleted)
	require.Empty(t, response.Failures)

	// Confirm the actual remaining data, not just the reported deletion count.
	require.ElementsMatch(t, []string{name + "/1", name + "/4", name + "/5"}, hitKeys(searchDocuments(t, name, allDocuments())))
	for _, id := range []string{"2", "3"} {
		apiJSON(t, "GET", "/api/"+name+"/_doc/"+id, nil, 400, nil)
	}

	// Empty matches and malformed requests must leave remaining documents intact.
	apiJSON(t, "POST", "/es/"+name+"/_delete_by_query", map[string]any{
		"query": map[string]any{
			"match": map[string]any{
				"title": "absent",
			},
		},
	}, 200, &response)
	require.Zero(t, response.Total)
	require.Zero(t, response.Deleted)

	apiRaw(t, "POST", "/es/"+name+"/_delete_by_query", "invalid", 400, nil)
	require.Equal(t, 3, searchDocuments(t, name, allDocuments()).Hits.Total.Value)
}

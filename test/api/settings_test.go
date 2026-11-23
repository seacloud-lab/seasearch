package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnalysisConfiguration(t *testing.T) {
	for _, entry := range []string{"create", "settings"} {
		t.Run(entry, func(t *testing.T) {
			name := indexResource(t)

			// Both creation paths must publish the same final analysis configuration.
			settings := map[string]any{
				"analysis": map[string]any{
					"analyzer": map[string]any{
						"exact": map[string]any{
							"type":      "custom",
							"tokenizer": "keyword",
							"filter":    []string{"lowercase"},
						},
					},
				},
			}
			if entry == "create" {
				createIndex(t, name, map[string]any{
					"settings": settings,
					"mappings": map[string]any{
						"properties": map[string]any{
							"title": map[string]any{
								"type":            "text",
								"analyzer":        "exact",
								"search_analyzer": "exact",
							},
						},
					},
				})
			} else {
				apiJSON(t, "PUT", "/api/"+name+"/_settings", settings, 200, nil)
				apiJSON(t, "PUT", "/api/"+name+"/_mapping", map[string]any{
					"properties": map[string]any{
						"title": map[string]any{
							"type":            "text",
							"analyzer":        "exact",
							"search_analyzer": "exact",
						},
					},
				}, 200, nil)
			}

			// Empty updates are harmless; even identical non-empty analysis is rejected.
			var before map[string]any
			apiJSON(t, "GET", "/api/"+name+"/_settings", nil, 200, &before)
			for _, empty := range []any{
				map[string]any{},
				map[string]any{
					"analysis": map[string]any{},
				},
			} {
				apiJSON(t, "PUT", "/api/"+name+"/_settings", empty, 200, nil)
			}
			apiJSON(t, "PUT", "/api/"+name+"/_settings", settings, 400, nil)

			var after map[string]any
			apiJSON(t, "GET", "/api/"+name+"/_settings", nil, 200, &after)
			require.Equal(t, before, after)

			analyze := func(overrides map[string]any) []string {
				body := map[string]any{
					"analyzer": "exact",
					"text":     "Mixed CASE",
				}
				for key, value := range overrides {
					body[key] = value
				}

				var result struct {
					Tokens []struct {
						Token string `json:"token"`
					} `json:"tokens"`
				}
				apiJSON(t, "POST", "/api/"+name+"/_analyze", body, 200, &result)

				var tokens []string
				for _, token := range result.Tokens {
					tokens = append(tokens, token.Token)
				}
				return tokens
			}

			// Request overrides must not mutate the analyzer later used by the Writer.
			require.Equal(t, []string{"mixed case"}, analyze(nil))
			require.Equal(t, []string{"mixed", "case"}, analyze(map[string]any{
				"tokenizer": "standard",
			}))
			require.Equal(t, []string{"MIXED CASE"}, analyze(map[string]any{
				"filter": []string{"uppercase"},
			}))
			require.Equal(t, []string{"mixed case"}, analyze(nil))

			putDocument(t, name, "1", map[string]any{
				"title": "Mixed CASE",
			})
			for term, count := range map[string]int{"MIXED CASE": 1, "mixed": 0} {
				result := searchDocuments(t, name, map[string]any{
					"query": map[string]any{
						"match": map[string]any{
							"title": term,
						},
					},
				})
				require.Equal(t, count, result.Hits.Total.Value)
			}
		})
	}
}

package api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

func TestBulkDocuments(t *testing.T) {
	for _, prefix := range []string{"/api/", "/es/"} {
		for _, targeted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/targeted=%t", prefix, targeted), func(t *testing.T) {
				a, b := indexResource(t), indexResource(t)
				putDocument(t, a, "1", map[string]any{
					"title": "old",
				})
				putDocument(t, a, "2", map[string]any{
					"title": "delete",
				})

				// Item metadata may override the default index supplied by the path.
				uri := prefix + "_bulk"
				defaultIndex := a
				if targeted {
					uri = prefix + a + "/_bulk"
					defaultIndex = ""
				}

				body := ndjson(t,
					map[string]any{
						"index": map[string]any{
							"_index": defaultIndex,
							"_id":    "1",
						},
					},
					map[string]any{
						"title": "new",
					},
					map[string]any{
						"delete": map[string]any{
							"_index": defaultIndex,
							"_id":    "2",
						},
					},
					map[string]any{
						"create": map[string]any{
							"_index": b,
							"_id":    "1",
						},
					},
					map[string]any{
						"title": "other",
					},
				)

				// Native bulk reports a count; ES bulk returns per-item results.
				if prefix == "/api/" {
					var response meta.HTTPResponseRecordCount
					apiRaw(t, "POST", uri, body, 200, &response)
					require.EqualValues(t, 3, response.RecordCount)
				} else {
					var response struct {
						Errors bool `json:"errors"`
						Items  []map[string]struct {
							Index  string `json:"_index"`
							ID     string `json:"_id"`
							Status int    `json:"status"`
						} `json:"items"`
					}
					apiRaw(t, "POST", uri, body, 200, &response)
					require.False(t, response.Errors)
					require.Len(t, response.Items, 3)

					var items []string
					for _, item := range response.Items {
						for action, value := range item {
							require.Equal(t, 200, value.Status)
							items = append(items, action+":"+value.Index+"/"+value.ID)
						}
					}
					require.ElementsMatch(t, []string{"index:" + a + "/1", "delete:" + a + "/2", "index:" + b + "/1"}, items)
				}

				// Verify actual effects independently of the response envelope.
				require.Equal(t, "new", getDocument(t, a, "1").Source["title"])
				require.Equal(t, "other", getDocument(t, b, "1").Source["title"])
				require.Equal(t, []string{a + "/1"}, hitKeys(searchDocuments(t, a, allDocuments())))
			})
		}
	}
}

func TestIngestBatchWindows(t *testing.T) {
	// Exercise multiple store windows without allocating thousands of documents.
	// duplicate_across_windows is a regression assertion, not an expected failure:
	// a new ID repeated in a later window must replace its earlier document. Keep
	// this assertion even while the current insert preparation produces duplicates.
	original := config.Global.BatchSize
	config.Global.BatchSize = 2
	t.Cleanup(func() { config.Global.BatchSize = original })

	for _, format := range []string{"bulk", "multi", "bulkv2"} {
		t.Run(format, func(t *testing.T) {
			for _, scenario := range []string{"unique_ids", "duplicate_same_window", "duplicate_across_windows", "existing_id_across_windows"} {
				t.Run(scenario, func(t *testing.T) {
					name := indexResource(t)
					docs := []map[string]any{
						{
							"_id":   "1",
							"title": "first",
							"old":   "remove",
						},
						{
							"_id":   "2",
							"title": "two",
						},
						{
							"_id":   "3",
							"title": "three",
						},
						{
							"_id":   "1",
							"title": "last",
						},
						{
							"_id":   "4",
							"title": "four",
						},
					}
					expected := []string{name + "/1", name + "/2", name + "/3", name + "/4"}
					switch scenario {
					case "unique_ids":
						docs[0] = map[string]any{
							"_id":   "5",
							"title": "five",
						}
						expected = append(expected, name+"/5")
					case "duplicate_same_window":
						// Move the repeated ID into the first two-operation window.
						docs[1], docs[3] = docs[3], docs[1]
					case "existing_id_across_windows":
						putDocument(t, name, "1", map[string]any{
							"title": "existing",
						})
					}

					var response meta.HTTPResponseRecordCount
					switch format {
					case "bulk":
						var lines []any
						for _, doc := range docs {
							lines = append(lines, map[string]any{
								"index": map[string]any{
									"_id": doc["_id"],
								},
							}, doc)
						}
						apiRaw(t, "POST", "/api/"+name+"/_bulk", ndjson(t, lines...), 200, &response)
					case "multi":
						var lines []any
						for _, doc := range docs {
							lines = append(lines, doc)
						}
						apiRaw(t, "POST", "/api/"+name+"/_multi", ndjson(t, lines...), 200, &response)
					case "bulkv2":
						apiJSON(t, "POST", "/api/_bulkv2", map[string]any{
							"index":   name,
							"records": docs,
						}, 200, &response)
					}

					require.EqualValues(t, len(docs), response.RecordCount)

					// All formats must converge to the same unique documents and sources.
					result := searchDocuments(t, name, allDocuments())
					require.Equal(t, len(expected), result.Hits.Total.Value, "same document ID must not create extra hits across batch windows")
					require.ElementsMatch(t, expected, hitKeys(result))

					hit := getDocument(t, name, "1")
					require.Equal(t, "last", hit.Source["title"])
					require.NotContains(t, hit.Source, "old")

					for id, title := range map[string]string{"2": "two", "3": "three", "4": "four"} {
						require.Equal(t, title, getDocument(t, name, id).Source["title"])
					}
				})
			}
		})
	}
}

func TestBulkGroupedDeleteBeforeWrite(t *testing.T) {
	name := indexResource(t)
	putDocument(t, name, "1", map[string]any{
		"title": "old",
	})

	// Deletes precede writes, regardless of their line order. This intentionally
	// captures SeaSearch's existing contract rather than ES streaming semantics.
	body := ndjson(t,
		map[string]any{
			"index": map[string]any{
				"_id": "1",
			},
		},
		map[string]any{
			"title": "new",
		},
		map[string]any{
			"delete": map[string]any{
				"_index": "",
				"_id":    "1",
			},
		},
	)
	apiRaw(t, "POST", "/api/"+name+"/_bulk", body, 200, nil)

	require.Equal(t, "new", getDocument(t, name, "1").Source["title"])
	require.Equal(t, 1, searchDocuments(t, name, allDocuments()).Hits.Total.Value)
}

func TestIngestInvalidInput(t *testing.T) {
	name := indexResource(t)
	putDocument(t, name, "1", map[string]any{
		"title": "unchanged",
	})

	apiRaw(t, "POST", "/api/"+name+"/_bulkv2", "invalid", 400, nil)
	apiRaw(t, "POST", "/api/"+name+"/_bulk", `{"index":"invalid"}`, 500, nil)

	// _multi skips malformed lines and retains valid records.
	var response meta.HTTPResponseRecordCount
	apiRaw(t, "POST", "/api/"+name+"/_multi", "invalid\n"+ndjson(t, map[string]any{
		"_id":   "2",
		"title": "valid",
	}), 200, &response)
	require.EqualValues(t, 1, response.RecordCount)
	require.Equal(t, "unchanged", getDocument(t, name, "1").Source["title"])
	require.Equal(t, "valid", getDocument(t, name, "2").Source["title"])
}

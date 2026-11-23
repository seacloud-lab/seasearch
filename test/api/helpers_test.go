package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

var resourceSequence uint64

// Shared immutable input, never shared persisted test state.
const indexData = `{
	"Athlete": "DEMTSCHENKO, Albert", "City": "Turin", "Country": "RUS",
	"Discipline": "Luge", "Event": "Singles", "Gender": "Men", "Medal": "Silver",
	"Season": "winter", "Sport": "Luge", "Year": 2006
}`

// Unique across repeated runs in the same test process, and valid as an index,
// alias or template name. Tests deliberately do not run in parallel: configuration
// and application services are process-global.
func resourceName() string {
	return fmt.Sprintf("api-regression-%d", atomic.AddUint64(&resourceSequence, 1))
}

func indexResource(t *testing.T) string {
	t.Helper()

	name := resourceName()
	t.Cleanup(func() { apiJSON(t, http.MethodDelete, "/api/index/"+name, nil, http.StatusOK, nil) })

	return name
}

func apiRaw(t *testing.T, method, uri, body string, status int, result any) *httptest.ResponseRecorder {
	t.Helper()

	resp := request(method, uri, bytes.NewBufferString(body))
	require.Equal(t, status, resp.Code, "%s %s: %s", method, uri, resp.Body.String())

	if result != nil {
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), result), "%s %s: %s", method, uri, resp.Body.String())
	}
	return resp
}

func apiJSON(t *testing.T, method, uri string, body any, status int, result any) *httptest.ResponseRecorder {
	t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		require.NoError(t, err)
	}

	return apiRaw(t, method, uri, string(data), status, result)
}

func createIndex(t *testing.T, name string, config any) {
	t.Helper()
	if config == nil {
		config = map[string]any{}
	}
	apiJSON(t, http.MethodPut, "/api/index/"+name, config, http.StatusOK, nil)
}

func putDocument(t *testing.T, name, id string, doc any) {
	t.Helper()
	apiJSON(t, http.MethodPut, "/api/"+name+"/_doc/"+id, doc, http.StatusOK, nil)
}

type documentHit struct {
	Source map[string]any `json:"_source"`
}

func getDocument(t *testing.T, name, id string) *documentHit {
	t.Helper()

	var hit documentHit
	apiJSON(t, http.MethodGet, "/api/"+name+"/_doc/"+id, nil, http.StatusOK, &hit)
	return &hit
}

func hitSource(t *testing.T, hit meta.Hit) map[string]any {
	t.Helper()
	source, ok := hit.Source.(map[string]any)
	require.True(t, ok, "unexpected source: %#v", hit.Source)
	return source
}

func searchDocuments(t *testing.T, target string, query any) *meta.SearchResponse {
	t.Helper()

	var result meta.SearchResponse
	apiJSON(t, http.MethodPost, "/es/"+target+"/_search", query, http.StatusOK, &result)
	return &result
}

func hitKeys(result *meta.SearchResponse) []string {
	keys := make([]string, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		keys = append(keys, hit.Index+"/"+hit.ID)
	}
	return keys
}

func ndjson(t *testing.T, lines ...any) string {
	t.Helper()
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, line := range lines {
		require.NoError(t, encoder.Encode(line))
	}
	return buf.String()
}

func allDocuments() map[string]any {
	return map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
		"size": 100,
	}
}

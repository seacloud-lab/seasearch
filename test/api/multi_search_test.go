package api

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

var mbulkData = `{ "index" : {"_index" : "index_m_001" } } 
{"name": "Jack"}
{ "index" : {"_index" : "index_m_001" } } 
{"name": "Tom"}
{ "index" : {"_index" : "index_m_002" } } 
{"name": "Barry"}`

func TestMultiSearch(t *testing.T) {
	t.Run("init data for search", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		body.WriteString(mbulkData)
		resp := request("POST", "/es/_bulk", body)
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("search from 1 and 2", func(t *testing.T) {
		data := `{"index":["index_m_001","index_m_002"]}
	{"query":{"match_all":{}},"size":10}`
		body := bytes.NewBuffer(nil)
		body.WriteString(data)
		resp := request("POST", "/es/_msearch", body)
		assert.Equal(t, http.StatusOK, resp.Code)
		var result map[string][]*meta.SearchResponse
		b := resp.Body.Bytes()
		_ = json.Unmarshal(b, &result)
		res := result["responses"]
		assert.Equal(t, 3, res[0].Hits.Total.Value)
	})

	t.Run("search 1 and 2", func(t *testing.T) {
		data := `{"index":"index_m_001"}
	{"query":{"match_all":{}},"size":10}
	{"index":"index_m_002"}
	{"query":{"match_all":{}},"size":10}
`
		body := bytes.NewBuffer(nil)
		body.WriteString(data)
		resp := request("POST", "/es/_msearch", body)
		assert.Equal(t, http.StatusOK, resp.Code)
		var result map[string][]*meta.SearchResponse
		b := resp.Body.Bytes()
		_ = json.Unmarshal(b, &result)
		res := result["responses"]
		assert.Equal(t, 2, res[0].Hits.Total.Value)
		assert.Equal(t, 1, res[1].Hits.Total.Value)
	})

	t.Run("search 1 not exist in 2", func(t *testing.T) {
		data := `{"index":"index_m_001"}
	{"query":{"match_all":{}},"size":10}
	{"index":"index_m_002"}
	{"query":{"match":"xxxx"},"size":10}
`
		body := bytes.NewBuffer(nil)
		body.WriteString(data)
		resp := request("POST", "/es/_msearch", body)
		assert.Equal(t, http.StatusOK, resp.Code)
		var result map[string][]*meta.SearchResponse
		b := resp.Body.Bytes()
		_ = json.Unmarshal(b, &result)
		res := result["responses"]
		assert.Equal(t, 2, res[0].Hits.Total.Value)
		assert.Equal(t, 0, res[1].Hits.Total.Value)
	})
}

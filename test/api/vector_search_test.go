package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/lru_cache"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

var (
	vecIndexName1 = "paper"
	vecIndexName2 = "paper2"

	vecIndexMeta1 = `
		{
			"name":"paper",
			"mappings":{
				"properties":{
					"title-vec":{
						"type":"vector",
						"dims": 4,
						"vec_index_type":"flat"
					},
					"paper-id":{
						"type":"keyword"
					}
				}
			}
		}`
	vecIndexMeta2 = `
		{
			"name":"paper2",
			"mappings":{
				"properties":{
					"title-vec":{
						"type":"vector",
						"dims": 4,
						"vec_index_type":"flat"
					},
					"paper-id":{
						"type":"keyword"
					}
				}
			}
		}`
)

var documents = `{ "index" : {"_index" : "paper" } }
{"paper-id": "001","title-vec":[10.2,10.40,9.5,22.2]}
{ "index" : {"_index" : "paper" } }
{"paper-id": "002","title-vec":[10.2,11.40,9.5,22.2]}
{ "index" : {"_index" : "paper" } }
{"paper-id": "003","title-vec":[10.2,12.40,9.5,22.2]}
{ "index" : {"_index" : "paper" } }
{"paper-id": "004","title-vec":[10.2,10.39,9.5,22.2]}
{ "index" : {"_index" : "paper2" } }
{"paper-id": "005","title-vec":[10.2,10.37,9.5,22.2]}
`

var searchParam = `
{
    "query_field":"title-vec",
    "k":3,
    "return_fields":["paper-id"],
    "vector":[10.2,10.39,9.5,22.2],
    "_source": ["paper-id"]
}
`

// result by faiss index
var vectorSearchResult = `
{
	"took": 0,
	"timed_out": false,
	"_shards": {
		"total": 1,
		"successful": 1,
		"skipped": 0,
		"failed": 0
	},
	"hits": {
		"total": {
			"value": 3
		},
		"max_score": 1,
		"hits": [
			{
				"_index": "paper",
				"_type": "_doc",
				"_id": "27OC23UOaQP",
				"_score": 1,
				"_source": {
					"paper-id": "004"
				},
				"fields": {
					"paper-id": [
						"004"
					]
				}
			},
			{
				"_index": "paper",
				"_type": "_doc",
				"_id": "27OC23UOaQM",
				"_score": 0.9998999834060669,
				"_source": {
					"paper-id": "001"
				},
				"fields": {
					"paper-id": [
						"001"
					]
				}
			},
			{
				"_index": "paper",
				"_type": "_doc",
				"_id": "27OC23UOaQN",
				"_score": 0.49502530694007874,
				"_source": {
					"paper-id": "002"
				},
				"fields": {
					"paper-id": [
						"002"
					]
				}
			}
		]
	}
}
`

var deleteByQuery = `
{
    "query": {
        "bool": {
            "filter": [
                {
                    "term": {"paper-id":"003"}
                }
            ]
        }
    }
}
`

func TestVectorSearch(t *testing.T) {
	// config storage here
	// config.Global.StorageType = "s3"
	// config.Global.S3.Bucket = "zincsearch"
	// config.Global.S3.AccessId = "admin"
	// config.Global.S3.AccessSecret = "12345678"
	// config.Global.S3.Endpoint = "127.0.0.1:9000"
	// config.Global.StorageType = "oss"
	// config.Global.Oss.Endpoint = ""
	// config.Global.Oss.AccessId = ""
	// config.Global.Oss.AccessSecret = ""
	config.Global.Oss.Bucket = "zincsearch-test"
	config.Global.EnableWal = false
	config.Global.LogConfig.LogLevel = "Debug"
	defer func() {
		config.Global.StorageType = "disk"
	}()
	lru_cache.Init()
	defer lru_cache.ShutDown()

	t.Run("init metaData for vectorSearch", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		body.WriteString(vecIndexMeta1)
		resp := request("PUT", "/api/index/"+vecIndexName1, body)
		assert.Equal(t, http.StatusOK, resp.Code)
		body.Reset()
		body.WriteString(vecIndexMeta2)
		resp = request("PUT", "/api/index/"+vecIndexName2, body)
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("init document for vectorSearch", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		body.WriteString(documents)
		resp := request("POST", "/es/_bulk", body)
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("search for document", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		body.WriteString(deleteByQuery)
		resp := request("POST", "/es/"+vecIndexName1+"/_search", body)
		assert.Equal(t, http.StatusOK, resp.Code)
		var result = &meta.SearchResponse{}
		b := resp.Body.Bytes()
		_ = json.Unmarshal(b, &result)
		assert.Equal(t, 1, result.Hits.Total.Value)
	})

	t.Run("search for vector", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		body.WriteString(searchParam)
		resp := request("POST", "/api/"+vecIndexName1+"/_search/vector", body)
		assert.Equal(t, http.StatusOK, resp.Code)
		var result = &meta.SearchResponse{}
		b := resp.Body.Bytes()
		_ = json.Unmarshal(b, &result)
		var vecResult = &meta.SearchResponse{}
		_ = json.Unmarshal([]byte(vectorSearchResult), &vecResult)
		assert.Equal(t, vecResult.Hits.Total.Value, result.Hits.Total.Value)
		assert.Equal(t, vecResult.Hits.MaxScore, result.Hits.MaxScore)
		for i := 0; i < len(vecResult.Hits.Hits); i++ {
			assert.Equal(t, vecResult.Hits.Hits[i].Source, result.Hits.Hits[i].Source) // paper-id
			assert.Equal(t, vecResult.Hits.Hits[i].Fields, result.Hits.Hits[i].Fields) // paper-id
			assert.Equal(t, vecResult.Hits.Hits[i].Score, result.Hits.Hits[i].Score)
		}
	})

	t.Run("search multi-vector", func(t *testing.T) {
		uri := fmt.Sprintf("/api/%v,%v/_search/vector", vecIndexName1, vecIndexName2)
		body := bytes.NewBuffer(nil)
		body.WriteString(searchParam)
		resp := request("POST", uri, body)
		assert.Equal(t, http.StatusOK, resp.Code)
		var result = &meta.SearchResponse{}
		b := resp.Body.Bytes()
		_ = json.Unmarshal(b, &result)
		var ids []string
		for _, hit := range result.Hits.Hits {
			v, ok := hit.Fields["paper-id"].([]any)
			if !ok || len(v) != 1 {
				continue
			}
			id, ok := v[0].(string)
			if !ok {
				continue
			}
			ids = append(ids, id)
		}
		assert.Equal(t, ids, []string{"004", "001", "005"})
	})

	t.Run("delete by query", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		body.WriteString(deleteByQuery)
		resp := request("POST", "/es/"+vecIndexName1+"/_delete_by_query", body)
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("search for vector", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		body.WriteString(searchParam)
		resp := request("POST", "/api/"+vecIndexName1+"/_search/vector", body)
		assert.Equal(t, http.StatusOK, resp.Code)
		var result = &meta.SearchResponse{}
		b := resp.Body.Bytes()
		_ = json.Unmarshal(b, &result)
		assert.Equal(t, 3, result.Hits.Total.Value)

		var vecResult = &meta.SearchResponse{}
		// after delete 003
		_ = json.Unmarshal([]byte(vectorSearchResult), &vecResult)
		assert.Equal(t, vecResult.Hits.Total.Value, result.Hits.Total.Value)
		assert.Equal(t, vecResult.Hits.MaxScore, result.Hits.MaxScore)
		for i := 0; i < len(vecResult.Hits.Hits); i++ {
			assert.Equal(t, vecResult.Hits.Hits[i].Source, result.Hits.Hits[i].Source) // paper-id
			assert.Equal(t, vecResult.Hits.Hits[i].Fields, result.Hits.Hits[i].Fields) // paper-id
			assert.Equal(t, vecResult.Hits.Hits[i].Score, result.Hits.Hits[i].Score)
		}
	})

	t.Run("delete Index", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		resp := request("DELETE", "/api/index/"+vecIndexName1, body)
		assert.Equal(t, http.StatusOK, resp.Code)
		if resp.Code != http.StatusOK {
			bd, _ := io.ReadAll(resp.Body)
			t.Error(string(bd))
		}
		resp = request("DELETE", "/api/index/"+vecIndexName2, body)
		assert.Equal(t, http.StatusOK, resp.Code)
		if resp.Code != http.StatusOK {
			bd, _ := io.ReadAll(resp.Body)
			t.Error(string(bd))
		}
		checkExists(t)
	})
}

func checkExists(t *testing.T) {
	var exists bool
	var err error
	if config.Global.StorageType == "oss" {
		exists, err = directory.OssExists(vecIndexName1)
	} else if config.Global.StorageType == "s3" {
		exists, err = directory.S3Exists(vecIndexName1)
	}
	assert.Nil(t, err)
	assert.False(t, exists)
	_, err = os.Stat(path.Join(config.Global.DataPath, vecIndexName1))
	exists = os.IsExist(err)
	assert.False(t, exists)
}

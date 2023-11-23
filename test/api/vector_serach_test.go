package api

import (
	"bytes"
	"net/http"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

var vecIndexName = "paper"

var vecIndexMeta = `
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
}
`

var documents = `{ "index" : {"_index" : "paper" } } 
{"paper-id": "001","title-vec":[10.2,10.40,9.5,22.2]}
{ "index" : {"_index" : "paper" } } 
{"paper-id": "002","title-vec":[10.2,11.40,9.5,22.2]}
{ "index" : {"_index" : "paper" } } 
{"paper-id": "003","title-vec":[10.2,12.40,9.5,22.2]}`

var searchParam = `
{
    "query_field":"title-vec",
    "k":3,
    "return_fields":["paper-id"],
    "vector":[10.2,10.39,9.5,22.2],
    "_source": false
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
		config.Global.EnableWal = true
		config.Global.StorageType = "disk"
	}()

	core.InitVecIndexManager()

	t.Run("init metaData for vectorSearch", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		body.WriteString(vecIndexMeta)
		resp := request("PUT", "/api/index/"+vecIndexName, body)
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
		resp := request("POST", "/es/"+vecIndexName+"/_search", body)
		assert.Equal(t, http.StatusOK, resp.Code)
		var result = &meta.SearchResponse{}
		b := resp.Body.Bytes()
		_ = json.Unmarshal(b, &result)
		assert.Equal(t, 1, result.Hits.Total.Value)
	})

	t.Run("search for vector", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		body.WriteString(searchParam)
		resp := request("POST", "/api/"+vecIndexName+"/_search/vector", body)
		assert.Equal(t, http.StatusOK, resp.Code)
		var result = &meta.SearchResponse{}
		b := resp.Body.Bytes()
		_ = json.Unmarshal(b, &result)
		assert.Equal(t, 3, result.Hits.Total.Value)
	})

	t.Run("delete by query", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		body.WriteString(deleteByQuery)
		resp := request("POST", "/es/"+vecIndexName+"/_delete_by_query", body)
		assert.Equal(t, http.StatusOK, resp.Code)
	})

	t.Run("search for vector", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		body.WriteString(searchParam)
		resp := request("POST", "/api/"+vecIndexName+"/_search/vector", body)
		assert.Equal(t, http.StatusOK, resp.Code)
		var result = &meta.SearchResponse{}
		b := resp.Body.Bytes()
		_ = json.Unmarshal(b, &result)
		assert.Equal(t, 2, result.Hits.Total.Value)
	})

	t.Run("delete Index", func(t *testing.T) {
		body := bytes.NewBuffer(nil)
		resp := request("DELETE", "/api/index/"+vecIndexName, body)
		assert.Equal(t, http.StatusOK, resp.Code)
		checkExists(t)
	})
}

func checkExists(t *testing.T) {
	var exists bool
	var err error
	if config.Global.StorageType == "oss" {
		exists, err = directory.OssExists(vecIndexName)
	} else if config.Global.StorageType == "s3" {
		exists, err = directory.S3Exists(vecIndexName)
	}
	assert.Nil(t, err)
	assert.False(t, exists)
	_, err = os.Stat(path.Join(config.Global.DataPath, vecIndexName))
	exists = os.IsExist(err)
	assert.False(t, exists)
}

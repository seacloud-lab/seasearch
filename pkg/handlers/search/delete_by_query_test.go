package search

import (
	"net/http/httptest"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"

	"github.com/zincsearch/zincsearch/pkg/ider"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store"
	"github.com/zincsearch/zincsearch/test/utils"
)

type arg struct {
	doc    map[string]interface{}
	query  string
	params map[string]string
}

type body struct {
	is       string
	contains string
}

type success struct {
	outcome    bool
	statusCode int
	body       body
}

type failure struct {
	statusCode int
	body       body
}

type want struct {
	success success
	failure failure
}

func TestDeleteByQuery(t *testing.T) {
	tests := []struct {
		name string
		arg  arg
		want want
	}{
		{
			name: "should delete matched documents",
			arg: arg{
				doc: map[string]interface{}{
					"name": "zinc",
				},
				query: `{"query":{"match":{"name":"zinc"}},"size":10}`,
				params: map[string]string{
					"target": "TestDeleteByQuery.index",
				},
			},
			want: want{
				success: success{
					outcome:    true,
					statusCode: 200,
					body: body{
						contains: `"time_out":false,"total":1,"deleted":1,"batches":0,"version_conflicts":0,"noops":0,"failures":[],"retries":{"bulk":0,"search":0},"throttled_millis":0,"requests_per_second":-1,"throttled_until_millis":0}`,
					},
				},
			},
		},
		{
			name: "should return bad request with invalid json body",
			arg: arg{
				doc: map[string]interface{}{
					"name": "zinc",
				},
				query: `invalid { json }`,
				params: map[string]string{
					"target": "TestDeleteByQuery.index",
				},
			},
			want: want{
				failure: failure{
					statusCode: 400,
					body: body{
						is: `{"error":"invalid character 'i' looking for beginning of value"}`,
					},
				},
			},
		},
		{
			name: "should return bad request when no matching indices are found",
			arg: arg{
				doc: map[string]interface{}{
					"name": "zinc",
				},
				query: `{"query":{"match":{"name":"zinc"}},"size":10}`,
				params: map[string]string{
					"target": "noneMatchingIndex",
				},
			},
			want: want{
				failure: failure{
					statusCode: 400,
					body: body{
						is: `{"error":"index noneMatchingIndex does not exists: key not found"}`,
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const indexName = "TestDeleteByQuery.index"
			index, err := store.Create(&meta.IndexSimple{Name: indexName})
			if !assert.NoError(t, err) {
				return
			}
			defer func() {
				assert.NoError(t, store.Delete(indexName))
			}()
			id := ider.Generate()
			err = index.CreateDocument(id, test.arg.doc, false)
			index.Close()
			if !assert.NoError(t, err) {
				return
			}

			c, w := utils.NewGinContext()
			utils.SetGinRequestData(c, test.arg.query)
			utils.SetGinRequestParams(c, test.arg.params)
			DeleteByQuery(c)

			if test.want.success.outcome {
				assertHTTPResponse(t, w, test.want.success.statusCode, test.want.success.body)
				index, err := store.Open(indexName)
				if !assert.NoError(t, err) {
					return
				}
				defer index.Close()
				assertZeruResultQuery(t, index, test.arg.query)
			} else {
				assertHTTPResponse(t, w, test.want.failure.statusCode, test.want.failure.body)
			}

		})

	}
}

func assertHTTPResponse(t *testing.T, w *httptest.ResponseRecorder, statusCode int, body body) {
	assert.Equal(t, w.Code, statusCode)
	if body.is != "" {
		assert.Equal(t, w.Body.String(), body.is)
	} else {
		assert.Contains(t, w.Body.String(), body.contains)
	}
}

func assertZeruResultQuery(t *testing.T, index *store.Index, query string) {
	var request meta.ZincQuery
	if !assert.NoError(t, json.Unmarshal([]byte(query), &request)) {
		return
	}
	search, serr := index.Search(&request)
	if !assert.NoError(t, serr) {
		return
	}
	assert.Equal(t, 0, search.Hits.Total.Value)
}

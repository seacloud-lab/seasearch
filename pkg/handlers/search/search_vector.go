package search

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"golang.org/x/sync/errgroup"
)

func SearchVector(c *gin.Context) {
	indexName := c.Param("target")
	query := &core.VectorQuery{K: 10, Nprobe: 1}
	if err := zutils.GinBindJSON(c, query); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	indexNameList := strings.Split(indexName, ",")
	for _, name := range indexNameList {
		if !cluster.AssignCheck(name) {
			zutils.GinRenderJSON(c, http.StatusNotAcceptable, meta.HTTPResponseError{Error: core.ErrIndexServerMismatch.Error()})
			return
		}
	}
	eg := &errgroup.Group{}
	mutex := sync.Mutex{}

	var result = &meta.SearchResponse{Hits: meta.Hits{Hits: []meta.Hit{}}}
	for _, index := range indexNameList {
		name := index
		eg.Go(func() error {
			zincIndex, ok := core.GetIndex(name)
			if !ok {
				return fmt.Errorf("vector search error: index %s not found", name)
			}
			zincIndex.GetMappings()
			mappings := zincIndex.GetMappings()
			prop, ok := mappings.Properties[query.QueryField]
			if !ok {
				return fmt.Errorf("vector search error: field %s not found in mapping", query.QueryField)
			}
			if prop.Type != "vector" {
				return fmt.Errorf("vector search error: field %s is not vector field", query.QueryField)
			}
			if prop.Dims != len(query.Vector) {
				return fmt.Errorf("vector search error: invalid query vector, the dims should be %d", prop.Dims)
			}
			rsp, err := core.VectorSearch(zincIndex, mappings, query)
			if err != nil {
				return err
			}
			mutex.Lock()
			result.Hits.Total.Value += rsp.Hits.Total.Value
			result.Hits.MaxScore = math.Max(result.Hits.MaxScore, rsp.Hits.MaxScore)
			result.Hits.Hits = append(result.Hits.Hits, rsp.Hits.Hits...)
			mutex.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	if len(indexNameList) > 1 {
		sort.Slice(result.Hits.Hits, func(i, j int) bool {
			return result.Hits.Hits[i].Score > result.Hits.Hits[j].Score
		})
	}
	zutils.GinRenderJSON(c, http.StatusOK, result)
}

type recallResult struct {
	Recall float32 `json:"recall"`
}
type recallRequest struct {
	Field      string `json:"field"`
	Nprobe     int    `json:"nprobe"`
	QueryCount int    `json:"query_count"`
	K          int    `json:"k"`
}

func VectorRecall(c *gin.Context) {
	indexName := c.Param("target")

	if !cluster.AssignCheck(indexName) {
		zutils.GinRenderJSON(c, http.StatusNotAcceptable, meta.HTTPResponseError{Error: core.ErrIndexServerMismatch.Error()})
		return
	}

	zincIndex, ok := core.GetIndex(indexName)
	if !ok {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("vector search error: index %s not found", indexName).Error()})
		return
	}
	request := &recallRequest{
		Nprobe:     5,
		QueryCount: 100,
		K:          10,
	}
	err := zutils.GinBindJSON(c, request)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	if request.QueryCount <= 0 {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("invalid query count").Error()})
		return
	}
	if request.Nprobe <= 0 {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("invalid nprobe").Error()})
		return
	}
	if request.K <= 0 {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("invalid k").Error()})
		return
	}

	fieldName := request.Field
	mappings := zincIndex.GetMappings()
	prop, ok := mappings.Properties[fieldName]
	if !ok {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("vector search error: field %s not found in mapping", fieldName).Error()})
		return
	}
	if prop.Type != "vector" {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("vector search error: field %s is not vector field", fieldName).Error()})
		return
	}
	recall, err := core.VectorRecall(zincIndex, fieldName, request.QueryCount, int64(request.K),
		request.Nprobe)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	var result = &recallResult{
		Recall: recall,
	}
	zutils.GinRenderJSON(c, http.StatusOK, result)

}

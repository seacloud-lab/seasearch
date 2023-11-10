package search

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"golang.org/x/sync/errgroup"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
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

	var result = &meta.SearchResponse{}
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
			rsp, err := core.VectorSearch(zincIndex, mappings, query)
			if err != nil {
				return err
			}
			mutex.Lock()
			result.Hits.Total.Value += rsp.Hits.Total.Value
			result.Hits.MaxScore = math.Max(result.Hits.MaxScore, rsp.Hits.MaxScore)
			result.Hits.Hits = append(result.Hits.Hits, rsp.Hits.Hits...)
			result.Shards.Total += rsp.Shards.Total
			result.Shards.Skipped += rsp.Shards.Skipped
			result.Shards.Failed += rsp.Shards.Failed
			result.Shards.Successful += rsp.Shards.Successful
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

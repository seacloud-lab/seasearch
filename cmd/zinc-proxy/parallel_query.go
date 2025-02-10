package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/handlers/search"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"golang.org/x/sync/errgroup"
)

// Check if the parallel search is applicable,
// if the index second shard count is less than threshold,
// the request will be forwarded directly to the corresponding node.
func checkAndDoParallelQuery(c *gin.Context, index string, body io.ReadCloser, query *meta.ZincQuery) {
	indexMeta, err := metadata.Index.Get(index)
	if err != nil {
		if errors.Is(err, errors.ErrKeyNotFound) {
			zutils.GinRenderJSON(c, http.StatusNotFound, meta.HTTPResponseError{Error: err.Error()})
			return
		}
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	// only process single shard
	if indexMeta.ShardNum != 1 {
		// rewind body
		c.Request.Body = body
		directForwarding(c)
		return
	}

	auth := c.Request.Header.Get("Authorization")

	secondShardList := make([]*meta.IndexSecondShard, 0)
	for _, shard := range indexMeta.Shards {
		secondShardList = append(secondShardList, shard.Shards...)
	}

	// directly forward
	if len(secondShardList) < conf.General.IndexParallelQueryThreshold {
		// rewind body
		c.Request.Body = body
		directForwarding(c)
		return
	}
	nodeList := GetNodeList()
	if len(nodeList) == 0 {
		zutils.GinRenderJSON(c, http.StatusBadGateway, meta.HTTPResponseError{Error: "there is not available query node"})
		return
	}

	if len(nodeList) == 1 {
		// rewind body
		c.Request.Body = body
		directForwarding(c)
		return
	}

	// key node addr -> secondShard ids
	partialQueryMap := make(map[string][]int)
	for _, secondShard := range secondShardList {
		remainder := int(secondShard.ID) % len(nodeList)
		addr := nodeList[remainder].addr

		if ids, ok := partialQueryMap[addr]; ok {
			ids = append(ids, int(secondShard.ID))
			partialQueryMap[addr] = ids
		} else {
			partialQueryMap[addr] = []int{int(secondShard.ID)}
		}
	}

	var clientErr *HttpClientError
	result, err := execParallelQuery(index, partialQueryMap, query, auth)
	if errors.As(err, &clientErr) {
		zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
		return
	} else if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	zutils.GinRenderJSON(c, http.StatusOK, result)
}

func execParallelQuery(index string, secondShardQueryMap map[string][]int, query *meta.ZincQuery, auth string) (*meta.SearchResponse, error) {
	var eg errgroup.Group
	eg.SetLimit(6)

	var (
		result = &meta.SearchResponse{}
		mutex  = sync.Mutex{}
	)

	for addr, secondShardIds := range secondShardQueryMap {
		addr := addr
		secondShardIds := secondShardIds
		eg.Go(
			func() error {
				req := search.ParallelQueryRequest{
					Index:          index,
					SecondShardIds: secondShardIds,
					Query:          query,
				}
				reqBody, _ := json.Marshal(req)
				var res *meta.SearchResponse
				err := fetchHTTP(http.MethodPost, addr, "/api/internal/partial_query", "", bytes.NewBuffer(reqBody), &res, auth, true)
				if err != nil {
					return err
				}

				mutex.Lock()
				result.Hits.Total.Value += res.Hits.Total.Value
				result.Hits.MaxScore = math.Max(result.Hits.MaxScore, res.Hits.MaxScore)
				result.Hits.Hits = append(result.Hits.Hits, res.Hits.Hits...)
				result.Took = res.Took
				result.Shards.Total += res.Shards.Total
				result.Shards.Skipped += res.Shards.Skipped
				result.Shards.Failed += res.Shards.Failed
				result.Shards.Successful += res.Shards.Successful
				mutex.Unlock()
				return nil
			},
		)
	}

	err := eg.Wait()
	return result, err
}

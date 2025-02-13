package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
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
	result, err := execSingleParallelQuery(index, partialQueryMap, query, auth)
	if errors.As(err, &clientErr) {
		zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
		return
	} else if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	zutils.GinRenderJSON(c, http.StatusOK, result)
}

func execSingleParallelQuery(index string, secondShardQueryMap map[string][]int, query *meta.ZincQuery, auth string) (*meta.SearchResponse, error) {
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

// checkMultiIndexParallelQuery will Filter out the indexes that should execute parallel search,
// and assign the indexes and second shards according to the query nodes.
func checkMultiIndexParallelQuery(indexes []string) (map[string]map[string][]int, map[string]struct{}, error) {
	// node -> index -> second ids
	nodeIndexMap := make(map[string]map[string][]int)
	parallelIndex := make(map[string]struct{})

	nodeList := GetNodeList()
	if len(nodeList) == 0 {
		return nodeIndexMap, parallelIndex, nil
	}
	for _, index := range indexes {
		indexMeta, err := metadata.Index.Get(index)
		if err != nil {
			return nil, nil, err
		}
		// only process single shard
		if indexMeta.ShardNum != 1 {
			continue
		}
		secondShardList := make([]*meta.IndexSecondShard, 0)
		for _, shard := range indexMeta.Shards {
			secondShardList = append(secondShardList, shard.Shards...)
		}

		if len(secondShardList) < conf.General.IndexParallelQueryThreshold {
			continue
		}
		parallelIndex[index] = struct{}{}

		for _, secondShard := range secondShardList {
			remainder := int(secondShard.ID) % len(nodeList)
			addr := nodeList[remainder].addr

			if indexMp, ok := nodeIndexMap[addr]; ok {
				if ids, ok := indexMp[index]; ok {
					ids = append(ids, int(secondShard.ID))
					indexMp[index] = ids
				} else {
					indexMp[index] = []int{int(secondShard.ID)}
				}
			} else {
				nodeIndexMap[addr] = map[string][]int{
					index: {int(secondShard.ID)},
				}
			}
		}
	}

	return nodeIndexMap, parallelIndex, nil
}

func execMultiParallelQuery(auth string, parallelNodeMap map[string]map[string][]int, indexQueryMap map[string][]*meta.ZincQuery, stats *zincsearch.UnifiedStats) ([]interface{}, error) {
	var (
		mutex     sync.Mutex
		responses = make([]interface{}, 0)
	)
	var eg errgroup.Group
	eg.SetLimit(6)
	for addr, indexMap := range parallelNodeMap {
		host := addr
		indexMap := indexMap
		eg.Go(func() error {
			var req = search.ParallelMultiQueryRequest{
				Stats: stats,
			}
			var queries = make([]search.MultiSearchQuery, 0, len(indexMap))
			for idx, ids := range indexMap {
				if queryList, ok := indexQueryMap[idx]; ok {
					for _, q := range queryList {
						queries = append(queries, search.MultiSearchQuery{
							Index:          idx,
							SecondShardIds: ids,
							Query:          q,
						})
					}
				}
			}
			req.Queries = queries

			reqBody, _ := json.Marshal(req)
			var res multiSearchRsp
			err := fetchHTTP(http.MethodPost, host, "/api/internal/partial_multi_query", "", bytes.NewBuffer(reqBody), &res, auth, true)
			if err != nil {
				return err
			}
			mutex.Lock()
			responses = append(responses, res.Rsp...)
			mutex.Unlock()
			return nil
		})
	}
	err := eg.Wait()
	return responses, err
}

func parallelGetStatsInfo(auth string, parallelNodeMap map[string]map[string][]int, indexQueryMap map[string][]*meta.ZincQuery) (*zincsearch.UnifiedStats, error) {
	var (
		stats = &zincsearch.UnifiedStats{}
		mutex = sync.Mutex{}
	)
	var eg errgroup.Group
	eg.SetLimit(6)
	for addr, indexMap := range parallelNodeMap {
		host := addr
		indexMap := indexMap
		eg.Go(func() error {
			var req = search.ParallelQueryStatsInfoRequest{}
			req.IndexMap = indexMap
			for index := range indexMap {
				queries := indexQueryMap[index]
				// we use first query for stats info
				if len(queries) > 0 && req.Query == nil {
					req.Query = queries[0]
					break
				}
			}

			reqBody, _ := json.Marshal(req)
			var result *zincsearch.UnifiedStats
			err := fetchHTTP(http.MethodPost, host, "/api/internal/partial_get_stats", "", bytes.NewBuffer(reqBody), &result, auth, true)
			if err != nil {
				return err
			}

			mutex.Lock()
			stats.Merge(result)
			mutex.Unlock()
			return nil
		})
	}
	err := eg.Wait()
	return stats, err
}

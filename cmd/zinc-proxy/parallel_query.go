package main

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/handlers/search"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"golang.org/x/sync/errgroup"
)

func UnifiedSearch(c *gin.Context) {
	var req = search.UnifiedSearchRequest{}
	if err := zutils.GinBindJSON(c, &req); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	if len(req.IndexQueries) == 0 {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "No index is specified."})
		return
	}

	indexList := make([]string, 0, len(req.IndexQueries))
	indexQueryMap := make(map[string]*meta.ZincQuery)
	for _, q := range req.IndexQueries {
		if q.Query == nil {
			zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "require query"})
			return
		}
		indexQueryMap[q.Index] = q.Query
		indexList = append(indexList, q.Index)
	}
	processUnifiedSearch(c, indexList, indexQueryMap)
}

// ProcessUnifiedSearch first determines which indexes each node should process
// then obtains statistics from those nodes,
// and finally performs a query. This can process all indexes, whether or not parallel search is required.
func processUnifiedSearch(c *gin.Context, indexes []string, indexQueryMap map[string]*meta.ZincQuery) {
	auth := c.Request.Header.Get("Authorization")

	// we only use the first query to get statistics info
	var query *meta.ZincQuery
	for _, q := range indexQueryMap {
		query = q
		break
	}

	parallelNodeMp, err := calcNodeIndexMap(indexes)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	var stats *zincsearch.UnifiedStats
	var clientErr *HttpClientError
	// more than one query node, we need collect stats info from all nodes.
	if len(parallelNodeMp) > 1 {
		stats, err = parallelGetStats(auth, parallelNodeMp, query)
		if errors.As(err, &clientErr) {
			zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
			return
		} else if err != nil {
			log.Err(err).Msgf("parallel get stats info err: ")
			zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: "internal server error"})
			return
		}
	}

	res, err := execParallelQueries(auth, parallelNodeMp, stats, indexQueryMap)
	if errors.As(err, &clientErr) {
		zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
		return
	} else if err != nil {
		log.Err(err).Msgf("exec parallel multi search err: ")
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: "internal server error"})
		return
	}
	zutils.GinRenderJSON(c, http.StatusOK, res)
}

// calcNodeIndexMap determines which indexes and second shard searches should be assigned to each node
// and returns the corresponding map.
// If an index needs to be processed in parallel, it will be distributed across multiple nodes,
// with each node handling a subset of the second shards.
// If parallel processing is not needed, the index will be assigned to a single node
// according to the allocation rules.
func calcNodeIndexMap(indexes []string) (map[string]core.PartialIndexes, error) {
	// node -> index -> second ids
	nodeIndexMap := make(map[string]core.PartialIndexes)

	nodeList := GetNodeList()
	if len(nodeList) == 0 {
		return nodeIndexMap, nil
	}
	for _, index := range indexes {
		indexMeta, err := metadata.Index.Get(index)
		if err != nil {
			return nil, err
		}

		secondShardList := make([]*meta.IndexSecondShard, 0)
		for _, shard := range indexMeta.Shards {
			secondShardList = append(secondShardList, shard.Shards...)
		}

		if indexMeta.ShardNum != 1 || len(secondShardList) < conf.General.IndexParallelQueryThreshold {
			// the node will process the full index search.
			addr, err := GetAddrByIndex(index)
			if err != nil {
				return nil, err
			}
			if partialIndexes, ok := nodeIndexMap[addr]; ok {
				partialIndexes[index] = []int{}
			} else {
				nodeIndexMap[addr] = core.CreatePartialIndexes(index, []int{})
			}
			continue
		}

		for _, secondShard := range secondShardList {
			remainder := int(secondShard.ID) % len(nodeList)
			addr := nodeList[remainder].addr
			if partialIndexes, ok := nodeIndexMap[addr]; ok {
				partialIndexes.AddSecondShardId(index, int(secondShard.ID))
			} else {
				nodeIndexMap[addr] = core.CreatePartialIndexes(index, []int{int(secondShard.ID)})
			}
		}
	}
	return nodeIndexMap, nil
}

func parallelGetStats(auth string, parallelNodeMap map[string]core.PartialIndexes, query *meta.ZincQuery) (*zincsearch.UnifiedStats, error) {
	var (
		stats = &zincsearch.UnifiedStats{}
		mutex = sync.Mutex{}
	)
	var eg errgroup.Group
	eg.SetLimit(6)
	for addr, partialIndexes := range parallelNodeMap {
		host := addr
		partialIndexes := partialIndexes
		eg.Go(func() error {
			var req = search.InternalQueryStatsRequest{
				IndexList: partialIndexes,
				Query:     query,
			}
			reqBody, _ := json.Marshal(req)
			var result *zincsearch.UnifiedStats
			err := fetchHTTP(http.MethodPost, host, "/api/internal/get_stats", "", bytes.NewBuffer(reqBody), &result, auth, true)
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

func execParallelQueries(auth string, parallelNodeMap map[string]core.PartialIndexes, stats *zincsearch.UnifiedStats, indexQueryMap map[string]*meta.ZincQuery) (*meta.SearchResponse, error) {
	var (
		mutex sync.Mutex
	)
	var eg errgroup.Group
	var query *meta.ZincQuery
	for _, q := range indexQueryMap {
		if query == nil {
			query = q
			break
		}
	}

	result := &meta.SearchResponse{}
	eg.SetLimit(6)

	for addr, partialIndexes := range parallelNodeMap {
		host := addr
		partialIndexes := partialIndexes
		eg.Go(func() error {
			var req = search.InternalUnifiedSearchRequest{
				Stats: stats,
			}
			for idx, secondIds := range partialIndexes {
				q := indexQueryMap[idx]
				req.IndexQueries = append(req.IndexQueries, core.IndexQueryRequest{
					Index:         idx,
					SecondShardId: secondIds,
					Query:         q,
				})
			}

			reqBody, _ := json.Marshal(req)
			var res meta.SearchResponse
			err := fetchHTTP(http.MethodPost, host, "/api/internal/unified_search", "", bytes.NewBuffer(reqBody), &res, auth, true)
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
		})
	}
	err := eg.Wait()

	sort.Slice(result.Hits.Hits, func(i, j int) bool {
		return result.Hits.Hits[i].Score > result.Hits.Hits[j].Score
	})

	if len(result.Hits.Hits) > query.Size {
		result.Hits.Hits = result.Hits.Hits[:query.Size]
	}

	return result, err
}

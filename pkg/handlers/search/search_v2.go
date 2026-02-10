/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package search

import (
	"bufio"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"

	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/cluster"
	"golang.org/x/sync/errgroup"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

// SearchDSL searches the index for the given http request from end user
//
// @Id Search
// @Summary Search V2 DSL for compatible ES
// @security BasicAuth
// @Tags    Search
// @Accept  json
// @Produce json
// @Param   index  path  string  true  "Index"
// @Param   query  body  meta.ZincQueryForSDK true  "Query"
// @Success 200 {object} meta.SearchResponse
// @Failure 400 {object} meta.HTTPResponseError
// @Router /es/{index}/_search [post]
func SearchDSL(c *gin.Context) {
	indexName := c.Param("target")
	query := &meta.ZincQuery{Size: 10}
	if err := zutils.GinBindJSON(c, query); err != nil {
		log.Printf("handlers.search.searchDSL: %s", err.Error())
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	resp, err := searchIndex(strings.Split(indexName, ","), query)
	if err != nil {
		if errors.Is(err, core.ErrIndexServerMismatch) {
			zutils.GinRenderJSON(c, http.StatusNotAcceptable, meta.HTTPResponseError{Error: err.Error()})
		}
		errors.HandleError(c, err)
		return
	}

	if indexName != "" {
		// TODO: adapt this to allow strings.Split(indexName, ",") slice
		idx, ok := core.ZINC_INDEX_LIST.Get(indexName)
		if ok {
			storageSize := idx.GetStats().StorageSize
			eventData := make(map[string]interface{})
			eventData["search_type"] = "query_dsl"
			eventData["search_index_storage"] = idx.GetStorageType()
			eventData["search_index_size_in_mb"] = storageSize / 1024 / 1024
			eventData["time_taken_to_search_in_ms"] = resp.Took
			eventData["aggregations_count"] = len(query.Aggregations)
			core.Telemetry.Event("search", eventData)
		}
	}

	zutils.GinRenderJSON(c, http.StatusOK, resp)
}

// MultipleSearch like bulk searches
//
// @Id MSearch
// @Summary Search V2 MultipleSearch for compatible ES
// @security BasicAuth
// @Tags    Search
// @Accept  plain
// @Produce json
// @Param   query  body  string  true  "Query"
// @Success 200 {object} meta.SearchResponse
// @Failure 400 {object} meta.HTTPResponseError
// @Router /es/_msearch [post]
func MultipleSearch(c *gin.Context) {
	indexName := c.Param("target")
	defaultIndexNames := make([]string, 0)
	if indexName != "" {
		defaultIndexNames = strings.Split(indexName, ",")
	}

	responses := make([]interface{}, 0)
	searches := make([]search, 0)
	// Prepare to read the entire raw text of the body
	scanner := bufio.NewScanner(c.Request.Body)
	defer c.Request.Body.Close()

	maxCapacityPerLine := config.Global.MaxDocumentSize
	buf := make([]byte, maxCapacityPerLine)
	scanner.Buffer(buf, maxCapacityPerLine)

	indexNames := make([]string, 0)
	nextLineIsData := false

	var doc map[string]interface{}
	var err error
	for scanner.Scan() { // Read each line
		if nextLineIsData {
			nextLineIsData = false
			query := &meta.ZincQuery{Size: 10}
			if err = json.Unmarshal(scanner.Bytes(), &query); err != nil {
				log.Error().Msgf("handlers.search.MultipleSearch.json.Unmarshal: %s, err %s", scanner.Text(), err.Error())
				responses = append(responses, &meta.SearchResponse{Error: err.Error()})
				continue
			}
			// search query
			names := make([]string, 0, len(indexNames))
			names = append(names, indexNames...)
			searches = append(searches, search{
				indexNames: names,
				query:      query,
			})
		} else {
			nextLineIsData = true
			indexNames = indexNames[:0]
			if err = json.Unmarshal(scanner.Bytes(), &doc); err != nil {
				log.Error().Msgf("handlers.search.MultipleSearch.json.Unmarshal: %s, err %s", scanner.Text(), err.Error())
				continue
			}
			if v, ok := doc["index"]; ok {
				switch v := v.(type) {
				case string:
					indexNames = append(indexNames, v)
				case []interface{}:
					for _, v := range v {
						indexNames = append(indexNames, v.(string))
					}
				}
			} else {
				indexNames = append(indexNames, defaultIndexNames...)
			}
		}
	}

	responses = append(responses, multiSearchIndex(searches)...)

	zutils.GinRenderJSON(c, http.StatusOK, gin.H{"responses": responses})
}

type search struct {
	indexNames []string
	query      *meta.ZincQuery
}

func multiSearchIndex(searches []search) []interface{} {
	eg := errgroup.Group{}
	eg.SetLimit(config.Global.Shard.GoroutineNum)
	var res = make([]interface{}, len(searches))

	for i, s := range searches {
		s := s
		i := i
		eg.Go(func() error {
			rsp, err := searchIndex(s.indexNames, s.query)
			if err != nil {
				res[i] = &meta.SearchResponse{Error: err.Error()}
			} else {
				res[i] = rsp
			}
			return nil
		})
	}

	_ = eg.Wait()
	return res
}

func searchIndex(indexNames []string, query *meta.ZincQuery) (*meta.SearchResponse, error) {
	indexName := ""
	if len(indexNames) > 0 {
		indexName = indexNames[0]
	}
	var err error
	var resp *meta.SearchResponse
	if indexName == "" || strings.HasSuffix(indexName, "*") || strings.HasPrefix(indexName, "*") || len(indexNames) > 1 {
		resp, err = core.MultiSearch(indexNames, query)
	} else {
		if !cluster.AssignCheck(indexName) {
			return nil, core.ErrIndexServerMismatch
		}
		index, exists := core.GetIndex(indexName)
		if !exists {
			return nil, fmt.Errorf("index %s does not exists", indexName)
		}
		resp, err = index.Search(query)
	}
	return resp, err
}

type UnifiedSearchRequest struct {
	IndexQueries []IndexQueryRequest `json:"index_queries"`
}

type IndexQueryRequest struct {
	Index string          `json:"index"`
	Query *meta.ZincQuery `json:"query"`
}

// UnifiedSearch queries the specified indexes using the provided queries,
// leveraging the statistics information collected from all indexes.
// It's only called in single-node mode.
func UnifiedSearch(c *gin.Context) {
	var req = UnifiedSearchRequest{}
	if err := zutils.GinBindJSON(c, &req); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	if len(req.IndexQueries) == 0 {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "No index is specified."})
		return
	}
	IndexList := make([]string, 0, len(req.IndexQueries))
	var query *meta.ZincQuery
	for _, q := range req.IndexQueries {
		if q.Query == nil {
			zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "require query"})
			return
		}
		if query == nil {
			query = q.Query
		}
		IndexList = append(IndexList, q.Index)
	}

	stats, err := core.QueryStatsInfo(IndexList, query)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	var result = &meta.SearchResponse{
		Hits: meta.Hits{
			Hits: []meta.Hit{},
		},
	}
	var mutex = sync.Mutex{}

	var eg errgroup.Group
	eg.SetLimit(config.Global.Shard.GoroutineNum)
	for _, q := range req.IndexQueries {
		q := q
		eg.Go(func() error {
			index, exists := core.GetIndex(q.Index)
			// some index not exists, we ignore
			if !exists {
				return nil
			}
			res, err := index.SearchWithStats(q.Query, stats)
			if err != nil {
				return err
			}
			mutex.Lock()
			result.Hits.Total.Value += res.Hits.Total.Value
			result.Hits.MaxScore = math.Max(result.Hits.MaxScore, res.Hits.MaxScore)
			result.Hits.Hits = append(result.Hits.Hits, res.Hits.Hits...)
			result.Took = res.Took
			mutex.Unlock()
			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	sort.Slice(result.Hits.Hits, func(i, j int) bool {
		return result.Hits.Hits[i].Score > result.Hits.Hits[j].Score
	})
	if len(result.Hits.Hits) > query.Size {
		result.Hits.Hits = result.Hits.Hits[:query.Size]
	}

	zutils.GinRenderJSON(c, http.StatusOK, result)
}

type InternalUnifiedSearchRequest struct {
	Stats        *zincsearch.UnifiedStats `json:"stats"`
	IndexQueries []core.IndexQueryRequest `json:"index_queries"`
}

// InternalUnifiedSearch performs a unified search using the provided statistics information.
// Each index may execute a different query, but all results will be merged and sorted by score.
// All the queries must be the same for all indexes,
// except for the filter conditions in them, which doesn't affect scores.
// It's called by SeaSearch proxy to implement parallel unified search.
func InternalUnifiedSearch(c *gin.Context) {
	var request InternalUnifiedSearchRequest
	if err := zutils.GinBindJSON(c, &request); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	var query *meta.ZincQuery
	for _, q := range request.IndexQueries {
		query = q.Query
		break
	}

	// There is no statistical information, it means
	// the query is only executed on the current node.
	// We need collect local stats info
	if request.Stats == nil {
		var err error
		indexMp := make(core.PartialIndexes)
		for _, idx := range request.IndexQueries {
			indexMp[idx.Index] = idx.SecondShardId
		}
		request.Stats, err = core.QueryStatsInfoWithSecondShardIds(indexMp, query)
		if err != nil {
			if errors.Is(err, errors.ErrKeyNotFound) {
				zutils.GinRenderJSON(c, http.StatusNotFound, meta.HTTPResponseError{Error: err.Error()})
				return
			}
			log.Err(err).Msgf("get local stats info err: ")
			zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
			return
		}
	}

	var result = &meta.SearchResponse{}
	var mutex = sync.Mutex{}

	var eg errgroup.Group
	eg.SetLimit(config.Global.Shard.GoroutineNum)
	for _, indexReq := range request.IndexQueries {
		index, err := core.GetZincIndexFromMetadata(indexReq.Index)
		if err != nil {
			if errors.Is(err, errors.ErrKeyNotFound) {
				zutils.GinRenderJSON(c, http.StatusNotFound, meta.HTTPResponseError{Error: err.Error()})
				return
			}
			log.Err(err).Msgf("get local index metadata err: ")
			zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
			return
		}
		secondIds := indexReq.SecondShardId
		q := indexReq.Query
		eg.Go(func() error {
			res, err := index.PartialSearch(secondIds, q, request.Stats)
			if err != nil {
				return err
			}
			mutex.Lock()
			result.Hits.Total.Value += res.Hits.Total.Value
			result.Hits.MaxScore = math.Max(result.Hits.MaxScore, res.Hits.MaxScore)
			result.Hits.Hits = append(result.Hits.Hits, res.Hits.Hits...)
			result.Took = res.Took
			mutex.Unlock()
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		if errors.Is(err, errors.ErrKeyNotFound) {
			zutils.GinRenderJSON(c, http.StatusNotFound, meta.HTTPResponseError{Error: err.Error()})
			return
		}
		log.Err(err).Msgf("exec local parallel multi search err: ")
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	sort.Slice(result.Hits.Hits, func(i, j int) bool {
		return result.Hits.Hits[i].Score > result.Hits.Hits[j].Score
	})
	if len(result.Hits.Hits) > query.Size {
		result.Hits.Hits = result.Hits.Hits[:query.Size]
	}

	zutils.GinRenderJSON(c, http.StatusOK, result)
}

type InternalQueryStatsRequest struct {
	IndexList core.PartialIndexes `json:"index_list"`
	Query     *meta.ZincQuery     `json:"Query"`
}

// InternalQueryStats collects statistical information from the local node based on the provided query.
// If the input index does not include any secondary shard IDs, statistics for the entire index are collected.
// It's called by SeaSearch proxy to implement parallel unified search.
func InternalQueryStats(c *gin.Context) {
	var req *InternalQueryStatsRequest
	if err := zutils.GinBindJSON(c, &req); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	stats, err := core.QueryStatsInfoWithSecondShardIds(req.IndexList, req.Query)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	zutils.GinRenderJSON(c, http.StatusOK, stats)
}

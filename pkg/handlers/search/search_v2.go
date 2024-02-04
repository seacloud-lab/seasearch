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
	"net/http"
	"strings"

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

	unifyScore := strings.ToUpper(c.Query("unify_score")) == "TRUE"

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

	if unifyScore {
		responses = append(responses, unifiedMultiSearch(searches)...)
	} else {
		responses = append(responses, multiSearchIndex(searches)...)
	}

	zutils.GinRenderJSON(c, http.StatusOK, gin.H{"responses": responses})
}

type PreSearchRequest struct {
	IndexList []string        `json:"index_list"`
	Query     *meta.ZincQuery `json:"query"`
}

func QueryStats(c *gin.Context) {
	var req *PreSearchRequest
	if err := zutils.GinBindJSON(c, &req); err != nil {
		log.Printf("handlers.search.preSearch: %s", err.Error())
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	stats, err := core.QueryStatsInfo(req.IndexList, req.Query)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	zutils.GinRenderJSON(c, http.StatusOK, stats)
}

func MultiSearchWithStatistics(c *gin.Context) {
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

	// the first line is stats info
	scanner.Scan()
	var stats *zincsearch.UnifiedStats
	if err := json.Unmarshal(scanner.Bytes(), &stats); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, gin.H{"error": "invalid stats"})
		log.Error().Msgf("handlers.search.MultipleSearch.json.Unmarshal: %s, err %s", scanner.Text(), err.Error())
		return
	}

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

	responses = append(responses, unifiedSearchWithStats(searches, stats)...)
	zutils.GinRenderJSON(c, http.StatusOK, gin.H{"responses": responses})
}

type search struct {
	indexNames []string
	query      *meta.ZincQuery
}

func multiSearchIndex(searches []search) []interface{} {
	eg := errgroup.Group{}
	eg.SetLimit(config.Global.Shard.LoadObjGoroutineNum)
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

func unifiedMultiSearch(searches []search) []interface{} {
	if len(searches) == 0 {
		return []interface{}{}
	}

	allNameMap := make(map[string]struct{})
	for _, search := range searches {
		indexNames, err := core.GetMatchedIndexNames(search.indexNames)
		if err != nil {
			return []interface{}{&meta.SearchResponse{Error: err.Error()}}
		}
		for _, idx := range indexNames {
			allNameMap[idx] = struct{}{}
		}
	}
	allNames := make([]string, 0, len(allNameMap))
	for key := range allNameMap {
		allNames = append(allNames, key)
	}
	// for multi query, there may be many queries,
	// we simplify the processing and use only one of them to get the term list, which is sufficient for our scenario.
	stats, err := core.QueryStatsInfo(allNames, searches[0].query)
	if err != nil {
		return []interface{}{&meta.SearchResponse{Error: err.Error()}}
	}

	return unifiedSearchWithStats(searches, stats)
}

func unifiedSearchWithStats(searches []search, stats *zincsearch.UnifiedStats) []interface{} {
	eg := errgroup.Group{}
	eg.SetLimit(config.Global.Shard.LoadObjGoroutineNum)
	var res = make([]interface{}, len(searches))

	for i, s := range searches {
		s := s
		i := i
		eg.Go(func() error {
			searchIndexNames, err := core.GetMatchedIndexNames(s.indexNames)
			if err != nil {
				res[i] = &meta.SearchResponse{Error: err.Error()}
				return nil
			}
			rsp, err := core.MultiSearchWithStats(searchIndexNames, stats, s.query)
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

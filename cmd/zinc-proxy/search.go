package main

import (
	"bufio"
	"bytes"

	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/handlers/search"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
	"golang.org/x/sync/errgroup"
)

func SearchDSL(c *gin.Context) {
	indexName := c.Param("target")
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	body := io.NopCloser(bytes.NewBuffer(bodyBytes))
	_ = c.Request.Body.Close()

	query := &meta.ZincQuery{Size: 10}
	if err := json.Unmarshal(bodyBytes, query); err != nil {
		log.Printf("handlers.search.searchDSL: %s", err.Error())
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	var result = &meta.SearchResponse{}
	var indexNameList []string

	if indexName == "" {
		// search all indexes
		var err error
		indexNameList, err = metadata.Index.ListNames(0, 0)
		if err != nil {
			zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
			return
		}
	} else {
		indexNameList = strings.Split(indexName, ",")
	}

	// we don't support multi index aggregations
	if len(indexNameList) > 1 && len(query.Aggregations) > 0 {
		err := errors.New(errors.ErrorTypeInvalidArgument, "invalid multi index aggregations")
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	if len(indexNameList) == 1 {
		checkAndDoParallelQuery(c, indexName, body, query)
		return
	}

	// addr -> index names
	reqMap := make(map[string][]string)

	for _, index := range indexNameList {
		addr, err := GetAddrByIndex(index)
		if err != nil {
			c.JSON(http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
			return
		}
		if list, ok := reqMap[addr]; ok {
			list = append(list, index)
			reqMap[addr] = list
		} else {
			reqMap[addr] = []string{index}
		}
	}

	var (
		clientErr *HttpClientError
		auth      = c.Request.Header.Get("Authorization")
		mutex     = sync.Mutex{}
	)
	var eg errgroup.Group
	eg.SetLimit(6)
	for addr, data := range reqMap {
		host := addr
		indexes := data
		eg.Go(func() error {
			path := fmt.Sprintf("/es/%s/_search", strings.Join(indexes, ","))
			var res = meta.SearchResponse{}
			err := fetchHTTP(http.MethodPost, host, path, "", body, &res, auth, true)
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
	err = eg.Wait()
	if errors.As(err, &clientErr) {
		zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
		return
	} else if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	zutils.GinRenderJSON(c, http.StatusOK, result)
}

type multiSearchRsp struct {
	Rsp []interface{} `json:"responses"`
}

func MultiSearch(c *gin.Context) {
	indexName := c.Param("target")
	defaultIndexNames := make([]string, 0)
	if indexName != "" {
		defaultIndexNames = strings.Split(indexName, ",")
	}
	unifyScore := strings.ToUpper(c.Query("unify_score")) == "TRUE"

	// indexName -> queries
	indexQueryMap := make(map[string][]*meta.ZincQuery)

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
				continue
			}
			// search query
			for _, name := range indexNames {
				if list, ok := indexQueryMap[name]; ok {
					list = append(list, query)
					indexQueryMap[name] = list
				} else {
					list = []*meta.ZincQuery{query}
					indexQueryMap[name] = list
				}
			}
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

	// addr -> []index
	addrIndexMap := make(map[string][]string)

	for index := range indexQueryMap {
		addr, err := GetAddrByIndex(index)
		if err != nil {
			zutils.GinRenderJSON(c, http.StatusOK, &meta.HTTPResponseError{Error: err.Error()})
			return
		}
		if list, ok := addrIndexMap[addr]; ok {
			list = append(list, index)
			addrIndexMap[addr] = list
		} else {
			list = []string{index}
			addrIndexMap[addr] = list
		}
	}

	if unifyScore {
		unifiedMultiSearch(c, addrIndexMap, indexQueryMap)
		return
	}

	var (
		mutex     sync.Mutex
		clientErr *HttpClientError
		auth      = c.Request.Header.Get("Authorization")
		responses = make([]interface{}, 0)
	)

	var eg errgroup.Group
	eg.SetLimit(6)
	for addr, indexList := range addrIndexMap {
		host := addr
		indexes := indexList
		eg.Go(func() error {
			buf := bytes.Buffer{}
			for _, index := range indexes {
				queries := indexQueryMap[index]
				for _, query := range queries {
					indexLine, _ := json.Marshal(map[string]string{
						"index": index,
					})
					_, err := fmt.Fprintln(&buf, string(indexLine))
					if err != nil {
						return err
					}
					queryLine, _ := json.Marshal(query)
					_, err = fmt.Fprintln(&buf, string(queryLine))
					if err != nil {
						return err
					}
				}
			}
			var result multiSearchRsp

			err := fetchHTTP(http.MethodPost, host, c.Request.URL.Path, "", &buf, &result, auth, true)
			if err != nil {
				return err
			}

			mutex.Lock()
			responses = append(responses, result.Rsp...)
			mutex.Unlock()

			return nil
		})
	}
	err = eg.Wait()
	if errors.As(err, &clientErr) {
		zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
		return
	} else if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	zutils.GinRenderJSON(c, http.StatusOK, gin.H{"responses": responses})
}

func unifiedMultiSearch(c *gin.Context, addrIndexMap map[string][]string, indexQueryMap map[string][]*meta.ZincQuery) {
	var (
		mutex     sync.Mutex
		clientErr *HttpClientError
		responses = make([]interface{}, 0)
		auth      = c.Request.Header.Get("Authorization")
		stats     = &zincsearch.UnifiedStats{
			FieldStats: map[string]*zincsearch.FieldStats{},
		}
	)

	// get stats info
	var eg errgroup.Group
	eg.SetLimit(6)
	for addr, indexList := range addrIndexMap {
		host := addr
		indexes := indexList
		eg.Go(func() error {
			var req = &search.PreSearchRequest{
				IndexList: indexes,
			}
			for _, index := range indexes {
				queries := indexQueryMap[index]
				// we use first query for stats info
				if len(queries) > 0 && req.Query == nil {
					req.Query = queries[0]
					break
				}
			}
			reqBody, err := json.Marshal(req)
			if err != nil {
				return err
			}
			var result *zincsearch.UnifiedStats

			err = fetchHTTP(http.MethodPost, host, "/api/internal/get_stats", "", bytes.NewBuffer(reqBody), &result, auth, true)
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
	if errors.As(err, &clientErr) {
		zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
		return
	} else if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	statsBytes, err := json.Marshal(stats)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	// do search
	for addr, indexList := range addrIndexMap {
		host := addr
		indexes := indexList
		eg.Go(func() error {
			buf := bytes.Buffer{}
			// first line is stats data
			_, err := fmt.Fprintln(&buf, string(statsBytes))
			if err != nil {
				return err
			}

			for _, index := range indexes {
				queries := indexQueryMap[index]
				for _, query := range queries {
					indexLine, _ := json.Marshal(map[string]string{
						"index": index,
					})
					_, err := fmt.Fprintln(&buf, string(indexLine))
					if err != nil {
						return err
					}
					queryLine, _ := json.Marshal(query)
					_, err = fmt.Fprintln(&buf, string(queryLine))
					if err != nil {
						return err
					}
				}
			}

			var result multiSearchRsp
			err = fetchHTTP(http.MethodPost, host, "/api/internal/unify_search", "", &buf, &result, auth, true)
			if err != nil {
				return err
			}

			mutex.Lock()
			responses = append(responses, result.Rsp...)
			mutex.Unlock()

			return nil
		})
	}
	err = eg.Wait()

	if errors.As(err, &clientErr) {
		zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
		return
	} else if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	zutils.GinRenderJSON(c, http.StatusOK, gin.H{"responses": responses})
}

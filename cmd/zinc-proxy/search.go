package main

import (
	"bufio"
	"bytes"
	"sort"

	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/errors"
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
	_ = c.Request.Body.Close()

	query := &meta.ZincQuery{Size: 10}
	if err := json.Unmarshal(bodyBytes, query); err != nil {
		log.Printf("handlers.search.searchDSL: %s", err.Error())
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

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

	indexQueryMap := make(map[string]*meta.ZincQuery)
	for _, idx := range indexNameList {
		indexQueryMap[idx] = query
	}
	processUnifiedSearch(c, indexNameList, indexQueryMap)
}

type multiSearchRsp struct {
	Rsp []*meta.SearchResponse `json:"responses"`
}

type queryWithOrder struct {
	order int
	q     *meta.ZincQuery
}

func MultiSearch(c *gin.Context) {
	indexName := c.Param("target")
	defaultIndexNames := make([]string, 0)
	if indexName != "" {
		defaultIndexNames = strings.Split(indexName, ",")
	}

	// indexName -> queries
	indexQueryMap := make(map[string][]*queryWithOrder)

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
	order := 0
	for scanner.Scan() { // Read each line
		if nextLineIsData {
			nextLineIsData = false
			query := &meta.ZincQuery{Size: 10}
			if err = json.Unmarshal(scanner.Bytes(), &query); err != nil {
				log.Error().Msgf("handlers.search.MultipleSearch.json.Unmarshal: %s, err %s", scanner.Text(), err.Error())
				continue
			}
			order++
			// search query
			for _, name := range indexNames {
				if list, ok := indexQueryMap[name]; ok {
					list = append(list, &queryWithOrder{
						q:     query,
						order: order,
					})
					indexQueryMap[name] = list
				} else {
					list = []*queryWithOrder{{
						q:     query,
						order: order,
					}}
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

	var (
		mutex     sync.Mutex
		clientErr *HttpClientError
		auth      = c.Request.Header.Get("Authorization")
		responses = make([]*meta.SearchResponse, 0)
	)

	var eg errgroup.Group
	eg.SetLimit(6)

	for addr, indexList := range addrIndexMap {
		host := addr
		indexes := indexList
		eg.Go(func() error {
			buf := bytes.Buffer{}
			queryOrders := make([]int, 0)
			for _, index := range indexes {
				queries := indexQueryMap[index]
				for _, q := range queries {
					queryOrders = append(queryOrders, q.order)
				}

				for _, query := range queries {
					indexLine, _ := json.Marshal(map[string]string{
						"index": index,
					})
					_, err := fmt.Fprintln(&buf, string(indexLine))
					if err != nil {
						return err
					}
					queryLine, _ := json.Marshal(query.q)
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

			for i, rsp := range result.Rsp {
				if len(queryOrders) > 0 && i < len(queryOrders) {
					rsp.Order = queryOrders[i]
				}
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

	sort.Slice(responses, func(i, j int) bool {
		return responses[i].Order < responses[j].Order
	})

	zutils.GinRenderJSON(c, http.StatusOK, gin.H{"responses": responses})
}

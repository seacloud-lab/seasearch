package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
	"golang.org/x/sync/errgroup"
	"io"
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

	bodyBytes, _ := json.Marshal(query)
	body := io.NopCloser(bytes.NewBuffer(bodyBytes))
	if len(indexNameList) == 1 {
		// rewind body
		c.Request.Body = body
		directForwarding(c)
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
			path := fmt.Sprintf("/api/%s/_search/vector", strings.Join(indexes, ","))
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
	err := eg.Wait()
	if errors.As(err, &clientErr) {
		zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
		return
	} else if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	if len(reqMap) > 1 {
		sort.Slice(result.Hits.Hits, func(i, j int) bool {
			return result.Hits.Hits[i].Score > result.Hits.Hits[j].Score
		})
	}

	zutils.GinRenderJSON(c, http.StatusOK, result)

}

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/handlers/document"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"golang.org/x/sync/errgroup"
)

func Bulk(c *gin.Context) {
	target := c.Param("target")
	defer c.Request.Body.Close()

	nodeDataMap, err := processBody(target, c.Request.Body)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	ret := &meta.HTTPResponseRecordCount{Message: "bulk data inserted"}

	var (
		mutex     sync.Mutex
		clientErr *HttpClientError
		auth      = c.Request.Header.Get("Authorization")
	)
	var eg errgroup.Group
	eg.SetLimit(6)
	for addr, data := range nodeDataMap {
		addr := addr
		data := data
		eg.Go(func() error {
			buf := bytes.Buffer{}
			for _, line := range data {
				_, err := fmt.Fprintln(&buf, line)
				if err != nil {
					return err
				}
			}
			result := meta.HTTPResponseRecordCount{}
			err := fetchHTTP(http.MethodPost, addr, c.Request.URL.Path, "", &buf, &result, auth, true)
			if err != nil {
				return err
			}

			mutex.Lock()
			ret.RecordCount += result.RecordCount
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

	zutils.GinRenderJSON(c, http.StatusOK, ret)
}

func EsBulk(c *gin.Context) {
	target := c.Param("target")
	defer c.Request.Body.Close()

	ret := &document.BulkResponse{Items: []map[string]document.BulkResponseItem{}}
	nodeDataMap, err := processBody(target, c.Request.Body)
	if err != nil {
		ret.Error = err.Error()
		startTime := time.Now()
		ret.Took = int(time.Since(startTime) / time.Millisecond)
		zutils.GinRenderJSON(c, http.StatusOK, ret)
		return
	}

	var (
		mutex     sync.Mutex
		clientErr *HttpClientError
		auth      = c.Request.Header.Get("Authorization")
	)
	var eg errgroup.Group
	eg.SetLimit(6)
	for addr, data := range nodeDataMap {
		addr := addr
		data := data
		eg.Go(func() error {
			buf := bytes.Buffer{}
			for _, line := range data {
				_, err := fmt.Fprintln(&buf, line)
				if err != nil {
					return err
				}
			}
			result := document.BulkResponse{Items: []map[string]document.BulkResponseItem{}}
			err := fetchHTTP(http.MethodPost, addr, c.Request.URL.Path, "", &buf, &result, auth, true)
			if err != nil {
				return err
			}

			mutex.Lock()
			ret.Items = append(ret.Items, result.Items...)
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
	zutils.GinRenderJSON(c, http.StatusOK, ret)
}

// processBody
// read request body, and find out which index the action belongs to
func processBody(target string, body io.Reader) (map[string][]string, error) {
	// Prepare to read the entire raw text of the body
	scanner := bufio.NewScanner(body)

	// Set 1 MB max per line. docs at - https://pkg.go.dev/bufio#pkg-constants
	// This is the max size of a line in a file that we will process
	maxCapacityPerLine := conf.General.MaxDocumentSize
	buf := make([]byte, maxCapacityPerLine)
	scanner.Buffer(buf, maxCapacityPerLine)

	nextLineIsData := false
	lastLineMetaData := make(map[string]interface{})

	// index -> line bytes
	indexLineMap := make(map[string][]string)

	var doc map[string]interface{}
	var err error
	for scanner.Scan() { // Read each line
		for k := range doc {
			delete(doc, k)
		}
		if err = json.Unmarshal(scanner.Bytes(), &doc); err != nil {
			log.Error().Msgf("bulk.json.Unmarshal: %s, err %s", scanner.Text(), err.Error())
			continue
		}

		// write data line into indexDocMap
		if nextLineIsData {
			nextLineIsData = false
			indexName := lastLineMetaData["_index"].(string)
			if lines, ok := indexLineMap[indexName]; !ok {
				lines = append(lines, scanner.Text())
				indexLineMap[indexName] = lines
			} else {
				lines = append(lines, scanner.Text())
				indexLineMap[indexName] = lines
			}
		} else { // write metadata line into indexDocMap
			for k, v := range doc {
				vm, ok := v.(map[string]interface{})
				if !ok {
					return nil, errors.New("bulk index data format error")
				}
				for k := range lastLineMetaData {
					delete(lastLineMetaData, k)
				}
				if k == "index" || k == "create" || k == "update" {
					nextLineIsData = true
					if vm["_index"] != "" { // if index is specified in metadata then it overtakes the index in the query path
						lastLineMetaData["_index"] = vm["_index"]
					} else {
						lastLineMetaData["_index"] = target
					}
					if lastLineMetaData["_index"] == "" {
						return nil, errors.New("bulk index data format error")
					}

					indexName := lastLineMetaData["_index"].(string)
					if lines, ok := indexLineMap[indexName]; !ok {
						lines = append(lines, scanner.Text())
						indexLineMap[indexName] = lines
					} else {
						lines = append(lines, scanner.Text())
						indexLineMap[indexName] = lines
					}
				} else if k == "delete" {
					nextLineIsData = false
					indexName := target
					if vm["_index"] != "" { // if index is specified in metadata then it overtakes the index in the query path
						indexName = vm["_index"].(string)
					}
					if indexName == "" {
						return nil, errors.New("bulk index data format error")
					}

					if lines, ok := indexLineMap[indexName]; !ok {
						lines = make([]string, 0)
						lines = append(lines, scanner.Text())
						indexLineMap[indexName] = lines
					} else {
						lines = append(lines, scanner.Text())
						indexLineMap[indexName] = lines
					}
				} else {
					continue
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	nodeAddrDocMap := make(map[string][]string)
	for index, lines := range indexLineMap {
		addr, err := GetAddrByIndex(index)
		if err != nil {
			return nil, err
		}
		if list, ok := nodeAddrDocMap[addr]; !ok {
			nodeAddrDocMap[addr] = lines
		} else {
			list = append(list, lines...)
			nodeAddrDocMap[addr] = list
		}
	}

	return nodeAddrDocMap, nil
}

func BulkV2(c *gin.Context) {
	data, err := io.ReadAll(c.Request.Body)
	c.Request.Body.Close()
	if err != nil {
		c.JSON(http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	var body meta.JSONIngest
	err = json.Unmarshal(data, &body)
	if err != nil {
		c.JSON(http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	target := c.Param("target")
	if target == "" {
		target = body.Index
	}
	if target == "" {
		err := errors.New("index should not be empty")
		c.JSON(http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	// After changing the request body, we need to reset the Content-Length
	// field and header. Otherwise, the request will be broken.
	contentLength := int64(len(data))
	c.Request.Body = io.NopCloser(bytes.NewReader(data))
	c.Request.ContentLength = contentLength
	c.Request.Header.Set("Content-Length", strconv.FormatInt(contentLength, 10))
	addr, err := GetAddrByIndex(target)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	u, err := url.Parse(addr)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	proxyPool.Get(u).ServeHTTP(c.Writer, c.Request)

}

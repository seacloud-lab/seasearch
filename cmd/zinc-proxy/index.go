package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/zincsearch/zincsearch/pkg/core"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/zutils"

	"github.com/gin-gonic/gin"
	"golang.org/x/sync/errgroup"
)

type IndexListResponse struct {
	List []*core.Index `json:"list"`
	Page *meta.Page    `json:"page"`
}

func List(c *gin.Context) {
	page := meta.NewPage(c)
	sortBy := c.DefaultQuery("sort_by", "name")
	desc, _ := strconv.ParseBool(c.DefaultQuery("desc", "false"))
	name := c.DefaultQuery("name", "")

	items, err := core.GetZincIndexesFromMetadata()
	if err != nil {
		c.JSON(http.StatusInternalServerError, &meta.HTTPResponseError{Error: err.Error()})
		return
	}

	if len(name) > 0 {
		var res []*core.Index
		for _, item := range items {
			if strings.Contains(item.GetName(), name) {
				res = append(res, item)
			}
		}
		items = res
	}

	switch sortBy {
	case "doc_num":
		sort.Slice(items, func(i, j int) bool {
			if desc {
				return items[i].GetStats().DocNum > items[j].GetStats().DocNum
			} else {
				return items[i].GetStats().DocNum < items[j].GetStats().DocNum
			}
		})
	case "shard_num":
		sort.Slice(items, func(i, j int) bool {
			if desc {
				return items[i].GetShardNum() > items[j].GetShardNum()
			} else {
				return items[i].GetShardNum() < items[j].GetShardNum()
			}
		})
	case "storage_size":
		sort.Slice(items, func(i, j int) bool {
			if desc {
				return items[i].GetStats().StorageSize > items[j].GetStats().StorageSize
			} else {
				return items[i].GetStats().StorageSize < items[j].GetStats().StorageSize
			}
		})
	case "storage_type":
		sort.Slice(items, func(i, j int) bool {
			if desc {
				return items[i].GetStorageType() > items[j].GetStorageType()
			} else {
				return items[i].GetStorageType() < items[j].GetStorageType()
			}
		})
	case "wal_size":
		sort.Slice(items, func(i, j int) bool {
			if desc {
				return items[i].GetWALSize() > items[j].GetWALSize()
			} else {
				return items[i].GetWALSize() < items[j].GetWALSize()
			}
		})
	case "name":
		fallthrough
	default:
		sort.Slice(items, func(i, j int) bool {
			if desc {
				return items[i].GetName() > items[j].GetName()
			} else {
				return items[i].GetName() < items[j].GetName()
			}
		})
	}

	page.Total = int64(len(items))
	startIndex, endIndex := page.GetStartEndIndex()
	if endIndex > 0 {
		items = items[startIndex:endIndex]
	} else {
		items = []*core.Index{}
	}

	c.JSON(http.StatusOK, IndexListResponse{
		List: items,
		Page: page,
	})
}

func IndexNameList(c *gin.Context) {
	queryName := strings.ToLower(c.DefaultQuery("name", ""))
	var items []string

	names, err := metadata.Index.ListNames(0, 0)
	if err != nil {
		c.JSON(http.StatusInternalServerError, &meta.HTTPResponseError{Error: err.Error()})
		return
	}
	items = make([]string, 0)

	if queryName == "" {
		items = names
	} else {
		for _, name := range names {
			if strings.Contains(strings.ToLower(name), queryName) {
				items = append(items, name)
			}
		}
	}

	count := 30
	if len(items) > count {
		c.JSON(http.StatusOK, items[0:count])
	} else {
		c.JSON(http.StatusOK, items)
	}
}

func Create(c *gin.Context) {
	data, err := io.ReadAll(c.Request.Body)
	c.Request.Body.Close()
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	var newIndex meta.IndexSimple
	err = json.Unmarshal(data, &newIndex)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	indexName := c.Param("target")
	if newIndex.Name == "" && indexName != "" {
		newIndex.Name = indexName
	}

	if newIndex.Name == "" {
		err := errors.New("index.name should be not empty")
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	c.Request.Body = io.NopCloser(bytes.NewReader(data))
	host, err := GetAddrByIndex(newIndex.Name)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	u := &url.URL{Scheme: "http", Host: host}
	proxyPool.Get(u).ServeHTTP(c.Writer, c.Request)
}

func Delete(c *gin.Context) {
	indexNames := c.Param("target")
	if indexNames == "" {
		c.JSON(http.StatusBadRequest, meta.HTTPResponseError{Error: "index name cannot be empty"})
		return
	}

	indexes := strings.Split(indexNames, ",")

	// addr -> index names
	reqMap := make(map[string][]string)

	for _, indexName := range indexes {
		addr, err := GetAddrByIndex(indexName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
			return
		}
		if list, ok := reqMap[addr]; ok {
			list = append(list, indexName)
			reqMap[addr] = list
		} else {
			reqMap[addr] = []string{addr}
		}
	}

	var (
		clientErr *HttpClientError
		auth      = c.Request.Header.Get("Authorization")
	)
	var eg errgroup.Group
	eg.SetLimit(6)
	for addr, data := range reqMap {
		host := addr
		data := data
		eg.Go(func() error {
			path := fmt.Sprintf("/api/index/%s", strings.Join(data, ","))
			err := fetchHTTP(http.MethodDelete, host, path, "", nil, nil, auth, false)
			if err != nil {
				return err
			}
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

	c.JSON(http.StatusOK, meta.HTTPResponse{
		Message: "deleted",
	})
}

func Exists(c *gin.Context) {
	indexName := c.Param("target")

	_, err := metadata.Index.Get(indexName)
	if err != nil {
		if errors.Is(err, zincerrors.ErrKeyNotFound) {
			c.Status(http.StatusNotFound)
			return
		}
		c.Status(http.StatusInternalServerError)
		return
	}

	c.JSON(http.StatusOK, meta.HTTPResponse{Message: "ok"})
}

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"golang.org/x/sync/errgroup"
)

func SearchVector(c *gin.Context) {
	indexName := c.Param("target")
	query := &core.VectorQuery{K: 10, Nprobe: 1}
	if err := zutils.GinBindJSON(c, query); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	auth := c.Request.Header.Get("Authorization")
	var clientErr *HttpClientError
	zincIndex, err := core.GetZincIndexFromMetadata(indexName)
	if errors.As(err, &clientErr) {
		zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
		return
	} else if err != nil {
		log.Err(err).Msgf("get zinc index metadata err: ")
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: "internal server error"})
		return
	}

	zincIndex.GetMappings()
	mappings := zincIndex.GetMappings()
	prop, ok := mappings.Properties[query.QueryField]
	if !ok {
		err := fmt.Errorf("vector search error: field %s not found in mapping", query.QueryField)
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	if prop.Type != "vector" {
		err := fmt.Errorf("vector search error: field %s is not vector field", query.QueryField)
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	if prop.Dims != len(query.Vector) {
		err := fmt.Errorf("vector search error: invalid query vector, the dims should be %d", prop.Dims)
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	vecIndexMeta, ok := zincIndex.GetVecIndex(query.QueryField)
	if !ok {
		err := fmt.Errorf("vector search error: vector index %s not found", query.QueryField)
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	nodeSegmentsMap, needParallelSearch, err := calcNodeSegmentsMap(indexName, vecIndexMeta)
	if err != nil {
		log.Err(err).Msgf("exec parallel multi vector search err: ")
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: "internal server error"})
		return
	}
	if !needParallelSearch {
		bodyBytes, _ := json.Marshal(query)
		body := io.NopCloser(bytes.NewBuffer(bodyBytes))
		// rewind body
		c.Request.Body = body
		directForwarding(c)
		return
	}

	vecResult := make(map[string]float32)
	mutex := sync.Mutex{}

	eg := errgroup.Group{}
	eg.SetLimit(6)
	for addr, segments := range nodeSegmentsMap {
		host := addr
		segments := segments
		eg.Go(func() error {
			var req = core.InternalVectorQuery{
				Index:      indexName,
				QueryField: query.QueryField,
				K:          query.K,
				Vector:     query.Vector,
				Nprobe:     query.Nprobe,
				Segments:   segments,
			}

			reqBody, _ := json.Marshal(req)
			var res core.InternalVectorSearchResponse
			err := fetchHTTP(http.MethodPost, host, "/api/internal/vector_search", "", bytes.NewBuffer(reqBody), &res, auth, true)
			if err != nil {
				return err
			}
			mutex.Lock()
			for docId, distance := range res.MatchedIds {
				vecResult[docId] = distance
			}
			mutex.Unlock()
			return nil
		})
	}

	err = eg.Wait()
	if errors.As(err, &clientErr) {
		zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
		return
	} else if err != nil {
		log.Err(err).Msgf("exec parallel vector search err: ")
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: "internal server error"})
		return
	}

	// get original document by ids
	docIdSlice := make([]string, len(vecResult))
	i := 0
	for docId := range vecResult {
		docIdSlice[i] = docId
		i++
	}

	// query document by Id
	idQuery := &meta.ZincQuery{
		Query: &meta.Query{
			Ids: &meta.IdsQuery{
				Values: docIdSlice,
			},
		},
		Size: len(docIdSlice),
	}

	parallelNodeMp, err := calcNodeIndexMap([]string{indexName})
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	res, err := execParallelQueries(auth, parallelNodeMp, nil, map[string]*meta.ZincQuery{
		indexName: idQuery,
	})
	if errors.As(err, &clientErr) {
		zutils.GinRenderJSON(c, clientErr.Code, meta.HTTPResponseError{Error: clientErr.Error()})
		return
	} else if err != nil {
		log.Err(err).Msgf("exec parallel multi search err: ")
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: "internal server error"})
		return
	}

	var maxScore float64 = 0
	for i := range res.Hits.Hits {
		h1d, ok := vecResult[res.Hits.Hits[i].ID]
		if !ok {
			// that's impassable
			res.Hits.Hits[i].Score = 0
			continue
		}
		score := float64(1 / (1 + h1d))
		res.Hits.Hits[i].Score = score
		maxScore = math.Max(maxScore, score)
	}

	// sort by score
	sort.Slice(res.Hits.Hits, func(i, j int) bool {
		return res.Hits.Hits[i].Score > res.Hits.Hits[j].Score
	})
	res.Hits.MaxScore = maxScore
	if len(res.Hits.Hits) > int(query.K) {
		res.Hits.Hits = res.Hits.Hits[:query.K]
	}
	zutils.GinRenderJSON(c, http.StatusOK, res)
}

// calcNodeSegmentsMap determines which vector segment searches should be assigned to each node
// and returns the corresponding map.
func calcNodeSegmentsMap(indexName string, vecIndex *meta.VecIndex) (map[string][]int, bool, error) {
	// node -> segment ids
	nodeSegmentMap := make(map[string][]int)
	nodeList := GetNodeList()
	if len(nodeList) == 0 || vecIndex.TargetType != vector.IvfPQ || len(vecIndex.Segments) < conf.General.ParallelQueryThreshold {
		return nodeSegmentMap, false, nil
	}

	nodes, err := getQueryNodes(nodeList, indexName)
	if err != nil {
		return nodeSegmentMap, false, err
	}

	for _, segment := range vecIndex.Segments {
		addr := nodes.Next().addr
		if segments, ok := nodeSegmentMap[addr]; ok {
			segments = append(segments, segment.Id)
			nodeSegmentMap[addr] = segments
		} else {
			nodeSegmentMap[addr] = []int{int(segment.Id)}
		}
	}

	return nodeSegmentMap, true, nil
}

package search

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

func SearchVector(c *gin.Context) {
	indexName := c.Param("target")
	query := &core.VectorQuery{K: 10, Nprobe: 1}
	if err := zutils.GinBindJSON(c, query); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	zincIndex, ok := core.GetIndex(indexName)
	if !ok {
		err := fmt.Errorf("vector search error: index %s not found", indexName)
		zutils.GinRenderJSON(c, http.StatusNotFound, meta.HTTPResponseError{Error: err.Error()})
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
	rsp, err := core.VectorSearch(zincIndex, mappings, query)
	if err != nil {
		log.Err(err).Msgf("vector search error:")
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: "internal server error"})
		return
	}

	zutils.GinRenderJSON(c, http.StatusOK, rsp)
}

type recallResult struct {
	Recall float32 `json:"recall"`
}
type recallRequest struct {
	Field      string `json:"field"`
	Nprobe     int    `json:"nprobe"`
	QueryCount int    `json:"query_count"`
	K          int    `json:"k"`
}

func VectorRecall(c *gin.Context) {
	indexName := c.Param("target")

	if !cluster.AssignCheck(indexName) {
		zutils.GinRenderJSON(c, http.StatusNotAcceptable, meta.HTTPResponseError{Error: core.ErrIndexServerMismatch.Error()})
		return
	}

	zincIndex, ok := core.GetIndex(indexName)
	if !ok {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("vector search error: index %s not found", indexName).Error()})
		return
	}
	request := &recallRequest{
		Nprobe:     5,
		QueryCount: 100,
		K:          10,
	}
	err := zutils.GinBindJSON(c, request)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	if request.QueryCount <= 0 {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("invalid query count").Error()})
		return
	}
	if request.Nprobe <= 0 {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("invalid nprobe").Error()})
		return
	}
	if request.K <= 0 {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("invalid k").Error()})
		return
	}

	fieldName := request.Field
	mappings := zincIndex.GetMappings()
	prop, ok := mappings.Properties[fieldName]
	if !ok {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("vector search error: field %s not found in mapping", fieldName).Error()})
		return
	}
	if prop.Type != "vector" {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: fmt.Errorf("vector search error: field %s is not vector field", fieldName).Error()})
		return
	}
	recall, err := core.VectorRecall(zincIndex, fieldName, request.QueryCount, int64(request.K),
		request.Nprobe)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	var result = &recallResult{
		Recall: recall,
	}
	zutils.GinRenderJSON(c, http.StatusOK, result)

}

// InternalSearchVector queries the specified vector index segments and just returns the matched doc ids and the distances.
// The proxy needs to perform an additional internal search based on the document IDs,
// because the assignments of the vector index may differ from the assignments of the full-text index.
func InternalSearchVector(c *gin.Context) {
	query := &core.InternalVectorQuery{}
	if err := zutils.GinBindJSON(c, query); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	res, err := core.InternalVectorSearch(query)
	if err != nil {
		log.Err(err).Msgf("internal vector search error:")
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: "internal server error"})
		return
	}
	zutils.GinRenderJSON(c, http.StatusOK, res)
}

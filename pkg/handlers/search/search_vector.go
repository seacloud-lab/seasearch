package search

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

func SearchVector(c *gin.Context) {
	query := &store.VectorQuery{K: 10, Nprobe: 1}
	if err := zutils.GinBindJSON(c, query); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	if query.K <= 0 || query.Nprobe <= 0 {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "vector query k and nprobe must be positive"})
		return
	}

	indexNames := strings.Split(c.Param("target"), ",")
	var response *meta.SearchResponse
	var err error
	if len(indexNames) > 1 || strings.Contains(indexNames[0], "*") {
		// This is local owner-only matching, not cross-node vector fanout.
		response, err = store.MultiVectorSearch(c.Request.Context(), indexNames, query)
	} else {
		index, openErr := store.Open(indexNames[0])
		if openErr != nil {
			vectorError(c, openErr)
			return
		}
		defer index.Close()
		if !validateVectorField(c, index, query.QueryField, len(query.Vector)) {
			return
		}
		response, err = index.VectorSearch(c.Request.Context(), query)
	}
	if err != nil {
		vectorError(c, err)
		return
	}
	zutils.GinRenderJSON(c, http.StatusOK, response)
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
	request := &recallRequest{Nprobe: 5, QueryCount: 100, K: 10}
	if err := zutils.GinBindJSON(c, request); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	if request.QueryCount <= 0 || request.Nprobe <= 0 || request.K <= 0 {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "recall query count, k and nprobe must be positive"})
		return
	}
	index, err := store.Open(c.Param("target"))
	if err != nil {
		vectorError(c, err)
		return
	}
	defer index.Close()
	if !validateVectorField(c, index, request.Field, -1) {
		return
	}
	recall, err := index.VectorRecall(c.Request.Context(), request.Field, request.QueryCount, int64(request.K), request.Nprobe)
	if err != nil {
		vectorError(c, err)
		return
	}
	zutils.GinRenderJSON(c, http.StatusOK, recallResult{Recall: recall})
}

// Validate a detached mapping snapshot for HTTP errors. Store revalidates the
// authoritative state under its metadata lock before using any resources.
func validateVectorField(c *gin.Context, index *store.Index, field string, dimensions int) bool {
	mappings := index.GetMappings()
	if mappings == nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "index has no mappings"})
		return false
	}
	prop, ok := mappings.GetProperty(field)
	if !ok || prop.Type != "vector" {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "field is not a vector field"})
		return false
	}
	if dimensions >= 0 && prop.Dims != dimensions {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "invalid query vector dimensions"})
		return false
	}
	return true
}

func vectorError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, zincerrors.ErrIndexServerMismatch):
		zutils.GinRenderJSON(c, http.StatusNotAcceptable, meta.HTTPResponseError{Error: err.Error()})
	case errors.Is(err, store.ErrIndexNotFound):
		zutils.GinRenderJSON(c, http.StatusNotFound, meta.HTTPResponseError{Error: "index not found"})
	default:
		log.Error().Err(err).Msg("vector operation failed")
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: "internal server error"})
	}
}

package search

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

// DeleteByQuery deletes the documents selected by one query's from/size/sort window.
//
// @Id DeleteByQuery
// @Summary Deletes documents selected by the query window
// @security BasicAuth
// @Tags    Search
// @Accept  json
// @Produce json
// @Param   index  path  string  true  "Index"
// @Param   query  body  meta.ZincQueryForSDK true  "Query"
// @Success 200 {object} meta.HTTPResponseDeleteByQuery
// @Failure 400 {object} meta.HTTPResponseError
// @Router /es/{index}/_delete_by_query [post]
func DeleteByQuery(c *gin.Context) {
	start := time.Now()
	query := &meta.ZincQuery{Size: 10}
	if err := zutils.GinBindJSON(c, query); err != nil {
		log.Printf("handlers.search.searchDSL: %s", err.Error())
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	indexName := c.Param("target")
	index, err := store.Open(indexName)
	if err != nil {
		if errors.Is(err, store.ErrIndexNotFound) {
			err = fmt.Errorf("index %s does not exists: %w", indexName, zincerrors.ErrKeyNotFound)
		}
		zincerrors.HandleError(c, err)
		return
	}
	defer index.Close()

	result, err := index.DeleteByQuery(query)
	if result == nil {
		zincerrors.HandleError(c, err)
		return
	}
	deleted := len(result.IDs)
	failures := []string{}
	if err != nil {
		log.Error().Err(err).Str("index", indexName).Msg("delete by query failed")
		// Batch/save errors do not provide exact per-document outcomes. Report
		// every target as unconfirmed, even though some changes may have applied.
		failures = result.IDs
		deleted = 0
	}

	zutils.GinRenderJSON(c, http.StatusOK, meta.HTTPResponseDeleteByQuery{
		Took:             time.Since(start).Milliseconds(),
		TimedOut:         false,
		Total:            result.Matched,
		Deleted:          deleted,
		Batches:          0,
		VersionConflicts: 0,
		Noops:            0,
		Failures:         failures,
		Retries: meta.HttpRetriesResponse{
			Bulk:   0,
			Search: 0,
		},
		ThrottledMillis:      0,
		RequestsPerSecond:    -1,
		ThrottledUntilMillis: 0,
	})
}

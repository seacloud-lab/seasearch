package index

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store"
	"github.com/zincsearch/zincsearch/pkg/vector"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

// SealVectorIndex accepts a current-state maintenance hint, not a completed seal.
// The worker acquires its own owner reference and revalidates the current field.
func SealVectorIndex(c *gin.Context) {
	index, err := store.Open(c.Param("target"))
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, store.ErrIndexNotFound) {
			code = http.StatusNotFound
		} else if errors.Is(err, zincerrors.ErrIndexServerMismatch) {
			code = http.StatusNotAcceptable
		}
		zutils.GinRenderJSON(c, code, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	defer index.Close()

	field := c.Param("field")
	mappings := index.GetMappings()
	if mappings == nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "field is not an IVFPQ vector field"})
		return
	}
	prop, ok := mappings.GetProperty(field)
	if !ok || prop.Type != "vector" || prop.VecIndexType != vector.IVFPQ {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "field is not an IVFPQ vector field"})
		return
	}
	if err := store.RequestVectorSeal(index.GetName(), field); err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, store.ErrVectorMaintenanceStopped) {
			code = http.StatusServiceUnavailable
		}
		zutils.GinRenderJSON(c, code, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	zutils.GinRenderJSON(c, http.StatusOK, meta.HTTPResponse{Message: "ok"})
}

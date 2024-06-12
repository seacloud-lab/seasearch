package index

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

func SealVectorIndex(c *gin.Context) {
	indexName := c.Param("target")
	fieldName := c.Param("field")

	index, ok := core.GetIndex(indexName)
	if !ok {
		zutils.GinRenderJSON(c, http.StatusNotFound, meta.HTTPResponseError{Error: "index not exists"})
		return
	}
	mp := index.GetMappings()
	prop, ok := mp.Properties[fieldName]
	if !ok || prop.Type != "vector" {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "field is not vector"})
		return
	}

	err := core.SealIndex(index.GetName(), fieldName)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}
}

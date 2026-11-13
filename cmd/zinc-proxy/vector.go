package main

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

// SearchVector forwards the complete request to one concrete index's owner.
// Vector segments are never queried on non-owner nodes.
func SearchVector(c *gin.Context) {
	if target := c.Param("target"); target == "" || strings.ContainsAny(target, ",*?") {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "vector search requires one concrete index name"})
		return
	}
	directForwarding(c)
}

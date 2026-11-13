/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package index

import (
	"net/http"

	"github.com/zincsearch/zincsearch/pkg/store"

	"github.com/gin-gonic/gin"

	"github.com/zincsearch/zincsearch/pkg/catalog"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

// @Id GetMapping
// @Summary Get index mappings
// @security BasicAuth
// @Tags    Index
// @Produce json
// @Param   index  path  string  true  "Index"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} meta.HTTPResponseError
// @Router /api/{index}/_mapping [get]
func GetMapping(c *gin.Context) {
	indexName := c.Param("target")
	index, err := catalog.GetIndexMetadata(indexName)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "index " + indexName + " does not exists"})
		return
	}

	// format mappings
	mappings := index.Mappings

	zutils.GinRenderJSON(c, http.StatusOK, gin.H{index.Name: gin.H{"mappings": mappings}})
}

// @Id SetMapping
// @Summary Set index mappings
// @security BasicAuth
// @Tags    Index
// @Accept  json
// @Produce json
// @Param   index   path  string        true  "Index"
// @Param   mapping body  meta.Mappings true  "Mapping"
// @Success 200 {object} meta.HTTPResponse
// @Failure 400 {object} meta.HTTPResponseError
// @Failure 500 {object} meta.HTTPResponseError
// @Router /api/{index}/_mapping [put]
func SetMapping(c *gin.Context) {
	indexName := c.Param("target")
	if indexName == "" {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: "index.name should be not empty"})
		return
	}

	var mappingRequest map[string]interface{}
	if err := zutils.GinBindJSON(c, &mappingRequest); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	index, _, err := store.OpenOrCreate(indexName)
	if err != nil {
		if zincerrors.Is(err, zincerrors.ErrIndexServerMismatch) {
			zutils.GinRenderJSON(c, http.StatusNotAcceptable, meta.HTTPResponseError{Error: err.Error()})
			return
		}
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}
	defer index.Close()

	if err := index.AddMappings(mappingRequest); err != nil {
		code := http.StatusInternalServerError
		var configErr *zincerrors.Error
		if zincerrors.As(err, &configErr) {
			code = http.StatusBadRequest
		}
		zutils.GinRenderJSON(c, code, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	zutils.GinRenderJSON(c, http.StatusOK, meta.HTTPResponse{Message: "ok"})
}

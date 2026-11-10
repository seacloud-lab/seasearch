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
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

// @Id CreateIndex
// @Summary Create index
// @security BasicAuth
// @Tags    Index
// @Accept  json
// @Produce json
// @Param   data body meta.IndexSimple true "Index data"
// @Success 200 {object} meta.HTTPResponseIndex
// @Failure 400 {object} meta.HTTPResponseError
// @Router /api/index [post]
func Create(c *gin.Context) {
	var newIndex meta.IndexSimple
	if err := zutils.GinBindJSON(c, &newIndex); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	indexName := c.Param("target")
	err := CreateIndexWorker(&newIndex, indexName)
	if err != nil {
		if errors.Is(err, zincerrors.ErrIndexServerMismatch) {
			zutils.GinRenderJSON(c, http.StatusNotAcceptable, meta.HTTPResponseError{Error: err.Error()})
			return
		}
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	zutils.GinRenderJSON(c, http.StatusOK, meta.HTTPResponseIndex{
		Message: "ok",
		Index:   newIndex.Name,
	})
}

// @Id ESCreateIndex
// @Summary Create index for compatible ES
// @security BasicAuth
// @Tags    Index
// @Produce json
// @Param   index path  string  true  "Index"
// @Param   data  body  meta.IndexSimple true "Index data"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} meta.HTTPResponse
// @Router /es/{index} [put]
func CreateES(c *gin.Context) {
	indexName := c.Param("target")

	var newIndex meta.IndexSimple
	if err := zutils.GinBindJSON(c, &newIndex); err != nil {
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	err := CreateIndexWorker(&newIndex, indexName)
	if err != nil {
		if errors.Is(err, zincerrors.ErrIndexServerMismatch) {
			zutils.GinRenderJSON(c, http.StatusNotAcceptable, meta.HTTPResponseError{Error: err.Error()})
			return
		}
		zutils.GinRenderJSON(c, http.StatusBadRequest, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	zutils.GinRenderJSON(c, http.StatusOK, gin.H{
		"acknowledged":        true,
		"shards_acknowledged": true,
		"index":               newIndex.Name,
	})
}

func CreateIndexWorker(newIndex *meta.IndexSimple, indexName string) error {

	if newIndex.Name == "" && indexName != "" {
		newIndex.Name = indexName
	}

	if newIndex.Name == "" {
		return errors.New("index.name should be not empty")
	}
	index, err := store.Create(newIndex)
	if err != nil {
		return err
	}
	defer index.Close()

	return nil
}

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

package routes

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils/logger"
)

func AccessLog(app *gin.Engine) {
	var accessLogger = logger.SetupAccessLog(config.Global.LogConfig.LogToStd, "seasearch-access.log", config.Global.LogConfig.LogDir, logger.ComponentSeaSearch)
	app.Use(func(c *gin.Context) {
		timeStart := time.Now()
		c.Writer.Header().Set("Zinc", meta.Version)

		c.Next()

		took := time.Since(timeStart).Seconds()
		accessLogger.Info().Msgf("\"%s %s %s\" %d %.3f", c.Request.Method, c.Request.RequestURI, c.Request.Proto, c.Writer.Status(), took)
	})
}

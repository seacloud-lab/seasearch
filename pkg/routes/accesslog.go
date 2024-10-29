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
	"fmt"
	"os"
	"path"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/zutils"

	"github.com/zincsearch/zincsearch/pkg/meta"
)

func AccessLog(app *gin.Engine) {
	var accessLogger zerolog.Logger
	var out *zutils.LogOuter
	if config.Global.LogConfig.SeafileLogToStd || config.Global.LogConfig.SeatableLogToStd {
		out = &zutils.LogOuter{LogToStdout: true, Out: os.Stdout}
	} else {
		file, err := os.OpenFile(path.Join(config.Global.LogConfig.LogDir, "seasearch-access.log"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal().Err(err).Msg("set log output to file error, cannot open file")
		}
		out = &zutils.LogOuter{LogToStdout: false, Out: file}
	}

	writer := zerolog.ConsoleWriter{Out: out, TimeFormat: "[2006-01-02 15:04:05]", NoColor: true}
	writer.FormatLevel = func(i interface{}) string {
		return ""
	}
	writer.FormatMessage = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}
	writer.FormatFieldName = func(i interface{}) string {
		return ""
	}
	writer.FormatFieldValue = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}
	writer.FormatCaller = func(i interface{}) string {
		return ""
	}
	writer.FormatErrFieldName = func(_ interface{}) string {
		return ""
	}
	writer.FormatErrFieldValue = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}
	accessLogger = zerolog.New(writer).With().Timestamp().Logger()

	app.Use(func(c *gin.Context) {
		timeStart := time.Now()
		c.Writer.Header().Set("Zinc", meta.Version)

		c.Next()

		took := time.Since(timeStart).Seconds()
		accessLogger.Printf("- %q - %q -%q - %d - %f", c.Request.Method, c.Request.RequestURI, c.Request.Proto, c.Writer.Status(), took)
	})
}

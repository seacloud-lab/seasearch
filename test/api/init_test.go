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

package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/zincsearch/zincsearch/pkg/auth"
	"github.com/zincsearch/zincsearch/pkg/bluge/analysis/lang/chs"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/ider"
	"github.com/zincsearch/zincsearch/pkg/lru_cache"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/routes"
	"github.com/zincsearch/zincsearch/test/storetest"
	"github.com/zincsearch/zincsearch/test/utils"

	"github.com/gin-gonic/gin"
)

var (
	username = "admin"
	password = "Complexpass#123"
)

var (
	r    *gin.Engine
	once sync.Once
)

func server() *gin.Engine {
	once.Do(func() {
		gin.SetMode(gin.ReleaseMode)
		r = gin.New()
		r.Use(gin.Recovery())
		routes.SetRoutes(r)
	})

	return r
}

func request(method, api string, body io.Reader) *httptest.ResponseRecorder {
	req, _ := http.NewRequest(method, api, body)
	req.SetBasicAuth(username, password)
	w := httptest.NewRecorder()
	server().ServeHTTP(w, req)
	return w
}

func TestMain(m *testing.M) {
	start := func() error {
		// Set before InitConfig: godotenv.Load must not select shared/remote
		// storage from a developer's .env. RunMain owns the temporary DataPath.
		for key, value := range map[string]string{
			"SS_STORAGE_TYPE": "disk", "ZINC_METADATA_STORAGE": "bolt",
			"SS_CLUSTER_ENABLE": "false", "ZINC_TELEMETRY": "false",
			"ZINC_PROFILER": "false", "ZINC_SENTRY": "false",
			"ZINC_BATCH_SIZE": "1024", "ZINC_MAX_RESULTS": "10000",
			"ZINC_SHARD_MAX_SIZE": "1073741824", "ZINC_SHARD_GOROUTINE_NUM": "3",
			"SS_VECTOR_IVFPQ_THRESHOLD": "100000", "SS_VECTOR_HNSW_MAX_LOGS": "10000",
			"ZINC_ENABLE_TEXT_KEYWORD_MAPPING": "false",
		} {
			if err := os.Setenv(key, value); err != nil {
				return err
			}
		}
		config.InitConfig()
		ider.InitIder()
		metadata.InitMetaStorage()
		auth.InitFirstUser()
		chs.InitGse()
		// init lruCache
		lru_cache.Init()
		return storetest.Start()
	}
	stop := func() error {
		storetest.Stop()
		lru_cache.ShutDown()
		return metadata.Close()
	}
	os.Exit(utils.RunMain(m, start, stop))
}

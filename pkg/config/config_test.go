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

package config

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/zincsearch/zincsearch/pkg/zutils"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	t.Run("prepare", func(t *testing.T) {
		os.Setenv("SS_ETCD_ENDPOINTS", "localhost:2379")
		os.Setenv("ZINC_MAX_DOCUMENT_SIZE", "1m")
		os.Setenv("ZINC_WAL_SYNC_INTERVAL", "10s")
	})

	t.Run("check", func(t *testing.T) {
		c := new(config)
		initConfig(c)

		assert.Equal(t, "release", c.GinMode)
		assert.Equal(t, "4080", c.ServerPort)
		assert.Equal(t, "./data", c.DataPath)
		assert.Equal(t, false, c.SentryEnable)
		assert.Equal(t, "https://15b6d9b8be824b44896f32b0234c32b7@o1218932.ingest.sentry.io/6360942", c.SentryDSN) // Add check for default value
		assert.Equal(t, false, c.TelemetryEnable)
		assert.Equal(t, false, c.PrometheusEnable)
		assert.Equal(t, 1000000, c.MaxDocumentSize)

		assert.Equal(t, 1024, c.BatchSize)
		assert.Equal(t, 10000, c.MaxResults)
		assert.Equal(t, 1000, c.AggregationTermsSize)
		assert.Equal(t, int64(10000000000), c.ObjCache.MaxCacheSize)
		assert.Equal(t, "disk", c.StorageType)
		assert.Equal(t, false, c.EnableWal)
		assert.Equal(t, 1, c.Cluster.NodeId)
		assert.Equal(t, int64(100000), c.VectorConfig.IvfPqThreshold)

		assert.Equal(t, 10*time.Second, c.WalSyncInterval)
		assert.Equal(t, []string{"localhost:2379"}, c.Etcd.Endpoints)

		assert.Equal(t, false, c.Plugin.GSE.Enable)
		assert.Equal(t, "small", c.Plugin.GSE.DictEmbed)
		assert.Equal(t, "./plugins/gse/dict", c.Plugin.GSE.DictPath)
	})

	t.Run("human check", func(t *testing.T) {
		tests := []struct {
			value  string
			expect int
		}{
			{
				value:  "2048576",
				expect: 2048576,
			},
			{
				value:  "1k",
				expect: 1000,
			},
			{
				value:  "1kb",
				expect: 1000,
			},
			{
				value:  "1m",
				expect: 1000000,
			},
			{
				value:  "1mb",
				expect: 1000000,
			},
			{
				value:  "1g",
				expect: 1000000000,
			},
			{
				value:  "1gb",
				expect: 1000000000,
			},
			{
				value:  "1G",
				expect: 1000000000,
			},
			{
				value:  "1GB",
				expect: 1000000000,
			},
		}
		for _, v := range tests {
			os.Setenv("ZINC_MAX_DOCUMENT_SIZE", v.value)

			c := new(config)
			zutils.LoadConfig(reflect.ValueOf(c).Elem())
			assert.Equal(t, c.MaxDocumentSize, v.expect)
		}

		dt := []struct {
			value  string
			expect time.Duration
		}{
			{
				value:  "1",
				expect: time.Nanosecond,
			},
			{
				value:  "1ns",
				expect: time.Nanosecond,
			},
			{
				value:  "1s",
				expect: time.Second,
			},
			{
				value:  "1m",
				expect: time.Minute,
			},
		}
		for _, v := range dt {
			os.Setenv("ZINC_WAL_SYNC_INTERVAL", v.value)

			c := new(config)
			zutils.LoadConfig(reflect.ValueOf(c).Elem())
			assert.Equal(t, c.WalSyncInterval, v.expect)
		}

	})

}
func TestConfigStorageType(t *testing.T) {
	t.Run("prepare", func(t *testing.T) {
		os.Setenv("SS_STORAGE_TYPE", "oss")
		os.Setenv("SS_OSS_ACCESS_ID", "admin")
		os.Setenv("SS_OSS_ACCESS_SECRET", "pwd")
		os.Setenv("SS_OSS_BUCKET", "bucket")
		os.Setenv("SS_OSS_ENDPOINT", "endpoint")

		os.Setenv("SS_DATA_PATH", "./ss_path")
	})

	t.Run("check", func(t *testing.T) {
		c := new(config)
		initConfig(c)

		assert.Equal(t, "oss", c.StorageType)
		assert.Equal(t, "admin", c.Oss.AccessId)
		assert.Equal(t, "pwd", c.Oss.AccessSecret)
		assert.Equal(t, "bucket", c.Oss.Bucket)
		assert.Equal(t, "endpoint", c.Oss.Endpoint)
		assert.Equal(t, "./ss_path", c.DataPath)
	})
}

func TestSentryDSNOverride(t *testing.T) {
	customDSN := "https://secretToken.my.sentry.com/1234"

	t.Run("prepare", func(t *testing.T) {
		os.Setenv("ZINC_SENTRY_DSN", customDSN)
	})

	t.Run("check", func(t *testing.T) {
		c := new(config)
		zutils.LoadConfig(reflect.ValueOf(c).Elem())

		assert.Equal(t, customDSN, c.SentryDSN)
	})
}

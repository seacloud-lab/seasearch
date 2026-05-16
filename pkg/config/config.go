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
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/zincsearch/zincsearch/pkg/zutils"

	"github.com/blugelabs/ice/compress"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
)

const (
	WalStorageTypeDisk  = "disk"
	WalStorageTypeRkv   = "rkv"
	WalStorageTypePgsql = "postgresql"
	WalStorageTypeMysql = "mysql"
)

type config struct {
	GinMode                   string        `env:"GIN_MODE,default=release"`
	ServerPort                string        `env:"ZINC_SERVER_PORT,default=4080"`
	ServerAddress             string        `env:"ZINC_SERVER_ADDRESS"`
	ServerTLSCertificateFile  string        `env:"ZINC_SERVER_TLS_CERTIFICATE_FILE"`
	ServerTLSKeyFile          string        `env:"ZINC_SERVER_TLS_KEY_FILE"`
	DataPath                  string        `env:"SS_DATA_PATH,default=./data"`
	MetadataStorage           string        `env:"ZINC_METADATA_STORAGE,default=bolt"`
	IceCompressor             string        `env:"ZINC_ICE_COMPRESSOR,default=zstd"`
	SentryEnable              bool          `env:"ZINC_SENTRY,default=false"`
	SentryDSN                 string        `env:"ZINC_SENTRY_DSN,default=https://15b6d9b8be824b44896f32b0234c32b7@o1218932.ingest.sentry.io/6360942"`
	ProfilerEnable            bool          `env:"ZINC_PROFILER,default=false"`
	ProfilerServer            string        `env:"ZINC_PROFILER_SERVER,default=https://pyroscope.dev.zincsearch.com"`
	ProfilerAPIKey            string        `env:"ZINC_PROFILER_API_KEY,default=psx-AfPbC5Bh6gI4dHkCMpoxM2Qd7Xblsqhip5nlwvHdhAE1"`
	ProfilerFriendlyProfileID string        `env:"ZINC_PROFILER_FRIENDLY_PROFILE_ID"`
	TelemetryEnable           bool          `env:"ZINC_TELEMETRY,default=false"`
	PrometheusEnable          bool          `env:"ZINC_PROMETHEUS_ENABLE,default=false"`
	EnableTextKeywordMapping  bool          `env:"ZINC_ENABLE_TEXT_KEYWORD_MAPPING,default=false"`
	BatchSize                 int           `env:"ZINC_BATCH_SIZE,default=1024"`
	MaxResults                int           `env:"ZINC_MAX_RESULTS,default=10000"`
	AggregationTermsSize      int           `env:"ZINC_AGGREGATION_TERMS_SIZE,default=1000"`
	MaxDocumentSize           int           `env:"ZINC_MAX_DOCUMENT_SIZE,default=1m"`      // Max size for a single document . Default = 1 MB = 1024 * 1024
	WalSyncInterval           time.Duration `env:"ZINC_WAL_SYNC_INTERVAL,default=1s"`      // sync wal to disk, 1s, 10ms
	WalRedoLogNoSync          bool          `env:"ZINC_WAL_REDOLOG_NO_SYNC,default=false"` // control sync after every write
	ZincSwaggerEnable         bool          `env:"ZINC_SWAGGER_ENABLE,default=true"`
	EnableWal                 bool          `env:"ZINC_WAL_ENABLE,default=false"`
	WalStorageType            string        `env:"ZINC_WAL_STORAGE_TYPE,default=disk"`
	WalConfig                 walConfig
	StorageType               string `env:"SS_STORAGE_TYPE,default=disk"`
	ObjCache                  objCache
	LogConfig                 logConfig
	VectorConfig              vectorConfig
	S3                        s3
	Oss                       oss
	Cluster                   cluster
	Shard                     shard
	Etcd                      etcd
	Plugin                    plugin
}

type logConfig struct {
	LogToStd bool   `env:"SS_LOG_TO_STDOUT"`
	LogDir   string `env:"SS_LOG_DIR,default=./log"`
	LogLevel string `env:"SS_LOG_LEVEL,default=debug"`
}

type objCache struct {
	MaxCacheSize int64 `env:"SS_MAX_OBJ_CACHE_SIZE,default=10GB"`
}

type s3 struct {
	AccessId         string `env:"SS_S3_ACCESS_ID"`
	AccessSecret     string `env:"SS_S3_ACCESS_SECRET"`
	Bucket           string `env:"SS_S3_BUCKET"`
	Endpoint         string `env:"SS_S3_ENDPOINT"`
	UseV4Signature   string `env:"SS_S3_USE_V4_SIGNATURE"`
	UseHttps         string `env:"SS_S3_USE_HTTPS"`
	PathStyleRequest string `env:"SS_S3_PATH_STYLE_REQUEST"`
	AwsRegion        string `env:"SS_S3_AWS_REGION"`
	SsecKey          string `env:"SS_S3_SSE_C_KEY"`
	PartSize         uint64 `env:"SS_S3_PART_SIZE"`
}

type oss struct {
	AccessId     string `env:"SS_OSS_ACCESS_ID"`
	AccessSecret string `env:"SS_OSS_ACCESS_SECRET"`
	Bucket       string `env:"SS_OSS_BUCKET"`
	Endpoint     string `env:"SS_OSS_ENDPOINT"`
}

type cluster struct {
	Enable bool `env:"SS_CLUSTER_ENABLE"`
	NodeId int  `env:"SS_CLUSTER_ID,default=1"`
}

type shard struct {
	// control goroutine number for read
	GoroutineNum int `env:"ZINC_SHARD_GOROUTINE_NUM,default=3"`
	// DefaultNum is the default number of shards.
	Num int64 `env:"ZINC_SHARD_NUM,default=1"`
	// MaxSize is the maximum size limit for one shard, or will create a new shard.
	MaxSize int64 `env:"ZINC_SHARD_MAX_SIZE,default=1073741824"`
}

type etcd struct {
	Endpoints []string `env:"SS_ETCD_ENDPOINTS"`
	Prefix    string   `env:"SS_ETCD_PREFIX,default=/seasearch"`
	Username  string   `env:"SS_ETCD_USERNAME"`
	Password  string   `env:"SS_ETCD_PASSWORD"`
}

type plugin struct {
	ES  elasticsearch
	GSE gse
}

type elasticsearch struct {
	Version string `env:"ZINC_PLUGIN_ES_VERSION"`
}

type gse struct {
	Enable     bool   `env:"ZINC_PLUGIN_GSE_ENABLE,default=false"`
	EnableStop bool   `env:"ZINC_PLUGIN_GSE_ENABLE_STOP,default=true"`
	EnableHMM  bool   `env:"ZINC_PLUGIN_GSE_ENABLE_HMM,default=true"`
	DictEmbed  string `env:"ZINC_PLUGIN_GSE_DICT_EMBED,default=small"`
	DictPath   string `env:"ZINC_PLUGIN_GSE_DICT_PATH,default=./plugins/gse/dict"`
}

type walConfig struct {
	RkvEndpoint []string `env:"ZINC_WAL_RKV_ENDPOINT"`

	Host     string `env:"ZINC_WAL_SQL_HOST"`
	Port     string `env:"ZINC_WAL_SQL_PORT"`
	Db       string `env:"ZINC_WAL_SQL_DB"`
	User     string `env:"ZINC_WAL_SQL_USER"`
	Password string `env:"ZINC_WAL_SQL_PWD"`
}

type vectorConfig struct {
	IvfPqThreshold int64 `env:"SS_VECTOR_IVFPQ_THRESHOLD,default=100000"`
}

var Global = new(config)

func InitConfig() {
	// support .env file for config
	_ = godotenv.Load()
	initConfig(Global)
}

func initConfig(c *config) {
	zutils.LoadConfig(reflect.ValueOf(c).Elem())

	// configure gin
	if c.GinMode == "release" {
		gin.SetMode(gin.ReleaseMode)
	}

	// check data path
	testPath := path.Join(c.DataPath, "_test_")
	if err := os.MkdirAll(testPath, 0755); err != nil {
		log.Fatal().Err(err).Msg("SS_DATA_PATH is not writable")
	}
	if err := os.Remove(testPath); err != nil {
		log.Fatal().Err(err).Msg("SS_DATA_PATH is not writable")
	}

	// configure ice compress algorithm
	switch strings.ToUpper(c.IceCompressor) {
	case "SNAPPY":
		compress.Algorithm = compress.SNAPPY
	case "S2":
		compress.Algorithm = compress.S2
	case "ZSTD":
		compress.Algorithm = compress.ZSTD
	}

	// check obj store backend config
	checkOss()
	checkS3()
	checkWalConfig()
}

func checkOss() {
	if Global.StorageType != "oss" {
		return
	}

	if Global.Oss.AccessId == "" {
		log.Fatal().Msg("require oss access id")
	}

	if Global.Oss.AccessSecret == "" {
		log.Fatal().Msg("require oss access secret")
	}

	if Global.Oss.Bucket == "" {
		log.Fatal().Msg("require oss access bucket")
	}

	if Global.Oss.Endpoint == "" {
		log.Fatal().Msg("require oss endpoint")
	}
}

func checkS3() {
	if Global.StorageType != "s3" {
		return
	}

	if Global.S3.AccessId == "" {
		log.Fatal().Msg("require s3 access id")
	}

	if Global.S3.AccessSecret == "" {
		log.Fatal().Msg("require s3 access secret")
	}

	if Global.S3.Bucket == "" {
		log.Fatal().Msg("require s3 access bucket")
	}

	if Global.S3.Endpoint == "" {
		log.Fatal().Msg("require s3 endpoint")
	}

	Global.S3.PartSize = Global.S3.PartSize * 1024 * 1024
}

func checkWalConfig() {
	if !Global.EnableWal {
		return
	}

	if Global.WalStorageType == WalStorageTypeDisk {
		return
	}

	if Global.WalStorageType == WalStorageTypeRkv {
		if len(Global.WalConfig.RkvEndpoint) == 0 {
			log.Fatal().Msg("require rkv")
		}
		return
	}
	if Global.WalStorageType != WalStorageTypeMysql && Global.WalStorageType != WalStorageTypePgsql {
		log.Fatal().Msg("unsupported wal type")
	}

	if Global.WalConfig.Db == "" || Global.WalConfig.Host == "" ||
		Global.WalConfig.Port == "" || Global.WalConfig.User == "" ||
		Global.WalConfig.Password == "" {
		log.Fatal().Msg("require wal sql config")
	}
}

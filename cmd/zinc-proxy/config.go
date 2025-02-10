package main

import (
	"reflect"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

type ProxyConfig struct {
	General GeneralConfig
	Proxy   ClusterProxyConfig
}

type GeneralConfig struct {
	LogToStd                    bool   `env:"SS_LOG_TO_STDOUT"`
	LogDir                      string `env:"SS_CLUSTER_PROXY_LOG_DIR,default=./log"`
	LogLevel                    string `env:"SS_CLUSTER_PROXY_LOG_LEVEL,default=INFO"`
	IndexParallelQueryThreshold int    `env:"SS_INDEX_PARALLEL_QUERY_THRESHOLD,default=3"`
}

type ClusterProxyConfig struct {
	ClusterManagerAddr string `env:"SS_CLUSTER_MANAGER_ADDR"`
	Host               string `env:"SS_CLUSTER_PROXY_HOST,default=0.0.0.0"`
	Port               int    `env:"SS_CLUSTER_PROXY_PORT,default=4082"`
}

func initProxyConfig() {
	// support .env file for config
	_ = godotenv.Load()
	zutils.LoadConfig(reflect.ValueOf(conf).Elem())
	gin.SetMode(gin.ReleaseMode)
	config.Global.ServerMode = config.ServerModeProxy
}

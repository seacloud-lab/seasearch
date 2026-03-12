package main

import (
	"reflect"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

var (
	conf = new(ClusterManagerConfig)
)

type ClusterManagerConfig struct {
	General GeneralConfig
	Manager ManagerConfig
	Cluster ClusterConfig
}

type GeneralConfig struct {
	LogToStd bool   `env:"SS_LOG_TO_STDOUT"`
	LogDir   string `env:"SS_CLUSTER_MANAGER_LOG_DIR,default=./log"`
	LogLevel string `env:"SS_CLUSTER_MANAGER_LOG_LEVEL,default=INFO"`
}

type ManagerConfig struct {
	Host string `env:"SS_CLUSTER_MANAGER_HOST,default=0.0.0.0"`
	Port int    `env:"SS_CLUSTER_MANAGER_PORT,default=4081"`
}

type ClusterConfig struct {
	EtcdEndpoints []string `env:"SS_ETCD_ENDPOINTS"`
	EtcdPrefix    string   `env:"SS_ETCD_PREFIX,default=/seasearch"`
}

func initConfig() {
	// support .env file for config
	_ = godotenv.Load()
	zutils.LoadConfig(reflect.ValueOf(conf).Elem())

	gin.SetMode(gin.ReleaseMode)
}

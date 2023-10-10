package main

import (
	"github.com/docker/go-units"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type ProxyConfig struct {
	General GeneralConfig
	Proxy   ClusterProxyConfig
}

type GeneralConfig struct {
	LogDir string `env:"ZINC_CLUSTER_PROXY_LOG_DIR"`
}

type ClusterProxyConfig struct {
	Host               string `env:"ZINC_CLUSTER_PROXY_HOST"`
	Port               int    `env:"ZINC_CLUSTER_PROXY_PORT"`
	ClusterManagerAddr string `env:"ZINC_CLUSTER_MANAGER_ADDR"`
}

func initConfig() {
	err := godotenv.Load()
	if err != nil {
		log.Print(err.Error())
	}
	loadConfig(reflect.ValueOf(conf).Elem())
	gin.SetMode(gin.ReleaseMode)
}

func loadConfig(rv reflect.Value) {
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		fv := rv.Field(i)
		ft := rt.Field(i)
		if ft.Type.Kind() == reflect.Struct {
			loadConfig(fv)
			continue
		}
		if ft.Tag.Get("env") != "" {
			tag := ft.Tag.Get("env")
			setField(fv, tag)
		}
	}
}

func setField(field reflect.Value, tag string) {
	if tag == "" {
		return
	}
	tagColumn := strings.Split(tag, ",")
	v := os.Getenv(tagColumn[0])
	if v == "" {
		if len(tagColumn) > 1 {
			tv := strings.Join(tagColumn[1:], ",")
			if strings.HasPrefix(tv, "default=") {
				v = tv[8:]
			}
		}
	}
	if v == "" {
		return
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		_, ok := field.Interface().(time.Duration)
		var (
			vi  int64
			err error
		)
		switch ok {
		case true:
			d, e := time.ParseDuration(v)
			if e != nil && strings.Contains(e.Error(), "time: missing unit in duration") {
				vi, err = strconv.ParseInt(v, 10, 64)
			} else {
				vi, err = int64(d), e
			}

		default:
			vi, err = units.FromHumanSize(v)
		}
		if err != nil {
			log.Fatal().Err(err).Msgf("env %s is not int", tag)
		}

		field.SetInt(int64(vi))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		vi, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			log.Fatal().Err(err).Msgf("env %s is not uint", tag)
		}
		field.SetUint(uint64(vi))
	case reflect.Bool:
		vi, err := strconv.ParseBool(v)
		if err != nil {
			log.Fatal().Err(err).Msgf("env %s is not bool", tag)
		}
		field.SetBool(vi)
	case reflect.String:
		field.SetString(v)
	case reflect.Slice:
		vs := strings.Split(v, ",")
		field.Set(reflect.ValueOf(vs))
		field.SetLen(len(vs))
	}
}

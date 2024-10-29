package main

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

func setLog() {
	var out *zutils.LogOuter
	if conf.General.SeafileLogToStd || conf.General.SeatableLogToStd {
		out = &zutils.LogOuter{Out: os.Stdout, LogToStdout: true}
	} else {
		err := os.MkdirAll(conf.General.LogDir, os.ModePerm)
		if err != nil {
			log.Fatal().Err(err).Msg("set log output to file error, cannot make dir")
		}
		file, err := os.OpenFile(path.Join(conf.General.LogDir, "zinc-cluster-manager.log"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal().Err(err).Msg("set log output to file error, cannot open file")
		}
		out = &zutils.LogOuter{Out: file, LogToStdout: false}
	}

	writer := zerolog.ConsoleWriter{Out: out, TimeFormat: "[2006-01-02 15:04:05]", NoColor: true}
	writer.FormatLevel = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("[%s]", i))
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
		return "err:"
	}
	writer.FormatErrFieldValue = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}

	log.Logger = zerolog.New(writer).With().Timestamp().Logger()
}

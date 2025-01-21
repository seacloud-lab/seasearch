package logger

import (
	"fmt"
	"io"
	stdlog "log"
	"os"
	"path"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type LogOuter struct {
	LogToStdout   bool
	Out           io.Writer
	componentName string
}

const ComponentSeaSearch = "[seasearch] "
const ComponentSeaSearchProxy = "[seasearch-proxy] "
const ComponentSeaSearchClusterManager = "[seasearch-cluster-manager] "

func (l *LogOuter) Write(data []byte) (n int, err error) {
	if l.LogToStdout {
		buf := make([]byte, 0, len(l.componentName)+len(data))
		buf = append(buf, []byte(l.componentName)...)
		_, err := l.Out.Write(append(buf, data...))
		return len(data), err
	}
	return l.Out.Write(data)
}

func SetupAccessLog(logToStdout bool, name, logDir, componentName string) zerolog.Logger {
	var out *LogOuter
	if logToStdout {
		out = &LogOuter{Out: os.Stdout, LogToStdout: true, componentName: componentName}
	} else {
		err := os.MkdirAll(logDir, os.ModePerm)
		if err != nil {
			log.Fatal().Err(err).Msg("set log output to file error, cannot make dir")
		}
		file, err := os.OpenFile(path.Join(logDir, name), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal().Err(err).Msg("set log output to file error, cannot open file")
		}
		out = &LogOuter{Out: file, LogToStdout: false, componentName: componentName}
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
		return "err:"
	}
	writer.FormatErrFieldValue = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}

	return zerolog.New(writer).With().Timestamp().Logger()
}

type stdLogAdapter struct {
	out zerolog.Logger
}

func (s *stdLogAdapter) Write(data []byte) (int, error) {
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}
	s.out.Error().Msg(string(data))
	return len(data), nil
}

func SetupMainLog(logToStdout bool, name, logDir, level, componentName string) zerolog.Logger {
	var out *LogOuter
	if logToStdout {
		out = &LogOuter{Out: os.Stdout, LogToStdout: true, componentName: componentName}
	} else {
		err := os.MkdirAll(logDir, os.ModePerm)
		if err != nil {
			log.Fatal().Err(err).Msg("set log output to file error, cannot make dir")
		}
		file, err := os.OpenFile(path.Join(logDir, name), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal().Err(err).Msg("set log output to file error, cannot open file")
		}
		// setup stdErr
		err = Dup(int(file.Fd()), int(os.Stderr.Fd()))
		if err != nil {
			log.Fatal().Err(err).Msgf("failed to dup stderr: %s", err)
		}
		out = &LogOuter{Out: file, LogToStdout: false, componentName: componentName}
	}

	lv, err := zerolog.ParseLevel(level)
	if err != nil {
		log.Fatal().Err(err).Msg("invalid log level")
	}

	zerolog.SetGlobalLevel(lv)
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

	zeroLogger := zerolog.New(writer).With().Timestamp().Logger()

	// we adjust stdlog to unify all output log formats
	stdlog.SetFlags(0)
	stdlog.SetOutput(&stdLogAdapter{out: zeroLogger})
	return zeroLogger
}

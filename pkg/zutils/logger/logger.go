package logger

import (
	"fmt"
	"io"
	stdlog "log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

const (
	ComponentSeaSearch               = "[seasearch] "
	ComponentSeaSearchProxy          = "[seasearch-proxy] "
	ComponentSeaSearchClusterManager = "[seasearch-cluster-manager] "
)

var (
	writersMutex sync.Mutex
	writers      []*RotatingWriter
)

type RotatingWriter struct {
	componentName  string
	filename       string
	logToStdOut    bool
	redirectStdErr bool

	mutex  sync.Mutex
	file   *os.File
	writer io.Writer
}

func newLogger(componentName, filename string, stdout, stderr bool) *RotatingWriter {
	w := new(RotatingWriter)
	w.componentName = componentName
	w.filename = filename
	w.logToStdOut = stdout
	w.redirectStdErr = stderr

	writersMutex.Lock()
	writers = append(writers, w)
	writersMutex.Unlock()

	return w
}

func (w *RotatingWriter) reopen() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.logToStdOut {
		w.writer = os.Stdout
		return nil
	}

	oldFile := w.file

	err := os.MkdirAll(filepath.Dir(w.filename), 0777)
	if err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}
	file, err := os.OpenFile(w.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("failed to open log file %v: %w", w.filename, err)
	}
	if w.redirectStdErr {
		err = unix.Dup2(int(file.Fd()), int(os.Stderr.Fd()))
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to duplicate stderr: %w", err)
		}
	}
	w.file = file
	w.writer = file

	if oldFile != nil {
		oldFile.Close()
	}

	return nil
}

func (w *RotatingWriter) Write(data []byte) (int, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.logToStdOut {
		return w.writer.Write(data)
	}

	buf := make([]byte, 0, len(w.componentName)+len(data))
	buf = append(buf, w.componentName...)
	buf = append(buf, data...)
	_, err := w.writer.Write(buf)
	return len(data), err
}

func SetupAccessLog(logToStdout bool, name, logDir, componentName string) zerolog.Logger {
	var out *RotatingWriter
	if logToStdout {
		out = newLogger(componentName, "", true, false)
	} else {
		out = newLogger(componentName, filepath.Join(logDir, name), false, false)
	}
	err := out.reopen()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open log")
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
	var out *RotatingWriter
	if logToStdout {
		out = newLogger(componentName, "", true, false)
	} else {
		out = newLogger(componentName, filepath.Join(logDir, name), false, true)
	}
	err := out.reopen()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open log")
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

func HandleRotateSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)

	for {
		<-ch

		writersMutex.Lock()
		for _, logger := range writers {
			err := logger.reopen()
			if err != nil {
				log.Error().Msgf("failed to rotate logs: %v", err)
			}
		}
		writersMutex.Unlock()
	}
}

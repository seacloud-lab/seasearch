package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

var proxyPool *ProxyPool
var accessLog zerolog.Logger

func init() {
	proxyPool = &ProxyPool{
		pool: make(map[string]*httputil.ReverseProxy),
	}
}

func setLog() {
	var out *zutils.LogOuter
	var accessOuter *zutils.LogOuter
	if conf.General.SeafileLogToStd || conf.General.SeatableLogToStd {
		out = &zutils.LogOuter{Out: os.Stdout, LogToStdout: true}
		accessOuter = &zutils.LogOuter{Out: os.Stdout, LogToStdout: true}
	} else {
		err := os.MkdirAll(conf.General.LogDir, os.ModePerm)
		if err != nil {
			log.Fatal().Err(err).Msg("set log output to file error, cannot make dir")
		}
		file, err := os.OpenFile(path.Join(conf.General.LogDir, "seasearch-proxy.log"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal().Err(err).Msg("set log output to file error, cannot open file")
		}
		// setup stdErr
		err = zutils.Dup(int(file.Fd()), int(os.Stderr.Fd()))
		if err != nil {
			log.Fatal().Err(err).Msgf("failed to dup stderr: %s", err)
		}
		out = &zutils.LogOuter{Out: file, LogToStdout: false}
		accessLogFile, err := os.OpenFile(path.Join(conf.General.LogDir, "seasearch-proxy-access.log"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal().Err(err).Msg("set log output to file error, cannot open file")
		}
		accessOuter = &zutils.LogOuter{Out: accessLogFile, LogToStdout: false}
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

	accessWriter := writer
	accessWriter.Out = accessOuter
	accessWriter.FormatLevel = func(i interface{}) string {
		return ""
	}
	accessLog = zerolog.New(accessWriter).With().Timestamp().Logger()
}

func fetchHTTP(method, host, path, query string, reqBody io.Reader, x interface{}, auth string, returnBody bool) error {
	u := &url.URL{
		Scheme:   "http",
		Host:     host,
		Path:     path,
		RawQuery: query,
	}
	var req *http.Request
	var err error
	if reqBody != nil {
		req, err = http.NewRequest(method, u.String(), reqBody)
	} else {
		req, err = http.NewRequest(method, u.String(), nil)
	}
	req.Header.Set("Authorization", auth)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	rsp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return fmt.Errorf("failed to send request to node %s: %w", host, err)
	}
	defer rsp.Body.Close()

	if rsp.StatusCode/100 == 4 {
		body, _ := io.ReadAll(rsp.Body)
		resp := &meta.HTTPResponseError{}
		_ = json.Unmarshal(body, &resp)
		return newHttpClientError(fmt.Sprintf("bad response from host: %s, err: %s", host, resp.Error), rsp.StatusCode)
	} else if rsp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get response from node %s: %s", host, rsp.Status)
	}
	if !returnBody {
		return nil
	}
	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body from node %s: %w", host, err)
	}
	err = json.Unmarshal(body, x)
	if err != nil {
		return fmt.Errorf("failed to unmarshal response body from node %s: %w", host, err)
	}

	return nil
}

type HttpClientError struct {
	Msg  string
	Code int
}

func newHttpClientError(msg string, code int) *HttpClientError {
	return &HttpClientError{Msg: msg, Code: code}
}

func (err *HttpClientError) Error() string {
	return err.Msg
}

type ProxyPool struct {
	mutex sync.Mutex
	pool  map[string]*httputil.ReverseProxy
}

// Get returns a proxy for the given shard.
func (pp *ProxyPool) Get(url *url.URL) *httputil.ReverseProxy {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()

	p, ok := pp.pool[url.Host]
	if !ok {
		rp := httputil.NewSingleHostReverseProxy(url)
		pp.pool[url.Host] = rp
		p = rp
	}

	return p
}

func rewindBody(data interface{}) io.ReadCloser {
	buf, _ := json.Marshal(data)
	return io.NopCloser(bytes.NewBuffer(buf))
}

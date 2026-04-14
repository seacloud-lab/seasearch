package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/zincsearch/zincsearch/pkg/meta"
)

var proxyPool *ProxyPool
var accessLog zerolog.Logger

func InitProxy() {
	proxyPool = &ProxyPool{
		pool: make(map[string]*httputil.ReverseProxy),
	}
}

func fetchHTTP(method, addr, path, query string, reqBody io.Reader, x interface{}, auth string, returnBody bool) error {
	u, err := url.Parse(addr)
	if err != nil {
		return fmt.Errorf("failed to parse addr %v: %w", addr, err)
	}
	u = u.JoinPath(path)
	u.RawQuery = query
	var req *http.Request
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
		return fmt.Errorf("failed to send request to node %s: %w", addr, err)
	}
	defer rsp.Body.Close()

	if rsp.StatusCode/100 == 4 {
		body, _ := io.ReadAll(rsp.Body)
		resp := &meta.HTTPResponseError{}
		_ = json.Unmarshal(body, &resp)
		return newHttpClientError(fmt.Sprintf("bad response from addr: %s, err: %s", addr, resp.Error), rsp.StatusCode)
	} else if rsp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get response from node %s: %s", addr, rsp.Status)
	}
	if !returnBody {
		return nil
	}
	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body from node %s: %w", addr, err)
	}
	err = json.Unmarshal(body, x)
	if err != nil {
		return fmt.Errorf("failed to unmarshal response body from node %s: %w", addr, err)
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
		rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Error().Msgf("http: proxy error: %v", err)
				w.WriteHeader(http.StatusBadGateway)
			}
		}
		pp.pool[url.Host] = rp
		p = rp
	}

	return p
}

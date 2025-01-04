package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

var (
	conf = new(ProxyConfig)
)

func main() {
	initConfig()
	log.Logger = zutils.SetupMainLog(conf.General.LogToStd, "seasearch-proxy.log", conf.General.LogDir)
	accessLog = zutils.SetupAccessLog(conf.General.LogToStd, "seasearch-proxy-access.log", conf.General.LogDir)
	StartProxy()

	app := gin.New()
	SetupHttp(app)

	log.Info().Msg("SeaSearch Proxy Start")
	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", conf.Proxy.Host, conf.Proxy.Port),
		Handler: app,
	}
	done := shutdown(func(grace bool, done chan<- struct{}) {
		// close http server
		if grace {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
			defer cancel()
			if err := server.Shutdown(ctx); err != nil {
				log.Fatal().Msg("Server Shutdown")
			}
		} else {
			server.Close()
		}
		ShutDownProxy()
		log.Info().Msg("SeaSearch Proxy Closed")

		// close metadata
		err := metadata.Close()
		log.Info().Err(err).Msgf("SeaSearch Proxy Metadata closed")

		done <- struct{}{}
	})

	err := server.ListenAndServe()
	if err != nil {
		if errors.Is(err, http.ErrServerClosed) {
			log.Info().Msg("SeaSearch Proxy Http Server closed")
		} else {
			log.Fatal().Msg("SeaSearch Proxy Http Server closed unexpect")
		}
	}
	<-done
	log.Info().Msg("SeaSearch Proxy Shutdown OK")
}

// shutdown support twice signal must exit
func shutdown(stop func(grace bool, done chan<- struct{})) <-chan struct{} {
	done := make(chan struct{})
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGQUIT, os.Interrupt, syscall.SIGTERM)
	go func() {
		s := <-sig
		go stop(s != syscall.SIGQUIT, done)
		<-sig
		os.Exit(128 + int(s.(syscall.Signal))) // second signal. Exit directly.
	}()
	return done
}

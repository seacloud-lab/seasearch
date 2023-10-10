package main

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	conf = new(ProxyConfig)
)

func main() {
	initConfig()
	setLog()
	StartProxy()

	app := gin.New()
	SetupHttp(app)

	log.Info().Msg("Server start")
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
		// close metadata
		err := metadata.Close()
		log.Info().Err(err).Msgf("Metadata closed")

		log.Info().Msg("Zinc-proxy Closed")

		done <- struct{}{}
	})

	err := server.ListenAndServe()
	if err != nil {
		if err == http.ErrServerClosed {
			log.Info().Msg("Server closed")
		} else {
			log.Fatal().Msg("Server closed unexpect")
		}
	}
	<-done
	log.Info().Msg("Server Shutdown OK")
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

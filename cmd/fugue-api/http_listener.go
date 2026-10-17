package main

import (
	"net"
	"net/http"
	"time"

	"fugue/internal/api/httpserver"
)

const apiHTTPIdleTimeout = 2 * time.Minute

func newAPIHTTPServer(addr string, handler http.Handler) *http.Server {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       apiHTTPIdleTimeout,
		Protocols:         protocols,
	}
}

func serveAPIHTTP(server *http.Server) error {
	addr := server.Addr
	if addr == "" {
		addr = ":http"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	return server.Serve(httpserver.WithInitialIdleTimeout(listener, server.IdleTimeout))
}

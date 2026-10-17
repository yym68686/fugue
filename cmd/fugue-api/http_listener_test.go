package main

import (
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestAPIHTTPServerKeepsHeaderBudgetAndBoundsIdle(t *testing.T) {
	server := newAPIHTTPServer("127.0.0.1:0", http.NotFoundHandler())
	if server.ReadHeaderTimeout != 10*time.Second || server.IdleTimeout != 2*time.Minute ||
		server.ReadTimeout != 0 || server.WriteTimeout != 0 ||
		!server.Protocols.HTTP1() || server.Protocols.UnencryptedHTTP2() || server.Protocols.HTTP2() {
		t.Fatalf("unexpected public API HTTP timeouts/protocols: %+v", server)
	}
}

func TestServeAPIHTTPReportsBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	err = serveAPIHTTP(newAPIHTTPServer(listener.Addr().String(), http.NotFoundHandler()))
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("bind error=%v", err)
	}
}

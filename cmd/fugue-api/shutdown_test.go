package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeUntilShutdownCompletesDrainsActiveRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, "completed")
	})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	t.Cleanup(func() { close(release) })
	shutdownDone, serveReturned := make(chan struct{}), make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		runDone <- serveUntilShutdownCompletes(func() error {
			err := server.Serve(listener)
			close(serveReturned)
			return err
		}, shutdownDone)
	}()
	response := make(chan string, 1)
	go func() {
		client := &http.Client{Timeout: 3 * time.Second}
		res, err := client.Get("http://" + listener.Addr().String())
		if err != nil {
			response <- err.Error()
			return
		}
		defer res.Body.Close()
		raw, err := io.ReadAll(res.Body)
		if err != nil {
			response <- err.Error()
			return
		}
		response <- string(raw)
	}()
	awaitShutdownSignal(t, started)
	shutdownResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		shutdownResult <- server.Shutdown(ctx)
		close(shutdownDone)
	}()
	awaitShutdownSignal(t, serveReturned)
	// The listener has stopped, but a live request still owns the server.
	select {
	case err := <-runDone:
		t.Fatalf("API lifetime ended while a request was still active: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case got := <-response:
		if got != "completed" {
			t.Fatalf("active response was truncated: %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("active request did not finish")
	}
	if err := <-shutdownResult; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("API lifetime did not end after shutdown")
	}
}

func TestServeUntilShutdownCompletesReturnsStartupFailure(t *testing.T) {
	want := errors.New("listen failed")
	done := make(chan error, 1)
	go func() {
		done <- serveUntilShutdownCompletes(func() error { return want }, make(chan struct{}))
	}()
	select {
	case got := <-done:
		if !errors.Is(got, want) {
			t.Fatalf("startup error = %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("startup failure waited for a shutdown that cannot occur")
	}
}

func awaitShutdownSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server lifecycle signal")
	}
}

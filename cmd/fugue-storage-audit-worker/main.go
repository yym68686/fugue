package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fugue/internal/storageaudit"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	addr := os.Getenv("FUGUE_STORAGE_AUDIT_BIND_ADDR")
	if addr == "" {
		addr = ":9090"
	}
	if err := run(ctx, addr, os.Getenv("FUGUE_STORAGE_AUDIT_DATABASE_URL")); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, addr, dsn string) error {
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	store, err := storageaudit.Open(startup, dsn)
	cancel()
	if err != nil {
		return err
	}
	defer store.Close()
	worker := storageaudit.NewWorker(store)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ctx.Err() != nil || !worker.Ready(time.Now()) {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		worker.WriteMetrics(w)
	})
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	serving, done := make(chan error, 1), make(chan struct{})
	go func() { serving <- server.Serve(listener) }()
	go func() { defer close(done); worker.Run(runCtx) }()
	select {
	case <-ctx.Done():
	case err = <-serving:
	}
	stop()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	shutdownErr := server.Shutdown(shutdown)
	select {
	case <-done:
	case <-shutdown.Done():
		shutdownErr = errors.Join(shutdownErr, errors.New("storage audit worker did not drain"))
	}
	return errors.Join(err, shutdownErr)
}

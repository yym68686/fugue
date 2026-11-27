package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fugue/internal/qualityrollup"
	"fugue/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	addr := os.Getenv("FUGUE_OBSERVATION_WORKER_BIND_ADDR")
	if addr == "" {
		addr = ":9090"
	}
	if err := run(ctx, addr, os.Getenv("FUGUE_OBSERVATION_DATABASE_URL"), log.Default()); err != nil {
		// PostgreSQL configuration errors can include connection details.
		log.Fatal(err)
	}
}

func run(ctx context.Context, addr, dsn string, logger *log.Logger) error {
	startup, cancelStartup := context.WithTimeout(ctx, 10*time.Second)
	storage, closePool, err := store.OpenEdgeQualityRollupWorkerStore(startup, dsn)
	cancelStartup()
	if err != nil {
		return errors.New("observation worker database prerequisites unavailable")
	}
	defer closePool()
	rollups := qualityrollup.NewWorker(storage, logger)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		readiness, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if ctx.Err() != nil || storage.CheckEdgeQualityRollupWorker(readiness) != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		rollups.WriteMetrics(w)
	})
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler: mux, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second,
		IdleTimeout: 30 * time.Second,
	}
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	done := make(chan struct{})
	go func() {
		defer close(done)
		rollups.Run(workerCtx)
	}()
	select {
	case <-ctx.Done():
	case err = <-serving:
	}
	stopWorker()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	shutdownErr := server.Shutdown(shutdown)
	select {
	case <-done:
	case <-shutdown.Done():
		return errors.Join(shutdownErr, errors.New("observation worker drain timed out"))
	}
	return errors.Join(err, shutdownErr)
}

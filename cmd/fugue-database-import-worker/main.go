package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"fugue/internal/appdbimport"
	"fugue/internal/dbimportworker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	addr := os.Getenv("FUGUE_DATABASE_IMPORT_WORKER_BIND_ADDR")
	if addr == "" {
		addr = ":9090"
	}
	if err := run(ctx, addr, os.Getenv("FUGUE_DATABASE_IMPORT_WORKER_DATABASE_URL"), log.Default()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, addr, dsn string, logger *log.Logger) error {
	for _, tool := range []string{"psql", "pg_restore"} {
		if _, err := exec.LookPath(tool); err != nil {
			return fmt.Errorf("database import worker is missing %s", tool)
		}
	}
	startup, cancelStartup := context.WithTimeout(ctx, 8*time.Second)
	worker, err := dbimportworker.OpenPostgresExecutor(startup, dsn)
	cancelStartup()
	if err != nil {
		return err
	}
	defer worker.Close()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	server := &http.Server{
		Handler: healthHandler(workerCtx, worker.Owner), ReadHeaderTimeout: 3 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10,
	}
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	done := make(chan struct{})
	go func() {
		defer close(done)
		appdbimport.Run(workerCtx, worker, nil, logger.Printf)
	}()
	select {
	case <-ctx.Done():
	case err = <-serving:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	stopWorker()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdownErr := server.Shutdown(shutdown)
	select {
	case <-done:
		return errors.Join(err, shutdownErr)
	case <-shutdown.Done():
		return errors.Join(err, shutdownErr, errors.New("database import worker drain timed out"))
	}
}

func healthHandler(ctx context.Context, owner func(context.Context) (string, error)) http.Handler {
	check := func(r *http.Request) (string, bool) {
		probe, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if ctx.Err() != nil {
			return "", false
		}
		value, err := owner(probe)
		return value, err == nil && (value == "api" || value == "worker")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := check(r); !ok {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		value, ok := check(r)
		if !ok {
			_, _ = w.Write([]byte("fugue_database_import_worker_store_available 0\n"))
			return
		}
		_, _ = w.Write([]byte("fugue_database_import_worker_store_available 1\n"))
		active := 0
		if value == "worker" {
			active = 1
		}
		_, _ = fmt.Fprintf(w, "fugue_database_import_worker_owns_execution %d\n", active)
	})
	return mux
}

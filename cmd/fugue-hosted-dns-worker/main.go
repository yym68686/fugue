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

	"fugue/internal/hosteddns"
	"fugue/internal/observability"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	addr := os.Getenv("FUGUE_HOSTED_DNS_WORKER_BIND_ADDR")
	if addr == "" {
		addr = ":9090"
	}
	if err := run(ctx, addr, os.Getenv("FUGUE_HOSTED_DNS_WORKER_DATABASE_URL")); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, addr, dsn string) error {
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	worker, err := hosteddns.OpenPostgresWorker(startup, dsn)
	cancel()
	if err != nil {
		return err
	}
	defer worker.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		check, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if _, err := worker.Progress(check); err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		check, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		progress, err := worker.Progress(check)
		const available = "fugue_hosted_dns_worker_storage_available"
		if err != nil {
			observability.WriteGaugeMetric(w, available, "Whether DNS reconciliation status is readable.", nil, 0)
			return
		}
		observability.WriteGaugeMetric(w, available, "Whether DNS reconciliation status is readable.", nil, 1)
		observability.WriteGaugeMetric(w, "fugue_hosted_dns_owner", "Current owner of DNS flatten reconciliation.",
			map[string]string{"owner": progress.Owner}, 1)
		if progress.CompletedAt.Valid {
			observability.WriteGaugeMetric(w, "fugue_hosted_dns_last_completed_at_seconds",
				"Timestamp of the last committed DNS flatten pass.", nil, float64(progress.CompletedAt.Time.Unix()))
		}
		if progress.Updated.Valid {
			observability.WriteGaugeMetric(w, "fugue_hosted_dns_last_updated_records",
				"Records refreshed in the last committed DNS flatten pass.", nil, float64(progress.Updated.Int64))
		}
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	workCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for workCtx.Err() == nil {
			pass, cancel := context.WithTimeout(workCtx, 50*time.Second)
			more, err := worker.Reconcile(pass)
			cancel()
			if err != nil && workCtx.Err() == nil {
				log.Print("hosted DNS refresh did not complete")
			}
			delay := time.Minute
			if more {
				delay = 2 * time.Second
			}
			timer := time.NewTimer(delay)
			select {
			case <-workCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	select {
	case <-ctx.Done():
	case err = <-serving:
	}
	stopWorker()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdownErr := server.Shutdown(shutdown)
	select {
	case <-done:
		return errors.Join(err, shutdownErr)
	case <-shutdown.Done():
		return errors.Join(err, shutdownErr, errors.New("hosted DNS worker did not drain"))
	}
}

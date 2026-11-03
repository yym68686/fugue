// Operator maintenance: default inspection is read-only. Rehearsals explicitly
// write and roll back; apply is one bounded input, never a complete store scan.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fugue/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

func main() {
	mode := flag.String("mode", "inspect", "inspect, migrate, rollback, collect, archive, observe")
	digest := flag.String("digest", "", "exact compiler input digest")
	expected := flag.String("expected-format", "", "expected old representation")
	apply := flag.Bool("apply", false, "commit one bounded operation")
	rehearse := flag.Bool("rehearse", false, "encode/verify then roll back (consumes WAL and I/O)")
	maxBytes := flag.Int64("max-input-bytes", 128<<20, "maximum logical input bytes; hard limit 128MiB")
	flag.Parse()
	readOnly := *mode == "inspect" || *mode == "observe" || *mode == "archive" && !*apply
	if flag.NArg() != 0 || *apply && *rehearse || *mode == "archive" && *rehearse || !readOnly && !*apply && !*rehearse || readOnly && (*apply || *rehearse) {
		fmt.Fprintln(os.Stderr, "select a read-only mode, or explicitly choose apply/rehearse")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *mode == "observe" {
		if err := observeStorage(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "runtime storage observation unavailable")
			os.Exit(1)
		}
		return
	}
	if *mode == "archive" {
		dsn := os.Getenv("FUGUE_DATABASE_URL")
		if dsn == "" {
			fmt.Fprintln(os.Stderr, "FUGUE_DATABASE_URL is required")
			os.Exit(2)
		}
		report, err := store.RunCompilerInputArchiveMigration(ctx, dsn, store.CompilerInputArchiveMigrationOptions{Digest: *digest, Apply: *apply})
		if err != nil {
			fmt.Fprintln(os.Stderr, "compiler input archive not confirmed; inspect archive ledger before retrying")
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
			os.Exit(1)
		}
		return
	}
	dsn := os.Getenv("FUGUE_DATABASE_URL")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "FUGUE_DATABASE_URL is required")
		os.Exit(2)
	}
	report, err := store.RunCompilerInputMaintenance(ctx, dsn, store.CompilerInputMaintenanceOptions{Action: *mode, Digest: *digest, ExpectedFormat: *expected, MaxInputBytes: *maxBytes, Apply: *apply})
	if err != nil {
		code := "unclassified"
		var pgError *pgconn.PgError
		switch {
		case errors.As(err, &pgError):
			code = pgError.Code
		case errors.Is(err, context.DeadlineExceeded):
			code = "deadline"
		case errors.Is(err, store.ErrConflict):
			code = "conflict"
		case errors.Is(err, store.ErrInvalidInput):
			code = "invalid-input"
		}
		fmt.Fprintln(os.Stderr, "compiler input maintenance completion not confirmed; inspect state before retrying; category="+code)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
		os.Exit(1)
	}
}

func observeStorage(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", "/tmp/fugue-observation.sock")
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://runtime/v1/snapshots/storage", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("storage observation HTTP status")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 || !json.Valid(raw) {
		return fmt.Errorf("storage observation exceeds budget or is invalid")
	}
	_, err = os.Stdout.Write(append(raw, '\n'))
	return err
}

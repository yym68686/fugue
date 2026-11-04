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
	mode := flag.String("mode", "inspect", "inspect, migrate, rollback, collect, archive, archive-route, archive-unroute, archive-write-fence, archive-retirement-plan, archive-retire, observe")
	digest := flag.String("digest", "", "exact compiler input digest")
	expected := flag.String("expected-format", "", "expected old representation")
	apply := flag.Bool("apply", false, "commit one bounded operation")
	disarm := flag.Bool("disarm", false, "remove a pre-retirement archive write fence")
	expectedPlanSHA256 := flag.String("expected-plan-sha256", "", "exact archive-retirement-plan hash to apply")
	currentReaderRelease := flag.String("current-reader-release", "", "40-hex release commit with archive reader lease support")
	rollbackReaderRelease := flag.String("rollback-reader-release", "", "40-hex rollback release commit with archive reader lease support")
	archiveRetentionPolicySHA256 := flag.String("archive-retention-policy-sha256", "", "64-hex operator attestation of the independently reviewed archive retention policy")
	readerDrainTimeout := flag.Duration("reader-drain-timeout", 30*time.Second, "maximum wait for session-level compiler-input readers")
	archiveLimit := flag.Int("archive-limit", 1, "maximum archive batch inputs, 1 through 1000")
	archiveSegmentBytes := flag.Int("archive-segment-bytes", 16<<20, "bounded archive segment bytes; applies only to new receipts")
	rehearse := flag.Bool("rehearse", false, "encode/verify then roll back (consumes WAL and I/O)")
	maxBytes := flag.Int64("max-input-bytes", 128<<20, "maximum logical input bytes; hard limit 128MiB")
	flag.Parse()
	readOnly := *mode == "inspect" || *mode == "observe" || *mode == "archive-retirement-plan" || (*mode == "archive" || *mode == "archive-batch") && !*apply
	routeMode := *mode == "archive-route" || *mode == "archive-unroute"
	writeFenceMode := *mode == "archive-write-fence"
	retirementMode := *mode == "archive-retire"
	if flag.NArg() != 0 || *apply && *rehearse || (*mode == "archive" || routeMode || writeFenceMode || retirementMode) && *rehearse || routeMode && !*apply || writeFenceMode && !*apply || retirementMode && !*apply || *disarm && !writeFenceMode || !readOnly && !*apply && !*rehearse || readOnly && (*apply || *rehearse) {
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
	if *mode == "archive-batch" {
		dsn := os.Getenv("FUGUE_DATABASE_URL")
		if dsn == "" || *rehearse {
			fmt.Fprintln(os.Stderr, "archive batch requires a database and explicit copy intent")
			os.Exit(2)
		}
		encoder := json.NewEncoder(os.Stdout)
		err := store.RunCompilerInputArchiveBatch(ctx, dsn, *archiveLimit, *apply, func(report store.CompilerInputArchiveMigrationReport) error { return encoder.Encode(report) })
		if err != nil {
			fmt.Fprintln(os.Stderr, "compiler input archive batch not complete; verified receipts remain resumable")
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
		report, err := store.RunCompilerInputArchiveMigration(ctx, dsn, store.CompilerInputArchiveMigrationOptions{Digest: *digest, Apply: *apply, SegmentBytes: *archiveSegmentBytes})
		if err != nil {
			fmt.Fprintln(os.Stderr, "compiler input archive not confirmed; inspect archive ledger before retrying")
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
			os.Exit(1)
		}
		return
	}
	if *mode == "archive-route" || *mode == "archive-unroute" {
		dsn := os.Getenv("FUGUE_DATABASE_URL")
		if dsn == "" {
			fmt.Fprintln(os.Stderr, "FUGUE_DATABASE_URL is required")
			os.Exit(2)
		}
		report, err := store.RunCompilerInputArchiveRoute(ctx, dsn, store.CompilerInputArchiveRouteOptions{Digest: *digest, Apply: *apply, Unroute: *mode == "archive-unroute"})
		if err != nil {
			fmt.Fprintln(os.Stderr, "compiler input archive route not confirmed; inspect the archive ledger before retrying")
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
			os.Exit(1)
		}
		return
	}
	if writeFenceMode {
		dsn := os.Getenv("FUGUE_DATABASE_URL")
		if dsn == "" {
			fmt.Fprintln(os.Stderr, "FUGUE_DATABASE_URL is required")
			os.Exit(2)
		}
		report, err := store.RunCompilerInputArchiveWriteFence(ctx, dsn, *apply, *disarm)
		if err != nil {
			fmt.Fprintln(os.Stderr, "compiler input write fence state not confirmed; inspect control state and database trigger")
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
			os.Exit(1)
		}
		return
	}
	if *mode == "archive-retirement-plan" || retirementMode {
		dsn := os.Getenv("FUGUE_DATABASE_URL")
		if dsn == "" {
			fmt.Fprintln(os.Stderr, "FUGUE_DATABASE_URL is required")
			os.Exit(2)
		}
		report, err := store.RunCompilerInputArchiveRetirement(ctx, dsn, store.CompilerInputArchiveRetirementOptions{
			Apply: *apply, ExpectedPlanSHA256: *expectedPlanSHA256,
			CurrentReaderRelease: *currentReaderRelease, RollbackReaderRelease: *rollbackReaderRelease,
			ArchiveRetentionPolicySHA256: *archiveRetentionPolicySHA256,
			ReaderDrainTimeout:           *readerDrainTimeout,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "compiler input retirement not confirmed; no success should be inferred; inspect control state and relation size")
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

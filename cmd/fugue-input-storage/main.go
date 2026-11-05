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
	"regexp"
	"syscall"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

func main() {
	mode := flag.String("mode", "inspect", "inspect, migrate, rollback, collect, archive, archive-batch, archive-route, archive-route-batch, archive-unroute, archive-hot-copy, archive-hot-restore, archive-hot-inventory, archive-hot-roots, archive-hot-local-readback, archive-local-pin, archive-local-unpin, archive-read-check, archive-write-fence, archive-retirement-plan, archive-retire, observe")
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
	hotMaxStoredBytes := flag.Int64("hot-copy-max-stored-bytes", 0, "explicit stored-byte budget for one archive hot copy or restore; source remains intact")
	readCheckMaxBytes := flag.Int64("read-check-max-bytes", 0, "explicit raw-byte ceiling for one complete read-only input check, at most 1GiB")
	localPinKey := flag.String("local-pin-key", "", "unique local pin key, never reused after release")
	localPinToken := flag.Int64("local-pin-token", 0, "exact positive token required to release a local pin")
	rehearse := flag.Bool("rehearse", false, "encode/verify then roll back (consumes WAL and I/O)")
	maxBytes := flag.Int64("max-input-bytes", 128<<20, "maximum logical input bytes; hard limit 128MiB")
	flag.Parse()
	hotMode := *mode == "archive-hot-copy" || *mode == "archive-hot-restore"
	localPinMode := *mode == "archive-local-pin" || *mode == "archive-local-unpin"
	readOnly := *mode == "inspect" || *mode == "observe" || *mode == "archive-hot-inventory" || *mode == "archive-hot-roots" || *mode == "archive-hot-local-readback" || *mode == "archive-read-check" || *mode == "archive-retirement-plan" || (*mode == "archive" || *mode == "archive-batch" || hotMode || *mode == "archive-local-pin") && !*apply
	routeMode := *mode == "archive-route" || *mode == "archive-route-batch" || *mode == "archive-unroute"
	writeFenceMode := *mode == "archive-write-fence"
	retirementMode := *mode == "archive-retire"
	if flag.NArg() != 0 || *apply && *rehearse || (*mode == "archive" || routeMode || hotMode || localPinMode || writeFenceMode || retirementMode) && *rehearse || routeMode && !*apply || writeFenceMode && !*apply || retirementMode && !*apply || *disarm && !writeFenceMode || !readOnly && !*apply && !*rehearse || readOnly && (*apply || *rehearse) {
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
	if localPinMode {
		dsn := os.Getenv("FUGUE_DATABASE_URL")
		if dsn == "" {
			fmt.Fprintln(os.Stderr, "FUGUE_DATABASE_URL is required")
			os.Exit(2)
		}
		report, err := store.RunCompilerInputLocalPin(ctx, dsn, store.CompilerInputLocalPinOptions{Key: *localPinKey, Digest: *digest, MaxRawBytes: *readCheckMaxBytes, Apply: *apply, Release: *mode == "archive-local-unpin", ExpectedToken: *localPinToken})
		if err != nil {
			// Registration can have committed a protective pending pin even if
			// readback subsequently failed. Preserve that bounded state receipt.
			_ = json.NewEncoder(os.Stdout).Encode(report)
			reportMaintenanceError(err)
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
			os.Exit(1)
		}
		return
	}
	if *mode == "archive-read-check" {
		dsn := os.Getenv("FUGUE_DATABASE_URL")
		if dsn == "" || !archiveReadCheckDigest.MatchString(*digest) || *readCheckMaxBytes < 1 || *readCheckMaxBytes > inputarchive.MaxInputBytes {
			fmt.Fprintln(os.Stderr, "archive-read-check requires a SHA-256 digest and explicit 1..1GiB raw-byte budget")
			os.Exit(2)
		}
		report, err := store.RunCompilerInputArchiveReadCheck(ctx, dsn, store.CompilerInputArchiveReadCheckOptions{Digest: *digest, MaxRawBytes: *readCheckMaxBytes})
		if err != nil {
			reportMaintenanceError(err)
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
			os.Exit(1)
		}
		return
	}
	if *mode == "archive-hot-local-readback" {
		dsn := os.Getenv("FUGUE_DATABASE_URL")
		if dsn == "" || !archiveReadCheckDigest.MatchString(*digest) || *readCheckMaxBytes < 1 || *readCheckMaxBytes > inputarchive.MaxInputBytes {
			fmt.Fprintln(os.Stderr, "archive-hot-local-readback requires a SHA-256 digest and explicit 1..1GiB raw-byte budget")
			os.Exit(2)
		}
		report, err := store.RunCompilerInputLocalReadback(ctx, dsn, store.CompilerInputLocalReadbackOptions{Digest: *digest, MaxRawBytes: *readCheckMaxBytes})
		if err != nil {
			reportMaintenanceError(err)
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
			os.Exit(1)
		}
		return
	}
	if *mode == "archive-hot-inventory" || *mode == "archive-hot-roots" {
		dsn := os.Getenv("FUGUE_DATABASE_URL")
		if dsn == "" {
			fmt.Fprintln(os.Stderr, "FUGUE_DATABASE_URL is required")
			os.Exit(2)
		}
		run := store.RunCompilerInputHotInventory
		if *mode == "archive-hot-roots" {
			run = store.RunCompilerInputHotRootsInventory
		}
		report, err := run(ctx, dsn)
		if err != nil {
			_ = writeHotInventoryFailure(os.Stdout, report, err)
			fmt.Fprintln(os.Stderr, "hot inventory incomplete; stage="+store.HotInventoryFailureStage(err))
			reportMaintenanceError(err)
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
			os.Exit(1)
		}
		return
	}
	if *mode == "archive-batch" || *mode == "archive-route-batch" {
		dsn := os.Getenv("FUGUE_DATABASE_URL")
		if dsn == "" || *rehearse {
			fmt.Fprintln(os.Stderr, "archive batch requires a database and explicit copy intent")
			os.Exit(2)
		}
		encoder := json.NewEncoder(os.Stdout)
		emit := func(report store.CompilerInputArchiveMigrationReport) error { return encoder.Encode(report) }
		var err error
		if *mode == "archive-route-batch" {
			err = store.RunCompilerInputArchiveRouteBatch(ctx, dsn, *archiveLimit, emit)
		} else {
			err = store.RunCompilerInputArchiveBatch(ctx, dsn, *archiveLimit, *apply, emit)
		}
		if err != nil {
			reportMaintenanceError(err)
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
			reportMaintenanceError(err)
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
			os.Exit(1)
		}
		return
	}
	if hotMode {
		dsn := os.Getenv("FUGUE_DATABASE_URL")
		if dsn == "" {
			fmt.Fprintln(os.Stderr, "FUGUE_DATABASE_URL is required")
			os.Exit(2)
		}
		options := store.CompilerInputArchiveHotOptions{Digest: *digest, Apply: *apply, MaxStoredBytes: *hotMaxStoredBytes}
		run := store.RunCompilerInputArchiveHotCopy
		if *mode == "archive-hot-restore" {
			run = store.RunCompilerInputArchiveHotRestore
		}
		report, err := run(ctx, dsn, options)
		if err != nil {
			reportMaintenanceError(err)
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
			reportMaintenanceError(err)
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
			writeRetirementError(os.Stderr, err)
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
		reportMaintenanceError(err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
		os.Exit(1)
	}
}

func writeRetirementError(output io.Writer, err error) {
	if errors.Is(err, store.ErrCompilerInputArchiveRetirementSafetyUnavailable) {
		fmt.Fprintln(output, "compiler input retirement refused before database/object access: full local hot dependencies, persistent reactivation admission and actual current/rollback reader proofs are not yet enforced")
		return
	}
	fmt.Fprintln(output, "compiler input retirement not confirmed; no success should be inferred; inspect control state and relation size")
}

var archiveReadCheckDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func reportMaintenanceError(err error) {
	fmt.Fprintln(os.Stderr, "compiler input maintenance completion not confirmed; inspect state before retrying; category="+maintenanceErrorCategory(err))
}

// Failure output is explicitly non-authoritative and aggregate only. Do not
// serialize an arbitrary error, partial graph, connection or compiler contents.
func writeHotInventoryFailure(output io.Writer, report store.CompilerInputHotInventory, err error) error {
	return json.NewEncoder(output).Encode(struct {
		Schema                string                          `json:"schema"`
		InspectionComplete    bool                            `json:"inspection_complete"`
		MetadataComplete      bool                            `json:"metadata_complete"`
		LocalCopiesVerified   bool                            `json:"local_copies_verified"`
		RetirementAuthorized  bool                            `json:"retirement_authorized"`
		PlanSHA256            string                          `json:"plan_sha256"`
		Stage                 string                          `json:"stage"`
		Reason                string                          `json:"reason"`
		Category              string                          `json:"category"`
		CoherentMetadata      bool                            `json:"coherent_metadata"`
		RootInventoryComplete bool                            `json:"root_inventory_complete"`
		RootCounts            map[string]int                  `json:"root_counts"`
		Progress              inputarchive.HotClosureProgress `json:"progress"`
		Cost                  store.CompilerInputHotCost      `json:"cost"`
	}{Schema: "fugue.compiler-input-hot-inventory-failure/v1",
		Stage: store.HotInventoryFailureStage(err), Reason: inputarchive.HotClosureFailureReason(err),
		Category: maintenanceErrorCategory(err), CoherentMetadata: report.CoherentMetadata,
		RootInventoryComplete: report.RootInventoryComplete, RootCounts: report.RootCounts, Progress: report.Progress, Cost: report.Cost})
}

// Return only a bounded category, never driver messages, SQL, URLs or keys.
func maintenanceErrorCategory(err error) string {
	var pgError *pgconn.PgError
	var networkError net.Error
	switch {
	case errors.As(err, &pgError):
		if len(pgError.Code) == 5 {
			for _, c := range pgError.Code {
				if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z') {
					return "unclassified"
				}
			}
			return pgError.Code
		}
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, store.ErrConflict):
		return "conflict"
	case errors.Is(err, store.ErrInvalidInput):
		return "invalid-input"
	case errors.As(err, &networkError):
		return "network"
	}
	return "unclassified"
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

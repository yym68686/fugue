// Operator maintenance: default inspection is read-only. Rehearsals explicitly
// write and roll back; apply is one bounded input, never a complete store scan.
package main

import (
	"bytes"
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

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/inputarchive"
	"fugue/internal/inputhistory"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

func main() {
	if handled, err := inputhistory.WriteCapabilities(os.Args[1:], "store", os.Stdout); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		return
	}
	mode := flag.String("mode", "inspect", "inspect, migrate, rollback, collect, archive, archive-copy, archive-batch, archive-route, archive-route-batch, archive-unroute, archive-hot-copy, archive-hot-restore, archive-hot-inventory, archive-hot-roots, archive-retention-inventory, archive-hot-index, archive-hot-local-readback, archive-local-pin, archive-local-unpin, archive-read-check, archive-write-fence, archive-retirement-plan, archive-retire, task-input-prepare, task-input-plan, task-input-restore, task-input-confirm, publication-input-plan, publication-input-restore, publication-input-confirm, consumer-input-plan, consumer-input-restore, consumer-input-confirm, compiler-source-verify, encoded-backup, decode-backup, observe")
	copyContract := flag.String("copy-contract", "", "strict single-input archive-copy JSON contract path")
	sourceVerificationContract := flag.String("source-verification-contract", "", "strict one-parent read-only compiler-source verification contract; no caller keys")
	backupContract := flag.String("backup-contract", "", "exact encoded-backup/decode-backup contract; independently authenticated original SHA")
	backupInput := flag.String("backup-input", "", "private encoded backup file for offline decode-backup")
	backupOutput := flag.String("backup-output", "", "new 0600 original-byte output in a private directory; offline decode-backup only")
	taskContract := flag.String("task-input-contract", "", "strict bounded task-input-prepare contract path; trusted API-role maintenance only")
	taskPlan := flag.String("task-input-plan", "", "strict signed task preparation plan path; contains a public verification key only")
	taskBackend := flag.String("task-input-backend", "", "exact archive backend fingerprint for trusted task-input-plan")
	publicationContract := flag.String("publication-input-contract", "", "strict bounded ordinary publication preparation contract; trusted plan only")
	publicationPlan := flag.String("publication-input-plan", "", "strict signed publication preparation plan with public verification key")
	publicationBackend := flag.String("publication-input-backend", "", "exact archive backend fingerprint for trusted publication-input-plan")
	consumerContract := flag.String("consumer-input-contract", "", "strict bounded consumer preparation contract; trusted plan only")
	consumerPlan := flag.String("consumer-input-plan", "", "strict signed consumer preparation plan with public verification key")
	consumerBackend := flag.String("consumer-input-backend", "", "exact archive backend fingerprint for trusted consumer-input-plan")
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
	hotIndexAfter := flag.String("hot-index-after", "", "exclusive artifact key cursor for one derived dependency index page")
	hotIndexLimit := flag.Int("hot-index-limit", 1, "maximum derived DNS dependency index entries, 1 through 32")
	archiveSegmentBytes := flag.Int("archive-segment-bytes", 16<<20, "bounded archive segment bytes; applies only to new receipts")
	hotMaxStoredBytes := flag.Int64("hot-copy-max-stored-bytes", 0, "explicit stored-byte budget for one archive hot copy or restore; source remains intact")
	readCheckMaxBytes := flag.Int64("read-check-max-bytes", 0, "explicit raw-byte ceiling for one complete read-only input check, at most 1GiB")
	localPinKey := flag.String("local-pin-key", "", "unique local pin key, never reused after release")
	localPinToken := flag.Int64("local-pin-token", 0, "exact positive token required to release a local pin")
	rehearse := flag.Bool("rehearse", false, "encode/verify then roll back (consumes WAL and I/O)")
	maxBytes := flag.Int64("max-input-bytes", 128<<20, "maximum logical input bytes; hard limit 128MiB")
	flag.Parse()
	hotMode := *mode == "archive-hot-copy" || *mode == "archive-hot-restore"
	taskTicketMode := *mode == "task-input-plan" || *mode == "task-input-restore" || *mode == "task-input-confirm"
	publicationTicketMode := *mode == "publication-input-plan" || *mode == "publication-input-restore" || *mode == "publication-input-confirm"
	consumerTicketMode := *mode == "consumer-input-plan" || *mode == "consumer-input-restore" || *mode == "consumer-input-confirm"
	localPinMode := *mode == "archive-local-pin" || *mode == "archive-local-unpin"
	readOnly := *mode == "inspect" || *mode == "observe" || *mode == "task-input-plan" || *mode == "publication-input-plan" || *mode == "consumer-input-plan" || *mode == "archive-hot-inventory" || *mode == "archive-hot-roots" || *mode == "archive-hot-local-readback" || *mode == "archive-read-check" || *mode == "archive-retirement-plan" || (*mode == "archive" || *mode == "archive-batch" || hotMode || *mode == "archive-local-pin" || *mode == "task-input-prepare") && !*apply
	readOnly = readOnly || *mode == "archive-retention-inventory"
	readOnly = readOnly || *mode == "compiler-source-verify"
	backupMode := *mode == "encoded-backup" || *mode == "decode-backup"
	readOnly = readOnly || backupMode
	routeMode := *mode == "archive-route" || *mode == "archive-route-batch" || *mode == "archive-unroute"
	writeFenceMode := *mode == "archive-write-fence"
	retirementMode := *mode == "archive-retire"
	if flag.NArg() != 0 || *apply && *rehearse || (*mode == "archive" || routeMode || hotMode || localPinMode || writeFenceMode || retirementMode) && *rehearse || routeMode && !*apply || writeFenceMode && !*apply || retirementMode && !*apply || *disarm && !writeFenceMode || !readOnly && !*apply && !*rehearse || readOnly && (*apply || *rehearse) {
		fmt.Fprintln(os.Stderr, "select a read-only mode, or explicitly choose apply/rehearse")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if backupMode {
		selected := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { selected[f.Name] = true })
		if !validEncodedBackupFlags(*mode, selected) || *backupContract == "" || (*mode == "encoded-backup" && os.Getenv("FUGUE_DATABASE_URL") == "") {
			fmt.Fprintln(os.Stderr, "backup requires only its exact contract and mode-specific private paths; export requires API-role database")
			os.Exit(2)
		}
		report, err := runEncodedBackup(ctx, *mode, *backupContract, *backupInput, *backupOutput, os.Stdout)
		if encodeErr := json.NewEncoder(os.Stderr).Encode(report); encodeErr != nil {
			os.Exit(1)
		}
		if err != nil {
			reportMaintenanceError(err)
			os.Exit(1)
		}
		return
	}
	if *backupContract != "" || *backupInput != "" || *backupOutput != "" {
		fmt.Fprintln(os.Stderr, "backup flags require encoded-backup or decode-backup mode")
		os.Exit(2)
	}
	if *mode == "compiler-source-verify" {
		selected := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { selected[f.Name] = true })
		if !validSourceVerificationFlags(selected) || *sourceVerificationContract == "" || os.Getenv("FUGUE_DATABASE_URL") == "" {
			fmt.Fprintln(os.Stderr, "compiler-source-verify requires only mode, exact contract and API-role database")
			os.Exit(2)
		}
		report, err := runSourceVerification(ctx, *sourceVerificationContract)
		if encodeErr := json.NewEncoder(os.Stdout).Encode(report); encodeErr != nil {
			os.Exit(1)
		}
		if err != nil {
			reportMaintenanceError(err)
			os.Exit(1)
		}
		return
	}
	if *sourceVerificationContract != "" {
		fmt.Fprintln(os.Stderr, "source verification contract requires compiler-source-verify mode")
		os.Exit(2)
	}
	if consumerTicketMode {
		allowed := map[string]bool{"mode": true}
		if *mode == "consumer-input-plan" {
			allowed["consumer-input-contract"], allowed["consumer-input-backend"] = true, true
		} else {
			allowed["consumer-input-plan"], allowed["apply"] = true, true
		}
		other := false
		flag.Visit(func(f *flag.Flag) { other = other || !allowed[f.Name] })
		if other || *rehearse {
			fmt.Fprintln(os.Stderr, "consumer preparation mode has unrelated flags")
			os.Exit(2)
		}
		output, err := runConsumerPreparation(ctx, *mode, *consumerContract, *consumerPlan, *consumerBackend)
		if output != nil {
			_ = json.NewEncoder(os.Stdout).Encode(output)
		}
		if err != nil {
			reportMaintenanceError(err)
			os.Exit(1)
		}
		return
	}
	if publicationTicketMode {
		allowed := map[string]bool{"mode": true}
		if *mode == "publication-input-plan" {
			allowed["publication-input-contract"], allowed["publication-input-backend"] = true, true
		} else {
			allowed["publication-input-plan"], allowed["apply"] = true, true
		}
		otherFlag := false
		flag.Visit(func(f *flag.Flag) { otherFlag = otherFlag || !allowed[f.Name] })
		if otherFlag || *rehearse {
			fmt.Fprintln(os.Stderr, "publication preparation mode has unrelated flags")
			os.Exit(2)
		}
		output, err := runPublicationPreparation(ctx, *mode, *publicationContract, *publicationPlan, *publicationBackend)
		if output != nil {
			_ = json.NewEncoder(os.Stdout).Encode(output)
		}
		if err != nil {
			reportMaintenanceError(err)
			os.Exit(1)
		}
		return
	}
	if taskTicketMode {
		allowed := map[string]bool{"mode": true}
		if *mode == "task-input-plan" {
			allowed["task-input-contract"], allowed["task-input-backend"] = true, true
		} else {
			allowed["task-input-plan"], allowed["apply"] = true, true
		}
		otherFlag := false
		flag.Visit(func(f *flag.Flag) { otherFlag = otherFlag || !allowed[f.Name] })
		if otherFlag || *rehearse {
			fmt.Fprintln(os.Stderr, "task preparation mode has unrelated flags")
			os.Exit(2)
		}
		var output any
		var err error
		if *mode == "task-input-plan" {
			var c store.NodeTaskInputPreparationContract
			c, err = readTaskPreparationContract(*taskContract)
			if err == nil {
				output, err = store.RunNodeTaskInputPreparationPlan(ctx, os.Getenv("FUGUE_DATABASE_URL"), taskPreparationKeyring(), c, *taskBackend)
			}
		} else {
			var plan store.NodeTaskInputPreparationPlan
			plan, err = readTaskInputPreparationPlan(*taskPlan)
			if err == nil && *mode == "task-input-confirm" {
				output, err = store.RunNodeTaskInputPreparationConfirmation(ctx, os.Getenv("FUGUE_DATABASE_URL"), taskPreparationKeyring(), plan.Ticket)
			} else if err == nil {
				var reader inputarchive.S3Config
				// The public key is pinned independently by the immutable trusted
				// runner intent, never accepted solely from the plan being read.
				key := os.Getenv("FUGUE_TASK_INPUT_PREPARATION_VERIFICATION_KEY")
				if key == "" || key != plan.VerificationKey {
					err = store.ErrConflict
				} else {
					reader, err = store.NodeTaskPreparationReaderFromEnv()
				}
				if err == nil {
					output, err = store.RunNodeTaskInputRestore(ctx, os.Getenv("FUGUE_DATABASE_URL"), plan.Ticket, key, reader)
				}
			}
		}
		if output != nil {
			_ = json.NewEncoder(os.Stdout).Encode(output)
		}
		if err != nil {
			reportMaintenanceError(err)
			os.Exit(1)
		}
		return
	}
	if *mode == "task-input-prepare" {
		otherFlag := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name != "mode" && f.Name != "apply" && f.Name != "task-input-contract" {
				otherFlag = true
			}
		})
		if *taskContract == "" || *rehearse || otherFlag {
			fmt.Fprintln(os.Stderr, "task-input-prepare requires an exact contract and no unrelated flags")
			os.Exit(2)
		}
		file, err := os.Open(*taskContract)
		if err != nil {
			fmt.Fprintln(os.Stderr, "task input contract unavailable")
			os.Exit(2)
		}
		c, err := decodeTaskInputPreparationContract(file, *apply)
		_ = file.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid task input contract")
			os.Exit(2)
		}
		cfg := config.APIFromEnv()
		keys := bundleauth.NewKeyring(cfg.BundleSigningKey, cfg.BundleSigningKeyID, cfg.BundleSigningPreviousKey, cfg.BundleSigningPreviousKeyID, cfg.BundleRevokedKeyIDs)
		report, err := store.RunNodeTaskInputPreparation(ctx, os.Getenv("FUGUE_DATABASE_URL"), keys, c, *apply)
		_ = json.NewEncoder(os.Stdout).Encode(report)
		if err != nil {
			reportMaintenanceError(err)
			os.Exit(1)
		}
		return
	}
	if *mode == "archive-copy" {
		var c store.CompilerInputArchiveCopyContract
		otherFlag := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name != "mode" && f.Name != "apply" && f.Name != "copy-contract" {
				otherFlag = true
			}
		})
		if *copyContract == "" || !*apply || *rehearse || otherFlag {
			fmt.Fprintln(os.Stderr, "archive-copy requires explicit apply and a copy contract")
			os.Exit(2)
		}
		file, err := os.Open(*copyContract)
		if err != nil {
			fmt.Fprintln(os.Stderr, "copy contract unavailable")
			os.Exit(2)
		}
		decoder := json.NewDecoder(io.LimitReader(file, 16385))
		decoder.DisallowUnknownFields()
		err = decoder.Decode(&c)
		var extra any
		if err != nil || decoder.Decode(&extra) != io.EOF || c.Validate() != nil {
			_ = file.Close()
			fmt.Fprintln(os.Stderr, "invalid copy contract")
			os.Exit(2)
		}
		_ = file.Close()
		report, err := store.RunCompilerInputArchiveCopy(ctx, os.Getenv("FUGUE_DATABASE_URL"), c)
		_ = writeArchiveCopyReport(os.Stdout, report, err)
		if err != nil {
			reportMaintenanceError(err)
			os.Exit(1)
		}
		return
	}
	if *mode == "observe" {
		if err := observeStorage(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "runtime storage observation unavailable")
			os.Exit(1)
		}
		return
	}
	if *mode == "archive-retention-inventory" {
		// An audit cannot be converted into a maintenance action by accepting
		// otherwise unrelated apply, route, contract or rehearsal flags.
		other := false
		flag.Visit(func(f *flag.Flag) { other = other || f.Name != "mode" })
		if other || os.Getenv("FUGUE_DATABASE_URL") == "" {
			fmt.Fprintln(os.Stderr, "archive-retention-inventory requires only mode and a database")
			os.Exit(2)
		}
		report, err := store.RunCompilerInputRetentionInventory(ctx, os.Getenv("FUGUE_DATABASE_URL"))
		if encodeErr := writeRetentionInventoryReport(os.Stdout, report, err); encodeErr != nil {
			os.Exit(1)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "retention inventory incomplete; stage="+store.HotInventoryFailureStage(err))
			os.Exit(1)
		}
		return
	}
	if *mode == "archive-hot-index" {
		if *hotIndexLimit < 1 || *hotIndexLimit > 32 || len(*hotIndexAfter) > 512 {
			fmt.Fprintln(os.Stderr, "archive-hot-index requires a bounded keyset page")
			os.Exit(2)
		}
		report, err := store.RunCompilerInputHotIndex(ctx, os.Getenv("FUGUE_DATABASE_URL"), store.CompilerInputHotIndexOptions{AfterID: *hotIndexAfter, Limit: *hotIndexLimit, Apply: *apply})
		if err != nil {
			reportMaintenanceError(err)
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(report); err != nil {
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

func decodeTaskInputPreparationContract(r io.Reader, apply bool) (store.NodeTaskInputPreparationContract, error) {
	var c store.NodeTaskInputPreparationContract
	raw, err := io.ReadAll(io.LimitReader(r, 16385))
	if err != nil || len(raw) > 16384 {
		return c, store.ErrInvalidInput
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, store.ErrInvalidInput
	}
	var extra any
	if d.Decode(&extra) != io.EOF || c.Validate(apply) != nil {
		return c, store.ErrInvalidInput
	}
	return c, nil
}

func readTaskPreparationContract(path string) (store.NodeTaskInputPreparationContract, error) {
	if path == "-" {
		return decodeTaskInputPreparationContract(os.Stdin, false)
	}
	file, err := os.Open(path)
	if err != nil {
		return store.NodeTaskInputPreparationContract{}, store.ErrInvalidInput
	}
	defer file.Close()
	return decodeTaskInputPreparationContract(file, false)
}

// Trusted coordination uses bounded stdin, never a file write or a shell in a
// serving API container. The isolated worker reads its immutable Secret mount.
func readTaskInputPreparationPlan(path string) (store.NodeTaskInputPreparationPlan, error) {
	if path == "-" {
		return decodeTaskInputPreparationPlan(os.Stdin, time.Now())
	}
	file, err := os.Open(path)
	if err != nil {
		return store.NodeTaskInputPreparationPlan{}, store.ErrInvalidInput
	}
	defer file.Close()
	return decodeTaskInputPreparationPlan(file, time.Now())
}

func taskPreparationKeyring() bundleauth.Keyring {
	cfg := config.APIFromEnv()
	return bundleauth.NewKeyring(cfg.BundleSigningKey, cfg.BundleSigningKeyID, cfg.BundleSigningPreviousKey, cfg.BundleSigningPreviousKeyID, cfg.BundleRevokedKeyIDs)
}

func decodeTaskInputPreparationPlan(r io.Reader, now time.Time) (store.NodeTaskInputPreparationPlan, error) {
	var p store.NodeTaskInputPreparationPlan
	// The ticket itself has an independent 512KiB signed bound. Include the
	// public-key envelope, but reject oversized whitespace as well as fields.
	raw, err := io.ReadAll(io.LimitReader(r, (513<<10)+1))
	if err != nil || len(raw) > 513<<10 {
		return p, store.ErrInvalidInput
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, store.ErrInvalidInput
	}
	var extra any
	if d.Decode(&extra) != io.EOF || p.Ticket.Validate(p.VerificationKey, now) != nil {
		return p, store.ErrInvalidInput
	}
	return p, nil
}

func writeRetirementError(output io.Writer, err error) {
	if errors.Is(err, store.ErrCompilerInputArchiveRetirementSafetyUnavailable) {
		fmt.Fprintln(output, "compiler input retirement refused before database/object access: full local hot dependencies, persistent reactivation admission and actual current/rollback reader proofs are not yet enforced")
		return
	}
	fmt.Fprintln(output, "compiler input retirement not confirmed; no success should be inferred; inspect control state and relation size")
}

var archiveReadCheckDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func writeArchiveCopyReport(output io.Writer, report store.CompilerInputArchiveCopyReport, err error) error {
	type failure struct {
		Stage    string `json:"stage"`
		Category string `json:"category"`
		Ordinal  int    `json:"segment_ordinal"`
		Received int    `json:"segment_bytes_read"`
	}
	receipt := struct {
		store.CompilerInputArchiveCopyReport
		Failure *failure `json:"failure,omitempty"`
	}{CompilerInputArchiveCopyReport: report}
	if err != nil {
		stage := "unclassified"
		ordinal, received, source := inputarchive.SourceReadFailure(err)
		if source {
			stage = "source-read"
		}
		receipt.Failure = &failure{Stage: stage, Category: maintenanceErrorCategory(err), Ordinal: ordinal, Received: received}
	}
	return json.NewEncoder(output).Encode(receipt)
}

func reportMaintenanceError(err error) {
	fmt.Fprintln(os.Stderr, "compiler input maintenance completion not confirmed; inspect state before retrying; category="+maintenanceErrorCategory(err))
}

func writeRetentionInventoryReport(output io.Writer, report store.CompilerInputRetentionInventory, err error) error {
	type failure struct {
		Stage    string `json:"stage"`
		Category string `json:"category"`
	}
	receipt := struct {
		store.CompilerInputRetentionInventory
		Failure *failure `json:"failure,omitempty"`
	}{CompilerInputRetentionInventory: report}
	if err != nil {
		// Preserve snapshot/cost diagnostics, but never a partial complement or
		// a digest/complete flag from an unsuccessful audit. Error text can
		// contain SQL, URLs or source data; emit only the existing safe enums.
		receipt.UniverseComplete, receipt.ClassificationComplete, receipt.ExecutionAuthorized = false, false, false
		receipt.Entries, receipt.Totals, receipt.AuditSHA256 = nil, nil, ""
		receipt.Failure = &failure{Stage: store.HotInventoryFailureStage(err), Category: maintenanceErrorCategory(err)}
	}
	return json.NewEncoder(output).Encode(receipt)
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
	case errors.Is(err, inputarchive.ErrBudget):
		return "budget"
	case errors.Is(err, inputarchive.ErrIntegrity):
		return "integrity"
	case errors.Is(err, inputarchive.ErrInvalid):
		return "invalid-input"
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

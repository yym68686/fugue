package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"time"

	"fugue/internal/inputarchive"
)

var compilerInputRetirementPlanHash = regexp.MustCompile(`^[0-9a-f]{64}$`)
var compilerInputRetirementRelease = regexp.MustCompile(`^[0-9a-f]{40}$`)

type CompilerInputArchiveRetirementOptions struct {
	Apply                        bool
	RoutingEnabled               bool
	ExpectedPlanSHA256           string
	CurrentReaderRelease         string
	RollbackReaderRelease        string
	ArchiveRetentionPolicySHA256 string
	ReaderDrainTimeout           time.Duration
}

type CompilerInputArchiveRetirementInput struct {
	Digest                string `json:"digest"`
	Format                string `json:"format,omitempty"`
	Location              string `json:"location,omitempty"`
	LegacyChunkRows       int64  `json:"legacy_chunk_rows"`
	FirstOrdinal          int64  `json:"first_ordinal"`
	LastOrdinal           int64  `json:"last_ordinal"`
	ExpectedChunks        int64  `json:"expected_chunks,omitempty"`
	LogicalBytes          int64  `json:"logical_bytes,omitempty"`
	Replacement           string `json:"replacement,omitempty"`
	ReplacementVerified   bool   `json:"replacement_verified"`
	ArchiveManifestSHA256 string `json:"archive_manifest_sha256,omitempty"`
	Blocker               string `json:"blocker,omitempty"`
}

type CompilerInputArchiveRetirementReport struct {
	Action                       string                                `json:"action"`
	Applied                      bool                                  `json:"applied"`
	Database                     string                                `json:"database"`
	LegacyRelationOID            int64                                 `json:"legacy_relation_oid"`
	LegacyHeapBytes              int64                                 `json:"legacy_heap_bytes"`
	LegacyIndexBytes             int64                                 `json:"legacy_index_bytes"`
	LegacyTotalRelationBytes     int64                                 `json:"legacy_total_relation_bytes"`
	LegacyChunkRows              int64                                 `json:"legacy_chunk_rows"`
	InputCount                   int64                                 `json:"input_count"`
	CoveredInputCount            int64                                 `json:"covered_input_count"`
	BlockedInputCount            int64                                 `json:"blocked_input_count"`
	OrphanChunkRows              int64                                 `json:"orphan_chunk_rows"`
	VerifiedArchiveInputs        int64                                 `json:"verified_archive_inputs"`
	VerifiedHotInputs            int64                                 `json:"verified_hot_inputs"`
	ActiveArchiveLeases          int64                                 `json:"active_archive_leases"`
	WritesFenced                 bool                                  `json:"writes_fenced"`
	WriteTriggerInstalled        bool                                  `json:"write_trigger_installed"`
	ArchiveRetentionGuards       bool                                  `json:"archive_retention_guards_installed"`
	RoutingEnabled               bool                                  `json:"routing_enabled"`
	ReplacementCoverageComplete  bool                                  `json:"replacement_coverage_complete"`
	SafetyBlockers               []string                              `json:"safety_blockers"`
	CanRetire                    bool                                  `json:"can_retire"`
	ReclaimableRelationBytes     int64                                 `json:"reclaimable_relation_bytes"`
	ReclaimableLogicalBytes      int64                                 `json:"reclaimable_logical_bytes"`
	PlanSHA256                   string                                `json:"plan_sha256"`
	CurrentReaderRelease         string                                `json:"current_reader_release,omitempty"`
	RollbackReaderRelease        string                                `json:"rollback_reader_release,omitempty"`
	ArchiveRetentionPolicySHA256 string                                `json:"archive_retention_policy_sha256,omitempty"`
	TransactionDrain             *CompilerInputArchiveTransactionDrain `json:"transaction_drain,omitempty"`
	Inputs                       []CompilerInputArchiveRetirementInput `json:"inputs"`
}

type compilerInputRetirementMetadata struct {
	Database string
	OID      int64
	Heap     int64
	Indexes  int64
	Total    int64
}

type compilerInputArchiveRetirementCandidate struct {
	item           CompilerInputArchiveRetirementInput
	source         inputManifest
	manifestValid  bool
	archiveState   sql.NullString
	archiveFormat  sql.NullString
	sourceFormat   sql.NullString
	rawBytes       sql.NullInt64
	backendHash    sql.NullString
	verifiedDigest sql.NullString
	sourceEpoch    sql.NullInt64
	fencingToken   sql.NullInt64
	segmentCount   sql.NullInt64
	verifiedAt     sql.NullTime
	routedAt       sql.NullTime
}

// RunCompilerInputArchiveRetirement inventories replacement storage, including
// full object readback. Replacement coverage does not authorize retirement:
// complete local hot dependencies, persistent reactivation admission and actual
// current/rollback reader compatibility are not yet enforced by this path.
// Apply therefore fails before opening a database or reading any object.
func RunCompilerInputArchiveRetirement(ctx context.Context, databaseURL string, options CompilerInputArchiveRetirementOptions) (CompilerInputArchiveRetirementReport, error) {
	if options.Apply {
		if !compilerInputRetirementPlanHash.MatchString(options.ExpectedPlanSHA256) ||
			!compilerInputRetirementRelease.MatchString(options.CurrentReaderRelease) ||
			!compilerInputRetirementRelease.MatchString(options.RollbackReaderRelease) ||
			!compilerInputRetirementPlanHash.MatchString(options.ArchiveRetentionPolicySHA256) {
			return CompilerInputArchiveRetirementReport{}, ErrInvalidInput
		}
		if options.ReaderDrainTimeout <= 0 {
			options.ReaderDrainTimeout = 30 * time.Second
		}
		if options.ReaderDrainTimeout > 5*time.Minute {
			return CompilerInputArchiveRetirementReport{}, ErrInvalidInput
		}
		return restrictCompilerInputArchiveRetirement(CompilerInputArchiveRetirementReport{
			Action: "archive-retire", Inputs: []CompilerInputArchiveRetirementInput{},
		}), ErrCompilerInputArchiveRetirementSafetyUnavailable
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return CompilerInputArchiveRetirementReport{}, err
	}
	defer db.Close()
	cfg, configured, err := resolveCompilerInputArchiveConfig(ctx, db, false)
	if err != nil {
		return CompilerInputArchiveRetirementReport{}, fmt.Errorf("compiler input archive configuration is invalid: %w", ErrConflict)
	}
	routingEnabled, err := inputarchive.RoutingEnabledFromEnv()
	if err != nil {
		return CompilerInputArchiveRetirementReport{}, fmt.Errorf("compiler input archive routing configuration is invalid: %w", ErrConflict)
	}
	var objects inputarchive.Objects
	backendHash := ""
	if configured {
		objects, err = inputarchive.NewS3(cfg)
		if err != nil {
			return CompilerInputArchiveRetirementReport{}, err
		}
		backendHash, err = cfg.Fingerprint()
		if err != nil {
			return CompilerInputArchiveRetirementReport{}, err
		}
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(ctx, 6*time.Hour)
	defer cancel()
	report, err := compilerInputArchiveRetirementPlan(ctx, db, objects, backendHash, true, routingEnabled)
	return restrictCompilerInputArchiveRetirement(report), err
}

func compilerInputArchiveRetirementPlan(ctx context.Context, db *sql.DB, objects inputarchive.Objects, backendHash string, verifyObjects bool, routingEnabled ...bool) (CompilerInputArchiveRetirementReport, error) {
	canRoute := true
	if len(routingEnabled) > 0 {
		canRoute = routingEnabled[0]
	}
	return compilerInputArchiveRetirementPlanFromQuery(ctx, db, db, objects, backendHash, verifyObjects, verifyObjects, canRoute)
}

func compilerInputArchiveRetirementPlanFromQuery(ctx context.Context, query inputQueryer, streamDB *sql.DB, objects inputarchive.Objects, backendHash string, verifyObjects, verifyHot, routingEnabled bool) (CompilerInputArchiveRetirementReport, error) {
	report := CompilerInputArchiveRetirementReport{Action: "archive-retirement-plan", Inputs: []CompilerInputArchiveRetirementInput{}}
	report.RoutingEnabled = routingEnabled
	var metadata compilerInputRetirementMetadata
	if err := query.QueryRowContext(ctx, `SELECT current_database(),c.oid::bigint,pg_relation_size(c.oid)::bigint,pg_indexes_size(c.oid)::bigint,pg_total_relation_size(c.oid)::bigint FROM pg_class c WHERE c.oid=to_regclass('fugue_platform_input_chunks')`).Scan(&metadata.Database, &metadata.OID, &metadata.Heap, &metadata.Indexes, &metadata.Total); err != nil {
		return report, fmt.Errorf("legacy compiler input relation is unavailable: %w", err)
	}
	report.Database, report.LegacyRelationOID = metadata.Database, metadata.OID
	report.LegacyHeapBytes, report.LegacyIndexBytes, report.LegacyTotalRelationBytes = metadata.Heap, metadata.Indexes, metadata.Total
	var controlInstalled bool
	if err := query.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_archive_control') IS NOT NULL`).Scan(&controlInstalled); err != nil || !controlInstalled {
		return report, fmt.Errorf("compiler input archive control is unavailable: %w", ErrConflict)
	}
	var retiredAt sql.NullTime
	if err := query.QueryRowContext(ctx, `SELECT writes_fenced,retirement_started_at FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&report.WritesFenced, &retiredAt); err != nil {
		return report, fmt.Errorf("compiler input archive control row is unavailable: %w", ErrConflict)
	}
	if retiredAt.Valid {
		return report, fmt.Errorf("legacy compiler input relation is already retired: %w", ErrConflict)
	}
	fence, err := compilerInputArchiveWriteFenceState(ctx, query)
	if err != nil {
		return report, err
	}
	inline, err := compilerInputArchiveInlineWriteFenceState(ctx, query)
	if err != nil {
		return report, err
	}
	report.WriteTriggerInstalled = fence.installed() && inline.installed()
	if err := query.QueryRowContext(ctx, `SELECT count(*) FROM fugue_platform_input_archive_leases WHERE expires_at>now()`).Scan(&report.ActiveArchiveLeases); err != nil {
		return report, err
	}
	var epoch int64
	if err := query.QueryRowContext(ctx, `SELECT epoch FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&epoch); err != nil {
		return report, err
	}
	rows, err := query.QueryContext(ctx, `SELECT content_hash,ordinal FROM fugue_platform_input_chunks ORDER BY content_hash,ordinal`)
	if err != nil {
		return report, err
	}
	candidates := make([]compilerInputArchiveRetirementCandidate, 0)
	for rows.Next() {
		var digest string
		var ordinal int64
		if err = rows.Scan(&digest, &ordinal); err != nil {
			break
		}
		if len(candidates) == 0 || candidates[len(candidates)-1].item.Digest != digest {
			candidates = append(candidates, compilerInputArchiveRetirementCandidate{item: CompilerInputArchiveRetirementInput{Digest: digest, FirstOrdinal: ordinal, LastOrdinal: ordinal}})
		}
		candidate := &candidates[len(candidates)-1]
		candidate.item.LegacyChunkRows++
		candidate.item.LastOrdinal = ordinal
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return report, err
	}
	digests := make([]string, len(candidates))
	for i := range candidates {
		digests[i] = candidates[i].item.Digest
		report.LegacyChunkRows += candidates[i].item.LegacyChunkRows
		report.InputCount++
	}
	if len(digests) != 0 {
		rows, err = query.QueryContext(ctx, `SELECT d.content_hash,
 m.format,m.size_bytes,m.chunk_size,m.chunk_count,m.created_at,COALESCE(to_jsonb(m)->>'storage_location','legacy'),
 a.state,a.archive_format,a.source_format,a.raw_bytes,a.backend_hash,a.verified_digest,a.source_epoch,a.fencing_token,a.segment_count,a.verified_at,a.routed_at
FROM unnest($1::text[]) AS d(content_hash)
LEFT JOIN fugue_platform_input_manifests m ON m.content_hash=d.content_hash
LEFT JOIN fugue_platform_input_archives a ON a.content_hash=d.content_hash
ORDER BY d.content_hash`, digests)
		if err != nil {
			return report, err
		}
		index := 0
		for rows.Next() {
			if index >= len(candidates) {
				err = fmt.Errorf("compiler input retirement metadata returned extra rows: %w", ErrConflict)
				break
			}
			candidate := &candidates[index]
			var digest string
			var manifestFormat, location sql.NullString
			var size, chunkSize, chunkCount sql.NullInt64
			var created sql.NullTime
			if err = rows.Scan(&digest, &manifestFormat, &size, &chunkSize, &chunkCount, &created, &location,
				&candidate.archiveState, &candidate.archiveFormat, &candidate.sourceFormat, &candidate.rawBytes, &candidate.backendHash, &candidate.verifiedDigest, &candidate.sourceEpoch, &candidate.fencingToken, &candidate.segmentCount, &candidate.verifiedAt, &candidate.routedAt); err != nil {
				break
			}
			if digest != candidate.item.Digest {
				err = fmt.Errorf("compiler input retirement metadata digest order changed: %w", ErrConflict)
				break
			}
			item := &candidate.item
			item.Format, item.Location = manifestFormat.String, location.String
			item.ExpectedChunks, item.LogicalBytes = chunkCount.Int64, size.Int64
			source := inputManifest{Digest: item.Digest, Format: manifestFormat.String, Location: location.String, Size: size.Int64, ChunkSize: int(chunkSize.Int64), Count: int(chunkCount.Int64)}
			if created.Valid {
				source.Created = created.Time
			}
			candidate.source = source
			switch {
			case !manifestFormat.Valid || !size.Valid || !chunkSize.Valid || !chunkCount.Valid || !created.Valid:
				item.Blocker = "orphan-legacy-chunk-rows"
				report.OrphanChunkRows += item.LegacyChunkRows
			case item.ExpectedChunks <= 0 || item.LegacyChunkRows != item.ExpectedChunks || item.FirstOrdinal != 0 || item.LastOrdinal != item.ExpectedChunks-1:
				item.Blocker = "legacy-chunk-coverage-mismatch"
			default:
				candidate.manifestValid = true
			}
			index++
		}
		if err == nil {
			err = rows.Err()
		}
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return report, err
		}
		if index != len(candidates) {
			return report, fmt.Errorf("compiler input retirement metadata rows missing: %w", ErrConflict)
		}
	}
	for _, candidate := range candidates {
		item := candidate.item
		if candidate.manifestValid {
			source := candidate.source
			switch {
			case source.Location == "hot":
				if verifyHot {
					if streamDB == nil {
						item.Blocker = "hot-copy-not-verified"
					} else {
						stream := &archiveInputStream{ctx: ctx, db: streamDB, m: source, hash: sha256.New(), sourceEpoch: epoch}
						if _, verifyErr := io.Copy(io.Discard, stream); verifyErr != nil || stream.err != nil || !stream.done || stream.count != source.Count || stream.size != source.Size || "sha256:"+hex.EncodeToString(stream.hash.Sum(nil)) != source.Digest {
							item.Blocker = "hot-copy-not-verified"
						} else {
							item.Replacement, item.ReplacementVerified = "verified-hot-chunks", true
							report.VerifiedHotInputs++
						}
					}
				} else {
					item.Replacement, item.ReplacementVerified = "verified-hot-chunks", true
				}
			case source.Location != "legacy" || source.Format != inputFormatV1 && source.Format != inputFormatCodecV2:
				item.Blocker = "unsupported-legacy-source"
			case !candidate.archiveState.Valid || candidate.archiveState.String != "routed" || candidate.archiveFormat.String != inputarchive.Format || candidate.sourceFormat.String != source.Format || candidate.rawBytes.Int64 != source.Size || candidate.backendHash.String != backendHash || candidate.verifiedDigest.String != source.Digest || candidate.sourceEpoch.Int64 < 0 || candidate.fencingToken.Int64 < 0 || candidate.segmentCount.Int64 < 1 || !candidate.verifiedAt.Valid || !candidate.routedAt.Valid:
				item.Blocker = "verified-archive-route-missing"
			default:
				archiveManifest, manifestErr := compilerInputArchiveManifest(ctx, query, source, backendHash, "routed")
				if manifestErr != nil {
					item.Blocker = "archive-route-receipts-invalid"
				} else {
					item.ArchiveManifestSHA256 = compilerInputArchiveManifestHash(archiveManifest)
					if verifyObjects {
						if objects == nil {
							item.Blocker = "archive-backend-unavailable"
						} else if verifyErr := inputarchive.Verify(ctx, objects, archiveManifest); verifyErr != nil {
							if ctx.Err() != nil {
								return report, ctx.Err()
							}
							item.Blocker = "external-archive-readback-failed"
						} else {
							item.Replacement, item.ReplacementVerified = "verified-external-archive", true
							report.VerifiedArchiveInputs++
						}
					} else {
						item.Replacement, item.ReplacementVerified = "verified-external-archive", true
					}
				}
			}
		}
		if item.ReplacementVerified {
			report.CoveredInputCount++
			report.ReclaimableLogicalBytes += item.LogicalBytes
		} else {
			report.BlockedInputCount++
		}
		report.Inputs = append(report.Inputs, item)
	}
	report.CanRetire = report.WritesFenced && report.WriteTriggerInstalled && report.RoutingEnabled && report.ActiveArchiveLeases == 0 && report.BlockedInputCount == 0 && report.OrphanChunkRows == 0
	if report.CanRetire {
		report.ReclaimableRelationBytes = report.LegacyTotalRelationBytes
	}
	binding := struct {
		Database string                                `json:"database"`
		OID      int64                                 `json:"oid"`
		Heap     int64                                 `json:"heap"`
		Indexes  int64                                 `json:"indexes"`
		Total    int64                                 `json:"total"`
		Rows     int64                                 `json:"rows"`
		Backend  string                                `json:"backend"`
		Fenced   bool                                  `json:"fenced"`
		Trigger  bool                                  `json:"trigger"`
		Routing  bool                                  `json:"routing"`
		Leases   int64                                 `json:"leases"`
		Inputs   []CompilerInputArchiveRetirementInput `json:"inputs"`
	}{metadata.Database, metadata.OID, metadata.Heap, metadata.Indexes, metadata.Total, report.LegacyChunkRows, backendHash, report.WritesFenced, report.WriteTriggerInstalled, report.RoutingEnabled, report.ActiveArchiveLeases, report.Inputs}
	encoded, err := json.Marshal(binding)
	if err != nil {
		return report, err
	}
	planHash := sha256.Sum256(encoded)
	report.PlanSHA256 = hex.EncodeToString(planHash[:])
	return report, nil
}

func compilerInputArchiveManifestHash(manifest inputarchive.Manifest) string {
	encoded, _ := json.Marshal(manifest)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// Storage retirement mechanics only. No production entrypoint calls this until
// the independent hot/admission/reader safety gates are implemented. Integration
// tests retain coverage of transaction draining, RESTRICT and atomic rollback;
// a successful storage fixture is not a complete retirement authorization.
func applyCompilerInputArchiveRetirement(ctx context.Context, db *sql.DB, backendHash string, options CompilerInputArchiveRetirementOptions, verified CompilerInputArchiveRetirementReport) (CompilerInputArchiveRetirementReport, error) {
	if options.ExpectedPlanSHA256 != verified.PlanSHA256 || !verified.CanRetire || !options.RoutingEnabled ||
		!compilerInputRetirementRelease.MatchString(options.CurrentReaderRelease) ||
		!compilerInputRetirementRelease.MatchString(options.RollbackReaderRelease) ||
		!compilerInputRetirementPlanHash.MatchString(options.ArchiveRetentionPolicySHA256) {
		return verified, fmt.Errorf("compiler input retirement attestations are incomplete or stale: %w", ErrConflict)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return verified, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return verified, err
	}
	defer tx.Rollback()
	for _, statement := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SELECT pg_advisory_xact_lock(315609238744290)`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return verified, err
		}
	}
	var fenced bool
	var retiredAt sql.NullTime
	if err = tx.QueryRowContext(ctx, `SELECT writes_fenced,retirement_started_at FROM fugue_platform_input_archive_control WHERE singleton FOR UPDATE`).Scan(&fenced, &retiredAt); err != nil || !fenced || retiredAt.Valid {
		return verified, fmt.Errorf("compiler input write fence changed before retirement: %w", ErrConflict)
	}
	fence, err := compilerInputArchiveWriteFenceState(ctx, tx)
	if err != nil || !fence.installed() {
		return verified, fmt.Errorf("compiler input database write trigger is unavailable: %w", ErrConflict)
	}
	inline, err := compilerInputArchiveInlineWriteFenceState(ctx, tx)
	if err != nil || !inline.installed() {
		return verified, fmt.Errorf("compiler input inline database write trigger is unavailable: %w", ErrConflict)
	}
	current, err := compilerInputArchiveRetirementPlanMetadata(ctx, db, backendHash, options.RoutingEnabled)
	if err != nil {
		return verified, err
	}
	if current.PlanSHA256 != verified.PlanSHA256 || !current.CanRetire {
		return verified, fmt.Errorf("compiler input retirement plan changed under writer fence: %w", ErrConflict)
	}
	deadline := time.NewTimer(options.ReaderDrainTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		var acquired bool
		if err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, compilerInputArchiveReadLeaseLockID).Scan(&acquired); err != nil {
			return verified, err
		}
		if acquired {
			break
		}
		select {
		case <-ctx.Done():
			return verified, ctx.Err()
		case <-deadline.C:
			return verified, fmt.Errorf("compiler input readers did not drain before timeout: %w", ErrConflict)
		case <-ticker.C:
		}
	}
	readLeaseHeld := true
	defer func() {
		if readLeaseHeld {
			_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, compilerInputArchiveReadLeaseLockID)
		}
	}()
	var activeLeases int64
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM fugue_platform_input_archive_leases WHERE expires_at>now()`).Scan(&activeLeases); err != nil || activeLeases != 0 {
		return verified, fmt.Errorf("compiler input archive leases became active before retirement: %w", ErrConflict)
	}
	drain, err := compilerInputArchiveTransactionDrain(ctx, tx)
	verified.TransactionDrain = &drain
	if err != nil {
		return verified, err
	}
	if err = installCompilerInputArchiveRetirementGuards(ctx, tx); err != nil {
		return verified, fmt.Errorf("compiler input archive retirement guards could not be installed: %w", ErrConflict)
	}
	verified.ArchiveRetentionGuards = true
	current, err = compilerInputArchiveRetirementPlanMetadata(ctx, db, backendHash, options.RoutingEnabled)
	if err != nil {
		return verified, err
	}
	if current.PlanSHA256 != verified.PlanSHA256 || !current.CanRetire {
		return verified, fmt.Errorf("compiler input retirement plan changed after archive ledgers were fenced: %w", ErrConflict)
	}
	drain, err = compilerInputArchiveTransactionDrain(ctx, tx)
	verified.TransactionDrain = &drain
	if err != nil {
		return verified, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1,retirement_plan_sha256=$1,retirement_reader_current=$2,retirement_reader_rollback=$3,retirement_archive_policy_sha256=$4,retirement_started_at=now(),updated_at=now() WHERE singleton AND writes_fenced=true AND retirement_started_at IS NULL`, verified.PlanSHA256, options.CurrentReaderRelease, options.RollbackReaderRelease, options.ArchiveRetentionPolicySHA256); err != nil {
		return verified, err
	}
	if _, err = tx.ExecContext(ctx, `TRUNCATE TABLE fugue_platform_input_chunks RESTRICT`); err != nil {
		return verified, err
	}
	if err = tx.Commit(); err != nil {
		return verified, fmt.Errorf("compiler input relation retirement outcome requires inspection: %w", err)
	}
	readLeaseHeld = false
	var unlocked bool
	if err = conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock($1)`, compilerInputArchiveReadLeaseLockID).Scan(&unlocked); err != nil || !unlocked {
		return verified, fmt.Errorf("compiler input reader barrier release is not confirmed: %w", ErrConflict)
	}
	verified.Action = "archive-retire"
	verified.Applied = true
	verified.CurrentReaderRelease = options.CurrentReaderRelease
	verified.RollbackReaderRelease = options.RollbackReaderRelease
	verified.ArchiveRetentionPolicySHA256 = options.ArchiveRetentionPolicySHA256
	return verified, nil
}

func installCompilerInputArchiveRetirementGuards(ctx context.Context, tx *sql.Tx) error {
	const ddl = `
CREATE OR REPLACE FUNCTION fugue_platform_input_archive_retirement_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS (SELECT 1 FROM fugue_platform_input_archive_control WHERE singleton AND retirement_started_at IS NOT NULL) THEN
  RAISE EXCEPTION 'compiler input archive is retired' USING ERRCODE='55000';
 END IF;
 IF TG_LEVEL='STATEMENT' THEN RETURN NULL; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER fugue_platform_input_archive_control_retired_row_guard BEFORE UPDATE OR DELETE ON fugue_platform_input_archive_control FOR EACH ROW EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archive_control_retired_truncate_guard BEFORE TRUNCATE ON fugue_platform_input_archive_control FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archives_retired_row_guard BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_input_archives FOR EACH ROW EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archives_retired_truncate_guard BEFORE TRUNCATE ON fugue_platform_input_archives FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archive_segments_retired_row_guard BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_input_archive_segments FOR EACH ROW EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archive_segments_retired_truncate_guard BEFORE TRUNCATE ON fugue_platform_input_archive_segments FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archive_leases_retired_row_guard BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_input_archive_leases FOR EACH ROW EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archive_leases_retired_truncate_guard BEFORE TRUNCATE ON fugue_platform_input_archive_leases FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();`
	if _, err := tx.ExecContext(ctx, ddl); err != nil {
		return err
	}
	installed, err := compilerInputArchiveRetirementGuardsInstalled(ctx, tx)
	if err != nil {
		return err
	}
	if !installed {
		return ErrConflict
	}
	return nil
}

func compilerInputArchiveRetirementGuardsInstalled(ctx context.Context, db inputQueryer) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgenabled IN ('O','A') AND tgfoid=to_regprocedure('fugue_platform_input_archive_retirement_guard()') AND tgname IN (
		'fugue_platform_input_archive_control_retired_row_guard','fugue_platform_input_archive_control_retired_truncate_guard',
		'fugue_platform_input_archives_retired_row_guard','fugue_platform_input_archives_retired_truncate_guard',
		'fugue_platform_input_archive_segments_retired_row_guard','fugue_platform_input_archive_segments_retired_truncate_guard',
		'fugue_platform_input_archive_leases_retired_row_guard','fugue_platform_input_archive_leases_retired_truncate_guard'
	)`).Scan(&count)
	return count == 8, err
}

func compilerInputArchiveRetirementPlanMetadata(ctx context.Context, db inputQueryer, backendHash string, routingEnabled bool) (CompilerInputArchiveRetirementReport, error) {
	// Database-side identity/coverage is re-read while the common writer lock
	// is held. External bytes were fully verified immediately before entering
	// this short critical section and are guarded by the dedicated bucket's
	// immutable-object retention policy.
	return compilerInputArchiveRetirementPlanFromQuery(ctx, db, nil, nil, backendHash, false, false, routingEnabled)
}

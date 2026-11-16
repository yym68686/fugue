package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

// Optional quality comparison history has no serving/rollback retention roots.
// Budgets count canonical payload bytes, not PostgreSQL relation allocation.
const QualityEvidenceBudgetBytes int64 = 1 << 30
const QualityEvidenceMaxObjectBytes = 64 << 20
const QualityEvidenceMaxObjects = 4096
const QualityEvidenceRetention = 14 * 24 * time.Hour
const qualityEvidenceLockID int64 = 315609238744329

type qualityEvidenceCounters struct {
	bytes, rows, attemptedAt, failed atomic.Int64
	deleted, skipped, misses         atomic.Uint64
}

func (s *Store) WritePlatformQualityEvidenceMetrics(w io.Writer) {
	for _, item := range []struct {
		name, help string
		value      float64
	}{
		{"payload_bytes", "Last observed pool payload bytes; max across replicas, not physical allocation.", float64(s.qualityEvidenceMetrics.bytes.Load())},
		{"objects", "Last observed retained quality objects; max across replicas.", float64(s.qualityEvidenceMetrics.rows.Load())},
		{"budget_bytes", "Independent quality evidence payload budget.", float64(QualityEvidenceBudgetBytes)},
		{"retention_seconds", "Maximum evidence-object age from the original input capture.", QualityEvidenceRetention.Seconds()},
		{"last_attempt_timestamp_seconds", "Last pool write or expiry cleanup attempt.", float64(s.qualityEvidenceMetrics.attemptedAt.Load())},
		{"last_run_failed", "Whether the last pool write or expiry cleanup failed.", float64(s.qualityEvidenceMetrics.failed.Load())},
		{"deleted_objects_total", "Objects removed only from the independent optional quality pool.", float64(s.qualityEvidenceMetrics.deleted.Load())},
		{"skipped_objects_total", "Empty, expired or oversized optional quality objects not persisted.", float64(s.qualityEvidenceMetrics.skipped.Load())},
		{"unavailable_reads_total", "Quality objects unavailable or invalid; never health evidence.", float64(s.qualityEvidenceMetrics.misses.Load())},
	} {
		observability.WriteGaugeMetric(w, "fugue_platform_quality_evidence_"+item.name, item.help, nil, item.value)
	}
}

func (s *Store) observeQualityPool(rows []model.PlatformQualityEvidence, incoming *model.PlatformQualityEvidence, victims []string) {
	drop := map[string]bool{}
	for _, digest := range victims {
		drop[digest] = true
	}
	var bytes, count int64
	for _, row := range rows {
		if !drop[row.ContentHash] {
			bytes += row.SizeBytes
			count++
		}
	}
	if incoming != nil {
		bytes += incoming.SizeBytes
		count++
	}
	s.qualityEvidenceMetrics.bytes.Store(bytes)
	s.qualityEvidenceMetrics.rows.Store(count)
	s.qualityEvidenceMetrics.deleted.Add(uint64(len(victims)))
}

// EnsurePlatformCompilationInputsContext preserves old double-write contracts
// verbatim. Only the explicitly selected v38 contract omits the complete input.
func (s *Store) EnsurePlatformCompilationInputsContext(ctx context.Context, c platformconfig.CompileResult) error {
	if c.Lineage.CompilerVersion != platformconfig.CompactOnlyInputCompilerVersion {
		return s.EnsurePlatformCompilerInputPairContext(ctx, c.InputSnapshot, c.Lineage.InputSnapshotDigest, c.RuntimeInput, c.Lineage.RuntimeInputDigest)
	}
	if c.RuntimeInput == nil || c.QualityInput == nil {
		return ErrInvalidInput
	}
	for _, check := range []struct {
		input  platformconfig.RuntimeSnapshot
		digest string
	}{
		{c.InputSnapshot, c.Lineage.InputSnapshotDigest},
		{platformconfig.ConfigurationRuntimeInput(c.InputSnapshot), c.Lineage.RuntimeInputDigest},
		{*c.RuntimeInput, c.Lineage.RuntimeInputDigest},
		{platformconfig.QualityEvidenceInput(c.InputSnapshot), c.ReleaseArtifact.Metadata[platformconfig.QualityEvidenceDigestMetadata]},
		{*c.QualityInput, c.ReleaseArtifact.Metadata[platformconfig.QualityEvidenceDigestMetadata]},
	} {
		digest, err := platformconfig.RuntimeSnapshotDigest(check.input)
		if err != nil || digest != check.digest {
			return ErrConflict
		}
	}
	if err := s.EnsurePlatformCompilerInputContext(ctx, *c.RuntimeInput, c.Lineage.RuntimeInputDigest); err != nil {
		return err
	}
	// Comparison history is optional. Its bounded transaction reports failures
	// through metrics, but cannot make an otherwise durable operational input
	// unavailable or force a producer to reuse old observations.
	_ = s.ensurePlatformQualityEvidence(ctx, *c.QualityInput, c.ReleaseArtifact.Metadata[platformconfig.QualityEvidenceDigestMetadata], time.Now().UTC())
	return nil
}

func qualityEvidenceObject(snapshot platformconfig.RuntimeSnapshot, digest string, now time.Time) (model.PlatformQualityEvidence, bool, error) {
	actual, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil || actual != digest || !inputDigestPattern.MatchString(digest) {
		return model.PlatformQualityEvidence{}, false, ErrConflict
	}
	if len(snapshot.DNSSelections) == 0 {
		return model.PlatformQualityEvidence{}, false, nil
	}
	raw, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
	if err != nil {
		return model.PlatformQualityEvidence{}, false, err
	}
	if len(raw) > QualityEvidenceMaxObjectBytes {
		return model.PlatformQualityEvidence{}, false, nil // optional, never block independent live capture
	}
	// Replaying an old body never grants it a new history lifetime.
	at := now
	if snapshot.CapturedAt != nil && snapshot.CapturedAt.After(now) {
		return model.PlatformQualityEvidence{}, false, nil
	}
	if snapshot.CapturedAt != nil && snapshot.CapturedAt.Before(at) {
		at = *snapshot.CapturedAt
	}
	for _, fact := range snapshot.DNSSelections {
		if fact.ObservedAt.After(now) {
			return model.PlatformQualityEvidence{}, false, nil
		}
		if !fact.ObservedAt.IsZero() && fact.ObservedAt.Before(at) {
			at = fact.ObservedAt
		}
	}
	expires := at.Add(QualityEvidenceRetention)
	if !expires.After(now) || at.After(now) {
		return model.PlatformQualityEvidence{}, false, nil
	}
	return model.PlatformQualityEvidence{ContentHash: digest, Evidence: raw, SizeBytes: int64(len(raw)), CreatedAt: now, ExpiresAt: expires}, true, nil
}

func qualityEvidenceVictims(rows []model.PlatformQualityEvidence, incoming *model.PlatformQualityEvidence, now time.Time) []string {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].ContentHash < rows[j].ContentHash
		}
		return rows[i].CreatedAt.Before(rows[j].CreatedAt)
	})
	var bytes int64
	count := 0
	var victims []string
	for _, row := range rows {
		if !row.ExpiresAt.After(now) {
			victims = append(victims, row.ContentHash)
			continue
		}
		bytes += row.SizeBytes
		count++
	}
	if incoming != nil {
		bytes += incoming.SizeBytes
		count++
	}
	for _, row := range rows {
		if bytes <= QualityEvidenceBudgetBytes && count <= QualityEvidenceMaxObjects {
			break
		}
		if row.ExpiresAt.After(now) {
			victims = append(victims, row.ContentHash)
			bytes -= row.SizeBytes
			count--
		}
	}
	return victims
}

func (s *Store) ensurePlatformQualityEvidence(ctx context.Context, snapshot platformconfig.RuntimeSnapshot, digest string, now time.Time) error {
	object, retain, err := qualityEvidenceObject(snapshot, digest, now)
	if err != nil {
		return err
	}
	if !retain {
		s.qualityEvidenceMetrics.skipped.Add(1)
		return nil
	}
	return s.maintainPlatformQualityEvidence(ctx, &object, now)
}

func (s *Store) maintainPlatformQualityEvidence(ctx context.Context, incoming *model.PlatformQualityEvidence, now time.Time) (resultErr error) {
	defer func() {
		s.qualityEvidenceMetrics.attemptedAt.Store(time.Now().Unix())
		failed := int64(0)
		if resultErr != nil {
			failed = 1
		}
		s.qualityEvidenceMetrics.failed.Store(failed)
	}()
	if !s.usingDatabase() {
		return s.withLockedState(true, func(st *model.State) error {
			for _, row := range st.PlatformQualityEvidence {
				if incoming != nil && row.ContentHash == incoming.ContentHash {
					incoming = nil // immutable insertion clock, including replays
					break
				}
			}
			victims := qualityEvidenceVictims(st.PlatformQualityEvidence, incoming, now)
			s.observeQualityPool(st.PlatformQualityEvidence, incoming, victims)
			drop := map[string]bool{}
			for _, digest := range victims {
				drop[digest] = true
			}
			kept := st.PlatformQualityEvidence[:0]
			for _, row := range st.PlatformQualityEvidence {
				if !drop[row.ContentHash] {
					kept = append(kept, row)
				}
			}
			st.PlatformQualityEvidence = kept
			if incoming != nil {
				st.PlatformQualityEvidence = append(st.PlatformQualityEvidence, *incoming)
			}
			return nil
		})
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SET LOCAL work_mem='4MB'`, `SET LOCAL max_parallel_workers_per_gather=0`} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, qualityEvidenceLockID); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT content_hash,size_bytes,created_at,expires_at FROM fugue_platform_quality_evidence ORDER BY created_at,content_hash LIMIT $1`, QualityEvidenceMaxObjects+1)
	if err != nil {
		return err
	}
	var inventory []model.PlatformQualityEvidence
	for rows.Next() {
		var row model.PlatformQualityEvidence
		if err := rows.Scan(&row.ContentHash, &row.SizeBytes, &row.CreatedAt, &row.ExpiresAt); err != nil {
			rows.Close()
			return err
		}
		inventory = append(inventory, row)
		if incoming != nil && incoming.ContentHash == row.ContentHash {
			incoming = nil
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if len(inventory) > QualityEvidenceMaxObjects {
		return ErrConflict
	}
	victims := qualityEvidenceVictims(inventory, incoming, now)
	if len(victims) != 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_platform_quality_evidence WHERE content_hash=ANY($1::text[])`, victims); err != nil {
			return err
		}
	}
	if incoming != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_platform_quality_evidence(content_hash,evidence,size_bytes,created_at,expires_at) VALUES($1,$2,$3,$4,$5)`, incoming.ContentHash, []byte(incoming.Evidence), incoming.SizeBytes, incoming.CreatedAt, incoming.ExpiresAt); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.observeQualityPool(inventory, incoming, victims)
	return nil
}

// RunPlatformQualityEvidenceRetention performs only the independent pool's
// expiry cleanup. FIFO capacity is enforced transactionally on every insert.
func (s *Store) RunPlatformQualityEvidenceRetention(ctx context.Context) {
	if !s.usingDatabase() {
		return
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		_ = s.maintainPlatformQualityEvidence(ctx, nil, time.Now().UTC())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// GetPlatformArtifactQualityInputContext never uses the compact input as a
// substitute for missing raw quality evidence. The caller independently admits
// and rechecks the selected publication; this pool confers no serving authority.
func (s *Store) GetPlatformArtifactQualityInputContext(ctx context.Context, a model.PlatformArtifact, needed func(CompilerDNSSelectionSummary) bool) (result CompilerInputDNSSelections, resultErr error) {
	defer func() {
		if resultErr != nil {
			s.qualityEvidenceMetrics.misses.Add(1)
		}
	}()
	retained, err := platformconfig.ArtifactRetainsOriginalInput(a)
	if err != nil || s.VerifyPlatformArtifactIntegrity(a) != nil {
		return CompilerInputDNSSelections{}, ErrConflict
	}
	if retained {
		return s.GetPlatformCompilerInputDNSSelectionsContext(ctx, a.Metadata["input_snapshot_digest"], needed)
	}
	digest := a.Metadata[platformconfig.QualityEvidenceDigestMetadata]
	var raw []byte
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		err = s.db.QueryRowContext(ctx, `SELECT evidence FROM fugue_platform_quality_evidence WHERE content_hash=$1 AND expires_at>now() AND size_bytes<=$2`, digest, QualityEvidenceMaxObjectBytes).Scan(&raw)
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
		}
	} else {
		err = s.withLockedState(false, func(st *model.State) error {
			for _, row := range st.PlatformQualityEvidence {
				if row.ContentHash == digest && row.ExpiresAt.After(time.Now()) && row.SizeBytes <= QualityEvidenceMaxObjectBytes {
					raw = append([]byte(nil), row.Evidence...)
					return nil
				}
			}
			return ErrNotFound
		})
	}
	if err != nil {
		return CompilerInputDNSSelections{}, err
	}
	var snapshot platformconfig.RuntimeSnapshot
	if len(raw) > QualityEvidenceMaxObjectBytes || json.Unmarshal(raw, &snapshot) != nil {
		return CompilerInputDNSSelections{}, ErrConflict
	}
	actual, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil || actual != digest || snapshot.IntentGeneration != a.Metadata["intent_generation"] || snapshot.PolicyGeneration != a.Metadata["policy_generation"] {
		return CompilerInputDNSSelections{}, ErrConflict
	}
	return selectCompilerDNSObservations(snapshot, needed), nil
}

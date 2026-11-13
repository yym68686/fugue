package store

import (
	"context"
	"database/sql"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

// Preserve the complete signed audit input independently of the operational
// input. Registration alone never shortens retention or enables a collector.
// Both inputs are durable before the caller may publish any artifact pair.
func (s *Store) EnsurePlatformCompilerInputPairContext(ctx context.Context, original platformconfig.RuntimeSnapshot, originalDigest string, runtime *platformconfig.RuntimeSnapshot, runtimeDigest string) error {
	if runtime == nil && runtimeDigest != "" {
		return ErrInvalidInput
	}
	if runtime != nil {
		expected, err := platformconfig.RuntimeSnapshotDigest(platformconfig.ConfigurationRuntimeInput(original))
		if err != nil || expected != runtimeDigest {
			return ErrConflict
		}
		actual, err := platformconfig.RuntimeSnapshotDigest(*runtime)
		if err != nil || actual != expected {
			return ErrConflict
		}
	}
	if err := s.EnsurePlatformCompilerInputContext(ctx, original, originalDigest); err != nil {
		return err
	}
	if runtime == nil {
		return nil
	}
	if err := s.EnsurePlatformCompilerInputContext(ctx, *runtime, runtimeDigest); err != nil {
		return err
	}
	if s.db == nil || originalDigest == runtimeDigest {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := beginInputArchiveLockedTx(ctx, s.db, false, true, true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Recheck both identities under the same lifecycle barrier as writers and
	// storage maintenance. Registration cannot resurrect a concurrently retired
	// source by merely retaining its caller-provided digest.
	for _, digest := range []string{originalDigest, runtimeDigest} {
		var present bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_input_manifests WHERE content_hash=$1) OR EXISTS(SELECT 1 FROM fugue_platform_artifact_contents WHERE content_hash=$1 AND content_kind IN ('','compiler-input-v1'))`, digest).Scan(&present); err != nil {
			return err
		}
		if !present {
			return ErrConflict
		}
	}
	// An original can have only one deterministic operational projection. An
	// inconsistent reuse fails closed; timestamps are never refreshed.
	if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_audit_bindings(original_digest,runtime_digest,registered_at) VALUES($1,$2,now()) ON CONFLICT(original_digest) DO NOTHING`, originalDigest, runtimeDigest); err != nil {
		return err
	}
	var bound string
	if err := tx.QueryRowContext(ctx, `SELECT runtime_digest FROM fugue_platform_input_audit_bindings WHERE original_digest=$1`, originalDigest).Scan(&bound); err != nil {
		return err
	}
	if bound != runtimeDigest {
		return ErrConflict
	}
	return tx.Commit()
}

type CompilerInputAuditRetentionEntry struct {
	Digest        string     `json:"digest"`
	RuntimeDigest string     `json:"runtime_digest"`
	RegisteredAt  time.Time  `json:"registered_at"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	State         string     `json:"state"`
}

type CompilerInputAuditRetentionPlan struct {
	Schema              string                             `json:"schema"`
	RetentionDays       int                                `json:"retention_days"`
	ExecutionAuthorized bool                               `json:"execution_authorized"`
	Native              CompilerInputRetentionInventory    `json:"native"`
	Entries             []CompilerInputAuditRetentionEntry `json:"entries"`
}

// Explicit read-only preview. Zero means retain forever, not "expire now".
// Uncovered retention authorities in the existing full-history inventory are
// blockers, never presumed absent. This API cannot authorize deletion.
func (s *Store) PlanCompilerInputAuditRetention(ctx context.Context, days int) (CompilerInputAuditRetentionPlan, error) {
	r := CompilerInputAuditRetentionPlan{Schema: "fugue.compiler-input-audit-retention-preview.v1", RetentionDays: days, Entries: []CompilerInputAuditRetentionEntry{}}
	if s.db == nil || days < 0 || days > 36500 {
		return r, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return r, err
		}
	}
	if err = schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return r, err
	}
	r.Native, err = compilerInputRetentionInventorySnapshot(ctx, tx)
	if err != nil {
		return r, err
	}
	protected := map[string]bool{}
	for _, e := range r.Native.Entries {
		if e.Class == "H" {
			protected[e.Digest] = true
		}
	}
	var now time.Time
	if err = tx.QueryRowContext(ctx, `SELECT transaction_timestamp()`).Scan(&now); err != nil {
		return r, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT original_digest,runtime_digest,registered_at FROM fugue_platform_input_audit_bindings ORDER BY original_digest LIMIT $1`, compilerInputRetentionUniverseLimit+1)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var e CompilerInputAuditRetentionEntry
		if err = rows.Scan(&e.Digest, &e.RuntimeDigest, &e.RegisteredAt); err != nil {
			break
		}
		if len(r.Entries) >= compilerInputRetentionUniverseLimit {
			err = ErrConflict
			break
		}
		e.State = "retain-indefinitely"
		if days > 0 {
			expiry := e.RegisteredAt.Add(time.Duration(days) * 24 * time.Hour)
			e.ExpiresAt = &expiry
			e.State = "within-retention"
			if !now.Before(expiry) {
				e.State = "blocked-incomplete-retention-authorities"
			}
		}
		if protected[e.Digest] {
			e.State = "protected-native-dependency"
		}
		r.Entries = append(r.Entries, e)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		r.Entries = nil
		return r, err
	}
	if err = tx.Commit(); err != nil {
		return r, err
	}
	return r, nil
}

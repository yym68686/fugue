package store

import (
	"context"
	"database/sql"
	"errors"
)

// Read existing admission state on every ordinary request, never a process
// cache or environment flag. The migration-only full-history preparation and
// enabling transaction have been removed; persisted obligations are not.
// Missing/invalid state fails closed, and fencing/retirement still disables
// capability compatibility even if an old database never enabled enforcement.
func compilerInputCapabilityPhase(ctx context.Context, tx *sql.Tx, lock bool) (bool, error) {
	query := `SELECT a.phase,a.plan_sha256,a.enforced_at,c.writes_fenced,c.retirement_started_at
 FROM fugue_platform_input_history_admission a CROSS JOIN fugue_platform_input_archive_control c
 WHERE a.singleton AND c.singleton`
	if lock {
		query += ` FOR SHARE OF a,c`
	}
	var phase, plan string
	var enforcedAt, retiredAt sql.NullTime
	var fenced bool
	if err := tx.QueryRowContext(ctx, query).Scan(&phase, &plan, &enforcedAt, &fenced, &retiredAt); err != nil {
		return false, errors.Join(err, ErrConflict)
	}
	if phase == "capability" && plan == "" && !enforcedAt.Valid {
		return !fenced && !retiredAt.Valid, nil
	}
	if phase != "enforced" || !compilerInputRetirementPlanHash.MatchString(plan) || !enforcedAt.Valid {
		return false, ErrConflict
	}
	return false, nil
}

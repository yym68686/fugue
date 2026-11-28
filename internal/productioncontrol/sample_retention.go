package productioncontrol

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strconv"
	"time"
)

const (
	ObservationSampleRetentionResource = "maintenance/observation-sample-retention-writer"
	SampleRetentionAPIStandby          = "api_writer_worker_standby"
	SampleRetentionWorkerPending       = "worker_writer_progress_pending"
	SampleRetentionWorkerObserved      = "worker_writer_progress_observed"
)

var sampleRetentionTargets = [...]string{"network", "performance", "resource-usage"}

type SampleRetentionProvider struct {
	SystemID string `json:"systemId"`
	Database string `json:"database"`
	Role     string `json:"role"`
}

type SampleRetentionProgress struct {
	Target          string    `json:"target"`
	Owner           string    `json:"owner"`
	DeleteFenced    bool      `json:"deleteFenced"`
	CompletedPasses int64     `json:"completedPasses"`
	TotalDeleted    int64     `json:"totalDeleted"`
	CompletedAt     time.Time `json:"completedAt,omitzero"`
}

type SampleRetentionObservation struct {
	Provider   SampleRetentionProvider   `json:"provider"`
	Resource   string                    `json:"resource"`
	State      string                    `json:"state"`
	TransferAt time.Time                 `json:"transferAt,omitzero"`
	Actions    []string                  `json:"actions"`
	Targets    []SampleRetentionProgress `json:"targets"`
}

func (p SampleRetentionProvider) valid() bool {
	id, err := strconv.ParseUint(p.SystemID, 10, 64)
	return err == nil && id != 0 && strconv.FormatUint(id, 10) == p.SystemID &&
		tokenPattern.MatchString(p.Database) && len(p.Database) <= 63 &&
		tokenPattern.MatchString(p.Role) && len(p.Role) <= 63
}

const sampleRetentionObserverIdentity = `
SELECT NOT pg_catalog.pg_is_in_recovery()
  AND control.system_identifier::text = $1
  AND pg_catalog.current_database() = $2
  AND current_user = $3 AND session_user = current_user
  AND r.rolcanlogin AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolbypassrls OR r.rolreplication)
  AND NOT EXISTS (SELECT FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid OR m.roleid=r.oid)
  AND NOT pg_catalog.has_schema_privilege(r.oid,'fugue_observation_maintenance','CREATE')
  AND NOT pg_catalog.has_schema_privilege(r.oid,'public','CREATE')
  AND EXISTS (
    SELECT FROM pg_catalog.pg_class v JOIN pg_catalog.pg_roles o ON o.oid=v.relowner
    WHERE v.oid=pg_catalog.to_regclass('fugue_observation_maintenance.sample_worker_progress') AND v.relkind='v' AND o.rolsuper)
  AND (
    SELECT pg_catalog.count(*)=2 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_roles o ON o.oid=p.proowner
    WHERE o.rolsuper AND (p.oid=pg_catalog.to_regprocedure(
      'fugue_observation_maintenance.prune_samples_v1(text,boolean,integer,integer,integer)') AND p.prosecdef
      OR p.oid=pg_catalog.to_regprocedure(
      'fugue_observation_maintenance.fence_legacy_sample_delete_v1()') AND NOT p.prosecdef))
  AND NOT EXISTS (
    SELECT FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='fugue_observation_maintenance'
      AND (p.proowner=r.oid OR pg_catalog.has_function_privilege(r.oid,p.oid,'EXECUTE')))
  AND NOT EXISTS (
    SELECT FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE c.relkind IN ('r','p','v') AND (n.nspname='fugue_observation_maintenance'
        OR (n.nspname='public' AND c.relname IN ('fugue_meta','fugue_edge_network_samples',
          'fugue_edge_performance_samples','fugue_resource_usage_samples')))
      AND (c.relowner=r.oid OR pg_catalog.has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE')
        OR pg_catalog.has_table_privilege(r.oid,c.oid,'DELETE,TRUNCATE,TRIGGER')))
FROM pg_catalog.pg_roles r CROSS JOIN pg_catalog.pg_control_system() control WHERE r.rolname=current_user`

// Observe reads the owned database resource, never the Git handoff declaration
// or an Actions receipt. It does not grant a worker or certify release images.
func (p SampleRetentionProvider) Observe(ctx context.Context, db *sql.DB) (SampleRetentionObservation, error) {
	if db == nil || !p.valid() {
		return SampleRetentionObservation{}, errors.New("invalid observation sample retention provider")
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return SampleRetentionObservation{}, errors.New("sample retention observation could not open a read-only transaction")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path = pg_catalog, pg_temp; SET LOCAL row_security = off`); err != nil {
		return SampleRetentionObservation{}, errors.New("sample retention observation could not pin database settings")
	}
	var matched bool
	if err := tx.QueryRowContext(ctx, sampleRetentionObserverIdentity, p.SystemID, p.Database, p.Role).Scan(&matched); err != nil || !matched {
		return SampleRetentionObservation{}, errors.New("sample retention observer identity, privileges or PostgreSQL primary did not match")
	}
	result, err := readSampleRetentionProgress(ctx, tx, p)
	if err != nil {
		return SampleRetentionObservation{}, err
	}
	if err := tx.Commit(); err != nil {
		return SampleRetentionObservation{}, errors.New("sample retention observation did not finish")
	}
	return result, nil
}

func readSampleRetentionProgress(ctx context.Context, tx *sql.Tx, p SampleRetentionProvider) (SampleRetentionObservation, error) {
	// A pre-migration observation may read the old view, which omits the
	// transfer timestamp. An import always needs the upgraded projection.
	rows, err := tx.QueryContext(ctx, `SELECT target,owner,fence_enforced,completed_passes,total_deleted,completed_at,
		to_jsonb(p) ? 'transfer_at', (to_jsonb(p)->>'transfer_at')::timestamptz
		FROM fugue_observation_maintenance.sample_worker_progress p ORDER BY target COLLATE pg_catalog."C"`)
	if err != nil {
		return SampleRetentionObservation{}, errors.New("sample retention progress was not readable")
	}
	defer rows.Close()
	targets := make([]SampleRetentionProgress, 0, len(sampleRetentionTargets))
	var transferAt time.Time
	var hasTransferColumn bool
	for rows.Next() {
		var entry SampleRetentionProgress
		var at, transfer sql.NullTime
		var hasTransfer bool
		if len(targets) == len(sampleRetentionTargets) ||
			rows.Scan(&entry.Target, &entry.Owner, &entry.DeleteFenced, &entry.CompletedPasses, &entry.TotalDeleted,
				&at, &hasTransfer, &transfer) != nil {
			return SampleRetentionObservation{}, errors.New("sample retention progress shape was invalid")
		}
		entry.CompletedAt = at.Time.UTC()
		if len(targets) == 0 {
			hasTransferColumn = hasTransfer
			transferAt = transfer.Time.UTC()
		} else if hasTransfer != hasTransferColumn || !transferAt.Equal(transfer.Time) {
			return SampleRetentionObservation{}, errors.New("sample retention targets disagree on transfer checkpoint")
		}
		targets = append(targets, entry)
	}
	if rows.Err() != nil {
		return SampleRetentionObservation{}, errors.New("sample retention progress read did not finish")
	}
	return sampleRetentionObservation(p, transferAt, hasTransferColumn, targets)
}

func sampleRetentionObservation(p SampleRetentionProvider, transferAt time.Time, hasTransferColumn bool, rows []SampleRetentionProgress) (SampleRetentionObservation, error) {
	inconsistent := errors.New("sample retention owner, fences or completed passes are inconsistent")
	if len(rows) != len(sampleRetentionTargets) {
		return SampleRetentionObservation{}, inconsistent
	}
	owner := rows[0].Owner
	if owner != "api" && owner != "worker" {
		return SampleRetentionObservation{}, inconsistent
	}
	worker := owner == "worker"
	if hasTransferColumn && worker == transferAt.IsZero() {
		return SampleRetentionObservation{}, inconsistent
	}
	passed := 0
	for i, row := range rows {
		if row.Target != sampleRetentionTargets[i] || row.Owner != owner || row.DeleteFenced != worker ||
			row.CompletedPasses < 0 || row.TotalDeleted < 0 || (row.CompletedPasses > 0) == row.CompletedAt.IsZero() ||
			(row.CompletedPasses == 0 && row.TotalDeleted != 0) || (!worker && row.CompletedPasses != 0) {
			return SampleRetentionObservation{}, inconsistent
		}
		if row.CompletedPasses > 0 {
			if hasTransferColumn && row.CompletedAt.Before(transferAt) {
				return SampleRetentionObservation{}, inconsistent
			}
			passed++
		}
	}
	r := SampleRetentionObservation{Provider: p, Resource: ObservationSampleRetentionResource, TransferAt: transferAt, Targets: slices.Clone(rows)}
	switch {
	case !worker:
		r.State = SampleRetentionAPIStandby
		r.Actions = []string{"drain_legacy_delete_and_worker_passes", "atomically_fence_all_legacy_deletes_and_select_worker", "observe_committed_pass_per_target"}
	case passed != len(rows):
		r.State = SampleRetentionWorkerPending
		r.Actions = []string{"observe_committed_pass_per_target", "retain_worker_writer_on_component_rollback"}
	default:
		r.State = SampleRetentionWorkerObserved
		r.Actions = []string{"retain_worker_writer_on_component_rollback"}
	}
	return r, nil
}

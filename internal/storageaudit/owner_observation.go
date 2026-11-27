package storageaudit

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strconv"
	"time"
)

// MaintenanceObserver identifies a dedicated, read-only connection to the
// storage audit provider. The system identifier survives primary failover.
type MaintenanceObserver struct {
	SystemID string `json:"systemId"`
	Database string `json:"database"`
	Role     string `json:"role"`
}

type OwnerObservation struct {
	Provider             MaintenanceObserver `json:"provider"`
	Owner                string              `json:"owner"`
	Policy               Policy              `json:"policy"`
	PolicyDigest         string              `json:"policyDigest"`
	Activation           *OwnerActivation    `json:"activation,omitempty"`
	LastPassFailed       bool                `json:"lastPassFailed"`
	CompletedPasses      int64               `json:"completedPasses"`
	SamplePolicyDigest   string              `json:"samplePolicyDigest,omitempty"`
	SampleAt             time.Time           `json:"sampleAt,omitzero"`
	PostActivationSample bool                `json:"postActivationSample"`
}

type OwnerActivation struct {
	DeclarationDigest string    `json:"declarationDigest"`
	PolicyDigest      string    `json:"policyDigest"`
	WorkerSecretUID   string    `json:"workerSecretUid"`
	ActivatedAt       time.Time `json:"activatedAt"`
}

var ownerObservationDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

const ownerObserverIdentitySQL = `
SELECT NOT pg_catalog.pg_is_in_recovery()
  AND control.system_identifier::text = $1
  AND pg_catalog.current_database() = $2
  AND current_user = $3 AND session_user = current_user
  AND r.rolcanlogin AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolbypassrls OR r.rolreplication)
  AND NOT EXISTS (SELECT FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid OR m.roleid=r.oid)
  AND NOT pg_catalog.has_schema_privilege(r.oid,'fugue_storage_audit','CREATE')
  AND NOT pg_catalog.has_function_privilege(r.oid,'fugue_storage_audit.sample_lifecycle_v1()','EXECUTE')
  AND NOT EXISTS (
    SELECT FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='fugue_storage_audit' AND c.relkind IN ('r','p','v')
      AND (c.relowner=r.oid OR pg_catalog.has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE')
        OR pg_catalog.has_table_privilege(r.oid,c.oid,'DELETE,TRUNCATE,TRIGGER')))
FROM pg_catalog.pg_roles r CROSS JOIN pg_catalog.pg_control_system() control WHERE r.rolname=current_user`

// Observe verifies the database and reads the redacted progress and ownership
// receipt in the same PostgreSQL snapshot. Repeated observations may report
// newer progress. The recorded activation remains tied to its own policy.
func (o MaintenanceObserver) Observe(ctx context.Context, db *sql.DB) (OwnerObservation, error) {
	if id, err := strconv.ParseUint(o.SystemID, 10, 64); db == nil || err != nil || id == 0 ||
		strconv.FormatUint(id, 10) != o.SystemID || o.Database == "" || len(o.Database) > 63 || o.Role == "" || len(o.Role) > 63 {
		return OwnerObservation{}, errors.New("invalid storage audit provider identity")
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return OwnerObservation{}, errors.New("storage audit observer could not open a read-only transaction")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path = pg_catalog, pg_temp`); err != nil {
		return OwnerObservation{}, errors.New("storage audit observer could not pin PostgreSQL name resolution")
	}
	var matched bool
	if err := tx.QueryRowContext(ctx, ownerObserverIdentitySQL, o.SystemID, o.Database, o.Role).Scan(&matched); err != nil || !matched {
		return OwnerObservation{}, errors.New("storage audit observer role or primary database identity did not match")
	}
	progress, err := ReadProgress(ctx, tx)
	if err != nil {
		return OwnerObservation{}, errors.New("storage audit progress was unavailable or inconsistent")
	}
	if progress.Owner != "worker" || progress.PolicySHA256 == "" {
		return OwnerObservation{}, errors.New("storage audit ownership has not durably transferred to the worker")
	}
	var activation OwnerActivation
	if err := tx.QueryRowContext(ctx, `SELECT approval_sha256, policy_sha256, worker_secret_uid::text, activated_at
		FROM fugue_storage_audit.lifecycle_activation WHERE singleton`).Scan(
		&activation.DeclarationDigest, &activation.PolicyDigest, &activation.WorkerSecretUID, &activation.ActivatedAt,
	); err != nil || !ownerObservationDigest.MatchString(activation.DeclarationDigest) || !ownerObservationDigest.MatchString(activation.PolicyDigest) ||
		activation.WorkerSecretUID == "" || activation.ActivatedAt.IsZero() {
		return OwnerObservation{}, errors.New("storage audit worker has no valid durable ownership receipt")
	}
	observed := OwnerObservation{
		Provider: o, Owner: progress.Owner, Policy: progress.Policy, PolicyDigest: progress.PolicySHA256,
		Activation: &activation, LastPassFailed: progress.LastFailed, CompletedPasses: progress.Completed,
	}
	if progress.Sample != nil {
		observed.SamplePolicyDigest = progress.Sample.PolicySHA256
		observed.SampleAt = progress.Sample.ObservedAt.UTC()
		observed.PostActivationSample = !progress.LastFailed && progress.Sample.PolicySHA256 == progress.PolicySHA256 &&
			!progress.Sample.ObservedAt.Before(activation.ActivatedAt)
	}
	if err := tx.Commit(); err != nil {
		return OwnerObservation{}, errors.New("storage audit owner observation did not finish")
	}
	return observed, nil
}

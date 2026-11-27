// Package storageaudit is the narrow PostgreSQL client for the storage audit
// worker. It does not import the API, Store, K8s, or a tenant data client.
package storageaudit

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Store struct{ db *sql.DB }

// The worker takes a dedicated login, not the application's schema-owning
// credential. Readiness alone cannot compensate for overbroad privileges.
const workerIdentitySQL = `
SELECT current_user = session_user AND r.rolcanlogin AND NOT
    (r.rolsuper OR r.rolinherit OR r.rolcreatedb OR r.rolcreaterole OR r.rolbypassrls OR r.rolreplication)
  AND NOT EXISTS (SELECT FROM pg_roles membership WHERE membership.oid <> r.oid
                  AND pg_has_role(r.oid, membership.oid, 'MEMBER'))
  AND NOT EXISTS (SELECT FROM pg_auth_members membership WHERE membership.roleid = r.oid)
  AND has_schema_privilege(current_user, 'fugue_storage_audit', 'USAGE')
  AND NOT has_schema_privilege(current_user, 'fugue_storage_audit', 'CREATE')
  AND has_function_privilege(current_user, 'fugue_storage_audit.sample_lifecycle_v1()', 'EXECUTE')
  AND NOT has_function_privilege(current_user, 'fugue_storage_audit.zero_temp_v1()', 'EXECUTE')
  AND has_table_privilege(current_user, 'fugue_storage_audit.lifecycle_status_v1', 'SELECT')
  AND NOT EXISTS (
      SELECT FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE ((n.nspname = 'public' AND left(c.relname, 6) = 'fugue_' AND c.relkind IN ('r','p','v','m','f'))
          OR (n.nspname = 'fugue_storage_audit' AND c.relname IN ('lifecycle_control','lifecycle_progress')))
        AND (has_any_column_privilege(current_user, c.oid, 'SELECT,INSERT,UPDATE,REFERENCES')
             OR has_table_privilege(current_user, c.oid, 'DELETE,TRUNCATE,TRIGGER')))
FROM pg_roles r WHERE r.rolname = current_user`

func Open(ctx context.Context, dsn string) (*Store, error) {
	if dsn == "" {
		return nil, errors.New("storage audit database credential is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, errors.New("storage audit database configuration is invalid")
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(time.Minute)
	var authorized bool
	if err = db.QueryRowContext(ctx, workerIdentitySQL).Scan(&authorized); err != nil || !authorized {
		_ = db.Close()
		return nil, errors.New("storage audit database identity is not an isolated worker")
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

type PassResult string

const (
	Standby    PassResult = "standby"
	Disabled   PassResult = "disabled"
	Busy       PassResult = "busy"
	NotDue     PassResult = "not_due"
	ReadFailed PassResult = "read_failed"
	Sampled    PassResult = "sampled"
)

// RunPass reports an outcome only after commit. If the connection disappears
// at commit, the next pass recovers from PostgreSQL's durable cursor and
// rate-limit rather than retrying an assumed failed write.
func (s *Store) RunPass(ctx context.Context) (PassResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SET LOCAL statement_timeout = '4s'`); err != nil {
		return "", err
	}
	// Validate the document in the same repeatable-read snapshot used by
	// the sampler. A policy edit between polling and the pass is not enough
	// to authorize an unvalidated sample.
	p, err := ReadProgress(ctx, tx)
	if err != nil {
		return "", err
	}
	if p.Owner == "api" {
		return Standby, nil
	}
	var result PassResult
	if err = tx.QueryRowContext(ctx, `SELECT fugue_storage_audit.sample_lifecycle_v1()`).Scan(&result); err != nil {
		return "", err
	}
	switch result {
	case Standby, Disabled, Busy, NotDue, ReadFailed, Sampled:
	default:
		return "", errors.New("unknown storage audit result")
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return result, nil
}

type Progress struct {
	Owner, Mode, PolicySHA256 string
	Policy                    Policy
	LastAttemptAt             sql.NullTime
	Duration                  float64
	LastFailed                bool
	Failures, Completed       int64
	Sample                    *Sample
}

const progressSQL = `SELECT owner, mode, policy_sha256, last_attempt_at,
duration_seconds, last_run_failed, failure_count, completed_passes, sample, policy_document
FROM fugue_storage_audit.lifecycle_status_v1`

func (s *Store) ReadProgress(ctx context.Context) (Progress, error) {
	return ReadProgress(ctx, s.db)
}

type progressReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// ReadProgress only requires SELECT on the redacted view; the API can
// maintain its query-free diagnostic cache without acquiring worker rights.
func ReadProgress(ctx context.Context, db progressReader) (Progress, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var progress Progress
	var raw, policy []byte
	err := db.QueryRowContext(ctx, progressSQL).Scan(&progress.Owner, &progress.Mode, &progress.PolicySHA256,
		&progress.LastAttemptAt, &progress.Duration, &progress.LastFailed, &progress.Failures, &progress.Completed, &raw, &policy)
	if err != nil {
		return Progress{}, err
	}
	if (progress.Owner != "api" && progress.Owner != "worker") || (progress.Mode != "audit" && progress.Mode != "disabled") ||
		progress.Failures < 0 || progress.Completed < 0 || progress.Duration < 0 ||
		math.IsNaN(progress.Duration) || math.IsInf(progress.Duration, 0) {
		return Progress{}, errors.New("invalid storage audit progress")
	}
	progress.Policy.Mode = "disabled"
	if len(policy) != 0 {
		p, err := ParsePolicy(policy)
		if err != nil || p.Mode != progress.Mode || p.Digest() != progress.PolicySHA256 {
			return Progress{}, errors.New("storage audit policy does not match its persisted identity")
		}
		progress.Policy = p
	} else if progress.Owner != "api" || progress.Mode != "disabled" || progress.PolicySHA256 != "" {
		return Progress{}, errors.New("storage audit policy is missing")
	}
	if len(raw) > 8192 {
		return Progress{}, errors.New("storage audit sample exceeds its bound")
	}
	if len(raw) != 0 {
		var sample Sample
		if err := json.Unmarshal(raw, &sample); err != nil || !sample.valid() {
			return Progress{}, errors.New("invalid storage audit sample")
		}
		progress.Sample = &sample
	}
	if (progress.Completed == 0) != (progress.Sample == nil) {
		return Progress{}, errors.New("storage audit sample and committed progress disagree")
	}
	return progress, nil
}

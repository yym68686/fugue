package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"fugue/internal/appdbimport"
	"fugue/internal/appmanageddb"
	"fugue/internal/model"
)

type AppDatabaseImportWriter string

const (
	AppDatabaseImportAPIWriter               AppDatabaseImportWriter = "api"
	AppDatabaseImportWorkerWriter            AppDatabaseImportWriter = "worker"
	appDatabaseImportOwnerKey                                        = "app-database-import/v1/writer"
	appDatabaseImportAttemptPrefix                                   = "app-database-import/v1/attempt/"
	AppDatabaseImportAmbiguousMessage                                = "database import outcome is unknown after SQL execution began; inspect the target database before submitting another import"
	appDatabaseImportQueueInvalidatedMessage                         = "queued database import did not run because an earlier import's outcome is unknown; inspect the target database before submitting another import"
)

var ErrAppDatabaseImportOutcomeUnknown = fmt.Errorf("%w: %s", ErrConflict, AppDatabaseImportAmbiguousMessage)

type apiDatabaseImportExecution struct{ store *Store }

// APIDatabaseImportExecution is the compatibility adapter. Its identity
// remains API even if a caller continues running after the handoff.
func (s *Store) APIDatabaseImportExecution() appdbimport.ExecutionStore {
	return apiDatabaseImportExecution{store: s}
}

func importExecutionRace(err error) error {
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		return appdbimport.ErrUnclaimable
	}
	return err
}

func (s apiDatabaseImportExecution) ListPending(ctx context.Context, limit int) ([]string, error) {
	tx, err := s.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := requireAppDatabaseImportWriter(ctx, tx, AppDatabaseImportAPIWriter); err != nil {
		return nil, importExecutionRace(err)
	}
	rows, err := tx.QueryContext(ctx, "SELECT pending.id "+appDatabaseImportPendingPredicate, model.OperationStatusPending, limit, model.TenantStatusAccessHeld)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, tx.Commit()
}

func (s apiDatabaseImportExecution) Claim(ctx context.Context, job, attempt string) (model.AppDatabaseImportJob, error) {
	result, err := s.store.ClaimAppDatabaseImportAttempt(ctx, AppDatabaseImportAPIWriter, job, attempt)
	return result, importExecutionRace(err)
}

func (s apiDatabaseImportExecution) AppendLog(ctx context.Context, job, attempt, message string) error {
	return s.store.AppendAppDatabaseImportAttemptLog(ctx, AppDatabaseImportAPIWriter, job, attempt, message)
}

func (s apiDatabaseImportExecution) Complete(ctx context.Context, job, attempt, status, message, detail string) error {
	_, err := s.store.CompleteAppDatabaseImportAttempt(ctx, AppDatabaseImportAPIWriter, job, attempt, status, message, detail)
	return err
}

func (s apiDatabaseImportExecution) BeginEffect(ctx context.Context, job, attempt string) error {
	return s.store.BeginAppDatabaseImportEffect(ctx, AppDatabaseImportAPIWriter, job, attempt)
}

func (s apiDatabaseImportExecution) Renew(ctx context.Context, job, attempt string) error {
	return s.store.RenewAppDatabaseImportAttempt(ctx, AppDatabaseImportAPIWriter, job, attempt)
}

func (s apiDatabaseImportExecution) RecoverExpired(ctx context.Context, limit int) error {
	return importExecutionRace(s.store.RecoverExpiredAppDatabaseImportAttempts(ctx, AppDatabaseImportAPIWriter, limit))
}

func (s apiDatabaseImportExecution) ReadConnection(ctx context.Context, job, attempt string) (appmanageddb.Connection, error) {
	return s.store.ReadAppDatabaseImportConnection(ctx, AppDatabaseImportAPIWriter, job, attempt)
}

func (s apiDatabaseImportExecution) ReadArchive(ctx context.Context, job, attempt string, maxBytes int) (appdbimport.Archive, error) {
	return s.store.ReadAppDatabaseImportArchive(ctx, AppDatabaseImportAPIWriter, job, attempt, maxBytes)
}

func readAppDatabaseImportOwner(ctx context.Context, tx *sql.Tx) (AppDatabaseImportWriter, error) {
	var owner AppDatabaseImportWriter
	err := tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, appDatabaseImportOwnerKey).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return AppDatabaseImportAPIWriter, nil
	}
	if err != nil {
		return "", err
	}
	if owner != AppDatabaseImportAPIWriter && owner != AppDatabaseImportWorkerWriter {
		return "", errors.New("unknown app database import writer")
	}
	return owner, nil
}

func requireAppDatabaseImportWriter(ctx context.Context, tx *sql.Tx, writer AppDatabaseImportWriter) error {
	if writer != AppDatabaseImportAPIWriter && writer != AppDatabaseImportWorkerWriter {
		return ErrInvalidInput
	}
	var held bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1,0))`, appDatabaseImportOwnerKey).Scan(&held); err != nil {
		return err
	}
	if !held {
		return ErrConflict
	}
	owner, err := readAppDatabaseImportOwner(ctx, tx)
	if err != nil {
		return err
	}
	if owner != writer {
		return ErrConflict
	}
	return nil
}

func requireAppDatabaseImportTenantActive(ctx context.Context, tx *sql.Tx, tenantID string) error {
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM fugue_tenants WHERE id=$1 FOR SHARE`, tenantID).Scan(&status); err != nil {
		return mapDBErr(err)
	}
	if status == model.TenantStatusAccessHeld {
		return ErrConflict
	}
	return nil
}

func (s *Store) ClaimAppDatabaseImportAttempt(ctx context.Context, writer AppDatabaseImportWriter, jobID, attempt string) (model.AppDatabaseImportJob, error) {
	if jobID == "" || attempt == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	return s.claimAppDatabaseImport(ctx, jobID, writer, attempt)
}

func (s *Store) CompleteAppDatabaseImportAttempt(ctx context.Context, writer AppDatabaseImportWriter, jobID, attempt, status, message, errorMessage string) (model.AppDatabaseImportJob, error) {
	status = normalizeTerminalAppDatabaseImportStatus(status)
	if jobID == "" || attempt == "" || status == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	return s.completeAppDatabaseImport(ctx, jobID, status, message, errorMessage, writer, attempt)
}

func (s *Store) AppendAppDatabaseImportAttemptLog(ctx context.Context, writer AppDatabaseImportWriter, jobID, attempt, message string) error {
	if jobID == "" || attempt == "" {
		return ErrInvalidInput
	}
	_, err := s.appendAppDatabaseImportLog(ctx, jobID, message, writer, attempt)
	return err
}

const appDatabaseImportAttemptFresh = "updated_at > clock_timestamp() - interval '60 seconds'"

// ReadAppDatabaseImportConnection resolves only the database belonging to an
// active preparation attempt. Keep managed-database lifecycle validation and
// binding resolution here so the executor never receives a general app read.
func (s *Store) ReadAppDatabaseImportConnection(ctx context.Context, writer AppDatabaseImportWriter, jobID, attempt string) (appmanageddb.Connection, error) {
	if jobID == "" || attempt == "" {
		return appmanageddb.Connection{}, ErrInvalidInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: true})
	if err != nil {
		return appmanageddb.Connection{}, err
	}
	defer tx.Rollback()
	if err := requireAppDatabaseImportWriter(ctx, tx, writer); err != nil {
		return appmanageddb.Connection{}, err
	}
	var appID, tenantID string
	err = tx.QueryRowContext(ctx, `
SELECT j.app_id,j.tenant_id
FROM fugue_app_database_import_jobs j JOIN fugue_meta a ON a.key=$1||j.id
WHERE j.id=$2 AND j.status='running' AND a.value::jsonb->>'attempt'=$3
  AND a.value::jsonb->>'phase'='preparing' AND a.updated_at > clock_timestamp() - interval '60 seconds'
`, appDatabaseImportAttemptPrefix, jobID, attempt).Scan(&appID, &tenantID)
	if err != nil {
		return appmanageddb.Connection{}, mapDBErr(err)
	}
	app, err := s.pgGetAppTx(ctx, tx, appID, false)
	if err != nil {
		return appmanageddb.Connection{}, mapDBErr(err)
	}
	if app.TenantID != tenantID || isDeletedApp(app) {
		return appmanageddb.Connection{}, ErrNotFound
	}
	if err := s.pgValidateAppDatabaseImportRunnableTx(ctx, tx, app); err != nil {
		return appmanageddb.Connection{}, err
	}
	if OwnedManagedPostgresSpec(app) == nil {
		return appmanageddb.Connection{}, errors.New("managed postgres is not configured for this app")
	}
	connection, err := appmanageddb.Resolve(app)
	if err != nil {
		return appmanageddb.Connection{}, err
	}
	if err := tx.Commit(); err != nil {
		return appmanageddb.Connection{}, err
	}
	return connection, nil
}

// ReadAppDatabaseImportArchive exposes only the source bound to this live
// attempt. The executor does not need upload download tokens or general
// access to tenants' source archives.
func (s *Store) ReadAppDatabaseImportArchive(ctx context.Context, writer AppDatabaseImportWriter, jobID, attempt string, maxBytes int) (appdbimport.Archive, error) {
	if jobID == "" || attempt == "" || maxBytes < 1 || maxBytes > appdbimport.MaxDumpBytes {
		return appdbimport.Archive{}, ErrInvalidInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: true})
	if err != nil {
		return appdbimport.Archive{}, err
	}
	defer tx.Rollback()
	if err := requireAppDatabaseImportWriter(ctx, tx, writer); err != nil {
		return appdbimport.Archive{}, err
	}
	var archive appdbimport.Archive
	var size int
	err = tx.QueryRowContext(ctx, `
SELECT u.filename, octet_length(u.archive_bytes),
       CASE WHEN octet_length(u.archive_bytes) <= $4 THEN u.archive_bytes END
FROM fugue_app_database_import_jobs j
JOIN fugue_source_uploads u ON u.id=j.source_upload_id AND u.tenant_id=j.tenant_id
JOIN fugue_meta a ON a.key=$1||j.id
WHERE j.id=$2 AND j.status='running' AND a.value::jsonb->>'attempt'=$3
  AND a.value::jsonb->>'phase'='preparing' AND a.updated_at > clock_timestamp() - interval '60 seconds'
`, appDatabaseImportAttemptPrefix, jobID, attempt, maxBytes).Scan(&archive.Filename, &size, &archive.Bytes)
	if err != nil {
		return appdbimport.Archive{}, mapDBErr(err)
	}
	if size > maxBytes {
		return appdbimport.Archive{}, fmt.Errorf("database import dump exceeds %d bytes", maxBytes)
	}
	if err := tx.Commit(); err != nil {
		return appdbimport.Archive{}, err
	}
	return archive, nil
}

func lockAppDatabaseImportAttempt(ctx context.Context, tx *sql.Tx, jobID, attempt string) (string, error) {
	var phase string
	err := tx.QueryRowContext(ctx, `
SELECT value::jsonb->>'phase' FROM fugue_meta
WHERE key=$1 AND value::jsonb->>'attempt'=$2 AND `+appDatabaseImportAttemptFresh+` FOR UPDATE
`, appDatabaseImportAttemptPrefix+jobID, attempt).Scan(&phase)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrConflict
	}
	return phase, err
}

func (s *Store) RenewAppDatabaseImportAttempt(ctx context.Context, writer AppDatabaseImportWriter, jobID, attempt string) error {
	return s.touchAppDatabaseImportAttempt(ctx, writer, jobID, attempt, false)
}

// BeginAppDatabaseImportEffect records the boundary before the client connects
// to the app database. Recovery may requeue preparation, but never SQL that
// might have reached the server.
func (s *Store) BeginAppDatabaseImportEffect(ctx context.Context, writer AppDatabaseImportWriter, jobID, attempt string) error {
	return s.touchAppDatabaseImportAttempt(ctx, writer, jobID, attempt, true)
}

func (s *Store) touchAppDatabaseImportAttempt(ctx context.Context, writer AppDatabaseImportWriter, jobID, attempt string, beginEffect bool) error {
	if jobID == "" || attempt == "" {
		return ErrInvalidInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := requireAppDatabaseImportWriter(ctx, tx, writer); err != nil {
		return err
	}
	var tenantID string
	if err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM fugue_app_database_import_jobs WHERE id=$1 AND status='running' FOR UPDATE`, jobID).Scan(&tenantID); err != nil {
		return mapDBErr(err)
	}
	phase, err := lockAppDatabaseImportAttempt(ctx, tx, jobID, attempt)
	if err != nil {
		return err
	}
	if beginEffect {
		if phase != "preparing" {
			return ErrConflict
		}
		if err := requireAppDatabaseImportTenantActive(ctx, tx, tenantID); err != nil {
			return err
		}
		phase = "executing"
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_meta SET value=jsonb_build_object('attempt',$2::text,'phase',$3::text)::text,updated_at=clock_timestamp() WHERE key=$1
`, appDatabaseImportAttemptPrefix+jobID, attempt, phase); err != nil {
		return err
	}
	return tx.Commit()
}

// RecoverExpiredAppDatabaseImportAttempts keeps the same job ID when no SQL
// could have run. Once execution starts, the existing failed status reports
// uncertainty and the old attempt can never complete or be replayed.
func (s *Store) RecoverExpiredAppDatabaseImportAttempts(ctx context.Context, writer AppDatabaseImportWriter, limit int) error {
	if limit < 1 || limit > 100 {
		return ErrInvalidInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := requireAppDatabaseImportWriter(ctx, tx, writer); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `
SELECT j.id, j.app_id
FROM fugue_app_database_import_jobs j
JOIN fugue_apps a ON a.id=j.app_id
JOIN fugue_meta m ON m.key=$1||j.id
WHERE j.status='running' AND m.updated_at <= clock_timestamp() - interval '60 seconds'
ORDER BY j.started_at,j.id LIMIT $2 FOR UPDATE OF a SKIP LOCKED
`, appDatabaseImportAttemptPrefix, limit)
	if err != nil {
		return err
	}
	type expiredJob struct{ id, appID string }
	var expired []expiredJob
	for rows.Next() {
		var job expiredJob
		if err := rows.Scan(&job.id, &job.appID); err != nil {
			rows.Close()
			return err
		}
		expired = append(expired, job)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	for _, job := range expired {
		// The app row serializes this recovery with creation and claim. Lock
		// the job next, following claim's order, then recheck the heartbeat.
		var running bool
		err = tx.QueryRowContext(ctx, `
SELECT status='running' FROM fugue_app_database_import_jobs WHERE id=$1 FOR UPDATE
`, job.id).Scan(&running)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && !running) {
			continue
		}
		if err != nil {
			return err
		}
		var phase string
		err = tx.QueryRowContext(ctx, `
SELECT value::jsonb->>'phase' FROM fugue_meta
WHERE key=$1 AND updated_at <= clock_timestamp() - interval '60 seconds'
`, appDatabaseImportAttemptPrefix+job.id).Scan(&phase)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		switch phase {
		case "preparing":
			_, err = tx.ExecContext(ctx, `
UPDATE fugue_app_database_import_jobs SET status='pending', started_at=NULL, updated_at=now() WHERE id=$1
`, job.id)
		case "executing":
			_, err = tx.ExecContext(ctx, `
UPDATE fugue_app_database_import_jobs
SET status='failed', error_message=$2, completed_at=now(), updated_at=now(),
    logs_json=COALESCE(NULLIF(logs_json,'null'::jsonb),'[]'::jsonb)||jsonb_build_array(jsonb_build_object('at',now(),'message',$2::text))
WHERE id=$1
`, job.id, AppDatabaseImportAmbiguousMessage)
			if err == nil {
				err = invalidateQueuedAppDatabaseImports(ctx, tx, job.appID)
			}
		default:
			return errors.New("unknown app database import phase")
		}
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_meta WHERE key=$1`, appDatabaseImportAttemptPrefix+job.id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// The caller holds the app row lock. These requests predate the unknown
// outcome, so they cannot represent a decision to import against current data.
func invalidateQueuedAppDatabaseImports(ctx context.Context, tx *sql.Tx, appID string) error {
	_, err := tx.ExecContext(ctx, `
UPDATE fugue_app_database_import_jobs
SET status='failed', error_message=$2, completed_at=now(), updated_at=now(),
    logs_json=COALESCE(NULLIF(logs_json,'null'::jsonb),'[]'::jsonb)||jsonb_build_array(jsonb_build_object('at',now(),'message',$2::text))
WHERE app_id=$1 AND status='pending'
`, appID, appDatabaseImportQueueInvalidatedMessage)
	return err
}

func (s *Store) TransferAppDatabaseImportsToWorker(ctx context.Context) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var held bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, appDatabaseImportOwnerKey).Scan(&held); err != nil || !held {
		return false, err
	}
	owner, err := readAppDatabaseImportOwner(ctx, tx)
	if err != nil {
		return false, err
	}
	if owner == AppDatabaseImportWorkerWriter {
		return true, tx.Commit()
	}
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fugue_app_database_import_jobs WHERE status='running')`).Scan(&active); err != nil || active {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_meta(key,value) VALUES($1,$2)
ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=now()`, appDatabaseImportOwnerKey, AppDatabaseImportWorkerWriter); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func validateAppDatabaseImportRetry(ctx context.Context, tx *sql.Tx, job model.AppDatabaseImportJob) error {
	if strings.TrimSpace(job.RetryOfJobID) == "" {
		return nil
	}
	var message string
	err := tx.QueryRowContext(ctx, `
SELECT error_message FROM fugue_app_database_import_jobs WHERE id=$1 AND app_id=$2 AND tenant_id=$3
`, job.RetryOfJobID, job.AppID, job.TenantID).Scan(&message)
	if err != nil {
		return mapDBErr(err)
	}
	if message == AppDatabaseImportAmbiguousMessage || message == appDatabaseImportQueueInvalidatedMessage {
		return ErrAppDatabaseImportOutcomeUnknown
	}
	return nil
}

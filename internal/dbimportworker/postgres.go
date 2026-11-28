package dbimportworker

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"fugue/internal/appdbimport"
	"fugue/internal/appmanageddb"
	"fugue/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const postgresWorkerRole = "fugue_app_db_import_worker"

type PostgresExecutor struct{ db *sql.DB }

var _ appdbimport.ExecutionStore = (*PostgresExecutor)(nil)

// OpenPostgresExecutor does not bootstrap schema and never accepts the API's
// general store credential. Errors do not include the DSN or server context.
func OpenPostgresExecutor(ctx context.Context, dsn string) (*PostgresExecutor, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("database import worker connection is not configured")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil || config.User != postgresWorkerRole {
		return nil, errors.New("database import worker requires its dedicated PostgreSQL login")
	}
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(time.Minute)
	worker := &PostgresExecutor{db: db}
	if _, err := worker.Owner(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return worker, nil
}

func (w *PostgresExecutor) Close() error { return w.db.Close() }

func (w *PostgresExecutor) Owner(ctx context.Context) (string, error) {
	var owner string
	err := w.db.QueryRowContext(ctx, `
SELECT fugue_app_db_import_worker.owner_v1()
WHERE session_user = 'fugue_app_db_import_worker' AND current_user = session_user
  AND current_setting('transaction_isolation') = 'read committed'
  AND current_setting('transaction_read_only') = 'off'
  AND NOT has_schema_privilege('fugue_app_db_import_worker','CREATE')
  AND NOT EXISTS (SELECT FROM pg_roles r WHERE r.rolname = current_user AND
       (NOT r.rolcanlogin OR r.rolinherit OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolbypassrls OR r.rolreplication))
  AND NOT EXISTS (SELECT FROM pg_auth_members m JOIN pg_roles r ON r.oid IN (m.roleid,m.member) WHERE r.rolname = current_user)
  AND NOT EXISTS (
    SELECT FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','f')
      AND (has_any_column_privilege(c.oid,'SELECT,INSERT,UPDATE,REFERENCES')
           OR has_table_privilege(c.oid,'DELETE,TRUNCATE,TRIGGER'))
  )
  AND NOT EXISTS (
    SELECT FROM unnest(ARRAY['pending_v1(integer)','claim_v1(text,text)','archive_v1(text,text,integer)',
      'connection_v1(text,text)','touch_v1(text,text,boolean)','recover_v1(integer)',
      'append_log_v1(text,text,text)','complete_v1(text,text,text,text,text)']) AS capability(signature)
    WHERE NOT coalesce(has_function_privilege(to_regprocedure('fugue_app_db_import_worker.'||signature),'EXECUTE'),false)
  )`).Scan(&owner)
	if err != nil || (owner != "api" && owner != "worker") {
		return "", errors.New("database import worker capabilities or ownership are unavailable")
	}
	return owner, nil
}

func (w *PostgresExecutor) ListPending(ctx context.Context, limit int) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, `SELECT * FROM fugue_app_db_import_worker.pending_v1($1)`, limit)
	if err != nil {
		return nil, errors.New("database import queue unavailable")
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) != nil {
			return nil, errors.New("database import queue returned invalid work")
		}
		ids = append(ids, id)
	}
	if rows.Err() != nil {
		return nil, errors.New("database import queue unavailable")
	}
	return ids, nil
}

func (w *PostgresExecutor) Claim(ctx context.Context, id, attempt string) (model.AppDatabaseImportJob, error) {
	var job model.AppDatabaseImportJob
	err := w.db.QueryRowContext(ctx, `SELECT job_id, import_format, clean FROM fugue_app_db_import_worker.claim_v1($1,$2)`, id, attempt).
		Scan(&job.ID, &job.Format, &job.Clean)
	if errors.Is(err, sql.ErrNoRows) {
		return job, appdbimport.ErrUnclaimable
	}
	if err != nil {
		return job, errors.New("database import claim unavailable")
	}
	return job, nil
}

func (w *PostgresExecutor) AppendLog(ctx context.Context, id, attempt, message string) error {
	return w.change(ctx, `SELECT fugue_app_db_import_worker.append_log_v1($1,$2,$3)`, id, attempt, message)
}

func (w *PostgresExecutor) Complete(ctx context.Context, id, attempt, state, message, detail string) error {
	return w.change(ctx, `SELECT fugue_app_db_import_worker.complete_v1($1,$2,$3,$4,$5)`, id, attempt, state, message, detail)
}

func (w *PostgresExecutor) BeginEffect(ctx context.Context, id, attempt string) error {
	return w.change(ctx, `SELECT fugue_app_db_import_worker.touch_v1($1,$2,true)`, id, attempt)
}

func (w *PostgresExecutor) Renew(ctx context.Context, id, attempt string) error {
	return w.change(ctx, `SELECT fugue_app_db_import_worker.touch_v1($1,$2,false)`, id, attempt)
}

func (w *PostgresExecutor) change(ctx context.Context, query string, args ...any) error {
	var changed bool
	if w.db.QueryRowContext(ctx, query, args...).Scan(&changed) != nil {
		return errors.New("database import attempt update unavailable")
	}
	if !changed {
		return appdbimport.ErrUnclaimable
	}
	return nil
}

func (w *PostgresExecutor) RecoverExpired(ctx context.Context, limit int) error {
	var count int
	if w.db.QueryRowContext(ctx, `SELECT fugue_app_db_import_worker.recover_v1($1)`, limit).Scan(&count) != nil {
		return errors.New("database import recovery unavailable")
	}
	return nil
}

func (w *PostgresExecutor) ReadConnection(ctx context.Context, id, attempt string) (appmanageddb.Connection, error) {
	var serviceID, tenantID, endpoint, database, user, password string
	if err := w.db.QueryRowContext(ctx, `SELECT service_id, tenant_id, endpoint, database_name, login_name, login_password
FROM fugue_app_db_import_worker.connection_v1($1,$2)`, id, attempt).
		Scan(&serviceID, &tenantID, &endpoint, &database, &user, &password); err != nil {
		return appmanageddb.Connection{}, errors.New("managed database connection unavailable")
	}
	// SQL attested the selected binding and only returned its connection
	// fields. Reuse the API's cluster-local endpoint and DSN validation.
	return appmanageddb.Resolve(model.App{
		ID: id, TenantID: tenantID,
		Bindings: []model.ServiceBinding{{TenantID: tenantID, AppID: id, ServiceID: serviceID}},
		BackingServices: []model.BackingService{{ID: serviceID, TenantID: tenantID, Type: model.BackingServiceTypePostgres,
			Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{EndpointServiceName: endpoint, Database: database, User: user, Password: password}},
		}},
	})
}

func (w *PostgresExecutor) ReadArchive(ctx context.Context, id, attempt string, maxBytes int) (appdbimport.Archive, error) {
	var archive appdbimport.Archive
	if err := w.db.QueryRowContext(ctx, `SELECT filename, archive FROM fugue_app_db_import_worker.archive_v1($1,$2,$3)`, id, attempt, maxBytes).
		Scan(&archive.Filename, &archive.Bytes); err != nil {
		return appdbimport.Archive{}, errors.New("database import dump unavailable or outside the configured size limit")
	}
	return archive, nil
}

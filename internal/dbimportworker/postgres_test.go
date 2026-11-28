package dbimportworker

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/appdbimport"

	"github.com/jackc/pgx/v5/pgconn"
)

// Use a fresh PostgreSQL database and roles in a disposable instance, not a
// clone of the store fixture: activation attaches triggers to fixed tables.
func TestAppDatabaseImportActivationPostgres(t *testing.T) {
	dsn := os.Getenv("FUGUE_IMPORT_FENCE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FUGUE_IMPORT_FENCE_TEST_DATABASE_URL to a fresh local PostgreSQL instance")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme != "postgresql" || u.Hostname() != "127.0.0.1" ||
		u.Path != "/fugue_test_import_fence" || u.RawQuery != "sslmode=disable" {
		t.Fatal("requires a fresh loopback fugue_test_import_fence database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(3)
	var fresh bool
	if err := db.QueryRowContext(ctx, `
SELECT NOT EXISTS (SELECT FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public')
   AND NOT EXISTS (SELECT FROM pg_roles WHERE rolname IN
       ('fugue_app_db_import_definer','fugue_app_db_import_worker','fixture_old_api'))
`).Scan(&fresh); err != nil || !fresh {
		t.Fatalf("database must be a fresh fixture: %v", err)
	}
	mustExec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	mustExec(`
CREATE ROLE fugue_app_db_import_definer NOLOGIN NOINHERIT;
CREATE ROLE fugue_app_db_import_worker LOGIN NOINHERIT;
CREATE ROLE fixture_old_api NOLOGIN NOINHERIT;
CREATE TABLE public.fugue_meta (key text PRIMARY KEY, value text NOT NULL, updated_at timestamptz NOT NULL);
CREATE TABLE public.fugue_tenants (id text PRIMARY KEY, status text NOT NULL);
CREATE TABLE public.fugue_source_uploads (id text PRIMARY KEY, tenant_id text NOT NULL, filename text NOT NULL, archive_bytes bytea NOT NULL);
CREATE TABLE public.fugue_apps (id text PRIMARY KEY, tenant_id text NOT NULL, name text NOT NULL,
    spec_json jsonb NOT NULL DEFAULT '{}', status_json jsonb NOT NULL DEFAULT '{}');
CREATE TABLE public.fugue_service_bindings (id text PRIMARY KEY, tenant_id text NOT NULL,
    app_id text NOT NULL, service_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE public.fugue_backing_services (id text PRIMARY KEY, tenant_id text NOT NULL, owner_app_id text,
    type text, provisioner text, status text, spec_json jsonb);
CREATE TABLE public.fugue_operations (app_id text, type text, status text);
CREATE TABLE public.fugue_app_database_import_jobs (
  id text PRIMARY KEY, status text NOT NULL, started_at timestamptz, completed_at timestamptz,
  tenant_id text, app_id text, source_upload_id text, created_at timestamptz NOT NULL DEFAULT now(),
  result_message text NOT NULL DEFAULT '', error_message text NOT NULL DEFAULT '',
  logs_json jsonb NOT NULL DEFAULT '[]',
  format text NOT NULL DEFAULT 'sql', clean boolean NOT NULL DEFAULT false, updated_at timestamptz NOT NULL DEFAULT now()
);
GRANT USAGE ON SCHEMA public TO fixture_old_api, fugue_app_db_import_definer;
GRANT SELECT, INSERT, UPDATE ON public.fugue_app_database_import_jobs TO fixture_old_api;
GRANT SELECT, UPDATE ON public.fugue_app_database_import_jobs TO fugue_app_db_import_definer;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.fugue_meta TO fixture_old_api, fugue_app_db_import_definer;
INSERT INTO public.fugue_app_database_import_jobs (id,status) VALUES ('old','running');
INSERT INTO public.fugue_tenants VALUES ('tenant','active'),('held','access-held');
INSERT INTO public.fugue_apps(id,tenant_id,name) VALUES
    ('another-app','tenant','another'),('held-app','held','held'),('fixture-app','tenant','fixture'),
    ('racing-app','tenant','racing'),('blocked-app','tenant','blocked');
INSERT INTO public.fugue_operations VALUES ('blocked-app','database-resize','waiting-agent');
INSERT INTO public.fugue_source_uploads VALUES ('upload','tenant','dump.sql',convert_to('select 1','UTF8'));
INSERT INTO public.fugue_backing_services VALUES
    ('shared','tenant',NULL,'postgres','managed','active',
      '{"postgres":{"service_name":"shared-rw","database":"shared","user":"fixture","password":"shared-password"}}'),
    ('owned','tenant','fixture-app','postgres','managed','active',
      '{"postgres":{"service_name":"old-rw","endpoint_service_name":"stable-rw","database":"owned","user":"fixture","password":"owned-password"}}'),
    ('cross-tenant','held','fixture-app','postgres','managed','active',
      '{"postgres":{"service_name":"private-rw","database":"private","user":"fixture","password":"private-password"}}');
INSERT INTO public.fugue_service_bindings (id,tenant_id,app_id,service_id,created_at) VALUES
    ('shared-binding','tenant','fixture-app','shared',now()-interval '1 minute'),
    ('owned-binding','tenant','fixture-app','owned',now());
INSERT INTO public.fugue_app_database_import_jobs(id,status,tenant_id,app_id,source_upload_id)
VALUES ('ready','pending','tenant','another-app',NULL),('held-job','pending','held','held-app',NULL);
`)
	capability, err := os.ReadFile("app_database_import_worker_reads.sql")
	if err != nil {
		t.Fatal(err)
	}
	mustExec(string(capability))
	workerURL := *u
	// A random base32 value also works on CI's password-authenticated fixture.
	password := rand.Text()
	mustExec("ALTER ROLE fugue_app_db_import_worker PASSWORD '" + password + "'")
	workerURL.User = url.UserPassword("fugue_app_db_import_worker", password)
	worker, err := sql.Open("pgx", workerURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	if wrong, err := OpenPostgresExecutor(ctx, dsn); err == nil {
		_ = wrong.Close()
		t.Fatal("adapter accepted the general database login")
	}
	adapter, err := OpenPostgresExecutor(ctx, workerURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	if owner, err := adapter.Owner(ctx); err != nil || owner != "api" {
		t.Fatalf("standby adapter did not observe the API owner: %q %v", owner, err)
	}
	if ids, err := adapter.ListPending(ctx, 3); err != nil || len(ids) != 0 {
		t.Fatalf("standby adapter unexpectedly saw work: %v %v", ids, err)
	}
	mustExec(`GRANT SELECT (id) ON public.fugue_backing_services TO fugue_app_db_import_worker`)
	if owner, err := adapter.Owner(ctx); err == nil || owner != "" {
		t.Fatal("readiness accepted a worker with general table access")
	}
	mustExec(`REVOKE SELECT (id) ON public.fugue_backing_services FROM fugue_app_db_import_worker`)
	checkWorker := func(query, want string, args ...any) {
		t.Helper()
		var got string
		if err := worker.QueryRowContext(ctx, query, args...).Scan(&got); err != nil || got != want {
			t.Fatalf("worker query %s = %q, %v; want %q", query, got, err, want)
		}
	}
	const ownerSQL = "SELECT fugue_app_db_import_worker.owner_v1()"
	const pendingSQL = "SELECT coalesce(string_agg(id,',' ORDER BY id),'') FROM fugue_app_db_import_worker.pending_v1(5) AS id"
	const archiveSQL = "SELECT coalesce(string_agg(filename || ':' || convert_from(archive,'UTF8'),','),'') FROM fugue_app_db_import_worker.archive_v1($1,$2,$3)"
	const connectionSQL = "SELECT coalesce(string_agg(concat_ws(':',service_id,tenant_id,endpoint,database_name,login_name,login_password),','),'') FROM fugue_app_db_import_worker.connection_v1($1,$2)"
	const touchSQL = "SELECT fugue_app_db_import_worker.touch_v1($1,$2,$3)::text"
	const claimSQL = "SELECT coalesce(string_agg(job_id || ':' || import_format || ':' || clean::text,','),'') FROM fugue_app_db_import_worker.claim_v1($1,$2)"
	const recoverSQL = "SELECT fugue_app_db_import_worker.recover_v1(5)::text"
	const appendSQL = "SELECT fugue_app_db_import_worker.append_log_v1($1,$2,$3)::text"
	const completeSQL = "SELECT fugue_app_db_import_worker.complete_v1($1,$2,$3,$4,$5)::text"
	checkWorker(ownerSQL, "api")
	checkWorker(pendingSQL, "")
	checkWorker(archiveSQL, "", "fresh", "lease-1", 16)
	checkWorker(connectionSQL, "", "fresh", "lease-1")
	checkWorker(touchSQL, "false", "old", "lease-1", false)
	checkWorker(claimSQL, "", "ready", "standby-lease")
	checkWorker(recoverSQL, "0")
	checkWorker(appendSQL, "false", "old", "lease-1", "standby log")
	checkWorker(completeSQL, "false", "old", "lease-1", "completed", "standby result", "")
	source, err := os.ReadFile("app_database_import_activate.sql")
	if err != nil {
		t.Fatal(err)
	}
	activate := func() error {
		t.Helper()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_, err = conn.ExecContext(ctx, string(source))
		if err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
		}
		return err
	}
	requireRejected := func(want string) {
		t.Helper()
		if err := activate(); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("activation should reject %q: %v", want, err)
		}
		var exists bool
		if err := db.QueryRowContext(ctx, `
SELECT EXISTS(SELECT FROM public.fugue_meta)
    OR to_regnamespace('fugue_app_db_import_fence') IS NOT NULL
`).Scan(&exists); err != nil || exists {
			t.Fatalf("rejected activation left ownership or a fence behind: %v", err)
		}
	}
	requireRejected("queue still has a running attempt")
	mustExec("UPDATE public.fugue_app_database_import_jobs SET status='completed' WHERE id='old'")
	block, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer block.Rollback()
	if _, err := block.ExecContext(ctx, "UPDATE public.fugue_app_database_import_jobs SET result_message='uncommitted' WHERE id='old'"); err != nil {
		t.Fatal(err)
	}
	requireRejected("could not obtain lock")
	if err := block.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := activate(); err != nil {
		t.Fatal(err)
	}
	checkWorker(ownerSQL, "worker")
	if owner, err := adapter.Owner(ctx); err != nil || owner != "worker" {
		t.Fatalf("active adapter did not observe its writer role: %q %v", owner, err)
	}
	checkWorker(pendingSQL, "ready")
	checkWorker(claimSQL, "", "held-job", "held-lease")
	for _, query := range []string{
		"SELECT key FROM public.fugue_meta",
		"SELECT id FROM public.fugue_app_database_import_jobs",
		"SELECT archive_bytes FROM public.fugue_source_uploads",
		"SELECT spec_json FROM public.fugue_backing_services",
		"SELECT fugue_app_db_import_worker.can_execute_v1()",
		"SELECT fugue_app_db_import_worker.fail_uncertain_v1('fixture-app','fresh',now())",
		"SELECT fugue_app_db_import_worker.lock_attempt_v1('fresh','lease-1')",
		"SET ROLE fugue_app_db_import_definer",
	} {
		var denied *pgconn.PgError
		if _, err := worker.ExecContext(ctx, query); !errors.As(err, &denied) || denied.Code != "42501" {
			t.Fatalf("worker's direct access was not denied: %s: %v", query, err)
		}
	}
	readOwner := func() time.Time {
		t.Helper()
		var owner string
		var activated time.Time
		if err := db.QueryRowContext(ctx, `SELECT value, updated_at FROM public.fugue_meta WHERE key='app-database-import/v1/writer'`).Scan(&owner, &activated); err != nil || owner != "worker" {
			t.Fatalf("activation did not persist worker owner: %q %v", owner, err)
		}
		return activated
	}
	first := readOwner()
	if err := activate(); err != nil || !readOwner().Equal(first) {
		t.Fatalf("activation retry altered ownership: %v", err)
	}
	runAs := func(role, query string) error {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE "+role); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return err
		}
		return tx.Commit()
	}
	if err := runAs("fixture_old_api", "INSERT INTO public.fugue_app_database_import_jobs(id,status,tenant_id,app_id,source_upload_id) VALUES ('fresh','pending','tenant','fixture-app','upload')"); err != nil {
		t.Fatalf("API cannot enqueue intent: %v", err)
	}
	if err := runAs("fixture_old_api", `INSERT INTO public.fugue_meta VALUES ('unrelated','a',clock_timestamp());
	    UPDATE public.fugue_meta SET value='b' WHERE key='unrelated';`); err != nil {
		t.Fatalf("unrelated metadata was blocked: %v", err)
	}
	checkWorker(claimSQL, "fresh:sql:false", "fresh", "lease-1")
	checkWorker(claimSQL, "", "fresh", "second-lease")
	for _, query := range []string{
		"UPDATE public.fugue_app_database_import_jobs SET status='running' WHERE id='fresh'",
		"UPDATE public.fugue_app_database_import_jobs SET result_message='legacy log' WHERE id='old'",
		"INSERT INTO public.fugue_app_database_import_jobs(id,status) VALUES ('stale','running')",
		"UPDATE public.fugue_meta SET value='api' WHERE key='app-database-import/v1/writer'",
		"DELETE FROM public.fugue_meta WHERE key='app-database-import/v1/writer'",
		"UPDATE public.fugue_meta SET value='stale' WHERE key='app-database-import/v1/attempt/fresh'",
		"DELETE FROM public.fugue_meta WHERE key='app-database-import/v1/attempt/fresh'",
		"UPDATE public.fugue_meta SET key='app-database-import/v1/attempt/forged' WHERE key='unrelated'",
	} {
		var rejected *pgconn.PgError
		err := runAs("fixture_old_api", query)
		if !errors.As(err, &rejected) || rejected.Code != "42501" {
			t.Fatalf("legacy execution was not fenced for %q: %v", query, err)
		}
	}
	if err := runAs("fixture_old_api", `INSERT INTO public.fugue_app_database_import_jobs
	    (id,status,tenant_id,app_id,source_upload_id) VALUES
	    ('race-1','pending','tenant','racing-app','upload'), ('race-2','pending','tenant','racing-app','upload'),
	    ('blocked-job','pending','tenant','blocked-app','upload')`); err != nil {
		t.Fatal(err)
	}
	checkWorker(claimSQL, "", "blocked-job", "blocked-lease")
	var blocked string
	if err := db.QueryRowContext(ctx, "SELECT status FROM public.fugue_app_database_import_jobs WHERE id='blocked-job'").Scan(&blocked); err != nil || blocked != "failed" {
		t.Fatalf("exclusive lifecycle operation did not fail the pending import: %q %v", blocked, err)
	}
	type claimResult struct {
		id, claim string
		err       error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	for _, id := range []string{"race-1", "race-2"} {
		go func(id string) {
			<-start
			var claim string
			err := worker.QueryRowContext(ctx, claimSQL, id, id+"-lease").Scan(&claim)
			results <- claimResult{id, claim, err}
		}(id)
	}
	close(start)
	wins := 0
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.claim != "" {
			if result.claim != result.id+":sql:false" {
				t.Fatalf("wrong claim: %v", result)
			}
			wins++
			checkWorker(archiveSQL, "dump.sql:select 1", result.id, result.id+"-lease", 16)
		} else {
			checkWorker(archiveSQL, "", result.id, result.id+"-lease", 16)
		}
	}
	if wins != 1 {
		t.Fatalf("concurrent claims for the same app: %d winners", wins)
	}
	checkWorker(archiveSQL, "dump.sql:select 1", "fresh", "lease-1", 16)
	checkWorker(connectionSQL, "owned:tenant:stable-rw:owned:fixture:owned-password", "fresh", "lease-1")
	checkWorker(connectionSQL, "", "fresh", "wrong-lease")
	checkWorker(connectionSQL, "", "ready", "lease-1")
	mustExec(`INSERT INTO public.fugue_service_bindings (id,tenant_id,app_id,service_id,created_at)
	    VALUES ('mismatched-binding','tenant','fixture-app','cross-tenant',now()-interval '2 minutes')`)
	checkWorker(connectionSQL, "", "fresh", "lease-1")
	mustExec(`DELETE FROM public.fugue_service_bindings WHERE id='mismatched-binding';
	    UPDATE public.fugue_backing_services SET status='deleted' WHERE id='owned'`)
	checkWorker(connectionSQL, "shared:tenant:shared-rw:shared:fixture:shared-password", "fresh", "lease-1")
	mustExec(`UPDATE public.fugue_backing_services SET status='active' WHERE id='owned';
	    INSERT INTO public.fugue_operations VALUES('fixture-app','database-resize','waiting-agent')`)
	checkWorker(connectionSQL, "", "fresh", "lease-1")
	mustExec(`DELETE FROM public.fugue_operations WHERE app_id='fixture-app'`)
	checkWorker(archiveSQL, "", "fresh", "wrong-lease", 16)
	checkWorker(archiveSQL, "", "old", "lease-1", 16)
	checkWorker(archiveSQL, "", "fresh", "lease-1", 7)
	checkWorker(touchSQL, "false", "fresh", "wrong-lease", false)
	checkWorker(touchSQL, "true", "fresh", "lease-1", false)
	mustExec("UPDATE public.fugue_tenants SET status='access-held' WHERE id='tenant'")
	checkWorker(archiveSQL, "", "fresh", "lease-1", 16)
	checkWorker(connectionSQL, "", "fresh", "lease-1")
	checkWorker(touchSQL, "false", "fresh", "lease-1", true)
	mustExec("UPDATE public.fugue_tenants SET status='active' WHERE id='tenant'")
	if err := runAs("fugue_app_db_import_definer", `UPDATE public.fugue_meta SET updated_at=clock_timestamp()-interval '61 seconds'
	    WHERE key='app-database-import/v1/attempt/fresh'`); err != nil {
		t.Fatal(err)
	}
	checkWorker(archiveSQL, "", "fresh", "lease-1", 16)
	checkWorker(connectionSQL, "", "fresh", "lease-1")
	checkWorker(touchSQL, "false", "fresh", "lease-1", false)
	checkWorker(touchSQL, "false", "fresh", "lease-1", true)
	checkWorker(recoverSQL, "1")
	checkJob := func(id, wantStatus, wantError string, wantAttempt bool) {
		t.Helper()
		var status, message string
		var hasAttempt, hasLog bool
		if err := db.QueryRowContext(ctx, `SELECT status, error_message,
        EXISTS(SELECT FROM public.fugue_meta WHERE key='app-database-import/v1/attempt/'||j.id),
        EXISTS(SELECT FROM jsonb_array_elements(logs_json) AS entry WHERE entry->>'message'=j.error_message)
    FROM public.fugue_app_database_import_jobs j WHERE id=$1`, id).Scan(&status, &message, &hasAttempt, &hasLog); err != nil || status != wantStatus || message != wantError || hasAttempt != wantAttempt || hasLog != (wantError != "") {
			t.Fatalf("job %s = %q, %q, attempt=%v, log=%v, err=%v", id, status, message, hasAttempt, hasLog, err)
		}
	}
	checkJob("fresh", "pending", "", false)
	checkWorker(claimSQL, "fresh:sql:false", "fresh", "recovered-lease")
	checkWorker(touchSQL, "false", "fresh", "lease-1", false)
	checkWorker(archiveSQL, "", "fresh", "lease-1", 16)
	checkWorker(archiveSQL, "dump.sql:select 1", "fresh", "recovered-lease", 16)
	checkWorker(touchSQL, "true", "fresh", "recovered-lease", true)
	checkWorker(connectionSQL, "", "fresh", "recovered-lease")
	checkWorker(touchSQL, "false", "fresh", "recovered-lease", true)
	checkWorker(touchSQL, "true", "fresh", "recovered-lease", false)
	checkWorker(archiveSQL, "", "fresh", "recovered-lease", 16)
	if err := runAs("fixture_old_api", `INSERT INTO public.fugue_app_database_import_jobs
	    (id,status,tenant_id,app_id,source_upload_id) VALUES ('queued','pending','tenant','fixture-app','upload')`); err != nil {
		t.Fatal(err)
	}
	if err := runAs("fugue_app_db_import_definer", `UPDATE public.fugue_meta SET updated_at=clock_timestamp()-interval '61 seconds'
	    WHERE key='app-database-import/v1/attempt/fresh'`); err != nil {
		t.Fatal(err)
	}
	checkWorker(recoverSQL, "1")
	checkWorker(recoverSQL, "0")
	checkJob("fresh", "failed", "database import outcome is unknown after SQL execution began; inspect the target database before submitting another import", false)
	checkJob("queued", "failed", "queued database import did not run because an earlier import's outcome is unknown; inspect the target database before submitting another import", false)
	checkJob("ready", "pending", "", false)
	checkWorker(claimSQL, "", "queued", "unsafe-retry")
	checkWorker(touchSQL, "false", "fresh", "recovered-lease", false)

	if err := runAs("fixture_old_api", `INSERT INTO public.fugue_app_database_import_jobs
	    (id,status,tenant_id,app_id,source_upload_id) VALUES ('prepare-error','pending','tenant','fixture-app','upload'),
	    ('success','pending','tenant','fixture-app','upload'),('execute-error','pending','tenant','fixture-app','upload'),
	    ('queue-after-error','pending','tenant','fixture-app','upload')`); err != nil {
		t.Fatal(err)
	}
	checkWorker(claimSQL, "prepare-error:sql:false", "prepare-error", "prepare-lease")
	checkWorker(completeSQL, "false", "prepare-error", "prepare-lease", "completed", "not executed", "")
	checkWorker(completeSQL, "true", "prepare-error", "prepare-lease", "failed", "", "could not prepare")
	checkJob("prepare-error", "failed", "could not prepare", false)
	checkJob("success", "pending", "", false)
	checkWorker(claimSQL, "success:sql:false", "success", "success-lease")
	checkWorker(appendSQL, "false", "success", "wrong-lease", "stale log")
	checkWorker(appendSQL, "true", "success", "success-lease", "starting")
	checkWorker(touchSQL, "true", "success", "success-lease", true)
	checkWorker(completeSQL, "false", "success", "wrong-lease", "completed", "stale result", "")
	checkWorker(completeSQL, "true", "success", "success-lease", "completed", "client reported success", "")
	checkWorker(completeSQL, "false", "success", "success-lease", "completed", "duplicate", "")
	checkWorker(appendSQL, "false", "success", "success-lease", "late log")
	checkJob("success", "completed", "", false)
	var result, messages string
	if err := db.QueryRowContext(ctx, `SELECT result_message,
	    (SELECT string_agg(entry->>'message', ',' ORDER BY n) FROM jsonb_array_elements(logs_json) WITH ORDINALITY AS l(entry,n))
	    FROM public.fugue_app_database_import_jobs WHERE id='success'`).Scan(&result, &messages); err != nil || result != "client reported success" || messages != "starting,database import completed" {
		t.Fatalf("unexpected successful result/logs: %q %q %v", result, messages, err)
	}
	checkWorker(claimSQL, "execute-error:sql:false", "execute-error", "execute-lease")
	checkWorker(touchSQL, "true", "execute-error", "execute-lease", true)
	checkWorker(completeSQL, "true", "execute-error", "execute-lease", "failed", "", "client exited with error")
	checkJob("execute-error", "failed", "database import outcome is unknown after SQL execution began; inspect the target database before submitting another import", false)
	checkJob("queue-after-error", "failed", "queued database import did not run because an earlier import's outcome is unknown; inspect the target database before submitting another import", false)
	if err := db.QueryRowContext(ctx, `SELECT logs_json->0->>'message' FROM public.fugue_app_database_import_jobs
	    WHERE id='execute-error'`).Scan(&messages); err != nil || messages != "client exited with error" {
		t.Fatalf("ambiguous failure lost the client diagnostic: %q %v", messages, err)
	}
	if err := runAs("fixture_old_api", `INSERT INTO public.fugue_app_database_import_jobs
	    (id,status,tenant_id,app_id,source_upload_id) VALUES ('adapter-job','pending','tenant','fixture-app','upload')`); err != nil {
		t.Fatal(err)
	}
	if job, err := adapter.Claim(ctx, "adapter-job", "adapter-lease"); err != nil || job.ID != "adapter-job" || job.Format != "sql" || job.Clean {
		t.Fatalf("adapter claim mismatch: %q %q %v %v", job.ID, job.Format, job.Clean, err)
	}
	if _, err := adapter.Claim(ctx, "adapter-job", "duplicate"); !errors.Is(err, appdbimport.ErrUnclaimable) {
		t.Fatalf("adapter did not recognize a lost claim: %v", err)
	}
	if archive, err := adapter.ReadArchive(ctx, "adapter-job", "adapter-lease", 16); err != nil || archive.Filename != "dump.sql" || string(archive.Bytes) != "select 1" {
		t.Fatal("adapter did not read the claimed archive")
	}
	if connection, err := adapter.ReadConnection(ctx, "adapter-job", "adapter-lease"); err != nil || connection.ServiceID != "owned" || connection.Host != "stable-rw.fg-tenant.svc.cluster.local" || connection.Database != "owned" {
		t.Fatal("adapter did not resolve the claimed database binding")
	}
	if err := adapter.AppendLog(ctx, "adapter-job", "adapter-lease", "adapter starting"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.BeginEffect(ctx, "adapter-job", "adapter-lease"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Renew(ctx, "adapter-job", "adapter-lease"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Complete(ctx, "adapter-job", "adapter-lease", "completed", "adapter done", ""); err != nil {
		t.Fatal(err)
	}
	checkJob("adapter-job", "completed", "", false)
	var denied *pgconn.PgError
	if err := runAs("fugue_app_db_import_definer", "UPDATE public.fugue_meta SET value='api' WHERE key='app-database-import/v1/writer'"); !errors.As(err, &denied) || denied.Code != "42501" {
		t.Fatalf("worker was allowed to reset ownership: %v", err)
	}
	mustExec("ALTER TABLE public.fugue_meta DISABLE TRIGGER fugue_app_db_import_meta_v1")
	if err := activate(); err == nil || !strings.Contains(err.Error(), "without its metadata fence") {
		t.Fatalf("activation retry accepted a missing metadata fence: %v", err)
	}
	mustExec("ALTER TABLE public.fugue_meta ENABLE ALWAYS TRIGGER fugue_app_db_import_meta_v1")
	mustExec("ALTER TABLE public.fugue_app_database_import_jobs DISABLE TRIGGER fugue_app_db_import_writer_v1")
	if err := activate(); err == nil || !strings.Contains(err.Error(), "without its write fence") {
		t.Fatalf("activation retry accepted a missing live fence: %v", err)
	}
	if !readOwner().Equal(first) {
		t.Fatal("invalid fence silently restored API ownership")
	}
}

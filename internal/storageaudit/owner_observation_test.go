package storageaudit

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresObserveStorageAuditOwner(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_STORAGE_AUDIT_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FUGUE_TEST_STORAGE_AUDIT_DATABASE_URL to an empty disposable PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var existing bool
	if err := admin.QueryRowContext(ctx, `SELECT to_regnamespace('fugue_storage_audit') IS NOT NULL`).Scan(&existing); err != nil || existing {
		t.Fatalf("expected an empty fixture database: %v", err)
	}
	run := func(query string, args ...any) {
		t.Helper()
		if _, err := admin.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	run(`
		CREATE ROLE fixture_storage_observer LOGIN NOINHERIT;
		CREATE TABLE public.fugue_source_uploads(id text, tenant_id text, created_at timestamptz, size_bytes bigint);
		CREATE TABLE public.fugue_apps(id text, tenant_id text, source_json jsonb);
		CREATE TABLE public.fugue_operations(id text, tenant_id text, desired_source_json jsonb);
		CREATE TABLE public.fugue_app_database_import_jobs(id text, tenant_id text, source_upload_id text);
	`)
	providerSchema, err := os.ReadFile("../schemamigrate/storage_lifecycle_audit.sql")
	if err != nil {
		t.Fatal(err)
	}
	run(string(providerSchema))
	run(`
		CREATE TABLE fugue_storage_audit.lifecycle_activation (
			singleton boolean PRIMARY KEY, approval_sha256 text NOT NULL, policy_sha256 text NOT NULL,
			worker_secret_uid uuid NOT NULL, activated_at timestamptz NOT NULL);
		GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO fixture_storage_observer;
		GRANT USAGE ON SCHEMA fugue_storage_audit TO fixture_storage_observer;
		GRANT SELECT ON fugue_storage_audit.lifecycle_status_v1, fugue_storage_audit.lifecycle_activation TO fixture_storage_observer;
	`)
	identity := MaintenanceObserver{Role: "fixture_storage_observer"}
	if err := admin.QueryRowContext(ctx, `SELECT system_identifier::text, current_database() FROM pg_control_system()`).Scan(&identity.SystemID, &identity.Database); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.User = identity.Role
	observer := stdlib.OpenDB(*cfg)
	defer observer.Close()
	if _, err := identity.Observe(ctx, admin); err == nil {
		t.Fatal("provider observer accepted the administrative connection")
	}
	if _, err := identity.Observe(ctx, observer); err == nil || !strings.Contains(err.Error(), "has not durably transferred") {
		t.Fatalf("standby should not be accepted as a verified worker activation: %v", err)
	}
	activatedAt := time.Date(2026, 11, 27, 15, 0, 0, 0, time.UTC)
	policy := Policy{Version: 1, Mode: "audit", EffectiveFrom: activatedAt.Add(-time.Hour), IntervalSeconds: 300, SourcePageRows: 24,
		HistoryDays: 14, MinimumVerifiedVersions: 5, UnusedUploadHours: 48, UnreferencedUploadDays: 30,
		WarningFreePercent: 20, PressureFreePercent: 15, CriticalFreePercent: 10}
	rawPolicy, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	run(`INSERT INTO fugue_storage_audit.lifecycle_control VALUES
		(true, 'api', 'audit', $1, $2, 300, 24, $3::jsonb)`, policy.Digest(), policy.EffectiveFrom, rawPolicy)
	run(`UPDATE fugue_storage_audit.lifecycle_control SET owner = 'worker' WHERE singleton`)
	if _, err := identity.Observe(ctx, observer); err == nil || !strings.Contains(err.Error(), "no valid durable ownership receipt") {
		t.Fatalf("active owner without a durable receipt was accepted: %v", err)
	}
	run(`INSERT INTO fugue_storage_audit.lifecycle_activation VALUES
		(true, $1, $2, '00000000-0000-4000-8000-000000000001', $3)`, strings.Repeat("a", 64), policy.Digest(), activatedAt)
	got, err := identity.Observe(ctx, observer)
	if err != nil || got.PolicyDigest != policy.Digest() || got.Activation == nil || got.PostActivationSample || got.CompletedPasses != 0 {
		t.Fatalf("wrong ownership observation: %+v, %v", got, err)
	}
	sample := Sample{ObservedAt: activatedAt.Add(time.Second), PolicySHA256: policy.Digest()}
	for _, relation := range []string{
		"fugue_source_uploads", "fugue_platform_artifacts", "fugue_platform_artifact_contents",
		"fugue_audit_events", "fugue_platform_input_shared_packs", "fugue_platform_input_shared_objects",
		"fugue_platform_input_component_refs", "fugue_platform_input_component_recipes",
		"fugue_platform_input_separated_refs", "fugue_platform_input_components", "fugue_platform_quality_evidence",
	} {
		sample.Relations = append(sample.Relations, RelationAllocation{Relation: relation})
	}
	rawSample, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	run(`INSERT INTO fugue_storage_audit.lifecycle_progress
		(singleton,policy_sha256,last_attempt_at,duration_seconds,last_run_failed,completed_passes,sample)
		VALUES (true,$1,$2,0,false,1,$3::jsonb)`, policy.Digest(), sample.ObservedAt, rawSample)
	got, err = identity.Observe(ctx, observer)
	if err != nil || !got.PostActivationSample || got.CompletedPasses != 1 || !got.SampleAt.Equal(sample.ObservedAt) {
		t.Fatalf("committed sample was not attributed to this owner and policy: %+v, %v", got, err)
	}
	run(`UPDATE fugue_storage_audit.lifecycle_progress SET last_run_failed=true WHERE singleton`)
	got, err = identity.Observe(ctx, observer)
	if err != nil || got.PostActivationSample || !got.LastPassFailed || got.CompletedPasses != 1 {
		t.Fatalf("failed progress was treated as a successful post-activation sample: %+v, %v", got, err)
	}
	if _, err := admin.ExecContext(ctx, `GRANT EXECUTE ON FUNCTION fugue_storage_audit.sample_lifecycle_v1() TO fixture_storage_observer`); err != nil {
		t.Fatal(err)
	}
	if _, err := identity.Observe(ctx, observer); err == nil || !strings.Contains(err.Error(), "observer role") {
		t.Fatalf("sampling-capable identity passed as a read-only observer: %v", err)
	}
}

package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
)

type postgresServiceNameJSONArg string

func (want postgresServiceNameJSONArg) Match(value driver.Value) bool {
	var raw []byte
	switch typed := value.(type) {
	case []byte:
		raw = typed
	case string:
		raw = []byte(typed)
	default:
		return false
	}
	var document struct {
		Postgres *struct {
			ServiceName string `json:"service_name"`
		} `json:"postgres"`
	}
	if err := json.Unmarshal(raw, &document); err != nil || document.Postgres == nil {
		return false
	}
	return document.Postgres.ServiceName == string(want)
}

func TestEnsureManagedPostgresServiceNamesTxCanonicalizesPersistedNames(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	mock.ExpectQuery(`SELECT id, spec_json FROM fugue_backing_services WHERE type = \$1 AND provisioner = \$2`).
		WithArgs("postgres", "managed").
		WillReturnRows(sqlmock.NewRows([]string{"id", "spec_json"}).
			AddRow("service-mixed", `{"postgres":{"service_name":"MecGod"}}`).
			AddRow("service-canonical", `{"postgres":{"service_name":"already-canonical"}}`))
	mock.ExpectExec(`UPDATE fugue_backing_services SET spec_json = \$2, updated_at = NOW\(\) WHERE id = \$1`).
		WithArgs("service-mixed", postgresServiceNameJSONArg("mecgod")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE fugue_service_bindings SET env_json = jsonb_set\(env_json, '\{DB_HOST\}', to_jsonb\(\$2::text\), true\), updated_at = NOW\(\) WHERE service_id = \$1 AND env_json IS NOT NULL AND LOWER\(BTRIM\(COALESCE\(env_json->>'DB_HOST', ''\)\)\) = LOWER\(\$3\)`).
		WithArgs("service-mixed", "mecgod", "MecGod").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE fugue_service_bindings SET env_json = jsonb_set\(env_json, '\{DB_HOST\}', to_jsonb\(\$2::text\), true\), updated_at = NOW\(\) WHERE service_id = \$1 AND env_json IS NOT NULL AND LOWER\(BTRIM\(COALESCE\(env_json->>'DB_HOST', ''\)\)\) = LOWER\(\$3\)`).
		WithArgs("service-mixed", "mecgod-rw", "MecGod-rw").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT id, spec_json FROM fugue_apps WHERE spec_json->'postgres' IS NOT NULL`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "spec_json"}).
			AddRow("app-mixed", `{"image":"example/app","replicas":1,"runtime_id":"runtime-a","postgres":{"service_name":"MecGod"}}`))
	mock.ExpectExec(`UPDATE fugue_apps SET spec_json = \$2, updated_at = NOW\(\) WHERE id = \$1`).
		WithArgs("app-mixed", postgresServiceNameJSONArg("mecgod")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := (&Store{}).ensureManagedPostgresServiceNamesTx(context.Background(), tx); err != nil {
		t.Fatalf("ensureManagedPostgresServiceNamesTx: %v", err)
	}

	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestApplyPostgresSchemaTxSkipsDDLWhenFingerprintMatches(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()

	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(postgresMetaSchemaStatement)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT value FROM fugue_meta WHERE key = $1`)).
		WithArgs(postgresSchemaFingerprintMetaKey).
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(postgresSchemaFingerprint()))

	applied, err := s.applyPostgresSchemaTx(context.Background(), tx)
	if err != nil {
		t.Fatalf("applyPostgresSchemaTx: %v", err)
	}
	if applied {
		t.Fatal("expected schema bootstrap to skip DDL when fingerprint matches")
	}

	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestApplyPostgresSchemaTxAppliesDDLAndStoresFingerprintWhenMissing(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()

	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(postgresMetaSchemaStatement)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT value FROM fugue_meta WHERE key = $1`)).
		WithArgs(postgresSchemaFingerprintMetaKey).
		WillReturnRows(sqlmock.NewRows([]string{"value"}))
	mock.ExpectExec(regexp.QuoteMeta(`SELECT set_config('lock_timeout', $1, true)`)).
		WithArgs(formatPostgresDuration(postgresBootstrapLockTimeout)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	for _, stmt := range postgresSchemaStatements[1:] {
		mock.ExpectExec(regexp.QuoteMeta(stmt)).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectExec(regexp.QuoteMeta(`
INSERT INTO fugue_meta (key, value, updated_at)
VALUES ($1, $2, NOW())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, updated_at = NOW()
`)).
		WithArgs(postgresSchemaFingerprintMetaKey, postgresSchemaFingerprint()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	applied, err := s.applyPostgresSchemaTx(context.Background(), tx)
	if err != nil {
		t.Fatalf("applyPostgresSchemaTx: %v", err)
	}
	if !applied {
		t.Fatal("expected schema bootstrap to apply DDL when fingerprint is missing")
	}

	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestIsRetryableBootstrapError(t *testing.T) {
	t.Parallel()

	retryable := fmt.Errorf("wrap: %w", &pgconn.PgError{Code: "40P01"})
	if !isRetryableBootstrapError(retryable) {
		t.Fatal("expected deadlock to be retryable during bootstrap")
	}

	nonRetryable := fmt.Errorf("wrap: %w", &pgconn.PgError{Code: "23505"})
	if isRetryableBootstrapError(nonRetryable) {
		t.Fatal("expected unique violation to stay non-retryable during bootstrap")
	}

	if isRetryableBootstrapError(sql.ErrNoRows) {
		t.Fatal("expected non-postgres errors to stay non-retryable")
	}
}

func TestPostgresBootstrapTimeoutCoversRollingStartupContention(t *testing.T) {
	t.Parallel()

	if postgresBootstrapTimeout < 5*time.Minute {
		t.Fatalf("postgres bootstrap timeout should cover rolling startup lock contention, got %s", postgresBootstrapTimeout)
	}
	if postgresPingTimeout+postgresBootstrapTimeout > 6*time.Minute {
		t.Fatalf("postgres bootstrap and ping timeout should fit within the default API startup probe window, got ping=%s bootstrap=%s", postgresPingTimeout, postgresBootstrapTimeout)
	}
}

func TestPostgresSchemaIncludesOperationLookupIndexes(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, indexName := range []string{
		"idx_fugue_operations_app_created_at",
		"idx_fugue_operations_app_status",
		"idx_fugue_operations_tenant_created_at",
		"idx_fugue_operations_tenant_app_status",
		"idx_fugue_operations_oom_right_sizing_event",
	} {
		if !strings.Contains(schema, indexName) {
			t.Fatalf("postgres schema is missing %s", indexName)
		}
	}
}

func TestPostgresSchemaIncludesBoundedAppPaginationIndexes(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, indexName := range []string{
		"idx_fugue_apps_created_id_desc",
		"idx_fugue_apps_updated_id_desc",
		"idx_fugue_apps_name_id_ci",
		"idx_fugue_apps_tenant_project_created_id_desc",
		"idx_fugue_apps_phase_created_id_desc",
		"idx_fugue_app_domains_verified_app_hostname_ci",
	} {
		if !strings.Contains(schema, indexName) {
			t.Fatalf("postgres schema is missing pagination index %s", indexName)
		}
	}
}

func TestPostgresSchemaIncludesExpectedConsumerSetPersistence(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS fugue_platform_expected_consumer_sets",
		"idx_fugue_platform_expected_consumers_release_revision",
		"idx_fugue_platform_expected_consumers_artifact_release",
		"idx_fugue_platform_expected_consumers_kind_scope",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("postgres schema is missing %s", required)
		}
	}
}

func TestPostgresSchemaIncludesPlatformConsumerHeartbeatEnvelope(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, required := range []string{
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS credential_id",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS release_set_id",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS expected_consumer_set_id",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS fencing_token",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS protocol_version",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS schema_version",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS sequence",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS issued_at",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS nonce",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS generation_sequence",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS evidence_hash",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS identity_verified",
		"idx_fugue_platform_consumers_release_set",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("postgres schema is missing %s", required)
		}
	}
}

func TestPostgresSchemaIncludesPlatformLKGHistoryRetention(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS fugue_platform_lkg_snapshot_history",
		"DROP CONSTRAINT IF EXISTS fugue_platform_lkg_snapshot_h_artifact_kind_scope_key_gener_key",
		"idx_fugue_platform_lkg_history_kind_scope_sequence",
		"idx_fugue_platform_lkg_history_kind_scope_verified",
		"FROM fugue_platform_lkg_snapshots",
		"ON CONFLICT (id) DO NOTHING",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("postgres schema is missing %s", required)
		}
	}
	if strings.Contains(schema, "UNIQUE (artifact_kind, scope_key, generation)") {
		t.Fatal("PostgreSQL LKG history must allow a rollback generation to be re-verified as a new immutable event")
	}
}

func TestPostgresSchemaAppliesToLiveTestDatabase(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run live Postgres schema integration test")
	}
	if !strings.Contains(databaseURL, "fugue-pgtest") && !strings.Contains(databaseURL, "fugue_test") {
		t.Fatalf("refusing to run schema integration test against non-test database URL %q", databaseURL)
	}
	s := New("", databaseURL)
	if err := s.Init(); err != nil {
		t.Fatalf("init store against live test database: %v", err)
	}
}

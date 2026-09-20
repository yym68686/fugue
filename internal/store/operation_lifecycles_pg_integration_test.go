package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestOperationLifecyclesPostgresIgnoresLargeTerminalPayloads(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable PostgreSQL integration")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires a disposable loopback fugue_test database")
	}
	db, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := fmt.Sprintf("lifecycle_test_%d", time.Now().UnixNano())
	if _, err := db.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP SCHEMA " + schema + " CASCADE")
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	isolated, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer isolated.Close()
	isolated.SetMaxOpenConns(1)
	s := &Store{db: isolated, databaseURL: u.String(), dbReady: true}
	// The extra columns deliberately match the old full operation reader. A
	// large history should not be transferred for either lifecycle consumer.
	_, err = isolated.Exec(`CREATE TABLE fugue_operations (
 id text PRIMARY KEY, app_id text NOT NULL, type text NOT NULL, status text NOT NULL,
 created_at timestamptz NOT NULL, started_at timestamptz, completed_at timestamptz,
 tenant_id text NOT NULL DEFAULT '', service_id text NOT NULL DEFAULT '', execution_mode text NOT NULL DEFAULT '',
 requested_by_type text NOT NULL DEFAULT '', requested_by_id text NOT NULL DEFAULT '', source_runtime_id text NOT NULL DEFAULT '',
 target_runtime_id text NOT NULL DEFAULT '', desired_replicas integer, desired_spec_json jsonb, desired_source_json jsonb,
 result_message text NOT NULL DEFAULT '', manifest_path text NOT NULL DEFAULT '', assigned_runtime_id text NOT NULL DEFAULT '',
 error_message text NOT NULL DEFAULT '', updated_at timestamptz NOT NULL DEFAULT now());
 CREATE INDEX ON fugue_operations(status,created_at);
 INSERT INTO fugue_operations(id,app_id,type,status,created_at,completed_at,desired_spec_json)
 SELECT 'history-'||i,'app','deploy','completed',now()-interval '1 year'+i*interval '1 second',now(),
   jsonb_build_object('image','registry.example/app:v1','files',jsonb_build_array(jsonb_build_object('path','/app/config','content',repeat('payload ',1024))))
 FROM generate_series(1,30000) i;
 INSERT INTO fugue_operations(id,app_id,type,status,created_at,started_at)
 VALUES ('old-active','app','deploy','waiting-agent',now()-interval '2 years',now()-interval '2 years');
 ANALYZE fugue_operations;`)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	active, err := s.ListActiveOperationLifecycles()
	if err != nil || len(active) != 1 || active[0].ID != "old-active" {
		t.Fatalf("active history query failed: %+v %v", active, err)
	}
	activeElapsed := time.Since(started)
	started = time.Now()
	all, err := s.ListOperationLifecycles()
	if err != nil || len(all) != 30001 {
		t.Fatalf("retention history truncated: %d %v", len(all), err)
	}
	allElapsed := time.Since(started)
	if !reflect.DeepEqual(all[0], active[0]) {
		t.Fatal("old active lifecycle changed between projections")
	}
	encoded, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "payload ") {
		t.Fatal("lifecycle result contains deployment files")
	}
	// Database privileges make payload independence a hard invariant, rather
	// than relying only on a wall-clock performance assertion.
	role := schema + "_reader"
	if _, err := db.Exec("CREATE ROLE " + role); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = isolated.Exec("RESET ROLE")
		_, _ = db.Exec("DROP OWNED BY " + role)
		_, _ = db.Exec("DROP ROLE " + role)
	}()
	if _, err := db.Exec("GRANT USAGE ON SCHEMA " + schema + " TO " + role + "; GRANT SELECT (id,app_id,type,status,created_at,started_at,completed_at) ON " + schema + ".fugue_operations TO " + role); err != nil {
		t.Fatal(err)
	}
	if _, err := isolated.Exec("SET ROLE " + role); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListOperations("", true); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("old reader did not require payload privileges: %v", err)
	}
	if _, err := s.ListActiveOperationLifecycles(); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ListOperationLifecycles(); err != nil || len(got) != len(all) {
		t.Fatalf("retention reader depended on payloads: %d %v", len(got), err)
	}
	// Retention dates must be exactly the stored dates, including NULL values.
	var completed sql.NullTime
	if err := isolated.QueryRowContext(context.Background(), `SELECT completed_at FROM fugue_operations WHERE id='old-active'`).Scan(&completed); err != nil || completed.Valid || active[0].CompletedAt != nil {
		t.Fatalf("NULL completion lost: %+v %v", completed, err)
	}
	if active[0].Status != model.OperationStatusWaitingAgent {
		t.Fatal("waiting-agent operation was not preserved")
	}
	t.Logf("30000 terminal payloads: active=%s complete retention inventory=%s projected_json_bytes=%d", activeElapsed, allElapsed, len(encoded))
}

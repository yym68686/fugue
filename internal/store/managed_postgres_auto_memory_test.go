package store

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func autoMemoryBackoffFixture() (time.Time, []model.Operation, []string, map[string]ManagedPostgresAutoMemoryBackoff) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	op := func(service, status string, age time.Duration) model.Operation {
		return model.Operation{TenantID: "tenant-a", AppID: "app-a", ServiceID: service, Type: model.OperationTypeDatabaseResize,
			Status: status, RequestedByType: model.ActorTypeSystem, RequestedByID: ManagedPostgresAutoMemoryRequestActorPrefix + "/intent",
			UpdatedAt: now.Add(-age)}
	}
	ops := []model.Operation{
		op("old-pending", model.OperationStatusPending, 1000*time.Hour),
		op("old-running", model.OperationStatusRunning, 1000*time.Hour),
		op("recent", model.OperationStatusFailed, 6*time.Hour-time.Second),
		op("recent", model.OperationStatusCompleted, time.Hour-time.Second),
		op("cooldown", model.OperationStatusCompleted, time.Hour-time.Second),
		op("at-cutoff", model.OperationStatusFailed, 6*time.Hour),
		op("at-cutoff", model.OperationStatusCompleted, time.Hour),
		op("outside", model.OperationStatusFailed, 7*time.Hour),
		op("foreign-tenant", model.OperationStatusPending, 0),
		op("foreign-app", model.OperationStatusPending, 0),
		op("wrong-type", model.OperationStatusRunning, 0),
		op("wrong-actor", model.OperationStatusFailed, 0),
		op("wrong-prefix", model.OperationStatusCompleted, 0),
		op("not-requested", model.OperationStatusRunning, 0),
	}
	ops[0].RequestedByType, ops[1].RequestedByType, ops[11].RequestedByType = "user", "user", "user"
	ops[8].TenantID, ops[9].AppID, ops[10].Type = "tenant-b", "app-b", model.OperationTypeDeploy
	ops[12].RequestedByID = ManagedPostgresAutoMemoryRequestActorPrefix + "-different/intent"
	want := map[string]ManagedPostgresAutoMemoryBackoff{
		"old-pending": {Blocked: true}, "old-running": {Blocked: true},
		"recent": {Blocked: true, Downscale: true}, "cooldown": {Downscale: true},
	}
	var ids []string
	for _, entry := range ops[:len(ops)-1] {
		ids = append(ids, entry.ServiceID)
	}
	return now, ops, ids, want
}

func assertAutoMemoryBackoffs(t *testing.T, s *Store, now time.Time, ids []string, want map[string]ManagedPostgresAutoMemoryBackoff) {
	t.Helper()
	got, err := s.ManagedPostgresAutoMemoryBackoffs(context.Background(), "tenant-a", "app-a", ids, now)
	if err != nil || !reflect.DeepEqual(want, got) {
		t.Fatalf("backoffs = %#v, %v; want %#v", got, err, want)
	}
}

func TestManagedPostgresAutoMemoryBackoffs(t *testing.T) {
	now, ops, ids, want := autoMemoryBackoffFixture()
	s := New(filepath.Join(t.TempDir(), "state.json"), "")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if err := s.withLockedState(true, func(st *model.State) error { st.Operations = ops; return nil }); err != nil {
		t.Fatal(err)
	}
	assertAutoMemoryBackoffs(t, s, now, ids, want)
}

func TestManagedPostgresAutoMemoryBackoffsPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("requires a disposable PostgreSQL database")
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
	db.SetMaxOpenConns(1)
	// Exclude the JSON/payload columns altogether: any accidental read of a
	// full operation fails against this dedicated temp table.
	if _, err := db.Exec(`CREATE TEMP TABLE fugue_operations (
		tenant_id text NOT NULL, app_id text NOT NULL, service_id text NOT NULL,
		type text NOT NULL, status text NOT NULL, requested_by_type text NOT NULL,
		requested_by_id text NOT NULL, updated_at timestamptz NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	now, ops, ids, want := autoMemoryBackoffFixture()
	for _, op := range ops {
		if _, err := db.Exec(`INSERT INTO fugue_operations VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			op.TenantID, op.AppID, op.ServiceID, op.Type, op.Status, op.RequestedByType, op.RequestedByID, op.UpdatedAt); err != nil {
			t.Fatal(err)
		}
	}
	assertAutoMemoryBackoffs(t, &Store{db: db, databaseURL: address, dbReady: true}, now, ids, want)
}

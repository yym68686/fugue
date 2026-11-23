package main

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// This intentionally exercises the entire real startup sequence, not just a
// single feature's DDL. The isolated database requires the normal Store bootstrap.
func TestSchemaStartupWithOccupiedServingTables(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_SCHEMA_REPLAY_URL")
	if dsn == "" {
		t.Skip("requires isolated prebootstrapped loopback PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/fugue_test") {
		t.Fatal("local disposable database required")
	}
	// A socket override may be used to reach a local private test instance.
	if host := u.Query().Get("host"); host != "" && !strings.HasPrefix(host, "/") {
		t.Fatal("remote host overrides are not allowed")
	}
	ctx := context.Background()
	if err := migrateSchema(ctx, dsn); err != nil {
		t.Fatal("initial installation:", err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// An open reader of the archive epoch, and ordinary concurrent writes
	// against routes, release lanes, current consumer receipts and LKG rows.
	if _, err = tx.ExecContext(ctx, "SELECT epoch FROM fugue_platform_input_archive_control WHERE singleton"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `LOCK TABLE fugue_platform_artifacts, fugue_platform_artifact_releases,
		fugue_platform_release_lanes, fugue_platform_lkg_snapshots, fugue_platform_lkg_snapshot_history,
		fugue_platform_consumer_instances, fugue_platform_shadow_consumers,
		fugue_platform_expected_consumer_sets IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	scope := func() (int, error) {
		var count int
		e := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_trigger WHERE tgname='fugue_retired_consumer_root'
		 AND tgenabled='A' AND tgtype=23 AND tgfoid='fugue_guard_retired_consumer_root()'::regprocedure`).Scan(&count)
		return count, e
	}
	if n, e := scope(); e != nil || n != 5 {
		t.Fatalf("unexpected consumer guards before replay: %d,%v", n, e)
	}
	if err = migrateSchema(ctx, dsn); err != nil {
		t.Fatal("startup during serving:", err)
	}
	if n, e := scope(); e != nil || n != 5 {
		t.Fatalf("consumer guards after replay: %d,%v", n, e)
	}
}

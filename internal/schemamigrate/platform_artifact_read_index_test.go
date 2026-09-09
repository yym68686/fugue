package schemamigrate

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPlatformArtifactReadIndexPostgres(t *testing.T) {
	dsn := os.Getenv("FUGUE_ARTIFACT_INDEX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FUGUE_ARTIFACT_INDEX_TEST_DATABASE_URL to a disposable loopback database")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/fugue_test_artifact_index") {
		t.Fatal("requires a disposable loopback fugue_test_artifact_index database")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil { t.Fatal(err) }
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `CREATE TABLE fugue_platform_artifacts (id text PRIMARY KEY, updated_at timestamptz NOT NULL, content_json jsonb NOT NULL)`); err != nil { t.Fatal(err) }
	t.Cleanup(func() { if _, err := db.Exec(`DROP TABLE fugue_platform_artifacts`); err != nil { t.Error(err) } })
	// Duplicate timestamps exercise the deterministic id tie-breaker.
	if _, err := db.ExecContext(ctx, `INSERT INTO fugue_platform_artifacts SELECT n::text, '2026-01-01'::timestamptz + (n / 3) * interval '1 second', jsonb_build_object('value',repeat(md5(n::text),40)) FROM generate_series(1,10000) n`); err != nil { t.Fatal(err) }
	const resultSQL = `SELECT md5(string_agg(id || content_json::text, ',' ORDER BY updated_at DESC, id ASC)) FROM (SELECT * FROM fugue_platform_artifacts ORDER BY updated_at DESC, id ASC LIMIT 500) selected`
	var before, after string
	if err := db.QueryRowContext(ctx, resultSQL).Scan(&before); err != nil { t.Fatal(err) }

	// Keep a writer transaction open so the online builder cannot finish its
	// first phase; concurrent updates must remain possible.
	writer, err := db.BeginTx(ctx, nil)
	if err != nil { t.Fatal(err) }
	defer writer.Rollback()
	if _, err := writer.ExecContext(ctx, `UPDATE fugue_platform_artifacts SET content_json=content_json WHERE id='1'`); err != nil { t.Fatal(err) }
	buildCtx, stopBuild := context.WithCancel(ctx)
	build := make(chan error, 1)
	go func() { build <- MigratePlatformArtifactReadIndex(buildCtx, dsn) }()
	defer stopBuild()
	waitForIndexBuild(t, ctx, db)
	if _, err := writer.ExecContext(ctx, `UPDATE fugue_platform_artifacts SET content_json=content_json WHERE id='2'`); err != nil { t.Fatalf("concurrent write blocked: %v",err) }
	stopBuild()
	if err := <-build; err == nil { t.Fatal("canceled builder succeeded") }
	if err := writer.Commit(); err != nil { t.Fatal(err) }
	// PostgreSQL leaves an invalid index after cancellation. A retry repairs
	// exactly that definition, and a completed second run retains its OID.
	valid, exists, err := inspectPlatformArtifactReadIndex(ctx, db)
	if err != nil || valid || !exists { t.Fatalf("canceled state valid=%v exists=%v error=%v",valid,exists,err) }
	if err := MigratePlatformArtifactReadIndex(ctx, dsn); err != nil { t.Fatal(err) }
	var oidBefore, oidAfter int64
	if err := db.QueryRowContext(ctx, `SELECT to_regclass($1)::oid`, platformArtifactReadIndexName).Scan(&oidBefore); err != nil { t.Fatal(err) }
	if err := MigratePlatformArtifactReadIndex(ctx, dsn); err != nil { t.Fatal(err) }
	if err := db.QueryRowContext(ctx, `SELECT to_regclass($1)::oid`, platformArtifactReadIndexName).Scan(&oidAfter); err != nil { t.Fatal(err) }
	if oidBefore != oidAfter { t.Fatal("idempotent migration rebuilt a valid index") }
	if err := db.QueryRowContext(ctx, resultSQL).Scan(&after); err != nil { t.Fatal(err) }
	if before != after { t.Fatal("index migration changed ordered data") }
	rows, err := db.QueryContext(ctx, `EXPLAIN SELECT * FROM fugue_platform_artifacts ORDER BY updated_at DESC, id ASC LIMIT 500`)
	if err != nil { t.Fatal(err) }
	var plan string
	for rows.Next() { var line string; if err := rows.Scan(&line);err!=nil { t.Fatal(err) }; plan+=line+"\n" }
	if err:=rows.Close();err!=nil {t.Fatal(err)}
	if !strings.Contains(plan, platformArtifactReadIndexName) || strings.Contains(plan,"Sort") {t.Fatalf("unexpected plan: %s",plan)}
	if _,err:=db.ExecContext(ctx,`DROP INDEX `+platformArtifactReadIndexName+`; CREATE INDEX `+platformArtifactReadIndexName+` ON fugue_platform_artifacts(id)`);err!=nil {t.Fatal(err)}
	if err:=MigratePlatformArtifactReadIndex(ctx,dsn);err==nil || !strings.Contains(err.Error(),"unexpected definition") {t.Fatalf("mismatched definition was not rejected: %v",err)}
}

func waitForIndexBuild(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	deadline:=time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err:=db.QueryRowContext(ctx,`SELECT count(*) FROM pg_stat_progress_create_index WHERE relid='fugue_platform_artifacts'::regclass`).Scan(&count);err!=nil {t.Fatal(err)}
		if count>0 {return}
		time.Sleep(10*time.Millisecond)
	}
	t.Fatal("online index builder did not reach the concurrent phase")
}

package schemamigrate

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestPlatformHistoryFKIndexesPostgres(t *testing.T) {
	dsn := os.Getenv("FUGUE_HISTORY_FK_INDEX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FUGUE_HISTORY_FK_INDEX_TEST_DATABASE_URL to a disposable loopback database")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme != "postgres" || u.Hostname() != "127.0.0.1" ||
		!strings.HasPrefix(u.Path, "/fugue_test_history_fk_index") {
		t.Fatal("requires a disposable loopback fugue_test_history_fk_index database")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exec := func(query string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	exec(`
CREATE TABLE fugue_platform_artifact_contents (content_hash text PRIMARY KEY);
CREATE TABLE fugue_platform_artifacts (id text PRIMARY KEY, content_ref_hash text);
ALTER TABLE fugue_platform_artifacts ADD FOREIGN KEY (content_ref_hash)
 REFERENCES fugue_platform_artifact_contents (content_hash) NOT VALID;
CREATE TABLE fugue_platform_lkg_snapshot_history (artifact_id text NOT NULL
 REFERENCES fugue_platform_artifacts(id) ON DELETE RESTRICT);
CREATE TABLE fugue_platform_release_messages (artifact_id text NOT NULL
 REFERENCES fugue_platform_artifacts(id) ON DELETE CASCADE);
INSERT INTO fugue_platform_artifact_contents SELECT 'body-'||i FROM generate_series(1,10000) i;
INSERT INTO fugue_platform_artifacts SELECT 'artifact-'||i,'body-'||i FROM generate_series(1,10000) i;
INSERT INTO fugue_platform_artifacts VALUES ('inline',NULL);
INSERT INTO fugue_platform_lkg_snapshot_history SELECT 'artifact-'||i FROM generate_series(1,9000) i;
INSERT INTO fugue_platform_release_messages SELECT 'artifact-'||i FROM generate_series(1,9500) i;
ANALYZE fugue_platform_artifacts;
ANALYZE fugue_platform_lkg_snapshot_history;
ANALYZE fugue_platform_release_messages;`)
	defer func() {
		_, err := db.Exec(`DROP TABLE fugue_platform_release_messages, fugue_platform_lkg_snapshot_history,
 fugue_platform_artifacts, fugue_platform_artifact_contents`)
		if err != nil {
			t.Error(err)
		}
	}()

	writer, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	if _, err := writer.ExecContext(ctx, `UPDATE fugue_platform_artifacts SET content_ref_hash=NULL WHERE id='inline'`); err != nil {
		t.Fatal(err)
	}
	buildCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- MigratePlatformHistoryFKIndexes(buildCtx, dsn) }()
	waitForIndexBuild(t, ctx, db)
	if _, err := writer.ExecContext(ctx, `INSERT INTO fugue_platform_artifacts VALUES ('other-inline',NULL)`); err != nil {
		t.Fatalf("online build blocked a writer: %v", err)
	}
	stop()
	if err := <-done; err == nil {
		t.Fatal("canceled index build unexpectedly succeeded")
	}
	if err := writer.Rollback(); err != nil {
		t.Fatal(err)
	}
	if ready, exists, err := inspectPlatformHistoryFKIndex(ctx, db, platformHistoryFKIndexes[0]); err != nil || ready || !exists {
		t.Fatalf("expected incomplete matching index: ready=%v exists=%v err=%v", ready, exists, err)
	}
	if err := MigratePlatformHistoryFKIndexes(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	oid := func(name string) (result int64) {
		t.Helper()
		if err := db.QueryRowContext(ctx, `SELECT to_regclass($1)::oid`, "public."+name).Scan(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	oids := make(map[string]int64)
	for _, index := range platformHistoryFKIndexes {
		oids[index.name] = oid(index.name)
	}
	if err := MigratePlatformHistoryFKIndexes(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	for _, index := range platformHistoryFKIndexes {
		if oid(index.name) != oids[index.name] {
			t.Fatalf("idempotent migration rebuilt %s", index.name)
		}
		var plan string
		// This is the equality/row lock used by the FK restrict check.
		q := "EXPLAIN SELECT 1 FROM ONLY public." + index.table + " WHERE " + index.column + " = 'absent' FOR KEY SHARE"
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			plan += line + "\n"
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		if !strings.Contains(plan, index.name) {
			t.Fatalf("FK lookup did not use %s: %s", index.name, plan)
		}
	}
	for _, query := range []string{
		`DELETE FROM fugue_platform_artifact_contents WHERE content_hash='body-1'`,
		`DELETE FROM fugue_platform_artifacts WHERE id='artifact-1'`,
	} {
		_, err := db.ExecContext(ctx, query)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
			t.Fatalf("foreign-key protection not preserved: %v", err)
		}
	}
	exec(`DELETE FROM fugue_platform_artifacts WHERE id='artifact-9500';
DELETE FROM fugue_platform_artifact_contents WHERE content_hash='body-9500';`)
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM fugue_platform_release_messages WHERE artifact_id='artifact-9500'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("FK cascade not preserved: count=%d err=%v", count, err)
	}

	wrong := platformHistoryFKIndexes[0].name
	exec("DROP INDEX public." + wrong + "; CREATE INDEX " + wrong + " ON public.fugue_platform_artifacts(id)")
	wrongOID := oid(wrong)
	if err := MigratePlatformHistoryFKIndexes(ctx, dsn); err == nil || !strings.Contains(err.Error(), "unexpected definition") {
		t.Fatalf("migration accepted a same-name wrong index: %v", err)
	}
	if oid(wrong) != wrongOID {
		t.Fatal("existing same-name object was replaced")
	}
}

package store

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// Dedicated, loopback-only database. Never use the production DATABASE_URL.
func TestStorageLifecyclePostgresReadOnlyPaginationAndPositiveReferences(t *testing.T) {
	dsn := os.Getenv("FUGUE_LIFECYCLE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set a disposable loopback FUGUE_LIFECYCLE_TEST_DATABASE_URL")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/fugue_test") {
		t.Fatal("only disposable loopback test databases are allowed")
	}
	db, err := openPostgresDatabase(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	schema := fmt.Sprintf("lifecycle_%d", time.Now().UnixNano())
	if _, err = db.Exec(`CREATE SCHEMA ` + schema + `; SET search_path TO ` + schema); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
	_, err = db.Exec(`
CREATE TABLE fugue_source_uploads(id text PRIMARY KEY, tenant_id text, created_at timestamptz, size_bytes bigint, archive_bytes bytea);
CREATE TABLE fugue_apps(id text PRIMARY KEY, tenant_id text, source_json jsonb);
CREATE TABLE fugue_operations(id text PRIMARY KEY, tenant_id text, desired_source_json jsonb);
CREATE TABLE fugue_app_database_import_jobs(id text PRIMARY KEY, tenant_id text, source_upload_id text);
INSERT INTO fugue_source_uploads VALUES ('a','tenant-a',now()-interval '100 days',100,'secret-body'),('b','tenant-a',now(),200,'body'),('c','tenant-a',now(),300,'body'),('d','tenant-a',now(),400,'body');
INSERT INTO fugue_apps VALUES('app','tenant-a','{"origin_source":{"upload_id":"b"}}');
INSERT INTO fugue_operations VALUES('op','tenant-a','{"desired_source":{"upload_id":"c"}}');
INSERT INTO fugue_app_database_import_jobs VALUES('job','tenant-a','d');
-- A cross-tenant identity match cannot claim a positive protected reference.
INSERT INTO fugue_apps VALUES('foreign','tenant-b','{"upload_id":"a"}');
-- A tripwire makes selecting a body fail. The audit selects metadata only.
ALTER TABLE fugue_source_uploads RENAME COLUMN archive_bytes TO body_not_in_audit;`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`ALTER TABLE fugue_source_uploads RENAME TO fixture_upload_rows;
CREATE FUNCTION require_readonly(n bigint) RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN
 IF current_setting('transaction_read_only') <> 'on' THEN RAISE EXCEPTION 'audit must be read only'; END IF;
 RETURN n;
END $$;
CREATE VIEW fugue_source_uploads AS SELECT id,tenant_id,created_at,require_readonly(size_bytes) AS size_bytes FROM fixture_upload_rows;`)
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{db: db}
	p := testLifecyclePolicy()
	now := time.Now().UTC()
	one, cursor, err := s.collectStorageLifecycleSample(t.Context(), p, "policy", "", now)
	if err != nil {
		t.Fatal(err)
	}
	if cursor != "b" || one.SourcePage.Rows != 2 || one.SourcePage.ReferencedRows != 1 || one.SourcePage.UnknownRows != 1 || one.SourcePage.OlderThan30DaysBytes != 100 || one.SourcePage.PageAtEnd {
		t.Fatalf("invalid first page: %+v cursor=%s", one.SourcePage, cursor)
	}
	two, cursor, err := s.collectStorageLifecycleSample(t.Context(), p, "policy", cursor, now)
	if err != nil || cursor != "" || two.SourcePage.ReferencedRows != 2 || !two.SourcePage.PageAtEnd || two.SourcePage.EligibleRows != 0 {
		t.Fatal(two.SourcePage, cursor, err)
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM fixture_upload_rows`).Scan(&count); err != nil || count != 4 {
		t.Fatal("audit mutated uploads", count, err)
	}
	// The tripwire also proves that a normal read-write transaction is refused.
	if err = db.QueryRow(`SELECT size_bytes FROM fugue_source_uploads LIMIT 1`).Scan(&count); err == nil {
		t.Fatal("read-only tripwire was not exercised")
	}
}

package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPlatformShadowConsumersMigrationPostgres(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable database not configured")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/fugue_test") {
		t.Fatal("disposable loopback database required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := fmt.Sprintf("shadow_migration_%d", time.Now().UnixNano())
	if _, err = db.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP SCHEMA " + schema + " CASCADE")
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	scoped := u.String()
	// Hold an unrelated serving writer open throughout migration.
	if _, err = db.Exec("CREATE TABLE " + schema + ".serving (id int primary key, value text)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("INSERT INTO " + schema + ".serving VALUES(1,'retained')"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = MigratePlatformShadowConsumers(context.Background(), scoped); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	scopedDB, err := sql.Open("pgx", scoped)
	if err != nil {
		t.Fatal(err)
	}
	defer scopedDB.Close()
	good := `{"consumer_id":"dns-server:cell-test:dns-a","artifact_kind":"dns_answer_bundle","scope_key":"authority-cell:cell-test"}`
	insert := `INSERT INTO fugue_platform_shadow_consumers(consumer_id,artifact_kind,scope_key,body) VALUES('dns-server:cell-test:dns-a','dns_answer_bundle','authority-cell:cell-test',$1)`
	if _, err = scopedDB.Exec(insert, good); err != nil {
		t.Fatal(err)
	}
	if _, err = scopedDB.Exec(insert, good); err == nil {
		t.Fatal("duplicate slot accepted")
	}
	if _, err = scopedDB.Exec(`UPDATE fugue_platform_shadow_consumers SET body=$1`, strings.Replace(good, "dns-a", "foreign", 1)); err == nil {
		t.Fatal("foreign identity accepted")
	}
	if _, err = scopedDB.Exec(`UPDATE fugue_platform_shadow_consumers SET body=$1`, strings.TrimSuffix(good, "}")+`,"oversized":"`+strings.Repeat("x", 65536)+`"}`); err == nil {
		t.Fatal("unbounded observation accepted")
	}
	var value string
	if err = scopedDB.QueryRow(`SELECT value FROM serving WHERE id=1`).Scan(&value); err != nil || value != "retained" {
		t.Fatal("serving write changed", err)
	}
}

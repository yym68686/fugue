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
	scopedDB, err := sql.Open("pgx", scoped)
	if err != nil {
		t.Fatal(err)
	}
	defer scopedDB.Close()
	// Upgrade the deployed DNS-only schema with an existing observation. The
	// fresh-table path is also exercised by the store integration fixture.
	_, err = scopedDB.Exec(`CREATE TABLE fugue_platform_shadow_consumers (
 consumer_id TEXT NOT NULL, artifact_kind TEXT NOT NULL CHECK (artifact_kind='dns_answer_bundle'),
 scope_key TEXT NOT NULL CHECK (scope_key LIKE 'authority-cell:cell-%'),
 body JSONB NOT NULL CHECK (jsonb_typeof(body)='object' AND octet_length(body::text)<=65536),
 PRIMARY KEY(consumer_id,artifact_kind,scope_key),
 CHECK(body->>'consumer_id'=consumer_id AND body->>'artifact_kind'=artifact_kind AND body->>'scope_key'=scope_key))`)
	if err != nil {
		t.Fatal(err)
	}
	good := `{"consumer_id":"dns-server:cell-test:dns-a","artifact_kind":"dns_answer_bundle","scope_key":"authority-cell:cell-test"}`
	insert := `INSERT INTO fugue_platform_shadow_consumers(consumer_id,artifact_kind,scope_key,body) VALUES('dns-server:cell-test:dns-a','dns_answer_bundle','authority-cell:cell-test',$1)`
	if _, err = scopedDB.Exec(insert, good); err != nil {
		t.Fatal(err)
	}
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
	busy, err := scopedDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Rollback()
	if _, err = busy.Exec(`LOCK TABLE fugue_platform_shadow_consumers IN ACCESS SHARE MODE`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err = MigratePlatformShadowConsumers(context.Background(), scoped); err == nil || time.Since(started) > 2*time.Second {
		t.Fatal("busy shadow table did not refuse bounded migration", err)
	}
	if err = busy.Rollback(); err != nil {
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
	var retained bool
	if err = scopedDB.QueryRow(`SELECT body=$1::jsonb FROM fugue_platform_shadow_consumers`, good).Scan(&retained); err != nil || !retained {
		t.Fatal("migration changed existing DNS observation", err)
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
	for _, kind := range []string{"edge_route_bundle", "caddy_route_config"} {
		for _, scope := range []string{"global", "authority-cell:cell-test"} {
			_, err = scopedDB.Exec(`INSERT INTO fugue_platform_shadow_consumers(consumer_id,artifact_kind,scope_key,body)
 VALUES('edge-worker:edge-a',$1,$2,jsonb_build_object('consumer_id','edge-worker:edge-a','artifact_kind',$1::text,'scope_key',$2::text))`, kind, scope)
			if err != nil {
				t.Fatal("edge slot refused", kind, scope, err)
			}
		}
	}
	if err = MigratePlatformShadowConsumers(context.Background(), scoped); err != nil {
		t.Fatal("repeat migration with edge slots failed", err)
	}
	if _, err = scopedDB.Exec(`UPDATE fugue_platform_shadow_consumers SET scope_key='global',body=jsonb_set(body,'{scope_key}','"global"') WHERE artifact_kind='dns_answer_bundle'`); err == nil {
		t.Fatal("global DNS shadow accepted")
	}
	var value string
	if err = scopedDB.QueryRow(`SELECT value FROM serving WHERE id=1`).Scan(&value); err != nil || value != "retained" {
		t.Fatal("serving write changed", err)
	}
}

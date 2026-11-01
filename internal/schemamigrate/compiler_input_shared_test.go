package schemamigrate

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCompilerInputSharedSchemaCompatibilityAndFences(t *testing.T) {
	dsn := os.Getenv("FUGUE_MEMORY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("disposable loopback only")
	}
	ctx := context.Background()
	for _, migrate := range []func(context.Context, string) error{MigrateBoundedContent, MigrateCompilerInputCodec, MigrateCompilerInputShared, MigrateCompilerInputShared, MigrateCompilerInputCodec, MigrateBoundedContent} {
		if err = migrate(ctx, dsn); err != nil {
			t.Fatal(err)
		}
	}
	db, err := openDatabase(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`INSERT INTO fugue_platform_input_manifests VALUES('sha256:'||repeat('e',64),'runtime-snapshot-shared-v3',100,262144,1,now())`); err != nil {
		t.Fatal(err)
	}
	var pack int64
	if err = tx.QueryRow(`INSERT INTO fugue_platform_input_shared_packs(pack_hash,size_bytes,stored_hash,data,created_at) VALUES(sha256('pack'::bytea),4,sha256('data'::bytea),'data'::bytea,now()) RETURNING pack_id`).Scan(&pack); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO fugue_platform_input_shared_refs VALUES('sha256:'||repeat('e',64),$1)`, pack); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`SAVEPOINT protect`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`DELETE FROM fugue_platform_input_shared_packs WHERE pack_id=$1`, pack); err == nil {
		t.Fatal("referenced pack deleted")
	}
	if _, err = tx.Exec(`ROLLBACK TO protect`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash='sha256:'||repeat('e',64); SET CONSTRAINTS ALL IMMEDIATE`); err == nil {
		t.Fatal("referenced manifest deleted")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var valid bool
	if err = db.QueryRow(`SELECT convalidated FROM pg_constraint WHERE conrelid='fugue_platform_input_manifests'::regclass AND conname='fugue_platform_input_manifests_format_check'`).Scan(&valid); err != nil || !valid {
		t.Fatal("format constraint not validated", err)
	}
	for i, ddl := range []string{
		`CREATE TABLE fugue_platform_input_shared_refs(content_hash text,pack_id bigint)`,
		`CREATE TABLE fugue_platform_input_shared_refs(content_hash text,pack_id text)`,
	} {
		// Isolated disposable schemas keep malformed-schema testing off the
		// shared test tables. Names contain only our generated numeric suffix.
		schema := fmt.Sprintf("shared_schema_test_%d_%d", time.Now().UnixNano(), i)
		if _, err = db.Exec("CREATE SCHEMA " + schema); err != nil {
			t.Fatal(err)
		}
		defer db.Exec("DROP SCHEMA " + schema + " CASCADE")
		isolated := *u
		q := isolated.Query()
		q.Set("search_path", schema)
		isolated.RawQuery = q.Encode()
		if err = MigrateBoundedContent(ctx, isolated.String()); err != nil {
			t.Fatal(err)
		}
		if err = MigrateCompilerInputCodec(ctx, isolated.String()); err != nil {
			t.Fatal(err)
		}
		bad, err := openDatabase(isolated.String())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = bad.Exec(ddl); err != nil {
			bad.Close()
			t.Fatal(err)
		}
		if err = MigrateCompilerInputShared(ctx, isolated.String()); err == nil {
			bad.Close()
			t.Fatal("malformed schema accepted")
		}
		var definition string
		if err = bad.QueryRow(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='fugue_platform_input_manifests'::regclass AND conname='fugue_platform_input_manifests_format_check'`).Scan(&definition); err != nil || strings.Contains(definition, "shared-v3") {
			bad.Close()
			t.Fatal("failed migration partially committed", err)
		}
		bad.Close()
	}
}

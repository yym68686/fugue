package schemamigrate

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestCompilerInputCodecSchemaIsIdempotentAndRollbackCompatible(t *testing.T) {
	dsn := os.Getenv("FUGUE_MEMORY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("disposable loopback only")
	}
	ctx := context.Background()
	if err = MigrateBoundedContent(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = MigrateCompilerInputCodec(ctx, dsn); err != nil {
			t.Fatal(err)
		}
	}
	// The previous schema release's migration does not contract v2 admission.
	if err = MigrateBoundedContent(ctx, dsn); err != nil {
		t.Fatal(err)
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
	if _, err = tx.Exec(`INSERT INTO fugue_platform_input_manifests VALUES('sha256:'||repeat('a',64),'runtime-snapshot-json-chunk-codec-v2',100,100,1,now())`); err != nil {
		t.Fatal(err)
	}
	// Unknown formats remain rejected; rollback removes the synthetic row.
	if _, err = tx.Exec(`INSERT INTO fugue_platform_input_manifests VALUES('sha256:'||repeat('b',64),'unknown',100,100,1,now())`); err == nil {
		t.Fatal("unknown format accepted")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var validated bool
	if err = db.QueryRow(`SELECT convalidated FROM pg_constraint WHERE conrelid='fugue_platform_input_manifests'::regclass AND conname='fugue_platform_input_manifests_format_check'`).Scan(&validated); err != nil || !validated {
		t.Fatal("constraint not validated", err)
	}
}

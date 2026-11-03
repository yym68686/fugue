package schemamigrate

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestCompilerInputSeparatedSchemaIdempotenceAndCompatibility(t *testing.T) {
	dsn := os.Getenv("FUGUE_MEMORY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("disposable loopback only")
	}
	for _, migrate := range []func(context.Context, string) error{MigrateBoundedContent, MigrateCompilerInputCodec, MigrateCompilerInputShared, MigrateCompilerInputSeparated, MigrateCompilerInputSeparated, MigrateCompilerInputCodec, MigrateCompilerInputShared, MigrateBoundedContent, MigrateCompilerInputSeparated} {
		if err := migrate(t.Context(), dsn); err != nil {
			t.Fatal(err)
		}
	}
	db, err := openDatabase(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := VerifyCompilerInputSeparated(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`ALTER TABLE fugue_platform_input_component_refs DROP CONSTRAINT fugue_platform_input_component_refs_pack_id_fkey`); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCompilerInputSeparated(t.Context(), tx); err == nil {
		t.Fatal("missing barrier accepted")
	}
}

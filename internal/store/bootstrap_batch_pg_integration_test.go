package store

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"

	"fugue/internal/schemamigrate"
)

func TestBootstrapBatchPostgresIntegration(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable PostgreSQL integration")
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
	s := &Store{db: db, databaseURL: address}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = s.applyPostgresSchemaTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, migrate := range []func(context.Context, string) error{
		schemamigrate.MigratePlatformState,
		schemamigrate.MigrateImageCacheManifestGraph,
		schemamigrate.MigrateEdgeInstanceFencing,
		schemamigrate.MigrateSourceUploadSessions,
	} {
		if err = migrate(ctx, address); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Init(); err != nil {
		t.Fatal(err)
	}
}

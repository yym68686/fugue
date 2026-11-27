package api

import (
	"database/sql"
	"path/filepath"
	"testing"

	"fugue/internal/store"
	"fugue/internal/testpostgres"
)

func openStoreTestSQL(t testing.TB, fixture string) *sql.DB {
	t.Helper()
	if fixture == "" {
		t.Fatal("SQL access requires a named store fixture")
	}
	db, err := sql.Open("pgx", testpostgres.Database(t, fixture))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newStoreTestDatabase(t testing.TB, fixture string) *store.Store {
	t.Helper()
	s := store.New(testpostgres.Database(t, fixture))
	t.Cleanup(func() { _ = s.Close() })
	if !filepath.IsAbs(fixture) {
		s.ConfigureDataBlobDir(t.TempDir())
	} else {
		s.ConfigureDataBlobDir(filepath.Join(filepath.Dir(fixture), "data-blobs"))
	}
	if err := s.Init(); err != nil {
		t.Fatal("init isolated PostgreSQL store: ", err)
	}
	s.ConfigureDatabasePool(0, 0)
	return s
}

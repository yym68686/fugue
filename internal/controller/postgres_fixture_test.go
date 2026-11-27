package controller

import (
	"database/sql"
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

func seedControllerStoreTenant(t testing.TB, fixture, tenantID string) {
	t.Helper()
	db := openStoreTestSQL(t, fixture)
	if _, err := db.Exec(`INSERT INTO fugue_tenants(id,name,slug,status,created_at,updated_at)
VALUES ($1,$1,$1,'active',now(),now())`, tenantID); err != nil {
		t.Fatal("seed tenant for synthetic identity: ", err)
	}
}

func newStoreTestDatabase(t testing.TB, fixture string) *store.Store {
	t.Helper()
	s := store.New(testpostgres.Database(t, fixture))
	t.Cleanup(func() { _ = s.Close() })
	s.ConfigureDataBlobDir(t.TempDir())
	if err := s.Init(); err != nil {
		t.Fatal("init isolated PostgreSQL store: ", err)
	}
	s.ConfigureDatabasePool(0, 0)
	return s
}

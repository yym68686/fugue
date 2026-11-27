package cli

import (
	"testing"

	"fugue/internal/store"
	"fugue/internal/testpostgres"
)

func newStoreTestDatabase(t testing.TB, fixture string) *store.Store {
	t.Helper()
	s := store.New(testpostgres.Database(t, fixture))
	t.Cleanup(func() { _ = s.Close() })
	s.ConfigureDataBlobDir(t.TempDir())
	if err := s.Init(); err != nil {
		t.Fatal("init isolated PostgreSQL store: ", err)
	}
	return s
}

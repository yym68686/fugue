package store

import (
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/testpostgres"
)

func TestStoreRequiresPostgreSQL(t *testing.T) {
	s := New("")
	for name, err := range map[string]error{"startup": s.Init(), "readiness": s.CheckReadiness(t.Context())} {
		if err == nil || !strings.Contains(err.Error(), "FUGUE_DATABASE_URL") {
			t.Errorf("%s without PostgreSQL: %v", name, err)
		}
	}
}

func newStoreTestDatabase(t testing.TB, fixture string) *Store {
	t.Helper()
	s := New(testpostgres.Database(t, fixture))
	t.Cleanup(func() { _ = s.Close() })
	if !filepath.IsAbs(fixture) {
		s.ConfigureDataBlobDir(t.TempDir())
	} else {
		s.ConfigureDataBlobDir(filepath.Join(filepath.Dir(fixture), "data-blobs"))
	}
	if err := s.Init(); err != nil {
		t.Fatal("init isolated PostgreSQL store: ", err)
	}
	return s
}

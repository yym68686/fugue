package store

import (
	"testing"

	"fugue/internal/model"
)

func TestNormalizeManagedPostgresSpecUsesDNS1035ServiceName(t *testing.T) {
	t.Parallel()

	spec := normalizeManagedPostgresSpec("001-demo", "runtime_demo", model.AppPostgresSpec{
		Password: "secret",
	})

	if got := spec.ServiceName; got != "postgres-001-demo-postgres" {
		t.Fatalf("expected DNS-1035 postgres service name, got %q", got)
	}
	if got := defaultPostgresBindingEnv(spec)["DB_HOST"]; got != "postgres-001-demo-postgres" {
		t.Fatalf("expected DB_HOST to use DNS-1035 postgres service name, got %q", got)
	}
}

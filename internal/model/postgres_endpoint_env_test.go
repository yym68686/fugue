package model

import (
	"reflect"
	"testing"
)

func TestRebindPostgresEndpointEnvironmentPreservesConnectionContract(t *testing.T) {
	pg := AppPostgresSpec{ServiceName: "restored-db", EndpointServiceName: "original-db", User: "managed", Password: "not-the-url-password", Database: "managed"}
	for _, tc := range []struct{ name, value, want string }{
		{"legacy_rw", "postgresql://owner:p%40ss@original-db-rw:6432/other?sslmode=require&options=-c%20x%3Dy", "postgresql://owner:p%40ss@original-db:6432/other?sslmode=require&options=-c%20x%3Dy"},
		{"physical_rw", "postgres://owner:fixture@restored-db-rw/db", "postgres://owner:fixture@original-db/db"},
		{"adapter_and_fqdn", "postgresql+asyncpg://owner:fixture@original-db-rw.tenant-demo.svc.cluster.local.:5432/db?application_name=a", "postgresql+asyncpg://owner:fixture@original-db.tenant-demo.svc.cluster.local.:5432/db?application_name=a"},
		{"qualified", "postgres://owner@restored-db.tenant-demo.svc/db", "postgres://owner@original-db.tenant-demo.svc/db"},
		{"already_stable", "postgres://owner:fixture@original-db:5432/db", "postgres://owner:fixture@original-db:5432/db"},
		{"external", "postgres://owner:fixture@external.example:5432/db", "postgres://owner:fixture@external.example:5432/db"},
		{"other_namespace", "postgres://owner@original-db-rw.other.svc.cluster.local/db", "postgres://owner@original-db-rw.other.svc.cluster.local/db"},
		{"lookalike", "postgres://owner@original-db-rw.example/db", "postgres://owner@original-db-rw.example/db"},
		{"other_scheme", "https://owner@original-db-rw/db", "https://owner@original-db-rw/db"},
		{"invalid", "postgres://%zz", "postgres://%zz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"DATABASE_URL": tc.value, "KEEP": "unchanged"}
			got := RebindPostgresEndpointEnvironment(env, pg, "tenant-demo")
			if got["DATABASE_URL"] != tc.want || got["KEEP"] != "unchanged" {
				t.Fatal("connection host rebinding changed more than the managed host")
			}
			if env["DATABASE_URL"] != tc.value {
				t.Fatal("persisted environment was mutated")
			}
			if again := RebindPostgresEndpointEnvironment(got, pg, "tenant-demo"); !reflect.DeepEqual(again, got) {
				t.Fatal("rebinding is not idempotent")
			}
		})
	}
	env := map[string]string{"PGHOST": "original-db-rw.tenant-demo", "DB_HOST": "restored-db:6543", "DB_URL": "postgres://owner@restored-db/db", "DATABASE_HOST": "external.example"}
	got := RebindPostgresEndpointEnvironment(env, pg, "tenant-demo")
	if got["PGHOST"] != "original-db.tenant-demo" || got["DB_HOST"] != "original-db:6543" || got["DB_URL"] != "postgres://owner@original-db/db" || got["DATABASE_HOST"] != env["DATABASE_HOST"] {
		t.Fatal("host env or alternate URL not rebound safely")
	}
	pg.EndpointServiceName = ""
	if got := RebindPostgresEndpointEnvironment(env, pg, "tenant-demo"); !reflect.DeepEqual(got, env) {
		t.Fatal("ordinary managed cluster environment changed")
	}
}

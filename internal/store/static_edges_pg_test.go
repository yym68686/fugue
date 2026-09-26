package store

import (
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"fugue/internal/model"
)

// Run only against a dedicated empty test database. This exercises the real
// PostgreSQL constraints and locking, rather than a SQL-shape mock.
func TestStaticEdgeRegistrationPostgresLifecycle(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_STATIC_EDGE_DATABASE_URL")
	if dsn == "" {
		t.Skip("dedicated PostgreSQL database not configured")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM pg_tables WHERE schemaname=current_schema()`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("test requires an empty database: tables=%d err=%v", count, err)
	}
	for _, statement := range postgresSchemaStatements {
		if strings.HasPrefix(statement, "CREATE TABLE IF NOT EXISTS fugue_tenants (") || strings.HasPrefix(statement, "CREATE TABLE IF NOT EXISTS fugue_projects (") || strings.Contains(statement, "fugue_static_edge_registrations") {
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.Exec(`INSERT INTO fugue_tenants (id,name,slug,status,created_at,updated_at) VALUES ('owner','Owner','owner','active',now(),now()), ('other','Other','other','active',now(),now()); INSERT INTO fugue_projects (id,tenant_id,name,slug,description,created_at,updated_at) VALUES ('project','owner','Project','project','',now(),now()), ('foreign','other','Foreign','foreign','',now(),now())`); err != nil {
		t.Fatal(err)
	}
	s := &Store{databaseURL: dsn, db: db, dbReady: true}
	filter := StaticEdgeRegistrationFilter{TenantID: "owner"}
	listed, err := s.ListStaticEdgeRegistrations(filter)
	if err != nil || listed == nil || len(listed) != 0 {
		t.Fatalf("empty list: %+v %v", listed, err)
	}
	in := model.StaticEdgeRegistration{TenantID: "owner", ProjectID: "project", Name: "Sample", EdgeID: "edge-one", Transport: "mtls", SigningKeyID: "key", PossessionProofDigest: "sha256:" + strings.Repeat("a", 64)}
	bad := in
	bad.ProjectID = "foreign"
	if _, err := s.CreateStaticEdgeRegistration(bad); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant project accepted: %v", err)
	}
	created, err := s.CreateStaticEdgeRegistration(in)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := in
	duplicate.Name, duplicate.EdgeID = "sample", "edge-two"
	if _, err := s.CreateStaticEdgeRegistration(duplicate); !errors.Is(err, ErrConflict) {
		t.Fatalf("case-insensitive duplicate accepted: %v", err)
	}
	updated, err := s.UpdateStaticEdgeRegistrationProof(created.ID, "owner", in.PossessionProofDigest, "key-two", true)
	if err != nil || updated.Status != model.StaticEdgeStatusReady || updated.LastProofAt == nil {
		t.Fatalf("ready update: %+v %v", updated, err)
	}
	if _, err := s.UpdateStaticEdgeRegistrationProof(created.ID, "other", in.PossessionProofDigest, "key", true); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign proof accepted: %v", err)
	}
	// Whichever concurrent writer wins first, revoke is terminal.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := s.RevokeStaticEdgeRegistration(created.ID, "owner", false); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		defer wg.Done()
		if _, err := s.UpdateStaticEdgeRegistrationProof(created.ID, "owner", in.PossessionProofDigest, "key", true); err != nil && !errors.Is(err, ErrConflict) {
			t.Error(err)
		}
	}()
	wg.Wait()
	if _, err := s.UpdateStaticEdgeRegistrationProof(created.ID, "owner", in.PossessionProofDigest, "key", true); !errors.Is(err, ErrConflict) {
		t.Fatalf("revoked proof must conflict: %v", err)
	}
	final, err := s.GetStaticEdgeRegistration(created.ID, "owner", false)
	if err != nil || final.Status != model.StaticEdgeStatusRevoked {
		t.Fatalf("revoke lost: %+v %v", final, err)
	}
}

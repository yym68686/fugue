package store

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestPendingResourceObservationPostgresReadOnlyIdentityAndNoBootstrap(t *testing.T) {
	s := pendingPolicyFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	before, err := s.ReadPendingRootDatabaseResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.DatabaseOID == 0 || before.Schema == "" || before.ServerAddress != "127.0.0.1" || before.ServerPort == 0 || before.PostmasterStarted.IsZero() || before.DataDirectory == "" || !before.ReadOnly || !before.TempFilesForbidden || before.InRecovery || len(before.LedgerRelations) != 4 {
		t.Fatal("missing actual source identity", before)
	}
	for _, r := range before.LedgerRelations {
		if r.OID != 0 || r.Filenode != 0 || r.AllocatedBytes != 0 {
			t.Fatal("observation installed ledger", r)
		}
	}
	// Install only in this disposable schema, then verify that real catalog
	// identities and allocation are exposed without inserting a pool/reservation.
	if _, err := s.db.Exec(schemamigrate.CompilerInputResourceReservationsSQL); err != nil {
		t.Fatal(err)
	}
	after, err := s.ReadPendingRootDatabaseResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range after.LedgerRelations {
		if r.OID == 0 || r.Filenode == 0 || r.AllocatedBytes == 0 {
			t.Fatal("existing ledger unmeasured", r)
		}
	}
	for _, table := range []string{"fugue_platform_input_resource_pool", "fugue_platform_input_resource_reservations", "fugue_platform_input_resource_outcomes"} {
		var count int
		if err := s.db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("observation wrote ledger", table, count, err)
		}
	}
}

func TestPendingResourceObservationPostgresPoolWaitIsCanceled(t *testing.T) {
	s := pendingPolicyFixture(t)
	s.db.SetMaxOpenConns(1)
	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	_, err = s.ReadPendingRootDatabaseResources(ctx)
	cancel()
	if err == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatal("pool acquisition ignored operation deadline", err, ctx.Err())
	}
	_ = conn.Close()
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := s.db.PingContext(ctx); err != nil {
		t.Fatal("canceled acquisition leaked connection", err)
	}
}

func TestPendingResourceObservationPostgresOrdinaryRoleDoesNotNeedSettingsGrant(t *testing.T) {
	s := pendingPolicyFixture(t)
	var schema string
	if err := s.db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	name := model.NewID("resource_observer_test")
	role := quotePostgresIntegrationIdentifier(t, name)
	if _, err := s.db.Exec(`CREATE ROLE ` + role + ` LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE; GRANT USAGE ON SCHEMA ` + quotePostgresIntegrationIdentifier(t, schema) + ` TO ` + role); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, q := range []string{`DROP OWNED BY ` + role, `DROP ROLE ` + role} {
			if _, err := s.db.Exec(q); err != nil {
				t.Error(err)
			}
		}
	})
	u, err := url.Parse(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.User(name)
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	value, err := (&Store{db: db, databaseURL: u.String()}).ReadPendingRootDatabaseResources(ctx)
	if err != nil {
		t.Fatal("ordinary role cannot read actual resources", err)
	}
	if value.DataDirectory != "" || value.DatabaseOID == 0 || !strings.HasPrefix(value.Schema, "hot_inventory_test_") || !value.ReadOnly || !value.TempFilesForbidden {
		t.Fatal("hidden setting or actual identity mishandled", value)
	}
}

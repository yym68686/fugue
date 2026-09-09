package store

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type billingPrepareCapture struct {
	postgresReadTracer
	queries map[string]bool
}

func (capture *billingPrepareCapture) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	if ctx.Value(readStageObserverKey{}) != nil {
		capture.queries[data.SQL] = true
	}
	return capture.postgresReadTracer.TracePrepareStart(ctx, conn, data)
}

func TestPreparedBillingStatementsPreserveResultsWithoutForegroundPreparation(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant := billingBatchPGTenant(t, s)
	config, err := pgx.ParseConfig(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	capture := &billingPrepareCapture{queries: map[string]bool{}}
	config.Tracer = capture
	firstDB := stdlib.OpenDB(*config)
	defer firstDB.Close()
	firstStore := &Store{databaseURL: s.databaseURL, db: firstDB}
	ctx := WithReadStageObserver(context.Background(), func(string, time.Duration) {})
	first, err := firstStore.GetTenantBillingSnapshot(ctx, []string{tenant.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(capture.queries) < 3 {
		t.Fatalf("expected input, locking and persistence preparations; got %d", len(capture.queries))
	}
	secondDB, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer secondDB.Close()
	secondDB.SetMaxOpenConns(1)
	secondStore := &Store{databaseURL: s.databaseURL, db: secondDB}
	var beforeWarm time.Time
	if err := s.db.QueryRow("SELECT last_accrued_at FROM fugue_tenant_billing WHERE tenant_id = $1", tenant.ID).Scan(&beforeWarm); err != nil {
		t.Fatal(err)
	}
	if err := secondStore.WarmBillingStatements(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	var lastAccruedAt time.Time
	if err := s.db.QueryRow("SELECT last_accrued_at FROM fugue_tenant_billing WHERE tenant_id = $1", tenant.ID).Scan(&lastAccruedAt); err != nil {
		t.Fatal(err)
	}
	if !lastAccruedAt.Equal(beforeWarm) {
		t.Fatal("preparation executed billing accrual")
	}
	stages := map[string]time.Duration{}
	second, err := secondStore.GetTenantBillingSnapshot(WithReadStageObserver(context.Background(), func(name string, duration time.Duration) { stages[name] += duration }), []string{tenant.ID})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"billing_inputs_prepare", "billing_locks_prepare", "billing_persist_prepare"} {
		if _, ok := stages[name]; ok {
			t.Fatalf("foreground still prepared %s", name)
		}
	}
	first.Billings[0].LastAccruedAt = time.Time{}
	first.Billings[0].UpdatedAt = time.Time{}
	second.Billings[0].LastAccruedAt = time.Time{}
	second.Billings[0].UpdatedAt = time.Time{}
	if !reflect.DeepEqual(first.Billings, second.Billings) {
		t.Fatal("preparation changed billing results")
	}
}

func TestBillingStatementWarmDistinctConnectionsAndReplacement(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	db, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxIdleConns(3)
	db.SetMaxOpenConns(3)
	warmStore := &Store{db: db}
	for round := 0; round < 2; round++ {
		if err := warmStore.WarmBillingStatements(context.Background(), 3); err != nil {
			t.Fatal(err)
		}
		var held []*sql.Conn
		for i := 0; i < 3; i++ {
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, conn)
			var count int
			err = conn.QueryRowContext(context.Background(), "SELECT count(*) FROM pg_prepared_statements WHERE statement = ANY($1::text[])", []string{billingSnapshotInputsSQL, billingSnapshotLocksSQL, billingSnapshotPersistSQL, billingSnapshotEventsSQL}).Scan(&count)
			if err != nil || count != 4 {
				t.Errorf("connection %d: prepared=%d error=%v", i, count, err)
			}
		}
		for _, conn := range held {
			_ = conn.Close()
		}
		// Replace all physical connections, as happens across a DB switch.
		db.SetMaxIdleConns(0)
		db.SetMaxIdleConns(3)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := warmStore.WarmBillingStatements(ctx, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if stats := db.Stats(); stats.InUse != 0 {
		t.Fatalf("connections leaked: %+v", stats)
	}
	if err := warmStore.WarmBillingStatements(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats.OpenConnections != 0 {
		t.Fatalf("disabled warmer opened connections: %+v", stats)
	}
}

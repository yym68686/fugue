package store

import (
	"context"
	"testing"
	"time"
)

func TestBillingReadTimingRecordsActualTransactionStages(t *testing.T) {
	s := billingBatchPGStore(t)
	db, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s.db = db
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant := billingBatchPGTenant(t, s)
	stages := map[string]time.Duration{}
	ctx := WithReadStageObserver(context.Background(), func(name string, duration time.Duration) { stages[name] += duration })
	snapshot, err := s.GetTenantBillingSnapshot(ctx, []string{tenant.ID})
	if err != nil || len(snapshot.Billings) != 1 || snapshot.Billings[0].TenantID != tenant.ID {
		t.Fatalf("billing snapshot failed: %v", err)
	}
	for _, name := range []string{"billing_begin", "billing_inputs", "billing_owners", "billing_locks", "billing_accrue", "billing_persist", "billing_commit"} {
		if _, ok := stages[name]; !ok {
			t.Fatalf("missing transaction stage %s", name)
		}
	}
	for _, name := range []string{"billing_inputs_prepare", "billing_inputs_query", "billing_locks_prepare", "billing_persist_query"} {
		if _, ok := stages[name]; !ok {
			t.Fatalf("driver omitted stage %s", name)
		}
	}
}

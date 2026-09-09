package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func billingBatchPGStore(t *testing.T) *Store {
	t.Helper()
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable PostgreSQL integration")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires a disposable loopback fugue_test database")
	}
	db, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return &Store{db: db, databaseURL: address}
}

func billingBatchPGTenant(t *testing.T, s *Store) model.Tenant {
	t.Helper()
	tenant, err := s.CreateTenant(model.NewID("billing_batch_test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.db.Exec(`DELETE FROM fugue_tenants WHERE id = $1`, tenant.ID); err != nil {
			t.Error(err)
		}
	})
	return tenant
}

func TestBillingBatchPostgresPreservesAccrualAndCredits(t *testing.T) {
	s := billingBatchPGStore(t)
	ctx := context.Background()
	owner := billingBatchPGTenant(t, s)
	consumer := billingBatchPGTenant(t, s)
	idle := billingBatchPGTenant(t, s)
	runtime, _, err := s.CreateRuntime(owner.ID, model.NewID("public"), model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	offer, err := normalizeRuntimePublicOffer(model.RuntimePublicOffer{
		ReferenceBundle:                 model.BillingResourceSpec{CPUMilliCores: 2000, MemoryMebibytes: 4096, StorageGibibytes: 30},
		ReferenceMonthlyPriceMicroCents: 400 * microCentsPerCent,
	})
	if err != nil {
		t.Fatal(err)
	}
	offerJSON, _ := json.Marshal(offer)
	if _, err := s.db.Exec(`UPDATE fugue_runtimes SET access_mode = 'public', public_offer_json = $2 WHERE id = $1`, runtime.ID, offerJSON); err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(consumer.ID, "billing", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(consumer.ID, project.ID, "public-app", "", model.AppSpec{
		Image: "registry.example/app:v1", Ports: []int{8080}, Replicas: 2, RuntimeID: runtime.ID,
		Resources: &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 512},
	})
	if err != nil {
		t.Fatal(err)
	}
	app.Status.CurrentRuntimeID = runtime.ID
	app.Status.CurrentReplicas = 2
	status, _ := json.Marshal(app.Status)
	if _, err := s.db.Exec(`UPDATE fugue_apps SET status_json = $2 WHERE id = $1`, app.ID, status); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	seed := make([]model.TenantBilling, 0, 3)
	for _, tenant := range []model.Tenant{owner, consumer, idle} {
		record := defaultTenantBilling(tenant.ID, stale)
		record.ManagedCap = model.BillingResourceSpec{}
		record.BalanceMicroCents = 10_000 * microCentsPerCent
		seed = append(seed, record)
		if err := s.pgUpdateTenantBillingRecordTx(ctx, tx, record); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 16; i++ {
		event := newTenantBillingBalanceAdjustedEvent(consumer.ID, 0, seed[1].BalanceMicroCents, stale.Add(time.Duration(i)*time.Second), nil)
		if err := s.pgInsertTenantBillingEventTx(ctx, tx, event); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result, missing, err := s.GetTenantBillingSummaries(ctx, []string{owner.ID, consumer.ID, "absent", idle.ID, consumer.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("three-tenant complete billing transaction: %s", time.Since(started))
	if len(result) != 3 || !reflect.DeepEqual(missing, []string{"absent"}) || result[0].TenantID != owner.ID || result[1].TenantID != consumer.ID {
		t.Fatalf("incorrect coverage/order: %+v missing=%v", result, missing)
	}
	transfer := result[1].HourlyRateMicroCents * result[1].LastAccruedAt.Sub(stale).Nanoseconds() / int64(time.Hour)
	if transfer <= 0 || result[1].BalanceMicroCents != seed[1].BalanceMicroCents-transfer || result[0].BalanceMicroCents != seed[0].BalanceMicroCents+transfer {
		t.Fatalf("unbalanced public runtime transfer %d: owner=%d consumer=%d", transfer, result[0].BalanceMicroCents, result[1].BalanceMicroCents)
	}
	if len(result[1].Events) != billingHistoryLimit || result[1].Events[0].Type != model.BillingEventTypePublicRuntimeDebit || result[0].Events[0].Type != model.BillingEventTypePublicRuntimeCredit {
		t.Fatalf("missing/truncated events: owner=%+v consumer=%+v", result[0].Events, result[1].Events)
	}
	var persisted int64
	if err := s.db.QueryRow(`SELECT balance_microcents FROM fugue_tenant_billing WHERE tenant_id = $1`, owner.ID).Scan(&persisted); err != nil || persisted != result[0].BalanceMicroCents {
		t.Fatalf("owner credit not committed: %d %v", persisted, err)
	}
	// Compare with the original PostgreSQL accrual path at exactly the same
	// instant, restoring the pre-call ledgers only within a rolled-back test tx.
	tx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, record := range seed {
		if err := s.pgUpdateTenantBillingRecordTx(ctx, tx, record); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(`DELETE FROM fugue_billing_events WHERE tenant_id = ANY($1::text[]) AND type IN ('public-runtime-debit', 'public-runtime-credit')`, []string{consumer.ID, owner.ID}); err != nil {
		t.Fatal(err)
	}
	record, state, index, err := s.pgAccrueTenantBillingTx(ctx, tx, consumer.ID, result[1].LastAccruedAt)
	if err != nil {
		t.Fatal(err)
	}
	legacy := buildTenantBillingSummaryWithIndex(&state, index, record)
	actual := result[1]
	for i := range legacy.Events {
		if legacy.Events[i].Type == model.BillingEventTypePublicRuntimeDebit {
			legacy.Events[i].ID = actual.Events[i].ID
		}
	}
	if !reflect.DeepEqual(legacy, actual) {
		t.Fatalf("batch differs from original accrual:\nlegacy=%+v\nbatch=%+v", legacy, actual)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	// A failed event write must roll back the earlier bulk ledger update.
	tx, _ = s.db.BeginTx(ctx, nil)
	defer tx.Rollback()
	corrupt := seed[0]
	corrupt.BalanceMicroCents = -99
	err = s.pgPersistBillingBatchTx(ctx, tx, []model.TenantBilling{corrupt}, []model.TenantBillingEvent{{ID: model.NewID("bad_event"), TenantID: "absent", Type: "top-up", CreatedAt: time.Now()}})
	if err == nil {
		t.Fatal("expected FK failure")
	}
	tx.Rollback()
	if err := s.db.QueryRow(`SELECT balance_microcents FROM fugue_tenant_billing WHERE tenant_id = $1`, owner.ID).Scan(&persisted); err != nil || persisted != result[0].BalanceMicroCents {
		t.Fatalf("failed transaction changed owner balance: %d %v", persisted, err)
	}
}

func TestBillingBatchPostgresConcurrentTopupsAndBoundedRead(t *testing.T) {
	s := billingBatchPGStore(t)
	ctx := context.Background()
	tenant := billingBatchPGTenant(t, s)
	baseline, err := s.GetTenantBillingSummary(tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errors := make(chan error, 20)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.TopUpTenantBilling(tenant.ID, 100, fmt.Sprintf("concurrent-%d", i))
			errors <- err
		}(i)
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := s.GetTenantBillingSummaries(ctx, []string{tenant.ID})
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	after, _, err := s.GetTenantBillingSummaries(ctx, []string{tenant.ID})
	if err != nil || after[0].BalanceMicroCents != baseline.BalanceMicroCents+800*microCentsPerCent {
		t.Fatalf("lost concurrent credit: %+v %v", after, err)
	}
	ids := []string{tenant.ID}
	for i := 0; i < 105; i++ {
		ids = append(ids, billingBatchPGTenant(t, s).ID)
	}
	start := time.Now()
	result, missing, err := s.GetTenantBillingSummaries(ctx, ids)
	if err != nil || len(result) != len(ids) || len(missing) != 0 {
		t.Fatalf("incomplete batch: %d %v %v", len(result), missing, err)
	}
	t.Logf("106-tenant complete billing transaction: %s", time.Since(start))
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := s.GetTenantBillingSummaries(cancelled, ids); err == nil {
		t.Fatal("cancelled request succeeded")
	}
}

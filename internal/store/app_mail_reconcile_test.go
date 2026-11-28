package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPostgresAppMailActivationRefusesUnsettledWorkAndKeepsCutover(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	ctx := t.Context()
	raw, err := os.ReadFile("../schemamigrate/app_mail_reconcile_activate.sql")
	if err != nil {
		t.Fatal(err)
	}
	activate := func() error {
		conn, err := s.db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = conn.ExecContext(ctx, string(raw))
		if err != nil {
			if _, rollbackErr := conn.ExecContext(ctx, "ROLLBACK"); rollbackErr != nil {
				t.Fatal(rollbackErr)
			}
		}
		return err
	}
	assertRefused := func(reason string) {
		t.Helper()
		if err := activate(); err == nil || !strings.Contains(err.Error(), reason) {
			t.Fatalf("handoff should fail with %q, got %v", reason, err)
		}
		var present bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_meta WHERE key=$1)`, appMailReconcileOwnerKey).Scan(&present); err != nil || present {
			t.Fatalf("refused handoff changed owner: %t %v", present, err)
		}
	}
	assertRefused("does not cover all current generations")
	if err := s.SaveMailPlatformConfig(model.MailPlatformConfig{MailflareAppID: "provider", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SaveAppMailBinding(model.AppMailBinding{AppID: "app", TenantID: "tenant", ProjectID: "project", Hostname: "app.apps.example.test", LocalPart: "support", State: "active"}, 0); err != nil {
		t.Fatal(err)
	}
	if ctl, err := s.PrepareAppMailReconciliation(ctx); err != nil || !ctl.Seeded || ctl.Owner != "api" {
		t.Fatalf("could not prepare schedule: %+v %v", ctl, err)
	}
	if ran, err := s.WithLegacyAppMailReconciliation(ctx, false, func() error {
		assertRefused("API writer has not drained")
		return nil
	}); err != nil || !ran {
		t.Fatalf("could not hold foreground writer gate: %t %v", ran, err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM fugue_app_mail_reconcile WHERE app_id='app'`); err != nil {
		t.Fatal(err)
	}
	assertRefused("does not cover all current generations")
	if _, err := s.SeedAppMailReconciliation(ctx, "", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE fugue_app_mail_bindings DISABLE TRIGGER fugue_app_mail_reconcile_enqueue`); err != nil {
		t.Fatal(err)
	}
	assertRefused("generation trigger is not enabled")
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE fugue_app_mail_bindings ENABLE TRIGGER fugue_app_mail_reconcile_enqueue`); err != nil {
		t.Fatal(err)
	}
	cutover := func() time.Time {
		t.Helper()
		var at time.Time
		if err := s.db.QueryRowContext(ctx, `SELECT updated_at FROM fugue_meta WHERE key=$1 AND value='controller'`, appMailReconcileOwnerKey).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	if err := activate(); err != nil {
		t.Fatal("safe handoff refused:", err)
	}
	start := cutover()
	if err := activate(); err != nil || !cutover().Equal(start) {
		t.Fatalf("replay changed activation: %v", err)
	}
	if ctl, err := s.PrepareAppMailReconciliation(ctx); err != nil || ctl.Owner != "controller" {
		t.Fatalf("controller cannot see handoff: %+v %v", ctl, err)
	}
	if ran, err := s.WithLegacyAppMailReconciliation(ctx, false, func() error {
		t.Fatal("legacy API sent a provider PUT after transfer")
		return nil
	}); err != nil || ran {
		t.Fatalf("legacy API was not fenced: %t %v", ran, err)
	}
}

func TestPostgresAppMailStandbyResumesSeedingAndRefusesMissingTrigger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mail-standby")
	ctx := t.Context()
	first := newStoreTestDatabase(t, path)
	if _, err := first.db.ExecContext(ctx, `
		ALTER TABLE fugue_app_mail_bindings DISABLE TRIGGER fugue_app_mail_reconcile_enqueue;
		INSERT INTO fugue_app_mail_bindings(app_id,generation,body)
			SELECT 'app-'||lpad(n::text,3,'0'),1,'{}'::jsonb FROM generate_series(1,101) n;
		ALTER TABLE fugue_app_mail_bindings ENABLE TRIGGER fugue_app_mail_reconcile_enqueue
	`); err != nil {
		t.Fatal(err)
	}
	if ctl, err := first.PrepareAppMailReconciliation(ctx); err != nil || ctl.Seeded || ctl.Owner != "api" {
		t.Fatalf("first standby page: %+v %v", ctl, err)
	}
	if _, err := first.GetAppMailReconciliation(ctx, "app-101"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("seed escaped the page bound: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := newStoreTestDatabase(t, path)
	if ctl, err := restarted.PrepareAppMailReconciliation(ctx); err != nil || !ctl.Seeded || ctl.Owner != "api" {
		t.Fatalf("restart failed to resume seed cursor: %+v %v", ctl, err)
	}
	if _, err := restarted.GetAppMailReconciliation(ctx, "app-101"); err != nil {
		t.Fatal("final binding missing:", err)
	}
	if lease, claimed, err := restarted.ClaimAppMailReconciliation(ctx); claimed || err != nil {
		t.Fatalf("standby acquired an execution lease: %+v %t %v", lease, claimed, err)
	}
	if _, err := restarted.db.ExecContext(ctx, `ALTER TABLE fugue_app_mail_bindings DISABLE TRIGGER fugue_app_mail_reconcile_enqueue`); err != nil {
		t.Fatal(err)
	}
	if ctl, err := restarted.PrepareAppMailReconciliation(ctx); err == nil || ctl.Seeded {
		t.Fatalf("missing enqueue fence did not disable readiness: %+v %v", ctl, err)
	}
}

func TestPostgresAppMailLegacyWriterSharesHandoffFence(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	ctx := t.Context()
	if ok, err := s.WithAdvisoryLock(ctx, appMailLegacyPeriodicLock, func() error {
		ran, err := s.WithLegacyAppMailReconciliation(ctx, true, func() error {
			t.Fatal("new timer overlapped the retired version's lock")
			return nil
		})
		if ran || err != nil {
			t.Fatalf("periodic contention: ran=%t err=%v", ran, err)
		}
		return nil
	}); !ok || err != nil {
		t.Fatalf("legacy timer setup: %t %v", ok, err)
	}
	request, cancel := context.WithCancel(ctx)
	defer cancel()
	if ran, err := s.WithLegacyAppMailReconciliation(request, false, func() error {
		cancel()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var exclusive bool
		if err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, appMailReconcileOwnerKey).Scan(&exclusive); err != nil {
			return err
		}
		if exclusive {
			t.Fatal("disconnect released the fence while the callback was still running")
		}
		return nil
	}); !ran || err != nil {
		t.Fatalf("legacy foreground request: %t %v", ran, err)
	}
	takeover, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer takeover.Rollback()
	var exclusive bool
	if err = takeover.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, appMailReconcileOwnerKey).Scan(&exclusive); err != nil || !exclusive {
		t.Fatalf("drained legacy request still held fence: %t %v", exclusive, err)
	}
	if _, err = takeover.ExecContext(ctx, `INSERT INTO fugue_meta(key,value) VALUES($1,'controller')`, appMailReconcileOwnerKey); err != nil {
		t.Fatal(err)
	}
	if err = takeover.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, periodic := range []bool{false, true} {
		if ran, err := s.WithLegacyAppMailReconciliation(ctx, periodic, func() error {
			t.Fatalf("legacy writer ran after takeover (periodic=%t)", periodic)
			return nil
		}); ran || err != nil {
			t.Fatalf("owner gate: ran=%t err=%v", ran, err)
		}
	}
}

func TestPostgresAppMailReconciliationGenerationLeaseAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mail-reconcile")
	api := newStoreTestDatabase(t, path)
	ctx := t.Context()
	mustExec := func(query string, args ...any) {
		t.Helper()
		if _, err := api.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := api.SaveMailPlatformConfig(model.MailPlatformConfig{MailflareAppID: "provider", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	binding, err := api.SaveAppMailBinding(model.AppMailBinding{AppID: "app", TenantID: "tenant", ProjectID: "project", Hostname: "app.apps.example.test", LocalPart: "support", State: "active"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	status, err := api.GetAppMailReconciliation(ctx, binding.AppID)
	if err != nil || status.Generation != 1 || status.LastResult != "" {
		t.Fatalf("intent/schedule not atomically visible: %+v %v", status, err)
	}
	assertNoClaim := func(s *Store) {
		t.Helper()
		if lease, ok, err := s.ClaimAppMailReconciliation(ctx); ok || err != nil {
			t.Fatalf("unexpected claim: %+v %t %v", lease, ok, err)
		}
	}
	assertNoClaim(api) // no owner: a standby release may not apply effects
	mustExec(`INSERT INTO fugue_meta(key,value) VALUES($1,'api')`, appMailReconcileOwnerKey)
	assertNoClaim(api)
	mustExec(`UPDATE fugue_meta SET value='controller' WHERE key=$1`, appMailReconcileOwnerKey)

	worker := newStoreTestDatabase(t, path)
	claim := func(s *Store, generation uint64) AppMailReconcileLease {
		t.Helper()
		l, ok, err := s.ClaimAppMailReconciliation(ctx)
		if !ok || err != nil || l.ID == "" || l.Binding.Generation != generation {
			t.Fatalf("expected generation %d, got %+v %t %v", generation, l, ok, err)
		}
		return l
	}
	expire := func() {
		t.Helper()
		mustExec(`UPDATE fugue_app_mail_reconcile SET lease_expires_at=statement_timestamp()-interval '1 second' WHERE app_id=$1`, binding.AppID)
	}
	first := claim(worker, 1)
	assertNoClaim(api)
	dns := []model.AppMailDNSRecord{{Name: "@", Type: "MX", Values: []string{"10 mx.example.test."}, TTL: 300}}
	wrong := first
	wrong.ID = "not-the-lease-owner"
	if err = api.SaveAppMailReconciliationDNS(ctx, wrong, dns); !errors.Is(err, ErrConflict) {
		t.Fatalf("unowned lease recorded provider DNS: %v", err)
	}
	if err = worker.SaveAppMailReconciliationDNS(ctx, first, dns); err != nil {
		t.Fatal("owned lease could not record provider DNS:", err)
	}
	binding.State = "suspended"
	binding, err = api.SaveAppMailBinding(binding, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertNoClaim(api) // newer intent must not steal the living lease
	if ok, err := worker.FinishAppMailReconciliation(ctx, first, AppMailReconcileSynced); ok || err != nil {
		t.Fatalf("old generation published success: %t %v", ok, err)
	}
	current := claim(worker, 2)
	expire() // lost worker after it may have sent the provider PUT
	if err = api.CheckAppMailReconciliationLease(ctx, current); !errors.Is(err, ErrConflict) {
		t.Fatalf("expired lease could start a provider request: %v", err)
	}
	if err = api.RevokeAppMailReconciliation(ctx, current); !errors.Is(err, ErrConflict) {
		t.Fatalf("expired lease revoked a binding: %v", err)
	}
	if ok, err := worker.FinishAppMailReconciliation(ctx, current, AppMailReconcileSynced); ok || err != nil {
		t.Fatalf("expired worker published success: %t %v", ok, err)
	}
	if err = worker.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := newStoreTestDatabase(t, path)
	resumed := claim(restarted, 2)
	if resumed.ID == current.ID || resumed.Binding.State != "suspended" {
		t.Fatalf("recovered wrong work: %+v", resumed)
	}
	if ok, err := api.FinishAppMailReconciliation(ctx, current, AppMailReconcileSynced); ok || err != nil {
		t.Fatalf("old token fenced after recovery: %t %v", ok, err)
	}
	if ok, err := restarted.FinishAppMailReconciliation(ctx, resumed, AppMailReconcileProviderUnavailable); !ok || err != nil {
		t.Fatalf("failed to persist backoff: %t %v", ok, err)
	}
	status, err = api.GetAppMailReconciliation(ctx, binding.AppID)
	if err != nil || status.Generation != 2 || status.AttemptCount != 1 || status.LastResult != AppMailReconcileProviderUnavailable || status.LastSuccessAt != nil || !status.NextAttemptAt.After(time.Now().Add(5*time.Second)) {
		t.Fatalf("unpersisted backoff: %+v %v", status, err)
	}
	assertNoClaim(api)
	mustExec(`UPDATE fugue_app_mail_reconcile SET next_attempt_at=statement_timestamp()-interval '1 second' WHERE app_id=$1`, binding.AppID)
	last := claim(restarted, 2)
	if ok, err := restarted.FinishAppMailReconciliation(ctx, last, AppMailReconcileSynced); !ok || err != nil {
		t.Fatalf("failed to persist success: %t %v", ok, err)
	}
	status, err = api.GetAppMailReconciliation(ctx, binding.AppID)
	if err != nil || status.AttemptCount != 0 || status.LastSuccessAt == nil || status.LastResult != AppMailReconcileSynced {
		t.Fatalf("unpersisted success: %+v %v", status, err)
	}
	mustExec(`UPDATE fugue_app_mail_reconcile SET next_attempt_at=statement_timestamp()-interval '1 second' WHERE app_id=$1`, binding.AppID)
	revocation := claim(restarted, 2)
	if err = restarted.RevokeAppMailReconciliation(ctx, revocation); err != nil {
		t.Fatal("valid claim could not persist revocation:", err)
	}
	if ok, err := restarted.FinishAppMailReconciliation(ctx, revocation, AppMailReconcileDesiredChanged); ok || err != nil {
		t.Fatalf("completed superseded revocation lease: %t %v", ok, err)
	}
	if after := claim(api, 3); after.Binding.State != "revoked" || len(after.Binding.ProviderDNSRecords) != 1 {
		t.Fatalf("new worker did not recover revocation intent with its original observation: %+v", after.Binding)
	}
}

func TestPostgresAppMailReconciliationSeedAndIntentRollback(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	ctx := t.Context()
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE fugue_app_mail_bindings DISABLE TRIGGER fugue_app_mail_reconcile_enqueue`); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveMailPlatformConfig(model.MailPlatformConfig{MailflareAppID: "provider", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if _, err := s.SaveAppMailBinding(model.AppMailBinding{AppID: id, TenantID: "tenant", ProjectID: "project", Hostname: id + ".apps.example.test", LocalPart: "support", State: "active"}, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE fugue_app_mail_bindings ENABLE TRIGGER fugue_app_mail_reconcile_enqueue`); err != nil {
		t.Fatal(err)
	}
	seed := func(after, want string) {
		t.Helper()
		if next, e := s.SeedAppMailReconciliation(ctx, after, 1); next != want || e != nil {
			t.Fatalf("seed %q: next=%q want=%q err=%v", after, next, want, e)
		}
	}
	seed("", "a")
	if _, err := s.GetAppMailReconciliation(ctx, "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unbounded seed: %v", err)
	}
	seed("a", "b")
	seed("b", "")
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE fugue_app_mail_bindings SET generation=generation+1 WHERE app_id='a'`); err != nil {
		t.Fatal(err)
	}
	var gen uint64
	if err = tx.QueryRowContext(ctx, `SELECT generation FROM fugue_app_mail_reconcile WHERE app_id='a'`).Scan(&gen); err != nil || gen != 2 {
		t.Fatalf("generation lacking matching work in write transaction: %d %v", gen, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if status, err := s.GetAppMailReconciliation(ctx, "a"); err != nil || status.Generation != 1 {
		t.Fatalf("canceled generation leaked: %+v %v", status, err)
	}
}

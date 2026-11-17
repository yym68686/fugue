package store

import (
	"database/sql/driver"
	"errors"
	"regexp"
	"sync"
	"testing"
	"time"

	"context"
	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"github.com/DATA-DOG/go-sqlmock"
)

func expectRecoveryGuardedAdmissionHydration(mock sqlmock.Sqlmock, appID string, serviceRows ...[]driver.Value) {
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM fugue_app_recoveries WHERE app_id=$1 AND active)`)).WithArgs(appID).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	expectPGLifecycleAppHydration(mock, appID, serviceRows...)
}

func recoveryTestRequest() model.AppRecoveryStartRequest {
	zero := int64(0)
	return model.AppRecoveryStartRequest{AuthorizeOverlap: true, StorageStrategy: "fresh", RetentionSeconds: &zero, Probes: []model.AppReleaseProbe{{Path: "/business", ExpectedBodyContains: "complete"}}}
}

func TestRecoveryCapacityReservationDoesNotActivateBilling(t *testing.T) {
	app := model.App{ID: "app_capacity", TenantID: "tenant_capacity", Spec: model.AppSpec{RuntimeID: "runtime_managed_shared", Replicas: 1, Resources: &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 512}}}
	st := model.State{Apps: []model.App{app}, Runtimes: []model.Runtime{{ID: "runtime_managed_shared", Type: model.RuntimeTypeManagedShared}}}
	billing := defaultTenantBilling(app.TenantID, time.Now())
	billing.ManagedCap = model.BillingResourceSpec{CPUMilliCores: 500, MemoryMebibytes: 1024}
	if err := validateRecoveryCapacity(&st, app, billing); err != nil {
		t.Fatal(err)
	}
	billing.ManagedCap.CPUMilliCores = 499
	if err := validateRecoveryCapacity(&st, app, billing); !errors.Is(err, ErrBillingCapExceeded) {
		t.Fatal("overlap bypassed cap", err)
	}
	billing.ManagedCap.CPUMilliCores = 500
	st.AppRecoveries = []model.AppRecoveryRecord{{AppRecovery: model.AppRecovery{AppID: app.ID, Active: true}}}
	summary := buildTenantBillingSummary(&st, billing)
	if summary.ManagedCommitted.CPUMilliCores != 0 || summary.ManagedRecoveryReserved.CPUMilliCores != 500 || summary.ManagedAvailable.CPUMilliCores != 0 || summary.HourlyRateMicroCents != 0 {
		t.Fatalf("reservation billed as usage or lost: %+v", summary)
	}
	st.Apps[0].Status.CurrentRuntimeID = app.Spec.RuntimeID
	st.Apps[0].Status.CurrentReplicas = 1
	if got := tenantRecoveryReservedResources(&st, app.TenantID).CPUMilliCores; got != 250 {
		t.Fatal("already accounted replica counted twice", got)
	}
	st.AppRecoveries[0].Active = false
	if got := tenantRecoveryReservedResources(&st, app.TenantID).CPUMilliCores; got != 0 {
		t.Fatal("released capacity retained", got)
	}
}

func TestRecoveryPostgresPersistsHoldAndCapacity(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateEdgeInstanceFencing(context.Background(), s.databaseURL); err != nil {
		t.Fatal(err)
	}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "recovery", "isolated integration fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{CPUMilliCores: 1000, MemoryMebibytes: 2048}); err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "gateway", "", model.AppSpec{Image: "registry.example/gateway:v1", RuntimeID: "runtime_managed_shared", Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.StartAppRecovery(app, recoveryTestRequest())
	if err != nil {
		t.Fatal(err)
	}
	reopened := &Store{db: s.db, databaseURL: s.databaseURL}
	records, err := reopened.ListAppRecoveries(app.ID)
	if err != nil || len(records) != 1 || records[0].ID != r.ID {
		t.Fatal("restart lost recovery", err)
	}
	if _, err := reopened.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDelete}); !errors.Is(err, ErrConflict) {
		t.Fatal("operation bypassed SQL hold", err)
	}
	summary, err := s.GetTenantBillingSummary(tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if summary.ManagedRecoveryReserved.CPUMilliCores < 2*appEffectiveResources(app.Spec).CPUMilliCores {
		t.Fatal("SQL snapshot lost reservation")
	}
	next := r
	next.Phase = "probing"
	spec := app.Spec
	spec.WorkloadGeneration = "sql-isolated"
	spec.Image = "registry.example/gateway@sha256:replacement"
	next.ReplacementSpec = &spec
	r, err = reopened.SaveAppRecovery(r, next, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.SaveAppRecovery(r, r, true); !errors.Is(err, ErrConflict) {
		t.Fatal("SQL cutover accepted missing business proof", err)
	}
	next = r
	next.ProbeResults = []model.AppReleaseProbeResult{{Status: model.AppReleaseGateStatusPass}}
	r, err = reopened.SaveAppRecovery(r, next, true)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := reopened.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := reopened.GetAppTrafficPolicy(tenant.ID, true, app.ID)
	if err != nil {
		t.Fatal(err)
	}
	release, err := reopened.GetAppRelease(tenant.ID, true, policy.StableReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Spec.WorkloadGeneration != spec.WorkloadGeneration || release.ResolvedImageRef != spec.Image || policy.StableWeight != 100 || policy.CandidateWeight != 0 || r.Phase != "publishing" || r.CutoverAt == nil {
		t.Fatal("SQL cutover did not atomically persist app, release, policy and recovery")
	}
	previous := r
	r.Active = false
	r.Phase = "released"
	if _, err := s.SaveAppRecovery(previous, r, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DELETE FROM fugue_apps WHERE id=$1`, app.ID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_app_recoveries WHERE app_id=$1`, app.ID).Scan(&count); err != nil || count != 0 {
		t.Fatal("private recovery snapshot survived app deletion", count, err)
	}
}

func TestRecoveryHoldSerializesAgainstAllOperations(t *testing.T) {
	s, _, _, app := newAppImageTrackingTestStore(t)
	r, err := s.StartAppRecovery(app, recoveryTestRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.StartAppRecovery(app, recoveryTestRequest()); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate recovery", err)
	}
	for _, kind := range []string{model.OperationTypeDeploy, "restart", model.OperationTypeScale, model.OperationTypeDelete, model.OperationTypeImport} {
		if _, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: kind}); !errors.Is(err, ErrConflict) {
			t.Fatalf("%s bypassed hold: %v", kind, err)
		}
	}
	if _, err := s.DeleteTenant(app.TenantID); !errors.Is(err, ErrConflict) {
		t.Fatal("tenant deletion bypassed hold", err)
	}
	if _, err := s.PurgeApp(app.ID); !errors.Is(err, ErrConflict) {
		t.Fatal("purge bypassed hold", err)
	}
	if _, err := s.CreateAppRelease(model.AppRelease{TenantID: app.TenantID, AppID: app.ID, Role: model.AppReleaseRoleCandidate, UpstreamURL: "http://candidate.internal"}); !errors.Is(err, ErrConflict) {
		t.Fatal("release bypassed hold", err)
	}
	requested, err := s.ReleaseAppRecovery(app.ID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	stale := r
	stale.Phase = "preparing"
	if _, err := s.SaveAppRecovery(r, stale, false); !errors.Is(err, ErrConflict) {
		t.Fatal("stale worker overwrote release", err)
	}
	requested.Active = false
	requested.Phase = "released"
	if _, err := s.SaveAppRecovery(requested, requested, false); err != nil {
		t.Fatal(err)
	}
	if held, err := s.AppRecoveryHeld(app.ID); err != nil || held {
		t.Fatal("hold not released", held, err)
	}
	if _, err := s.DeleteTenant(app.TenantID); err != nil {
		t.Fatal(err)
	}
	if records, err := s.ListAppRecoveries(app.ID); err != nil || len(records) != 0 {
		t.Fatal("private recovery history survived tenant deletion", err)
	}
}

func TestRecoveryOperationCreationRace(t *testing.T) {
	for n := 0; n < 10; n++ {
		s, _, _, app := newAppImageTrackingTestStore(t)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var recoveryErr, operationErr error
		go func() { defer wg.Done(); <-start; _, recoveryErr = s.StartAppRecovery(app, recoveryTestRequest()) }()
		go func() {
			defer wg.Done()
			<-start
			_, operationErr = s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec})
		}()
		close(start)
		wg.Wait()
		if (recoveryErr == nil) == (operationErr == nil) {
			t.Fatalf("exactly one must win: recovery=%v operation=%v", recoveryErr, operationErr)
		}
	}
}

func TestRecoveryCutoverIsAtomicAndRequiresBusinessPass(t *testing.T) {
	s, _, _, app := newAppImageTrackingTestStore(t)
	r, err := s.StartAppRecovery(app, recoveryTestRequest())
	if err != nil {
		t.Fatal(err)
	}
	next := r
	next.Phase = "probing"
	spec := app.Spec
	spec.WorkloadGeneration = "isolated"
	spec.Image = "registry.example/app@sha256:replacement"
	next.ReplacementSpec = &spec
	r, err = s.SaveAppRecovery(r, next, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SaveAppRecovery(r, r, true); !errors.Is(err, ErrConflict) {
		t.Fatal("cutover accepted missing business proof", err)
	}
	next = r
	next.ProbeResults = []model.AppReleaseProbeResult{{Status: model.AppReleaseGateStatusPass}}
	r, err = s.SaveAppRecovery(r, next, true)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := s.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil {
		t.Fatal(err)
	}
	release, err := s.GetAppRelease(app.TenantID, true, policy.StableReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.WorkloadGeneration != "isolated" || release.ResolvedImageRef != spec.Image || policy.CandidateWeight != 0 || policy.StableWeight != 100 || r.CutoverAt == nil || r.Phase != "publishing" {
		t.Fatal("cutover was incomplete")
	}
	if _, err := s.SaveAppRecovery(r, r, true); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate cutover", err)
	}
}

package controller

import (
	"context"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestStableTrafficRetriesDoNotStarveRetirement(t *testing.T) {
	s, app, previous, fixture := newDrainWorkloadFixture(t)
	app.Spec.Continuity.ZeroDowntime.RetireGraceSeconds = 1
	s.safeRolloutDrainMetricsQuerier = kubeSafeRolloutDrainObserver{service: s}
	policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil {
		t.Fatal(err)
	}
	principal := model.Principal{TenantID: app.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller"}
	service := s.appReleaseService()
	// Pre-traffic failures keep reaffirming stable 100% faster than grace can
	// elapse. Use the real abort/restore and Store paths, then the complete
	// Kubernetes/edge/drain/UID retirement path, not a synthetic clock predicate.
	for deadline := policy.UpdatedAt.Add(1100 * time.Millisecond); time.Now().Before(deadline); {
		if _, err := service.AbortRelease(context.Background(), principal, app, model.AppRelease{}, false, "candidate failed before traffic"); err != nil {
			t.Fatal(err)
		}
		if _, err := service.RestoreStableTraffic(context.Background(), principal, app); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.retryDrainingAppReleaseRetirement(ctx, app); err != nil {
		t.Fatal(err)
	}
	retired, err := s.Store.GetAppRelease(app.TenantID, true, previous.ID)
	if err != nil || retired.Status != model.AppReleaseStatusRetired || fixture.deleteCalls != 2 || fixture.proxyCalls < 4 {
		t.Fatalf("no-op retries starved verified retirement: status=%s deletes=%d observations=%d err=%v", retired.Status, fixture.deleteCalls, fixture.proxyCalls, err)
	}
	current, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil || current != policy {
		t.Fatal("no-op retries or retirement changed serving intent/version", err)
	}
}

func TestCanonicalStableBaselineRetriesPreserveTrafficVersion(t *testing.T) {
	st, app, _, op := newSafeRolloutTestState(t)
	s := &Service{Store: st}
	principal := model.Principal{TenantID: app.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller"}
	if _, err := s.ensureSafeRolloutCanonicalStableBaseline(context.Background(), op, app, principal); err != nil {
		t.Fatal(err)
	}
	policy, err := st.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.ensureSafeRolloutCanonicalStableBaseline(context.Background(), op, app, principal); err != nil {
			t.Fatal(err)
		}
		current, err := st.GetAppTrafficPolicy(app.TenantID, true, app.ID)
		if err != nil || current != policy {
			t.Fatal("baseline retry restarted retirement grace", err)
		}
	}
}

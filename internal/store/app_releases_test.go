package store

import (
	"testing"

	"fugue/internal/model"
)

func TestAppReleaseTrafficPolicyRoundTrip(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newAppImageTrackingTestStore(t)
	stable, err := s.CreateAppRelease(model.AppRelease{
		TenantID:    tenant.ID,
		AppID:       app.ID,
		Role:        model.AppReleaseRoleStable,
		UpstreamURL: "http://stable.internal",
		Status:      model.AppReleaseStatusReady,
	})
	if err != nil {
		t.Fatalf("create stable release: %v", err)
	}
	candidate, err := s.CreateAppRelease(model.AppRelease{
		TenantID:    tenant.ID,
		AppID:       app.ID,
		Role:        model.AppReleaseRoleCandidate,
		UpstreamURL: "http://candidate.internal",
		Status:      model.AppReleaseStatusReady,
	})
	if err != nil {
		t.Fatalf("create candidate release: %v", err)
	}

	policy, err := s.UpsertAppTrafficPolicy(model.AppTrafficPolicy{
		TenantID:           tenant.ID,
		AppID:              app.ID,
		Mode:               model.AppTrafficModeCanary,
		StableReleaseID:    stable.ID,
		CandidateReleaseID: candidate.ID,
		StableWeight:       90,
		CandidateWeight:    10,
	})
	if err != nil {
		t.Fatalf("upsert traffic policy: %v", err)
	}
	if policy.StableWeight != 90 || policy.CandidateWeight != 10 {
		t.Fatalf("unexpected policy weights: %+v", policy)
	}

	listed, err := s.ListAppReleases(model.AppReleaseFilter{TenantID: tenant.ID, AppID: app.ID})
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("expected two releases, got %+v", listed)
	}
	got, err := s.GetAppTrafficPolicy(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if got.ID != policy.ID || got.CandidateReleaseID != candidate.ID {
		t.Fatalf("unexpected traffic policy: %+v", got)
	}
}

func TestAppTrafficPolicyRejectsInvalidWeights(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newAppImageTrackingTestStore(t)
	_, err := s.UpsertAppTrafficPolicy(model.AppTrafficPolicy{
		TenantID:        tenant.ID,
		AppID:           app.ID,
		Mode:            model.AppTrafficModeCanary,
		StableReleaseID: "rel_stable",
		StableWeight:    90,
		CandidateWeight: 20,
	})
	if err == nil {
		t.Fatal("expected invalid weights to fail")
	}
}

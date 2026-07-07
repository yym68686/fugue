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

func TestListAppReleasesActiveOnlyFiltersRoutingReleases(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newAppImageTrackingTestStore(t)
	active := []model.AppRelease{
		{TenantID: tenant.ID, AppID: app.ID, Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusServing},
		{TenantID: tenant.ID, AppID: app.ID, Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusReady},
		{TenantID: tenant.ID, AppID: app.ID, Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining},
	}
	for _, release := range active {
		if _, err := s.CreateAppRelease(release); err != nil {
			t.Fatalf("create active release: %v", err)
		}
	}
	inactive := []model.AppRelease{
		{TenantID: tenant.ID, AppID: app.ID, Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusFailed},
		{TenantID: tenant.ID, AppID: app.ID, Role: model.AppReleaseRoleRetired, Status: model.AppReleaseStatusRetired},
	}
	for _, release := range inactive {
		if _, err := s.CreateAppRelease(release); err != nil {
			t.Fatalf("create inactive release: %v", err)
		}
	}
	listed, err := s.ListAppReleases(model.AppReleaseFilter{TenantID: tenant.ID, AppID: app.ID, ActiveOnly: true})
	if err != nil {
		t.Fatalf("list active releases: %v", err)
	}
	if len(listed) != len(active) {
		t.Fatalf("expected %d active releases, got %+v", len(active), listed)
	}
	for _, release := range listed {
		if !appReleaseActiveForTest(release) {
			t.Fatalf("listed inactive release with active filter: %+v", release)
		}
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

func TestCompletedDeploySyncsInactiveTrafficStableRelease(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newAppImageTrackingTestStore(t)
	oldStable, err := s.CreateAppRelease(model.AppRelease{
		TenantID:         tenant.ID,
		AppID:            app.ID,
		Role:             model.AppReleaseRoleStable,
		SourceRef:        "registry.example/app:old",
		ResolvedImageRef: "registry.example/app:old",
		UpstreamURL:      "http://old-service.internal:8080",
		RuntimeID:        app.Spec.RuntimeID,
		Status:           model.AppReleaseStatusServing,
	})
	if err != nil {
		t.Fatalf("create old stable release: %v", err)
	}
	candidate, err := s.CreateAppRelease(model.AppRelease{
		TenantID:    tenant.ID,
		AppID:       app.ID,
		Role:        model.AppReleaseRoleCandidate,
		UpstreamURL: "http://candidate.internal:8080",
		RuntimeID:   app.Spec.RuntimeID,
		Status:      model.AppReleaseStatusCreating,
	})
	if err != nil {
		t.Fatalf("create candidate release: %v", err)
	}
	if _, err := s.UpsertAppTrafficPolicy(model.AppTrafficPolicy{
		TenantID:           tenant.ID,
		AppID:              app.ID,
		Mode:               model.AppTrafficModeCanary,
		StableReleaseID:    oldStable.ID,
		CandidateReleaseID: candidate.ID,
		StableWeight:       100,
		CandidateWeight:    0,
	}); err != nil {
		t.Fatalf("upsert inactive traffic policy: %v", err)
	}

	nextSpec := app.Spec
	nextSpec.Image = "registry.example/app:new"
	nextSpec.Env = map[string]string{"CORS_ALLOWED_ORIGINS": "https://example.test"}
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &nextSpec,
		ExecutionMode: model.ExecutionModeManaged,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperationWithResult(deployOp.ID, "/tmp/app.yaml", "deployed", &nextSpec, nil); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	updatedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get updated app: %v", err)
	}
	policy, err := s.GetAppTrafficPolicy(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("get synced traffic policy: %v", err)
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.StableWeight != 100 || policy.CandidateWeight != 0 || policy.CandidateReleaseID != "" {
		t.Fatalf("expected single stable traffic after sync, got %+v", policy)
	}
	if policy.StableReleaseID == "" || policy.StableReleaseID == oldStable.ID {
		t.Fatalf("expected traffic policy to point at a new stable release, got %+v", policy)
	}
	if policy.UpdatedByType != model.ActorTypeSystem || policy.UpdatedByID != stableReleaseSyncActorID {
		t.Fatalf("expected system sync actor, got %+v", policy)
	}

	releases, err := s.ListAppReleases(model.AppReleaseFilter{TenantID: tenant.ID, AppID: app.ID, IncludeRetired: true})
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	var syncedStable model.AppRelease
	var oldStableRole string
	for _, release := range releases {
		if release.ID == policy.StableReleaseID {
			syncedStable = release
		}
		if release.ID == oldStable.ID {
			oldStableRole = release.Role
		}
	}
	if syncedStable.ID == "" {
		t.Fatalf("expected synced stable release in %+v", releases)
	}
	if syncedStable.Role != model.AppReleaseRoleStable || syncedStable.Status != model.AppReleaseStatusServing {
		t.Fatalf("unexpected synced stable release: %+v", syncedStable)
	}
	if syncedStable.ResolvedImageRef != nextSpec.Image || syncedStable.UpstreamURL != currentAppServiceURL(updatedApp) {
		t.Fatalf("synced stable release does not match current app: %+v app=%+v", syncedStable, updatedApp)
	}
	if syncedStable.SpecSnapshot == nil || syncedStable.SpecSnapshot.Env["CORS_ALLOWED_ORIGINS"] != "https://example.test" {
		t.Fatalf("expected synced release spec snapshot to include deployed env, got %+v", syncedStable.SpecSnapshot)
	}
	if oldStableRole != model.AppReleaseRolePrevious {
		t.Fatalf("expected old stable to become previous, got %q", oldStableRole)
	}
}

func TestCompletedDeployDoesNotOverrideActiveCanaryTraffic(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newAppImageTrackingTestStore(t)
	stable, err := s.CreateAppRelease(model.AppRelease{
		TenantID:    tenant.ID,
		AppID:       app.ID,
		Role:        model.AppReleaseRoleStable,
		UpstreamURL: "http://stable.internal:8080",
		RuntimeID:   app.Spec.RuntimeID,
		Status:      model.AppReleaseStatusServing,
	})
	if err != nil {
		t.Fatalf("create stable release: %v", err)
	}
	candidate, err := s.CreateAppRelease(model.AppRelease{
		TenantID:    tenant.ID,
		AppID:       app.ID,
		Role:        model.AppReleaseRoleCandidate,
		UpstreamURL: "http://candidate.internal:8080",
		RuntimeID:   app.Spec.RuntimeID,
		Status:      model.AppReleaseStatusReady,
	})
	if err != nil {
		t.Fatalf("create candidate release: %v", err)
	}
	if _, err := s.UpsertAppTrafficPolicy(model.AppTrafficPolicy{
		TenantID:           tenant.ID,
		AppID:              app.ID,
		Mode:               model.AppTrafficModeCanary,
		StableReleaseID:    stable.ID,
		CandidateReleaseID: candidate.ID,
		StableWeight:       90,
		CandidateWeight:    10,
	}); err != nil {
		t.Fatalf("upsert active traffic policy: %v", err)
	}

	nextSpec := app.Spec
	nextSpec.Image = "registry.example/app:new"
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &nextSpec,
		ExecutionMode: model.ExecutionModeManaged,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperationWithResult(deployOp.ID, "/tmp/app.yaml", "deployed", &nextSpec, nil); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	policy, err := s.GetAppTrafficPolicy(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.Mode != model.AppTrafficModeCanary ||
		policy.StableReleaseID != stable.ID ||
		policy.CandidateReleaseID != candidate.ID ||
		policy.StableWeight != 90 ||
		policy.CandidateWeight != 10 {
		t.Fatalf("expected active canary policy to remain unchanged, got %+v", policy)
	}
}

func TestCompletedDeployDoesNotAutoSyncStableReleaseForSafeZeroDowntimeApp(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newAppImageTrackingTestStore(t)
	oldStable, err := s.CreateAppRelease(model.AppRelease{
		TenantID:    tenant.ID,
		AppID:       app.ID,
		Role:        model.AppReleaseRoleStable,
		UpstreamURL: "http://stable.internal:8080",
		RuntimeID:   app.Spec.RuntimeID,
		Status:      model.AppReleaseStatusServing,
	})
	if err != nil {
		t.Fatalf("create stable release: %v", err)
	}
	if _, err := s.UpsertAppTrafficPolicy(model.AppTrafficPolicy{
		TenantID:        tenant.ID,
		AppID:           app.ID,
		Mode:            model.AppTrafficModeSingle,
		StableReleaseID: oldStable.ID,
		StableWeight:    100,
		CandidateWeight: 0,
	}); err != nil {
		t.Fatalf("upsert traffic policy: %v", err)
	}

	nextSpec := app.Spec
	nextSpec.Image = "registry.example/app:new"
	nextSpec.Continuity = &model.AppContinuityPolicy{ZeroDowntime: &model.AppZeroDowntimePolicy{
		Enabled:  true,
		Mode:     model.AppZeroDowntimeModeSafe,
		Strategy: model.AppZeroDowntimeStrategyStableCandidate,
	}}
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &nextSpec,
		ExecutionMode: model.ExecutionModeManaged,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperationWithResult(deployOp.ID, "/tmp/app.yaml", "deployed", &nextSpec, nil); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	policy, err := s.GetAppTrafficPolicy(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.StableReleaseID != oldStable.ID || policy.Mode != model.AppTrafficModeSingle || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected safe mode to preserve existing stable traffic policy, got %+v", policy)
	}
	releases, err := s.ListAppReleases(model.AppReleaseFilter{TenantID: tenant.ID, AppID: app.ID, IncludeRetired: true})
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	if len(releases) != 1 {
		t.Fatalf("expected no implicit stable sync release for safe mode, got %+v", releases)
	}
}

func appReleaseActiveForTest(release model.AppRelease) bool {
	switch release.Role {
	case model.AppReleaseRoleStable, model.AppReleaseRoleCandidate, model.AppReleaseRolePrevious:
	default:
		return false
	}
	switch release.Status {
	case model.AppReleaseStatusReady, model.AppReleaseStatusServing, model.AppReleaseStatusDraining:
		return true
	default:
		return false
	}
}

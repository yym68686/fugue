package store

import (
	"context"
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestReleaseRetirementTargetsDeduplicateAppsAndExcludeServingHistory(t *testing.T) {
	s, _, _, app := newAppImageTrackingTestStore(t)
	for _, r := range []model.AppRelease{
		{Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusServing},
		{Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusServing},
	} {
		r.TenantID, r.AppID = app.TenantID, app.ID
		if _, err := s.CreateAppRelease(r); err != nil {
			t.Fatal(err)
		}
	}
	if targets, err := s.ListAppReleaseRetirementTargets(context.Background()); err != nil || len(targets) != 0 {
		t.Fatal("serving history scheduled for retirement", targets, err)
	}
	for _, role := range []string{model.AppReleaseRoleCandidate, model.AppReleaseRoleCandidate, model.AppReleaseRolePrevious} {
		status := model.AppReleaseStatusFailed
		if role == model.AppReleaseRolePrevious {
			status = model.AppReleaseStatusDraining
		}
		if _, err := s.CreateAppRelease(model.AppRelease{TenantID: app.TenantID, AppID: app.ID, Role: role, Status: status}); err != nil {
			t.Fatal(err)
		}
	}
	targets, err := s.ListAppReleaseRetirementTargets(context.Background())
	if err != nil || len(targets) != 1 || targets[0].AppID != app.ID || targets[0].TenantID != app.TenantID {
		t.Fatal("target discovery lost isolation or deduplication", targets, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ListAppReleaseRetirementTargets(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("ignored leadership cancellation", err)
	}
}

package store

import (
	"context"
	"os"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestAppTrafficPolicyIdempotency(t *testing.T) {
	s, _, _, app := newAppImageTrackingTestStore(t)
	testAppTrafficPolicyIdempotency(t, s, app)
}

func TestAppTrafficPolicyIdempotencyPostgres(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateAppReleaseRetirement(context.Background(), os.Getenv("FUGUE_TEST_DATABASE_URL")); err != nil {
		t.Fatal(err)
	}
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "traffic-retry", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "traffic-retry", "", model.AppSpec{Image: "registry.example/app:v1", Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	testAppTrafficPolicyIdempotency(t, s, app)
}

func testAppTrafficPolicyIdempotency(t *testing.T, s *Store, app model.App) {
	t.Helper()
	createRelease := func() model.AppRelease {
		r, err := s.CreateAppRelease(model.AppRelease{AppID: app.ID, TenantID: app.TenantID, Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusReady})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	stable, candidate := createRelease(), createRelease()
	base := model.AppTrafficPolicy{AppID: app.ID, TenantID: app.TenantID, Mode: model.AppTrafficModeSingle,
		StableReleaseID: stable.ID, StableWeight: 100, StickyCookie: "release-stickiness",
		UpdatedByType: model.ActorTypeSystem, UpdatedByID: "promotion-controller"}
	policy, err := s.UpsertAppTrafficPolicy(base)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		retry := base // No ID or timestamps: a caller can reconstruct the same intent.
		retry.Mode = " SINGLE "
		retry.StableReleaseID = " " + stable.ID + " "
		retry.UpdatedByType, retry.UpdatedByID = "user", "different-retry-actor"
		got, err := s.UpsertAppTrafficPolicy(retry)
		if err != nil || !reflect.DeepEqual(got, policy) {
			t.Fatalf("identical normalized intent renewed traffic version or actor: got=%+v want=%+v err=%v", got, policy, err)
		}
	}
	changes := map[string]func(*model.AppTrafficPolicy){
		"mode":      func(p *model.AppTrafficPolicy) { p.Mode = model.AppTrafficModePaused },
		"stable":    func(p *model.AppTrafficPolicy) { p.StableReleaseID = candidate.ID },
		"candidate": func(p *model.AppTrafficPolicy) { p.CandidateReleaseID = candidate.ID },
		"weights": func(p *model.AppTrafficPolicy) {
			p.StableWeight, p.CandidateWeight, p.CandidateReleaseID = 99, 1, candidate.ID
		},
		"header": func(p *model.AppTrafficPolicy) { p.StickyHeader = "X-Release" },
		"cookie": func(p *model.AppTrafficPolicy) { p.StickyCookie = "new-stickiness" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			before, err := s.UpsertAppTrafficPolicy(base)
			if err != nil {
				t.Fatal(err)
			}
			updated := base
			change(&updated)
			updated.UpdatedByType, updated.UpdatedByID = "user", "intent-changing-actor"
			got, err := s.UpsertAppTrafficPolicy(updated)
			if err != nil || !got.UpdatedAt.After(before.UpdatedAt) || got.UpdatedByID != updated.UpdatedByID || got.ID != before.ID || !got.CreatedAt.Equal(before.CreatedAt) {
				t.Fatalf("real routing change did not advance its version and actor: got=%+v before=%+v err=%v", got, before, err)
			}
			stored, err := s.GetAppTrafficPolicy(app.TenantID, false, app.ID)
			if err != nil || !reflect.DeepEqual(stored, got) {
				t.Fatal("returned policy differs from persisted intent", err)
			}
		})
	}
}

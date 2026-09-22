package store

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestAppReleaseRetirementFence(t *testing.T) {
	s, _, _, app := newAppImageTrackingTestStore(t)
	testAppReleaseRetirementFence(t, s, app)
}

func TestAppReleaseRetirementFencePostgres(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateAppReleaseRetirement(context.Background(), os.Getenv("FUGUE_TEST_DATABASE_URL")); err != nil {
		t.Fatal(err)
	}
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "release-fence", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "release-fence", "", model.AppSpec{Image: "registry.example/app:v1", Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	testAppReleaseRetirementFence(t, s, app)
}

func testAppReleaseRetirementFence(t *testing.T, s *Store, app model.App) {
	t.Helper()
	create := func(role, status string) model.AppRelease {
		r, err := s.CreateAppRelease(model.AppRelease{AppID: app.ID, TenantID: app.TenantID, Role: role, Status: status})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	stable := create(model.AppReleaseRoleStable, model.AppReleaseStatusServing)
	previous := create(model.AppReleaseRolePrevious, model.AppReleaseStatusDraining)
	policy, err := s.UpsertAppTrafficPolicy(model.AppTrafficPolicy{AppID: app.ID, TenantID: app.TenantID, Mode: model.AppTrafficModeSingle, StableReleaseID: stable.ID, StableWeight: 100})
	if err != nil {
		t.Fatal(err)
	}
	bad := stable
	bad.Role, bad.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
	if _, err = s.UpdateAppRelease(bad); !errors.Is(err, ErrConflict) {
		t.Fatalf("serving release retired: %v", err)
	}
	retire := previous
	retire.Role, retire.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
	retired, err := s.UpdateAppRelease(retire)
	if err != nil {
		t.Fatal(err)
	}
	previous.Status, previous.Role = model.AppReleaseStatusServing, model.AppReleaseRoleStable
	if _, err = s.UpdateAppRelease(previous); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale writer reactivated release: %v", err)
	}
	if appReleaseMatchesCurrentStable(retired, retired) {
		t.Fatal("automatic deploy sync can resurrect retired release")
	}
	for _, candidate := range []bool{false, true} {
		patch := policy
		if candidate {
			patch.CandidateReleaseID = retired.ID
		} else {
			patch.StableReleaseID = retired.ID
		}
		if _, err = s.UpsertAppTrafficPolicy(patch); !errors.Is(err, ErrConflict) {
			t.Fatalf("retired reference accepted: %v", err)
		}
	}
	got, err := s.GetAppTrafficPolicy(app.TenantID, false, app.ID)
	if err != nil || !reflect.DeepEqual(got, policy) {
		t.Fatal("rejected reference changed serving policy", err)
	}
	// Both callers use the normal Store APIs, exercising its JSON file lock
	// or the database triggers rather than a manually sequenced simulation.
	for i := 0; i < 8; i++ {
		r := create(model.AppReleaseRolePrevious, model.AppReleaseStatusDraining)
		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			defer wg.Done()
			<-start
			r.Role, r.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
			_, err := s.UpdateAppRelease(r)
			results <- err
		}()
		go func(id string) {
			defer wg.Done()
			<-start
			p := policy
			p.StableReleaseID = id
			_, err := s.UpsertAppTrafficPolicy(p)
			results <- err
		}(r.ID)
		close(start)
		wg.Wait()
		close(results)
		wins := 0
		for err := range results {
			if err == nil {
				wins++
			}
		}
		if wins != 1 {
			t.Fatalf("competing writes committed %d times", wins)
		}
		if _, err = s.UpsertAppTrafficPolicy(policy); err != nil {
			t.Fatal(err)
		}
	}
	for _, change := range []string{"none", "previous", "stable", "policy", "retention", "canceled"} {
		t.Run("cas_"+change, func(t *testing.T) {
			r := create(model.AppReleaseRolePrevious, model.AppReleaseStatusDraining)
			stableNow, err := s.GetAppRelease(app.TenantID, false, stable.ID)
			if err != nil {
				t.Fatal(err)
			}
			p, err := s.GetAppTrafficPolicy(app.TenantID, false, app.ID)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch change {
			case "previous":
				copy := r
				copy.DeploymentName = "new-target"
				if _, err = s.UpdateAppRelease(copy); err != nil {
					t.Fatal(err)
				}
			case "stable":
				copy := stableNow
				copy.UpstreamURL = "http://different.example"
				if _, err = s.UpdateAppRelease(copy); err != nil {
					t.Fatal(err)
				}
			case "policy":
				copy := p
				copy.StickyCookie = "changed"
				if _, err = s.UpsertAppTrafficPolicy(copy); err != nil {
					t.Fatal(err)
				}
			case "retention":
				future := time.Now().Add(time.Hour)
				r.RetentionUntil = &future
				r, err = s.UpdateAppRelease(r)
				if err != nil {
					t.Fatal(err)
				}
			case "canceled":
				cancel()
			}
			result, err := s.RetireDrainedAppRelease(ctx, r, stableNow, p)
			if change == "none" {
				if err != nil || !AppReleaseIsRetired(result) {
					t.Fatal("verified retirement failed", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("retirement accepted changed %s", change)
			}
			kept, err := s.GetAppRelease(app.TenantID, false, r.ID)
			if err != nil || AppReleaseIsRetired(kept) {
				t.Fatal("conflict lost retained release", err)
			}
		})
	}
}

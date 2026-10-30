package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAppReleaseRetirementScanOrderResumesAcrossQueueChanges(t *testing.T) {
	now := time.Now()
	previous := model.AppRelease{ID: "previous", CreatedAt: now, Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining}
	failed := model.AppRelease{ID: "failed", CreatedAt: now.Add(time.Hour), Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusFailed}
	retired := model.AppRelease{ID: "retired", CreatedAt: now.Add(-time.Hour), Status: model.AppReleaseStatusRetired, RevisionWorkload: &model.AppReleaseWorkload{}}
	stable := model.AppRelease{ID: "stable", CreatedAt: now.Add(-2 * time.Hour), Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusServing}
	ids := func(releases []model.AppRelease) []string {
		var out []string
		for _, release := range releases {
			out = append(out, release.ID)
		}
		return out
	}
	queue := []model.AppRelease{failed, stable, previous, retired}
	for _, tc := range []struct {
		name     string
		queue    []model.AppRelease
		after    appReleaseRetirementPosition
		expected []string
	}{
		{"fresh_leader_oldest_first", queue, appReleaseRetirementPosition{}, []string{"retired", "previous", "failed"}},
		{"resume_after_unavailable", queue, appReleaseRetirementPositionOf(previous), []string{"failed", "retired", "previous"}},
		{"removed_cursor_and_new_arrival", []model.AppRelease{failed, stable, retired}, appReleaseRetirementPositionOf(previous), []string{"failed", "retired"}},
		{"wrap_after_last", queue, appReleaseRetirementPositionOf(failed), []string{"retired", "previous", "failed"}},
		{"empty", nil, appReleaseRetirementPositionOf(failed), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ids(orderAppReleaseRetirementScan(tc.queue, tc.after)); !reflect.DeepEqual(got, tc.expected) {
				t.Fatalf("queue=%v, want %v", got, tc.expected)
			}
		})
	}
	// Identical creation timestamps use a deterministic, role-independent key.
	failed.CreatedAt = previous.CreatedAt
	if got := ids(orderAppReleaseRetirementScan([]model.AppRelease{previous, failed}, appReleaseRetirementPositionOf(failed))); !reflect.DeepEqual(got, []string{"previous", "failed"}) {
		t.Fatal("equal-time cursor did not progress", got)
	}
	if queue[0].ID != failed.ID || queue[1].ID != stable.ID {
		t.Fatal("ordering mutated the Store projection")
	}
}

func TestAppReleaseRetirementScansSerializePerTenantAppAndBoundMemory(t *testing.T) {
	var scans appReleaseRetirementScans
	app := model.App{TenantID: "tenant-a", ID: "app-a"}
	_, finish, acquired := scans.begin(app)
	if !acquired {
		t.Fatal("initial scan not acquired")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, release, ok := scans.begin(app); ok {
				release(appReleaseRetirementPosition{})
				t.Error("concurrent scan for the same tenant/app")
			}
		}()
	}
	wg.Wait()
	for _, other := range []model.App{{TenantID: "tenant-b", ID: app.ID}, {TenantID: app.TenantID, ID: "app-b"}} {
		position, release, ok := scans.begin(other)
		if !ok || position.releaseID != "" {
			t.Fatal("unrelated tenant/app scan interfered")
		}
		release(appReleaseRetirementPosition{})
	}
	position := appReleaseRetirementPosition{time.Now(), "last-attempted"}
	finish(position)
	got, finish, acquired := scans.begin(app)
	if !acquired || got != position {
		t.Fatal("scan lost progress", got)
	}
	// A live scan is never evicted even when many other apps are reconciled.
	for i := 0; i < 2*maxAppReleaseRetirementScans; i++ {
		_, release, ok := scans.begin(model.App{TenantID: app.TenantID, ID: fmt.Sprintf("other-%d", i)})
		if !ok {
			t.Fatal("idle progress entries prevented scheduling")
		}
		release(appReleaseRetirementPosition{})
	}
	if len(scans.apps) != maxAppReleaseRetirementScans {
		t.Fatal("unbounded progress cache", len(scans.apps))
	}
	if _, release, ok := scans.begin(app); ok {
		release(position)
		t.Fatal("active scan was evicted")
	}
	finish(position)
	got, finish, acquired = scans.begin(app)
	if !acquired || got != position {
		t.Fatal("bounded cache lost active scan progress")
	}
	finish(position)
}

type retirementScanDrainFunc func(context.Context, model.App, model.AppRelease, time.Time) (safeRolloutDrainMetrics, error)

func (f retirementScanDrainFunc) QuerySafeRolloutDrainMetrics(ctx context.Context, app model.App, release model.AppRelease, since time.Time) (safeRolloutDrainMetrics, error) {
	return f(ctx, app, release, since)
}

func TestBackgroundRetirementResumesAfterUnavailableCandidateWithoutBypassingProofs(t *testing.T) {
	for _, scenario := range []string{"idle", "busy", "unconfirmed_traffic", "retention", "still_referenced", "changed_uid"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s, app, previous, fixture := newDrainWorkloadFixtureWithOutcome(t, true)
			op, err := s.Store.GetOperation(previous.RevisionWorkload.OperationID)
			if err != nil || op.CompletedAt == nil {
				t.Fatal(err)
			}
			promoted := op.CompletedAt.Add(-time.Nanosecond)
			previous.Role, previous.Status, previous.PromotedAt = model.AppReleaseRolePrevious, model.AppReleaseStatusDraining, &promoted
			previous, err = s.Store.UpdateAppRelease(previous)
			if err != nil {
				t.Fatal(err)
			}
			// The older failed candidate consumes the entire scan's budget on
			// every attempt. Never allow missing drain evidence to delete it.
			stalled := previous
			stalled.ID, stalled.Role, stalled.Status = "release-unavailable", model.AppReleaseRoleCandidate, model.AppReleaseStatusCreating
			stalled.DeploymentName, stalled.ServiceName = "workload-unavailable", "service-unavailable"
			stalled.CreatedAt, stalled.PromotedAt, stalled.RevisionWorkload = previous.CreatedAt.Add(-time.Hour), nil, nil
			stalled, err = s.Store.CreateAppRelease(stalled)
			if err != nil {
				t.Fatal(err)
			}
			failedOp, err := s.Store.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec, ExecutionMode: model.ExecutionModeManaged})
			if err != nil {
				t.Fatal(err)
			}
			if _, claimed, err := s.Store.TryClaimPendingOperation(failedOp.ID); err != nil || !claimed {
				t.Fatal("claim failed candidate operation", err)
			}
			binding := *previous.RevisionWorkload
			binding.OperationID, binding.BoundAt = failedOp.ID, time.Time{}
			binding.DeploymentName, binding.ServiceName = stalled.DeploymentName, stalled.ServiceName
			binding.DeploymentUID, binding.ServiceUID = "unavailable-deployment-uid", "unavailable-service-uid"
			stalled, err = s.Store.BindAppReleaseWorkload(context.Background(), stalled, binding)
			if err != nil {
				t.Fatal(err)
			}
			stalled.Status = model.AppReleaseStatusFailed
			stalled, err = s.Store.UpdateAppRelease(stalled)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Store.FailOperation(failedOp.ID, "candidate observer unavailable"); err != nil {
				t.Fatal(err)
			}
			s.safeRolloutDrainMetricsQuerier = retirementScanDrainFunc(func(ctx context.Context, app model.App, release model.AppRelease, since time.Time) (safeRolloutDrainMetrics, error) {
				if release.ID == stalled.ID {
					return safeRolloutDrainMetrics{}, errors.New("candidate observer unavailable")
				}
				return (kubeSafeRolloutDrainObserver{service: s}).QuerySafeRolloutDrainMetrics(ctx, app, release, since)
			})
			run := func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := s.retryDrainingAppReleaseRetirement(ctx, app); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			}
			policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "busy":
				fixture.active = 1
			case "unconfirmed_traffic":
				s.safeRolloutEdgeBundleObserver = staticSafeRolloutEdgeObserver{}
			case "retention":
				future := time.Now().Add(time.Hour)
				previous.RetentionUntil = &future
				if _, err := s.Store.UpdateAppRelease(previous); err != nil {
					t.Fatal(err)
				}
			case "still_referenced":
				policy.Mode, policy.CandidateReleaseID = model.AppTrafficModeCanary, previous.ID
				policy.StableWeight, policy.CandidateWeight = 99, 1
				if policy, err = s.Store.UpsertAppTrafficPolicy(policy); err != nil {
					t.Fatal(err)
				}
			case "changed_uid":
				fixture.deployment["metadata"].(map[string]any)["uid"] = "replacement-deployment"
			}
			run()
			got, err := s.Store.GetAppRelease(app.TenantID, true, previous.ID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "idle" {
				if got.Status != model.AppReleaseStatusRetired || fixture.deleteCalls != 2 || fixture.proxyCalls < 4 {
					t.Fatalf("unavailable candidate starved guarded retirement: status=%s deletes=%d observations=%d", got.Status, fixture.deleteCalls, fixture.proxyCalls)
				}
			} else if got.Status == model.AppReleaseStatusRetired || fixture.deleteCalls != 0 {
				t.Fatalf("fair scan bypassed %s proof: status=%s deletes=%d", scenario, got.Status, fixture.deleteCalls)
			}
			if got, err := s.Store.GetAppRelease(app.TenantID, true, stalled.ID); err != nil || got.Status != model.AppReleaseStatusFailed {
				t.Fatal("unavailable candidate was retired without evidence", err)
			}
			if got, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID); err != nil || got != policy {
				t.Fatal("scheduler changed serving intent/version", err)
			}
		})
	}
}

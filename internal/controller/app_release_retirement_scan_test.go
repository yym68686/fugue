package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

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

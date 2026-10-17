package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPromotedReleaseWithFailedOperationRetiresOnlyAfterDrainProof(t *testing.T) {
	for _, scenario := range []string{"idle", "busy", "unconfirmed_traffic", "still_referenced", "retention", "retire_grace", "unpromoted", "zero_promotion_time", "future_promotion", "promotion_before_creation", "changed_after_drain"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s, app, previous, fixture := newDrainWorkloadFixtureWithOutcome(t, true)
			op, err := s.Store.GetOperation(previous.RevisionWorkload.OperationID)
			if err != nil || op.CompletedAt == nil {
				t.Fatal("failed operation must have finished", err)
			}
			// Promotion succeeded before a later canonical-alignment failure.
			// A subsequent release has superseded this serving revision.
			promoted := op.CompletedAt.Add(-time.Nanosecond)
			previous.Role, previous.Status = model.AppReleaseRolePrevious, model.AppReleaseStatusDraining
			previous.PromotedAt = &promoted
			previous.StatusReason = "superseded by current release"
			switch scenario {
			case "unpromoted":
				previous.PromotedAt = nil
			case "zero_promotion_time":
				previous.PromotedAt = &time.Time{}
			case "future_promotion":
				future := time.Now().Add(time.Hour)
				previous.PromotedAt = &future
			case "promotion_before_creation":
				before := previous.CreatedAt.Add(-time.Second)
				previous.PromotedAt = &before
			case "retention":
				future := time.Now().Add(time.Hour)
				previous.RetentionUntil = &future
			}
			previous, err = s.Store.UpdateAppRelease(previous)
			if err != nil {
				t.Fatal(err)
			}
			policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
			if err != nil {
				t.Fatal(err)
			}
			s.safeRolloutDrainMetricsQuerier = kubeSafeRolloutDrainObserver{service: s}
			switch scenario {
			case "busy":
				fixture.active = 1
			case "unconfirmed_traffic":
				s.safeRolloutEdgeBundleObserver = staticSafeRolloutEdgeObserver{}
			case "still_referenced":
				policy.Mode, policy.CandidateReleaseID = model.AppTrafficModeCanary, previous.ID
				policy.StableWeight, policy.CandidateWeight = 99, 1
				if policy, err = s.Store.UpsertAppTrafficPolicy(policy); err != nil {
					t.Fatal(err)
				}
			case "retire_grace":
				app.Spec.Continuity.ZeroDowntime.RetireGraceSeconds = 60
			case "changed_after_drain":
				calls := 0
				s.safeRolloutEdgeBundleObserver = retirementTestObserver(func() {
					calls++
					if calls == 2 {
						previous.Status = model.AppReleaseStatusReady
						if _, err = s.Store.UpdateAppRelease(previous); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := s.retryDrainingAppReleaseRetirement(ctx, app); err != nil && !(scenario == "busy" && errors.Is(err, context.DeadlineExceeded)) {
				t.Fatal(err)
			}
			got, err := s.Store.GetAppRelease(app.TenantID, true, previous.ID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "idle" {
				if got.Status != model.AppReleaseStatusRetired || fixture.deleteCalls != 2 || fixture.proxyCalls < 4 {
					t.Fatalf("promoted revision from failed operation was not retired: status=%s deletes=%d observations=%d", got.Status, fixture.deleteCalls, fixture.proxyCalls)
				}
			} else if got.Status == model.AppReleaseStatusRetired || fixture.deleteCalls != 0 {
				t.Fatalf("revision retired without complete proof: status=%s deletes=%d", got.Status, fixture.deleteCalls)
			}
			currentOperation, err := s.Store.GetOperation(op.ID)
			if err != nil || currentOperation.Status != model.OperationStatusFailed {
				t.Fatal("retirement must preserve failed operation history", err)
			}
			currentPolicy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
			if err != nil || currentPolicy != policy {
				t.Fatal("retirement changed serving traffic", err)
			}
		})
	}
}

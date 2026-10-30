package controller

import (
	"context"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestRecordedCanonicalSnapshotRequiresExactReadyHistory(t *testing.T) {
	for _, scenario := range []string{"exact", "wrong_key", "not_ready", "wrong_image", "snapshot_changed", "missing_ready_receipt", "runtime_changed", "status_key_changed", "retired", "retired_unpromoted", "retired_not_ready", "retired_bad_order"} {
		t.Run(scenario, func(t *testing.T) {
			st, source, desired, _ := newSafeRolloutTestState(t)
			s := &Service{Store: st}
			live, _ := s.expectedManagedAppDeployment(s.Renderer.PrepareApp(source), runtime.SchedulingConstraints{})
			managedAppLiveGuardMarkReady(&live, 1)
			live.Metadata.UID = "canonical-uid"
			managed := managedAppLiveGuardObject(t, desired, runtime.SchedulingConstraints{})
			managed.Status.CurrentReleaseKey = live.Metadata.Annotations[runtime.FugueAnnotationReleaseKey]
			managed.Status.CurrentReleaseStartedAt = time.Now().Add(-time.Hour).Format(time.RFC3339)
			managed.Status.CurrentReleaseReadyAt = time.Now().Add(-time.Minute).Format(time.RFC3339)
			managed.Status.ObservedGeneration = managed.Metadata.Generation
			historical := model.AppRelease{ID: "historical-canonical", TenantID: source.TenantID, AppID: source.ID,
				Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusServing, ResolvedImageRef: source.Spec.Image, SpecSnapshot: cloneControllerAppSpec(&source.Spec)}
			if scenario == "retired" || scenario == "retired_unpromoted" || scenario == "retired_not_ready" || scenario == "retired_bad_order" {
				historical.Role, historical.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
				historical.CreatedAt = time.Now().Add(-2 * time.Hour)
				ready, retired := time.Now().Add(-time.Hour), time.Now().Add(-30*time.Minute)
				historical.ReadyAt, historical.PromotedAt, historical.RetiredAt = &ready, &ready, &retired
			}
			switch scenario {
			case "wrong_key":
				live.Spec.Template.Metadata.Annotations[runtime.FugueAnnotationReleaseKey] = "unverified"
			case "not_ready":
				live.Status.ReadyReplicas = 0
			case "wrong_image":
				historical.ResolvedImageRef = "ghcr.io/example/api:unknown"
			case "snapshot_changed":
				historical.SpecSnapshot.Resources.CPUMilliCores++
			case "missing_ready_receipt":
				managed.Status.CurrentReleaseReadyAt = ""
			case "runtime_changed":
				historical.SpecSnapshot.RuntimeID = "runtime-unverified"
			case "status_key_changed":
				managed.Status.CurrentReleaseKey = "unverified"
			case "retired_unpromoted":
				historical.PromotedAt = nil
			case "retired_not_ready":
				historical.ReadyAt = nil
			case "retired_bad_order":
				historical.RetiredAt = &historical.CreatedAt
			}
			if _, err := st.CreateAppRelease(historical); err != nil {
				t.Fatal(err)
			}
			got, ok, err := s.recoverRecordedCanonicalSnapshot(context.Background(), managed, live, desired, runtime.SchedulingConstraints{})
			if err != nil || ok != (scenario == "exact" || scenario == "retired") {
				t.Fatalf("recovery=%v err=%v", ok, err)
			}
			if ok && got.Spec.Image != source.Spec.Image {
				t.Fatal("recovery lost historical executable")
			}
			if scenario == "retired" {
				row, err := st.GetAppRelease(source.TenantID, true, historical.ID)
				if err != nil || row.Role != model.AppReleaseRoleRetired || row.Status != model.AppReleaseStatusRetired {
					t.Fatal("baseline recovery reactivated retired release", err)
				}
			}
		})
	}
}

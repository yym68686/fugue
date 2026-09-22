package store

import (
	"context"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestHistoricalReleaseWorkloadMigration(t *testing.T) {
	s, _, _, app := newAppImageTrackingTestStore(t)
	testHistoricalReleaseWorkloadMigration(t, s, app)
	testStoppedHistoricalRetirement(t, s, app)
}

func TestHistoricalReleaseWorkloadMigrationPostgres(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateAppReleaseWorkload(context.Background(), os.Getenv("FUGUE_TEST_DATABASE_URL")); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateAppReleaseRetirement(context.Background(), os.Getenv("FUGUE_TEST_DATABASE_URL")); err != nil {
		t.Fatal(err)
	}
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "history", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "history", "", model.AppSpec{Image: "registry.example/app:v1", Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	testHistoricalReleaseWorkloadMigration(t, s, app)
	testStoppedHistoricalRetirement(t, s, app)
}

func testStoppedHistoricalRetirement(t *testing.T, s *Store, app model.App) {
	for _, scenario := range []string{"valid", "missing_source", "policy_changed", "retention", "operation_changed", "release_changed", "canceled", "concurrent_reactivate"} {
		t.Run("atomic_stopped_"+scenario, func(t *testing.T) {
			op, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec, ExecutionMode: model.ExecutionModeManaged})
			if err != nil {
				t.Fatal(err)
			}
			if _, ok, err := s.TryClaimPendingOperation(op.ID); err != nil || !ok {
				t.Fatal(err)
			}
			r, err := s.CreateAppRelease(model.AppRelease{TenantID: app.TenantID, AppID: app.ID, Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining, RuntimeID: "runtime", ResolvedImageRef: app.Spec.Image, SpecSnapshot: &app.Spec})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			r.PromotedAt = &now
			r, err = s.UpdateAppRelease(r)
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "missing_source" {
				if err := s.AppendAuditEvent(model.AuditEvent{TenantID: app.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller", Action: "app.release.promote", TargetType: "app_release", TargetID: r.ID, Metadata: map[string]string{"app_id": app.ID, "app_release_id": r.ID, "operation_id": op.ID, "mode": "safe_zero_downtime"}}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = s.CompleteManagedOperation(op.ID, "", "done"); err != nil {
				t.Fatal(err)
			}
			op, err = s.GetOperation(op.ID)
			if err != nil {
				t.Fatal(err)
			}
			stable, err := s.CreateAppRelease(model.AppRelease{TenantID: app.TenantID, AppID: app.ID, Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusServing})
			if err != nil {
				t.Fatal(err)
			}
			policy, err := s.UpsertAppTrafficPolicy(model.AppTrafficPolicy{TenantID: app.TenantID, AppID: app.ID, Mode: model.AppTrafficModeSingle, StableReleaseID: stable.ID, StableWeight: 100})
			if err != nil {
				t.Fatal(err)
			}
			w := model.AppReleaseWorkload{OperationID: op.ID, Namespace: "tenant", DeploymentName: "revision", DeploymentUID: "dep-uid", DeploymentGeneration: 1, ServiceName: "revision", ServiceUID: "svc-uid", ReleaseKey: "observed-key", RuntimeID: r.RuntimeID, ImageRef: r.ResolvedImageRef}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "policy_changed":
				copy := policy
				copy.StableReleaseID = r.ID
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
			case "operation_changed":
				op.UpdatedAt = op.UpdatedAt.Add(-time.Second)
			case "release_changed":
				copy := r
				copy.UpstreamURL = "http://changed"
				if _, err = s.UpdateAppRelease(copy); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				cancel()
			}
			if scenario == "concurrent_reactivate" {
				start := make(chan struct{})
				results := make(chan error, 2)
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					_, err := s.RetireStoppedHistoricalAppRelease(ctx, r, stable, policy, op, w)
					results <- err
				}()
				go func() {
					defer wg.Done()
					<-start
					p := policy
					p.StableReleaseID = r.ID
					_, err := s.UpsertAppTrafficPolicy(p)
					results <- err
				}()
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
					t.Fatalf("concurrent commits=%d", wins)
				}
				stored, err := s.GetAppRelease(app.TenantID, false, r.ID)
				if err != nil {
					t.Fatal(err)
				}
				if (stored.RevisionWorkload != nil) != AppReleaseIsRetired(stored) {
					t.Fatal("binding visible outside tombstone")
				}
				return
			}
			retired, err := s.RetireStoppedHistoricalAppRelease(ctx, r, stable, policy, op, w)
			if scenario != "valid" {
				if err == nil {
					t.Fatal("unsafe atomic retirement accepted")
				}
				stored, _ := s.GetAppRelease(app.TenantID, false, r.ID)
				if stored.RevisionWorkload != nil || AppReleaseIsRetired(stored) {
					t.Fatal("failed transaction left binding or retired state")
				}
				return
			}
			if err != nil || !AppReleaseIsRetired(retired) || retired.RevisionWorkload == nil || !reflect.DeepEqual(retired.SpecSnapshot, r.SpecSnapshot) {
				t.Fatal("atomic stopped retirement failed", err)
			}
			if _, err = s.UpdateAppRelease(r); err == nil {
				t.Fatal("old writer resurrected tombstone")
			}
		})
	}
}

func testHistoricalReleaseWorkloadMigration(t *testing.T, s *Store, app model.App) {
	for _, scenario := range []string{"previous", "failed", "final_spec_changed", "missing_promotion", "promotion_after_receipt", "wrong_mode", "missing_source", "wrong_actor", "wrong_app", "ambiguous_source", "stale_release", "stale_operation", "running_operation", "canceled", "concurrent"} {
		t.Run(scenario, func(t *testing.T) {
			op, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec, ExecutionMode: model.ExecutionModeManaged})
			if err != nil {
				t.Fatal(err)
			}
			if _, claimed, err := s.TryClaimPendingOperation(op.ID); err != nil || !claimed {
				t.Fatal("claim", err)
			}
			role, status := model.AppReleaseRolePrevious, model.AppReleaseStatusDraining
			action := "app.release.promote"
			if scenario == "failed" {
				role, status, action = model.AppReleaseRoleCandidate, model.AppReleaseStatusFailed, "app.release.abort.auto"
			}
			r, err := s.CreateAppRelease(model.AppRelease{TenantID: app.TenantID, AppID: app.ID, Role: role, Status: status, SpecSnapshot: &app.Spec, RuntimeID: "runtime", ResolvedImageRef: app.Spec.Image, DeploymentName: "canonical", ServiceName: "canonical"})
			if err != nil {
				t.Fatal(err)
			}
			if status == model.AppReleaseStatusDraining {
				now := time.Now().UTC()
				r.PromotedAt = &now
				r, err = s.UpdateAppRelease(r)
				if err != nil {
					t.Fatal(err)
				}
			}
			event := model.AuditEvent{TenantID: app.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller", Action: action, TargetType: "app_release", TargetID: r.ID,
				Metadata: map[string]string{"app_id": app.ID, "app_release_id": r.ID, "operation_id": op.ID, "mode": "safe_zero_downtime"}}
			if scenario == "wrong_actor" {
				event.ActorID = "unrelated-controller"
			}
			if scenario == "wrong_app" {
				event.Metadata["app_id"] = "other-app"
			}
			if scenario == "wrong_mode" {
				event.Metadata["mode"] = "other"
			}
			if scenario != "missing_source" {
				if err := s.AppendAuditEvent(event); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "ambiguous_source" {
				event.Metadata["operation_id"] = "another-operation"
				if err := s.AppendAuditEvent(event); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "failed" {
				_, err = s.FailOperation(op.ID, "canary gate failed")
			} else if scenario == "final_spec_changed" {
				final := app.Spec
				final.Env = map[string]string{"REVISION": "later"}
				_, err = s.CompleteManagedOperationWithResult(op.ID, "", "done", &final, nil)
			} else if scenario != "running_operation" {
				_, err = s.CompleteManagedOperation(op.ID, "", "done")
			}
			if err != nil {
				t.Fatal(err)
			}
			op, err = s.GetOperation(op.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.SetOperationControllerTiming(op.ID, []model.OperationControllerTimingSegment{{Name: "apply", DurationMilliseconds: 42}}); err != nil {
				t.Fatal(err)
			}
			op, err = s.GetOperation(op.ID)
			if err != nil || len(op.ControllerTimingSegments) != 1 {
				t.Fatal("missing timing facts", err)
			}
			if scenario == "missing_promotion" || scenario == "promotion_after_receipt" {
				r.PromotedAt = nil
				if scenario == "promotion_after_receipt" {
					future := time.Now().Add(time.Hour)
					r.PromotedAt = &future
				}
				r, err = s.UpdateAppRelease(r)
				if err != nil {
					t.Fatal(err)
				}
			}
			w := model.AppReleaseWorkload{OperationID: op.ID, Namespace: "tenant", DeploymentName: "revision", DeploymentUID: "dep-uid", DeploymentGeneration: 1, ServiceName: "revision", ServiceUID: "svc-uid", ReleaseKey: "key", RuntimeID: r.RuntimeID, ImageRef: r.ResolvedImageRef}
			id, sourceErr := s.FindAppReleaseWorkloadSource(context.Background(), r)
			invalidSource := scenario == "missing_source" || scenario == "wrong_actor" || scenario == "wrong_app" || scenario == "ambiguous_source" || scenario == "wrong_mode" || scenario == "missing_promotion" || scenario == "promotion_after_receipt"
			if invalidSource == (sourceErr == nil) || !invalidSource && id != op.ID {
				t.Fatal("unexpected source resolution", id, sourceErr)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}
			if scenario == "stale_release" {
				updated := r
				updated.ServiceName = "changed"
				if _, err := s.UpdateAppRelease(updated); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "stale_operation" {
				op.UpdatedAt = op.UpdatedAt.Add(-time.Second)
			}
			if scenario == "concurrent" {
				var wg sync.WaitGroup
				results := make(chan error, 2)
				for _, uid := range []string{"first", "second"} {
					wg.Add(1)
					go func(uid string) {
						defer wg.Done()
						copy := w
						copy.DeploymentUID = uid
						_, err := s.MigrateAppReleaseWorkload(ctx, r, op, copy)
						results <- err
					}(uid)
				}
				wg.Wait()
				close(results)
				wins := 0
				for err := range results {
					if err == nil {
						wins++
					}
				}
				if wins != 1 {
					t.Fatalf("concurrent migration winners=%d", wins)
				}
				return
			}
			bound, err := s.MigrateAppReleaseWorkload(ctx, r, op, w)
			if scenario != "previous" && scenario != "failed" && scenario != "final_spec_changed" {
				if err == nil {
					t.Fatal("invalid migration accepted")
				}
				stored, _ := s.GetAppRelease(app.TenantID, false, r.ID)
				if stored.RevisionWorkload != nil {
					t.Fatal("rejection wrote a binding")
				}
				return
			}
			if err != nil || bound.RevisionWorkload == nil || bound.RevisionWorkload.BoundAt.IsZero() {
				t.Fatal("binding failed", err)
			}
			copy := bound
			copy.RevisionWorkload, copy.UpdatedAt = nil, r.UpdatedAt
			if !reflect.DeepEqual(copy, r) {
				t.Fatal("migration changed serving/lifecycle data")
			}
			if _, err := s.MigrateAppReleaseWorkload(ctx, r, op, w); err == nil {
				t.Fatal("stale migration rewrote immutable binding")
			}
		})
	}
}

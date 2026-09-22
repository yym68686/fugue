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
}

func TestHistoricalReleaseWorkloadMigrationPostgres(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateAppReleaseWorkload(context.Background(), os.Getenv("FUGUE_TEST_DATABASE_URL")); err != nil {
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
}

func testHistoricalReleaseWorkloadMigration(t *testing.T, s *Store, app model.App) {
	for _, scenario := range []string{"previous", "failed", "missing_source", "wrong_actor", "wrong_app", "ambiguous_source", "stale_release", "stale_operation", "running_operation", "canceled", "concurrent"} {
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
			event := model.AuditEvent{TenantID: app.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller", Action: action, TargetType: "app_release", TargetID: r.ID,
				Metadata: map[string]string{"app_id": app.ID, "app_release_id": r.ID, "operation_id": op.ID}}
			if scenario == "wrong_actor" {
				event.ActorID = "unrelated-controller"
			}
			if scenario == "wrong_app" {
				event.Metadata["app_id"] = "other-app"
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
			w := model.AppReleaseWorkload{OperationID: op.ID, Namespace: "tenant", DeploymentName: "revision", DeploymentUID: "dep-uid", DeploymentGeneration: 1, ServiceName: "revision", ServiceUID: "svc-uid", ReleaseKey: "key", RuntimeID: r.RuntimeID, ImageRef: r.ResolvedImageRef}
			id, sourceErr := s.FindAppReleaseWorkloadSource(context.Background(), r)
			invalidSource := scenario == "missing_source" || scenario == "wrong_actor" || scenario == "wrong_app" || scenario == "ambiguous_source"
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
			if scenario != "previous" && scenario != "failed" {
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

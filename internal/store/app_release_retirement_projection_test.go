package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestReleaseRetirementStatePreservesDrainConfigurationAndAllFacts(t *testing.T) {
	for _, backend := range []string{"file", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var s *Store
			var tenant model.Tenant
			if backend == "postgres" {
				s = billingBatchPGStore(t)
				tenant = billingBatchPGTenant(t, s)
			} else {
				s = New(filepath.Join(t.TempDir(), "state.json"))
				if err := s.Init(); err != nil {
					t.Fatal(err)
				}
				var err error
				tenant, err = s.CreateTenant("retirement projection")
				if err != nil {
					t.Fatal(err)
				}
			}
			project, err := s.CreateProject(tenant.ID, "projection", "")
			if err != nil {
				t.Fatal(err)
			}
			app, err := s.CreateApp(tenant.ID, project.ID, "projection", "", model.AppSpec{Image: "registry.example/app:v1", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Ports: []int{8080}})
			if err != nil {
				t.Fatal(err)
			}
			spec := &model.AppSpec{Image: "registry.example/previous:v1", RuntimeID: "previous-runtime", Replicas: 2, Env: map[string]string{"CONFIG": strings.Repeat("config", 10000)}, Command: []string{"serve"}, Args: []string{"--previous"}, Files: []model.AppFile{{Path: "/config", Content: "original", Secret: true}}}
			roles := []string{model.AppReleaseRoleStable, model.AppReleaseRoleCandidate, model.AppReleaseRolePrevious, model.AppReleaseRoleRetired}
			statuses := []string{model.AppReleaseStatusCreating, model.AppReleaseStatusReady, model.AppReleaseStatusServing, model.AppReleaseStatusDraining, model.AppReleaseStatusFailed, model.AppReleaseStatusRetired}
			binding := &model.AppReleaseWorkload{DeploymentName: "immutable-deployment", DeploymentUID: "deployment-uid", ServiceName: "immutable-service", ServiceUID: "service-uid", ReleaseKey: "original-key"}
			created := 0
			for _, role := range roles {
				for _, status := range statuses {
					// The file backend rejects a partially retired lifecycle state.
					if (role == model.AppReleaseRoleRetired) != (status == model.AppReleaseStatusRetired) {
						continue
					}
					if backend == "postgres" && status == model.AppReleaseStatusRetired {
						// Seed the immutable historical record in one insert. Never
						// update an already-retired row to construct the fixture.
						specJSON, _ := json.Marshal(spec)
						bindingJSON, _ := json.Marshal(binding)
						if _, err := s.db.Exec(`INSERT INTO fugue_app_releases(id,tenant_id,app_id,role,status,spec_snapshot_json,revision_workload_json,created_at,updated_at) VALUES($1,$2,$3,'retired','retired',$4,$5,now(),now())`, model.NewID("retired_projection"), tenant.ID, app.ID, specJSON, bindingJSON); err != nil {
							t.Fatal(err)
						}
						created++
						continue
					}
					if _, err := s.CreateAppRelease(model.AppRelease{TenantID: tenant.ID, AppID: app.ID, Role: role, Status: status, SpecSnapshot: spec, DeploymentName: role + "-" + status, ServiceName: role + "-" + status, RuntimeID: spec.RuntimeID, ResolvedImageRef: spec.Image}); err != nil {
						t.Fatal(err)
					}
					created++
				}
			}
			for _, snapshot := range []*model.AppSpec{nil, {}} {
				if _, err := s.CreateAppRelease(model.AppRelease{TenantID: tenant.ID, AppID: app.ID, Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining, SpecSnapshot: snapshot}); err != nil {
					t.Fatal(err)
				}
			}
			filter := model.AppReleaseFilter{TenantID: tenant.ID, AppID: app.ID, IncludeRetired: true}
			full, err := s.ListAppReleases(filter)
			if err != nil || len(full) != created+2 {
				t.Fatal("incomplete fixture", len(full), err)
			}
			// A retired binding must survive projection even though its executable
			// snapshot is no longer consumed by the cleanup retry.
			var bound model.AppRelease
			for _, release := range full {
				if release.Status == model.AppReleaseStatusRetired {
					bound = release
				}
			}
			if bound.ID == "" {
				t.Fatal("retired fixture missing")
			}
			if backend == "file" {
				if err := s.withLockedState(true, func(state *model.State) error {
					for i := range state.AppReleases {
						if state.AppReleases[i].ID == bound.ID {
							state.AppReleases[i].RevisionWorkload = binding
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			full, err = s.ListAppReleases(filter)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.ListAppReleaseRetirementState(context.Background(), filter)
			if err != nil || len(got) != len(full) {
				t.Fatal("retirement view omitted releases", len(got), err)
			}
			for i, want := range full {
				if !AppReleaseAwaitingDrain(want) {
					want.SpecSnapshot = nil
				}
				if !reflect.DeepEqual(got[i], want) {
					t.Fatalf("changed facts, ordering or required config for %s/%s", want.Role, want.Status)
				}
			}
			after, err := s.ListAppReleases(filter)
			if err != nil || !reflect.DeepEqual(after, full) {
				t.Fatal("retirement read changed retained snapshots", err)
			}
			filter.TenantID = "other-tenant"
			if other, err := s.ListAppReleaseRetirementState(context.Background(), filter); err != nil || len(other) != 0 {
				t.Fatal("cross-tenant retirement read", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := s.ListAppReleaseRetirementState(ctx, filter); !errors.Is(err, context.Canceled) {
				t.Fatal("ignored canceled retirement read", err)
			}
		})
	}
}

func TestReleaseRetirementPostgresRejectsInvalidRequiredConfiguration(t *testing.T) {
	s := billingBatchPGStore(t)
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "invalid-config", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "invalid-config", "", model.AppSpec{Image: "registry.example/app:v1", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	release, err := s.CreateAppRelease(model.AppRelease{TenantID: tenant.ID, AppID: app.ID, Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_app_releases SET spec_snapshot_json='{"replicas":[]}' WHERE id=$1`, release.ID); err != nil {
		t.Fatal(err)
	}
	filter := model.AppReleaseFilter{TenantID: tenant.ID, AppID: app.ID, IncludeRetired: true}
	if _, err := s.ListAppReleaseRetirementState(context.Background(), filter); err == nil {
		t.Fatal("invalid drain configuration was discarded")
	}
	// Retired cleanup does not execute the snapshot, but must still reject a
	// malformed immutable workload binding before any names can be deleted.
	if _, err := s.db.Exec(`INSERT INTO fugue_app_releases(id,tenant_id,app_id,role,status,revision_workload_json,created_at,updated_at) VALUES($1,$2,$3,'retired','retired','{"deployment_uid":[]}',now(),now())`, model.NewID("invalid_retired_projection"), tenant.ID, app.ID); err != nil {
		t.Fatal(err)
	}
	filter.Role = model.AppReleaseRoleRetired
	if _, err := s.ListAppReleaseRetirementState(context.Background(), filter); err == nil {
		t.Fatal("invalid retired workload identity was discarded")
	}
}

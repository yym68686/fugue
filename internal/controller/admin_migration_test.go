package controller

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestAdministratorMigrationAuthorityAndSafetyBoundary(t *testing.T) {
	t.Parallel()
	state := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := state.CreateTenant("Operations")
	if err != nil {
		t.Fatal(err)
	}
	admin, _, err := state.CreateAPIKey(tenant.ID, "operator", []string{"platform.admin"})
	if err != nil {
		t.Fatal(err)
	}
	regular, _, err := state.CreateAPIKey(tenant.ID, "deployer", []string{"app.migrate"})
	if err != nil {
		t.Fatal(err)
	}
	svc := &Service{Store: state, Renderer: runtime.Renderer{}}
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueWorkspaceRWO)
	desired := current
	desired.Spec.RuntimeID = "destination"
	for _, test := range []struct {
		name, actor, id, kind string
		allow                 bool
	}{
		{"root migration", model.ActorTypeBootstrap, "bootstrap-admin", model.OperationTypeMigrate, true},
		{"automated bootstrap", model.ActorTypeBootstrap, "automatic-recovery", model.OperationTypeMigrate, false},
		{"admin key", model.ActorTypeAPIKey, admin.ID, model.OperationTypeMigrate, true},
		{"scoped key", model.ActorTypeAPIKey, regular.ID, model.OperationTypeMigrate, false},
		{"unknown key", model.ActorTypeAPIKey, "missing", model.OperationTypeMigrate, false},
		{"ordinary deployment", model.ActorTypeBootstrap, "bootstrap-admin", model.OperationTypeDeploy, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			op := model.Operation{Type: test.kind, RequestedByType: test.actor, RequestedByID: test.id}
			decision := svc.zeroDowntimeRolloutGuardDecision(op, current, desired, runtime.SchedulingConstraints{})
			if decision.AdministratorMigration != test.allow || decision.Refused == test.allow {
				t.Fatalf("decision=%+v", decision)
			}
		})
	}
	op := model.Operation{Type: model.OperationTypeMigrate, RequestedByType: model.ActorTypeBootstrap, RequestedByID: "bootstrap-admin"}
	desired.Spec.Replicas = 2
	if decision := svc.zeroDowntimeRolloutGuardDecision(op, current, desired, runtime.SchedulingConstraints{}); !decision.Refused {
		t.Fatal("administrator migration bypassed single-writer data protection")
	}
	if _, err := state.DisableAPIKey(admin.ID); err != nil {
		t.Fatal(err)
	}
	if svc.administratorMigrationOperation(model.Operation{Type: model.OperationTypeMigrate, RequestedByType: model.ActorTypeAPIKey, RequestedByID: admin.ID}) {
		t.Fatal("revoked key retained administrator migration authority")
	}
}

func TestAdministratorMigrationLowLevelScopeCannotEscapeAppIdentity(t *testing.T) {
	t.Parallel()
	object := map[string]any{"metadata": map[string]any{
		"labels":      map[string]string{runtime.FugueLabelAppID: "sample", runtime.FugueLabelTenantID: "owner"},
		"annotations": map[string]string{runtime.FugueAnnotationZeroDowntimeRequired: "true"},
	}}
	ctx := context.WithValue(context.Background(), adminMigrationApplyKey{}, adminMigrationApplyIdentity{"sample", "owner"})
	if deploymentRequiresZeroDowntimeForApply(ctx, object) {
		t.Fatal("operation maintenance authority lost")
	}
	for _, identity := range []adminMigrationApplyIdentity{{"other", "owner"}, {"sample", "other"}, {}} {
		ctx := context.WithValue(context.Background(), adminMigrationApplyKey{}, identity)
		if !deploymentRequiresZeroDowntimeForApply(ctx, object) {
			t.Fatal("maintenance authority escaped app or tenant", identity)
		}
	}
	if !deploymentRequiresZeroDowntimeForApply(context.Background(), object) {
		t.Fatal("ordinary reconciliation gained maintenance authority")
	}
}

func TestRevisionServiceSelectorAcceptsOnlyKnownSchemas(t *testing.T) {
	t.Parallel()
	stable := map[string]string{runtime.FugueLabelManagedBy: "fugue", runtime.FugueLabelTenantID: "tenant", runtime.FugueLabelAppID: "app", runtime.FugueLabelAppWorkload: "revision", runtime.FugueLabelAppReleaseID: "release"}
	legacy := make(map[string]string)
	for k, v := range stable {
		legacy[k] = v
	}
	legacy[runtime.FugueLabelName] = "display"
	legacy[runtime.FugueLabelProjectID] = "project"
	legacy[runtime.FugueLabelOwnerAppID] = "app"
	service := map[string]any{"spec": map[string]any{"selector": stable}}
	deployment := map[string]any{"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"labels": legacy}}}}
	if !revisionServiceSelectorMatches(stable, service, deployment) || !revisionServiceSelectorMatches(legacy, service, deployment) {
		t.Fatal("known selector rejected")
	}
	for _, field := range []string{runtime.FugueLabelTenantID, runtime.FugueLabelAppWorkload, runtime.FugueLabelAppReleaseID, runtime.FugueLabelName, "extra"} {
		invalid := make(map[string]string)
		for k, v := range legacy {
			invalid[k] = v
		}
		invalid[field] = "different"
		if revisionServiceSelectorMatches(invalid, service, deployment) {
			t.Fatal("unsafe selector accepted", field)
		}
	}
	delete(legacy, runtime.FugueLabelAppReleaseID)
	if revisionServiceSelectorMatches(legacy, service, deployment) {
		t.Fatal("release isolation lost")
	}
}

func TestAdministratorMigrationResumptionRequiresStoredActiveAuthority(t *testing.T) {
	t.Parallel()
	st := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := st.CreateTenant("Maintenance")
	project, _ := st.CreateProject(tenant.ID, "applications", "")
	rt, _, _ := st.CreateRuntime(tenant.ID, "source", model.RuntimeTypeManagedOwned, "", nil)
	target, _, _ := st.CreateRuntime(tenant.ID, "destination", model.RuntimeTypeManagedOwned, "", nil)
	app, err := st.CreateApp(tenant.ID, project.ID, "sample", "", model.AppSpec{RuntimeID: rt.ID, Image: "example/sample:stable", Replicas: 1})
	if err != nil {
		t.Fatal(err)
	}
	desired := app.Spec
	desired.RuntimeID = target.ID
	op, err := st.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeMigrate, RequestedByType: model.ActorTypeBootstrap, RequestedByID: "bootstrap-admin", SourceRuntimeID: rt.ID, TargetRuntimeID: target.ID, DesiredSpec: &desired})
	if err != nil {
		t.Fatal(err)
	}
	svc := &Service{Store: st}
	svc.newKubeClient = func(string) (*kubeClient, error) {
		t.Fatal("administrator destination was pinned back to the current node")
		return nil, nil
	}
	explicit := withManagedAppApplySource(context.Background(), managedAppApplySourceOperation, op.ID)
	maintenance := zeroDowntimeGuardTestApp(model.AppStorageClassFugueWorkspaceRWO)
	maintenance.ID, maintenance.TenantID = app.ID, app.TenantID
	maintenance.Spec.RuntimeID = target.ID
	maintenance.Spec.RolloutIntent = model.AppRolloutIntentOnlineLifecycleUpdate
	base := runtime.SchedulingConstraints{NodeSelector: map[string]string{"runtime": target.ID}}
	if got := svc.onlineDurableRolloutScheduling(explicit, app, maintenance, base); !reflect.DeepEqual(got, base) {
		t.Fatal("administrator destination changed", got)
	}
	for _, ctx := range []context.Context{context.Background(), explicit} {
		if found, ok := svc.activeAdministratorMigration(ctx, app); !ok || found.ID != op.ID {
			t.Fatal("active operation not resumed")
		}
		other := app
		other.TenantID = "other"
		if _, ok := svc.activeAdministratorMigration(ctx, other); ok {
			t.Fatal("operation escaped tenant")
		}
	}
	unknown := withManagedAppApplySource(context.Background(), managedAppApplySourceOperation, "missing")
	if _, ok := svc.activeAdministratorMigration(unknown, app); ok {
		t.Fatal("explicit missing operation inherited another operation")
	}
	if _, err := st.FailOperation(op.ID, "test completion"); err != nil {
		t.Fatal(err)
	}
	for _, ctx := range []context.Context{context.Background(), explicit} {
		if _, ok := svc.activeAdministratorMigration(ctx, app); ok {
			t.Fatal("completed operation retained maintenance authority")
		}
	}
}

package store

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestImportDeployPreservesInterveningConfiguration(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("configuration isolation")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "site", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "site", "", model.AppSpec{
		Image: "registry.example/site:old", Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID,
		Env: map[string]string{"EDIT": "before", "REMOVE": "before", "KEEP": "before"},
	})
	if err != nil {
		t.Fatal(err)
	}
	requested := *cloneAppSpec(&app.Spec)
	requested.Env["BUILD_SETTING"] = "requested"
	parent, err := s.CreateOperation(model.Operation{
		TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, DesiredSpec: &requested,
		DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/site:new"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if parent.ConfigBaseSpec == nil || !reflect.DeepEqual(*parent.ConfigBaseSpec, app.Spec) {
		t.Fatal("import did not capture its acceptance-time config")
	}
	changed := *cloneAppSpec(&app.Spec)
	changed.Env["EDIT"] = "after"
	changed.Env["NEW"] = "after"
	delete(changed.Env, "REMOVE")
	changed.Replicas = 2
	configOp, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &changed})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteManagedOperation(configOp.ID, "", "configuration recovered"); err != nil {
		t.Fatal(err)
	}
	built := *cloneAppSpec(parent.DesiredSpec)
	built.Image = "registry.example/site@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	built.RestartToken = "restart_new"
	deploy, err := s.CreateDeployOperationAfterImport(parent.ID, model.Operation{
		TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &built,
		DesiredSource: parent.DesiredSource,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantEnv := map[string]string{"EDIT": "after", "NEW": "after", "KEEP": "before", "BUILD_SETTING": "requested"}
	if !reflect.DeepEqual(deploy.DesiredSpec.Env, wantEnv) || deploy.DesiredSpec.Replicas != 2 || deploy.DesiredSpec.Image != built.Image || deploy.DesiredSpec.RestartToken != "restart_new" {
		t.Fatalf("build reverted current configuration: %+v", deploy.DesiredSpec)
	}
	if _, err := s.CompleteManagedOperation(deploy.ID, "", "built artifact activated"); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetApp(app.ID)
	if err != nil || !reflect.DeepEqual(current.Spec.Env, wantEnv) {
		t.Fatalf("wrong persisted configuration: %+v %v", current.Spec.Env, err)
	}
	storedParent, err := s.GetOperation(parent.ID)
	if err != nil || storedParent.ConfigBaseSpec.Env["EDIT"] != "before" {
		t.Fatal("rebase mutated the immutable baseline")
	}
}

func TestImportConfigRebaseNestedDeletionAndImmutableInputs(t *testing.T) {
	base := model.AppSpec{Image: "old", Env: map[string]string{"A": "old"}, Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 128}}
	desired := *cloneAppSpec(&base)
	desired.Image = "new"
	desired.Resources.MemoryMebibytes = 256
	desired.Env["B"] = "build"
	current := *cloneAppSpec(&base)
	current.Resources.CPUMilliCores = 200
	current.Env = nil
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired}
	if err := rebaseImportDeployConfiguration(&op, model.App{Spec: current}, &base); err != nil {
		t.Fatal(err)
	}
	if op.DesiredSpec.Env != nil || op.DesiredSpec.Resources.CPUMilliCores != 200 || op.DesiredSpec.Resources.MemoryMebibytes != 256 || op.DesiredSpec.Image != "new" {
		t.Fatalf("wrong nested rebase: %+v", op.DesiredSpec)
	}
	if base.Resources.CPUMilliCores != 100 || desired.Env["B"] != "build" || desired.Resources.CPUMilliCores != 100 {
		t.Fatal("rebase mutated inputs")
	}
}

func TestImportConfigBasePersistsWithSourceEnvelope(t *testing.T) {
	want := model.AppSpec{Image: "registry.example/app:old", Env: map[string]string{"CONFIG": "value"}}
	model.ApplyAppSpecDefaults(&want)
	raw, err := marshalOperationSourceState(model.Operation{ConfigBaseSpec: &want, DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "new"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeOperationConfigBase(raw)
	if err != nil || !reflect.DeepEqual(got, &want) {
		t.Fatalf("baseline roundtrip: %+v %v", got, err)
	}
	source, _, err := decodeOperationSourceState(raw)
	if err != nil || source.ImageRef != "new" {
		t.Fatal("baseline changed source decoding")
	}
	legacy, _ := json.Marshal(model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "old"})
	if got, err := decodeOperationConfigBase(legacy); err != nil || got != nil {
		t.Fatal("legacy source fabricated a baseline")
	}
}

func TestLegacyImportRemainsExecutableWithoutInventingBaseline(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("legacy import")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "legacy", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "legacy", "", model.AppSpec{Image: "registry.example/legacy:old", Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, DesiredSpec: cloneAppSpec(&app.Spec), DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/legacy:new"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		state.Operations[findOperation(state, parent.ID)].ConfigBaseSpec = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	built := *cloneAppSpec(&app.Spec)
	built.Image = "registry.example/legacy:new"
	deploy, err := s.CreateDeployOperationAfterImport(parent.ID, model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &built})
	if err != nil || deploy.DesiredSpec.Image != built.Image {
		t.Fatalf("legacy import was blocked: %v", err)
	}
}

func TestImportConfigurationBaselinePostgres(t *testing.T) {
	s := billingBatchPGStore(t)
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "config", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "config", "", model.AppSpec{Image: "registry.example/config:old", Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID, Env: map[string]string{"CONFIG": "old"}})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, DesiredSpec: cloneAppSpec(&app.Spec), DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/config:new"}})
	if err != nil {
		t.Fatal(err)
	}
	parent, err = s.GetOperation(parent.ID)
	if err != nil || parent.ConfigBaseSpec == nil || parent.ConfigBaseSpec.Env["CONFIG"] != "old" {
		t.Fatalf("missing persisted acceptance baseline: %v", err)
	}
	changed := *cloneAppSpec(&app.Spec)
	changed.Env["CONFIG"] = "recovered"
	raw, _ := json.Marshal(changed)
	if _, err := s.db.Exec(`UPDATE fugue_apps SET spec_json=$2 WHERE id=$1`, app.ID, raw); err != nil {
		t.Fatal(err)
	}
	built := *cloneAppSpec(parent.DesiredSpec)
	built.Image = "registry.example/config:new"
	deploy, err := s.CreateDeployOperationAfterImport(parent.ID, model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &built})
	if err != nil {
		t.Fatal(err)
	}
	deploy, err = s.GetOperation(deploy.ID)
	if err != nil || deploy.DesiredSpec.Env["CONFIG"] != "recovered" || deploy.DesiredSpec.Image != built.Image {
		t.Fatalf("stale persisted deployment: %+v %v", deploy.DesiredSpec, err)
	}
	// Legacy persisted operations use both SQL NULL and JSON null. Removing a
	// private envelope field must never apply jsonb deletion to a scalar.
	if _, err := s.db.Exec(`UPDATE fugue_operations SET desired_source_json='null'::jsonb WHERE id=$1`, deploy.ID); err != nil {
		t.Fatal(err)
	}
	images, err := s.ListImageOperationsByApps(tenant.ID, false, []string{app.ID})
	if err != nil || len(images[app.ID]) != 2 {
		t.Fatalf("image history failed with JSON-null source: %d %v", len(images[app.ID]), err)
	}
	for _, operation := range images[app.ID] {
		if operation.ConfigBaseSpec != nil {
			t.Fatal("image history retained configuration baseline")
		}
	}
}

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
)

func routeBusinessExecutionFixture() model.App {
	return model.App{ID: "route-app", TenantID: "tenant", Spec: model.AppSpec{
		Image: "registry.example/app:v1", Ports: []int{8080}, Replicas: 2, RuntimeID: "runtime",
		Env:          map[string]string{model.AppEdgeRequestBodyPoliciesEnv: `[{"name":"upload"}]`, "CONFIG": "retained"},
		GeneratedEnv: map[string]model.AppGeneratedEnvSpec{"GENERATED": {Generate: "random", Length: 32}},
		Files:        []model.AppFile{{Path: "/config", Content: strings.Repeat("private-file", 100000), Secret: true}},
		Command:      []string{"run"}, Args: []string{"--serve"},
	}}
}

func assertRouteExecutionProjection(t *testing.T, got, full model.App) {
	t.Helper()
	if len(got.Spec.Files) != 0 || len(got.Spec.GeneratedEnv) != 0 || len(got.Spec.Command) != 0 || len(got.Spec.Args) != 0 {
		t.Fatal("compiler app view kept execution payload")
	}
	want := full
	want.Spec.Files, want.Spec.GeneratedEnv, want.Spec.Command, want.Spec.Args = nil, nil, nil, nil
	if !reflect.DeepEqual(got.Spec, want.Spec) {
		t.Fatal("compiler app view changed retained fields")
	}
	if got.Spec.Env[model.AppEdgeRequestBodyPoliciesEnv] != full.Spec.Env[model.AppEdgeRequestBodyPoliciesEnv] {
		t.Fatal("route request-body constraint was omitted")
	}
}

func TestRouteBusinessSnapshotFileOmitsExecutionPayloadWithoutChangingConfiguration(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	s := New(stateFile)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	app := routeBusinessExecutionFixture()
	if err := s.withLockedState(true, func(state *model.State) error {
		state.Apps = []model.App{app}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	full, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.CaptureRouteBusinessSnapshot(context.Background())
	if err != nil || len(first.Apps) != 1 {
		t.Fatal("capture", err)
	}
	assertRouteExecutionProjection(t, first.Apps[0], full)
	first.Apps[0].Spec.Env["CONFIG"] = "caller mutation"
	second, err := s.CaptureRouteBusinessSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertRouteExecutionProjection(t, second.Apps[0], full)
	after, err := os.ReadFile(stateFile)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("durable configuration changed", err)
	}
	retained, err := s.GetApp(app.ID)
	if err != nil || !reflect.DeepEqual(retained.Spec, full.Spec) {
		t.Fatal(err)
	}
}

func TestRouteBusinessSnapshotPostgresOmitsExecutionPayloadAndRetainsRouteEnvironment(t *testing.T) {
	s := billingBatchPGStore(t)
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "route-projection", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "route-projection", "", model.AppSpec{Image: "registry.example/app:v1", Ports: []int{8080}, Replicas: 1})
	if err != nil {
		t.Fatal(err)
	}
	spec := routeBusinessExecutionFixture().Spec
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE fugue_apps SET spec_json=$2 WHERE id=$1`, app.ID, raw); err != nil {
		t.Fatal(err)
	}
	full, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.CaptureRouteBusinessSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var got *model.App
	for i := range snapshot.Apps {
		if snapshot.Apps[i].ID == app.ID {
			got = &snapshot.Apps[i]
		}
	}
	if got == nil {
		t.Fatal("app missing")
	}
	assertRouteExecutionProjection(t, *got, full)
	// Read the actual SQL projection, before scanApp or normalize can discard
	// fields, so this also guards the database-to-API transfer reduction.
	var projected []byte
	if err := s.db.QueryRow(`SELECT spec_json FROM (SELECT `+routeBusinessAppSelectColumns+` FROM fugue_apps WHERE id=$1) projected`, app.ID).Scan(&projected); err != nil {
		t.Fatal(err)
	}
	var projectedSpec model.AppSpec
	if err := json.Unmarshal(projected, &projectedSpec); err != nil {
		t.Fatal(err)
	}
	assertRouteExecutionProjection(t, model.App{Spec: projectedSpec}, model.App{Spec: spec})
	if len(projected)*100 >= len(raw) {
		t.Fatalf("large execution payload crossed SQL projection: %d of %d bytes", len(projected), len(raw))
	}
	t.Logf("spec SQL result: %d -> %d bytes", len(raw), len(projected))
	var persisted []byte
	if err := s.db.QueryRow(`SELECT spec_json FROM fugue_apps WHERE id=$1`, app.ID).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	var retained model.AppSpec
	if err := json.Unmarshal(persisted, &retained); err != nil || !reflect.DeepEqual(retained, spec) {
		t.Fatal("configuration mutation", err)
	}
	for _, tc := range []struct {
		name, spec string
		valid      bool
	}{
		{"null", `null`, true},
		{"empty object", `{}`, true},
		{"array", `[]`, false},
		{"scalar", `"invalid"`, false},
		{"invalid route environment", `{"env":42}`, false},
		{"invalid ports", `{"ports":"invalid"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.db.Exec(`UPDATE fugue_apps SET spec_json=$2 WHERE id=$1`, app.ID, tc.spec); err != nil {
				t.Fatal(err)
			}
			full, fullErr := s.GetApp(app.ID)
			snapshot, snapshotErr := s.CaptureRouteBusinessSnapshot(context.Background())
			if (fullErr == nil) != tc.valid || (snapshotErr == nil) != tc.valid {
				t.Fatalf("decoder behavior changed: full=%v snapshot=%v", fullErr, snapshotErr)
			}
			if !tc.valid {
				if snapshot.Revision != "" || len(snapshot.Apps) != 0 {
					t.Fatal("failed capture returned a partial snapshot")
				}
				return
			}
			for _, projected := range snapshot.Apps {
				if projected.ID == app.ID {
					assertRouteExecutionProjection(t, projected, full)
					return
				}
			}
			t.Fatal("valid app missing")
		})
	}
}

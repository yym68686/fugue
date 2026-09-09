package store

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestAppSummaryPostgresPreservesReadFieldsAndFullConfiguration(t *testing.T) {
	s := billingBatchPGStore(t)
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "summary", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "summary", "", model.AppSpec{Image: "registry.example/app:v1", Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID, Resources: &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 512}})
	if err != nil {
		t.Fatal(err)
	}
	spec := app.Spec
	spec.Env = map[string]string{"TOKEN": "private"}
	spec.Files = []model.AppFile{{Path: "/config", Content: strings.Repeat("value", 10000), Secret: true}}
	spec.Command = []string{"run"}
	spec.Args = []string{"--serve"}
	raw, _ := json.Marshal(spec)
	if _, err := s.db.Exec(`UPDATE fugue_apps SET spec_json=$2 WHERE id=$1`, app.ID, raw); err != nil {
		t.Fatal(err)
	}
	full, err := s.ListApps(tenant.ID, false)
	if err != nil || len(full) != 1 {
		t.Fatal(err)
	}
	summary, err := s.ListAppSummaries(tenant.ID, false, true)
	if err != nil || len(summary) != 1 {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(summary[0], AppReadSummary(full[0])) {
		t.Fatal("summary changed retained app fields")
	}
	measured, timing, err := s.ListAppSummariesWithTiming(tenant.ID, false, true)
	if err != nil || !reflect.DeepEqual(measured, summary) {
		t.Fatalf("measured read changed response: %v", err)
	}
	if timing.Query <= 0 || timing.Rows <= 0 || timing.Decode <= 0 || timing.Services <= 0 {
		t.Fatalf("missing read stages: %+v", timing)
	}
	if full[0].Spec.Env["TOKEN"] != "private" || len(full[0].Spec.Files[0].Content) != 50000 {
		t.Fatal("summary mutated full configuration")
	}
	foreign, err := s.ListAppSummaries("foreign", false, true)
	if err != nil || len(foreign) != 0 {
		t.Fatal("summary crossed tenant boundary")
	}
	if _, err := s.db.Exec(`UPDATE fugue_apps SET spec_json='null'::jsonb WHERE id=$1`, app.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListAppSummaries(tenant.ID, false, false); err != nil {
		t.Fatalf("JSON-null config failed: %v", err)
	}
}

package store

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestImageCandidateOperationsConfigOnlyExcludesConfiguration(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "candidate-projection", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "candidate-projection", "", model.AppSpec{Image: "registry.example/app:v1", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seed, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, DesiredSpec: cloneAppSpec(&app.Spec), DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: app.Spec.Image}})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < 4; i++ {
		id := seed.ID
		if i > 0 {
			id = model.NewID("history")
			if _, err := s.db.Exec(`INSERT INTO fugue_operations SELECT (jsonb_populate_record(NULL::fugue_operations,to_jsonb(o) || jsonb_build_object('id',$2::text))).* FROM fugue_operations o WHERE id=$1`, seed.ID, id); err != nil {
				t.Fatal(err)
			}
		}
		raw, err := marshalOperationSourceState(model.Operation{ConfigBaseSpec: &model.AppSpec{Env: map[string]string{"PAYLOAD": strings.Repeat(string(rune('a'+i)), 1<<20)}}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		status := []string{model.OperationStatusPending, model.OperationStatusCompleted, model.OperationStatusCompleted, model.OperationStatusFailed}[i]
		if _, err := s.db.Exec(`UPDATE fugue_operations SET desired_source_json=$3::jsonb,created_at=$2,updated_at=$2,status=$4 WHERE id=$1`, id, base.Add(time.Duration(i)*time.Minute), raw, status); err != nil {
			t.Fatal(err)
		}
	}
	assertProjection := func(t *testing.T) {
		t.Helper()
		var oversized, configKeys int
		if err := s.db.QueryRow(`SELECT count(*) FILTER (WHERE octet_length(source::text)>1024),count(*) FILTER (WHERE source ? 'config_base_spec') FROM fugue_image_candidate_operations WHERE app_id=$1`, app.ID).Scan(&oversized, &configKeys); err != nil {
			t.Fatal(err)
		}
		if oversized != 0 || configKeys != 0 {
			t.Fatalf("image candidate projection retained deployment configuration: oversized=%d config_keys=%d", oversized, configKeys)
		}
		all, err := s.ListImageOperationsByApps(tenant.ID, false, []string{app.ID})
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(all[app.ID], func(i, j int) bool { return all[app.ID][i].CreatedAt.Before(all[app.ID][j].CreatedAt) })
		want := CompactImageCandidateOperations(all[app.ID])
		got, err := s.ListImageCandidateOperationsByApps(tenant.ID, false, []string{app.ID})
		if err != nil || !reflect.DeepEqual(got[app.ID], want) {
			t.Fatalf("candidate folding changed: got=%+v want=%+v err=%v", got[app.ID], want, err)
		}
		foundCompleted := false
		for _, op := range got[app.ID] {
			foundCompleted = foundCompleted || op.ID == ids[2] && op.Status == model.OperationStatusCompleted
		}
		if !foundCompleted {
			t.Fatal("latest completed provenance disappeared between incomplete attempts")
		}
		var sourceBytes int
		if err := s.db.QueryRow(`SELECT octet_length(desired_source_json->'config_base_spec'->'env'->>'PAYLOAD') FROM fugue_operations WHERE id=$1`, ids[0]).Scan(&sourceBytes); err != nil || sourceBytes != 1<<20 {
			t.Fatalf("authoritative configuration changed: bytes=%d err=%v", sourceBytes, err)
		}
	}
	t.Run("trigger", assertProjection)
	// Simulate rows written by the previous trigger, then exercise the actual
	// idempotent bootstrap backfill instead of repairing rows in the test.
	if _, err := s.db.Exec(`UPDATE fugue_image_candidate_operations p SET source=o.desired_source_json FROM fugue_operations o WHERE p.operation_id=o.id AND o.app_id=$1`, app.ID); err != nil {
		t.Fatal(err)
	}
	var backfill string
	for _, statement := range postgresSchemaStatements {
		if strings.HasPrefix(statement, "INSERT INTO fugue_image_candidate_operations (") {
			backfill = statement
		}
	}
	if backfill == "" {
		t.Fatal("candidate projection bootstrap backfill missing")
	}
	for i := 0; i < 2; i++ {
		if _, err := s.db.Exec(backfill); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("backfill", assertProjection)
	// Legacy unwrapped source fields must survive alongside the removed config.
	if _, err := s.db.Exec(`UPDATE fugue_operations SET desired_source_json=desired_source_json || jsonb_build_object('type','docker-image','image_ref','registry.example/app:v2') WHERE id=$1`, ids[1]); err != nil {
		t.Fatal(err)
	}
	t.Run("legacy_source", assertProjection)
}

func TestImageCandidateCompactionRetainsLatestCompletedProvenance(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ops := make([]model.Operation, 4)
	for i, status := range []string{model.OperationStatusPending, model.OperationStatusCompleted, model.OperationStatusCompleted, model.OperationStatusFailed} {
		ops[i] = model.Operation{ID: string(rune('a' + i)), AppID: "app", Status: status, CreatedAt: base.Add(time.Duration(i) * time.Minute), UpdatedAt: base.Add(time.Duration(i) * time.Minute), DesiredSpec: &model.AppSpec{Image: "registry.example/app@sha256:" + strings.Repeat("a", 64)}}
	}
	got := CompactImageCandidateOperations(ops)
	want := []model.Operation{ops[0], ops[2], ops[3]}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("last completed provenance lost: got=%+v want=%+v", got, want)
	}
}

package store

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestImageCandidateOperationsPostgresPreservesRepresentativesAndNull(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "images", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "images", "", model.AppSpec{Image: "registry.example/app:v1", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, DesiredSpec: cloneAppSpec(&app.Spec), DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/app:v1"}})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < 20; i++ {
		duplicate := cloneOperation(parent)
		duplicate.ID = model.NewID("history")
		duplicate.CreatedAt = parent.CreatedAt.Add(time.Duration(i+1) * time.Second)
		duplicate.UpdatedAt = parent.CreatedAt.Add(time.Duration((i*7)%11) * time.Minute)
		if _, err := tx.Exec(`INSERT INTO fugue_operations
SELECT (jsonb_populate_record(NULL::fugue_operations, to_jsonb(o) || jsonb_build_object('id',$2::text,'created_at',$3::timestamptz,'updated_at',$4::timestamptz))).*
FROM fugue_operations o WHERE id=$1`, parent.ID, duplicate.ID, duplicate.CreatedAt, duplicate.UpdatedAt); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Preserve JSON null and legacy unwrapped source objects in the same scan.
	if _, err := s.db.Exec(`UPDATE fugue_operations SET desired_source_json='null'::jsonb WHERE id=$1`, parent.ID); err != nil {
		t.Fatal(err)
	}
	all, err := s.ListImageOperationsByApps(tenant.ID, false, []string{app.ID})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(all[app.ID], func(i, j int) bool { return all[app.ID][i].CreatedAt.Before(all[app.ID][j].CreatedAt) })
	want := CompactImageCandidateOperations(all[app.ID])
	got, err := s.ListImageCandidateOperationsByApps(tenant.ID, false, []string{app.ID})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got[app.ID], want) {
		t.Fatalf("wrong SQL candidate projection: got=%+v want=%+v", got[app.ID], want)
	}
	foreign, err := s.ListImageCandidateOperationsByApps("foreign", false, []string{app.ID})
	if err != nil || len(foreign) != 0 {
		t.Fatal("tenant isolation failed")
	}
	measured, stages, err := s.ListImageCandidateOperationsByAppsWithTiming(tenant.ID, false, []string{app.ID})
	if err != nil || !reflect.DeepEqual(measured, got) {
		t.Fatalf("timed projection changed candidates: %v", err)
	}
	if stages.Acquire <= 0 || stages.Query <= 0 || stages.Rows <= 0 || stages.Decode <= 0 {
		t.Fatalf("missing disjoint timing stages: %+v", stages)
	}

	// Both SQL-null removal and a legacy writer's subsequent update must remain
	// transactional with the authoritative operation.
	if _, err := s.db.Exec(`UPDATE fugue_operations SET desired_source_json=NULL WHERE id=$1`, parent.ID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_image_candidate_operations WHERE operation_id=$1`, parent.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("SQL null retained an obsolete candidate: %d %v", count, err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_operations SET desired_source_json='{"type":"docker-image","image_ref":"registry.example/app:v2"}'::jsonb, desired_spec_json=jsonb_set(desired_spec_json,'{image}','"registry.example/app:v2"'::jsonb), completed_at=now() WHERE id=$1`, parent.ID); err != nil {
		t.Fatal(err)
	}
	var image, source string
	if err := s.db.QueryRow(`SELECT image,source->>'image_ref' FROM fugue_image_candidate_operations WHERE operation_id=$1`, parent.ID).Scan(&image, &source); err != nil || image != "registry.example/app:v2" || source != image {
		t.Fatalf("legacy update did not refresh both references: %q %q %v", image, source, err)
	}
	rollback, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rollback.Exec(`UPDATE fugue_operations SET desired_source_json=NULL WHERE id=$1`, parent.ID); err != nil {
		_ = rollback.Rollback()
		t.Fatal(err)
	}
	if err := rollback.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_image_candidate_operations WHERE operation_id=$1`, parent.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("rolled-back write changed the projection: %d %v", count, err)
	}
	if _, err := s.db.Exec(`DELETE FROM fugue_operations WHERE id=$1`, parent.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_image_candidate_operations WHERE operation_id=$1`, parent.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("deleted operation retained a candidate: %d %v", count, err)
	}
}

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
}

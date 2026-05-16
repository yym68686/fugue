package store

import (
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestListOperationSummariesOmitsDesiredState(t *testing.T) {
	t.Parallel()

	s := New(t.TempDir() + "/store.json")
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Summary Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "ops", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:old",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	desiredSpec := app.Spec
	desiredSpec.Image = "ghcr.io/example/demo:new"
	desiredSpec.Files = []model.AppFile{{
		Path:    "/app/large.txt",
		Content: "large inline content",
	}}
	desiredSource := model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "ghcr.io/example/demo:new",
	}
	op, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &desiredSpec,
		DesiredSource: &desiredSource,
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}

	summaries, err := s.ListOperationSummariesByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list operation summaries by app: %v", err)
	}
	if len(summaries) != 1 || summaries[0].ID != op.ID {
		t.Fatalf("unexpected summaries: %+v", summaries)
	}
	if summaries[0].DesiredSpec != nil || summaries[0].DesiredSource != nil || summaries[0].DesiredOriginSource != nil {
		t.Fatalf("expected desired state to be omitted from summary, got %+v", summaries[0])
	}

	fullOps, err := s.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list operations by app: %v", err)
	}
	if len(fullOps) != 1 || fullOps[0].DesiredSpec == nil || fullOps[0].DesiredSource == nil {
		t.Fatalf("expected full operation list to keep desired state, got %+v", fullOps)
	}
}

func TestPGListOperationSummariesByAppDoesNotSelectDesiredJSON(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()

	s := &Store{
		databaseURL: "postgres://example",
		db:          db,
		dbReady:     true,
	}

	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT o.id, o.tenant_id, o.type, o.status, o.execution_mode, o.requested_by_type, o.requested_by_id, o.app_id, o.service_id, o.source_runtime_id, o.target_runtime_id, o.desired_replicas, o.result_message, o.manifest_path, o.assigned_runtime_id, o.error_message, o.created_at, o.updated_at, o.started_at, o.completed_at
FROM fugue_operations o
WHERE o.tenant_id = $1
  AND o.app_id = $2
ORDER BY o.created_at ASC, o.id ASC`)).
		WithArgs("tenant_123", "app_123").
		WillReturnRows(sqlmock.NewRows([]string{
			"id",
			"tenant_id",
			"type",
			"status",
			"execution_mode",
			"requested_by_type",
			"requested_by_id",
			"app_id",
			"service_id",
			"source_runtime_id",
			"target_runtime_id",
			"desired_replicas",
			"result_message",
			"manifest_path",
			"assigned_runtime_id",
			"error_message",
			"created_at",
			"updated_at",
			"started_at",
			"completed_at",
		}).AddRow(
			"op_123",
			"tenant_123",
			model.OperationTypeDeploy,
			model.OperationStatusCompleted,
			model.ExecutionModeManaged,
			model.ActorTypeAPIKey,
			"key_123",
			"app_123",
			"",
			"",
			"",
			2,
			"deployed",
			"/manifests/app.yaml",
			"",
			"",
			now,
			now,
			nil,
			nil,
		))

	ops, err := s.ListOperationSummariesByApp("tenant_123", false, "app_123")
	if err != nil {
		t.Fatalf("list operation summaries by app: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected one operation summary, got %+v", ops)
	}
	if ops[0].DesiredSpec != nil || ops[0].DesiredSource != nil || ops[0].DesiredOriginSource != nil {
		t.Fatalf("expected desired state to be omitted from summary, got %+v", ops[0])
	}
	if ops[0].DesiredReplicas == nil || *ops[0].DesiredReplicas != 2 {
		t.Fatalf("expected desired replicas summary scalar to be retained, got %+v", ops[0].DesiredReplicas)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

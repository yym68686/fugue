package viewmodel

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestViewModelsNormalStateAndFieldMapping(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 6, 13, 1, 0, 0, 0, time.UTC)
	replicas := 2
	app := model.App{
		ID:        "app_123",
		TenantID:  "tenant_123",
		ProjectID: "project_123",
		Name:      "web",
		Route: &model.AppRoute{
			Hostname:    "web",
			PathPrefix:  "/",
			BaseDomain:  "example.com",
			PublicURL:   "https://web.example.com",
			ServicePort: 8080,
		},
		Spec: model.AppSpec{RuntimeID: "runtime_a", Replicas: replicas},
		Status: model.AppStatus{
			Phase:            "ready",
			CurrentRuntimeID: "runtime_a",
			CurrentReplicas:  2,
			LastOperationID:  "op_deploy",
		},
	}
	operations := []model.Operation{
		{ID: "op_import", AppID: app.ID, Type: "import", Status: "completed", CreatedAt: created, UpdatedAt: created},
		{ID: "op_deploy", AppID: app.ID, Type: "deploy", Status: "running", CreatedAt: created.Add(time.Minute), UpdatedAt: created.Add(time.Minute)},
	}
	service := model.BackingService{
		ID:         "svc_123",
		ProjectID:  "project_123",
		OwnerAppID: app.ID,
		Name:       "web-db",
		Type:       "postgres",
		Status:     "ready",
		Spec:       model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "runtime_a"}},
	}

	route := NewRoutePathFromApp(app)
	if route.State.Kind != StateReady || route.PublicURL != "https://web.example.com" || route.Tone != TonePositive {
		t.Fatalf("unexpected route view %+v", route)
	}

	timeline := NewOperationTimeline(operations)
	if timeline.State.Kind != StateReady || timeline.ActiveCount != 1 || timeline.LatestID != "op_deploy" || len(timeline.Steps) != 2 {
		t.Fatalf("unexpected timeline %+v", timeline)
	}
	if timeline.Steps[0].ID != "op_import" || timeline.Steps[1].ID != "op_deploy" || !timeline.Steps[1].Active {
		t.Fatalf("unexpected timeline steps %+v", timeline.Steps)
	}

	health := NewAppHealth(app, operations)
	if health.State.Kind != StateReady || health.Name != "web" || health.Phase != "ready" || health.Tone != TonePositive || health.Operations.ActiveCount != 1 {
		t.Fatalf("unexpected app health %+v", health)
	}

	project := model.Project{ID: "project_123", TenantID: "tenant_123", Name: "production", Description: "prod"}
	workbench := NewProjectWorkbench(project, []model.App{app}, []model.BackingService{service}, operations)
	if workbench.State.Kind != StateReady || workbench.AppCount != 1 || workbench.ServiceCount != 1 || workbench.OperationCount != 2 {
		t.Fatalf("unexpected project workbench %+v", workbench)
	}

	runtime := NewRuntimeCapacity(model.Runtime{ID: "runtime_a", Name: "runtime-a", Type: "external", Status: "active", ClusterNodeName: "node-a"})
	if runtime.State.Kind != StateReady || runtime.Tone != TonePositive || runtime.ClusterNodeName != "node-a" {
		t.Fatalf("unexpected runtime capacity %+v", runtime)
	}

	diagnosis := NewOperationDiagnosisEvidence(model.OperationDiagnosis{Category: "deploy-runtime-failed", Summary: "deploy failed", Hint: "inspect logs", Evidence: []string{"pod failed"}})
	if diagnosis.State.Kind != StateReady || diagnosis.Tone != ToneDanger || diagnosis.Category != "deploy-runtime-failed" || len(diagnosis.Evidence) != 1 {
		t.Fatalf("unexpected diagnosis evidence %+v", diagnosis)
	}

	action := NewActionPlan("restart", "web", "project production", "POST /v1/apps/app_123/restart", "restart", false)
	if action.State.Kind != StateReady || action.Action != "restart" || action.Target != "web" || action.APICall == "" {
		t.Fatalf("unexpected action plan %+v", action)
	}
}

func TestViewModelsEmptyErrorAndPermissionStates(t *testing.T) {
	t.Parallel()

	err := errors.New("api unavailable")
	cases := []struct {
		name       string
		empty      State
		errorState State
		permission State
	}{
		{name: "app health", empty: EmptyAppHealth("no app").State, errorState: ErrorAppHealth(err).State, permission: PermissionAppHealth("forbidden").State},
		{name: "project workbench", empty: EmptyProjectWorkbench("no project").State, errorState: ErrorProjectWorkbench(err).State, permission: PermissionProjectWorkbench("forbidden").State},
		{name: "route path", empty: EmptyRoutePath("no route").State, errorState: ErrorRoutePath(err).State, permission: PermissionRoutePath("forbidden").State},
		{name: "operation timeline", empty: EmptyOperationTimeline("no ops").State, errorState: ErrorOperationTimeline(err).State, permission: PermissionOperationTimeline("forbidden").State},
		{name: "runtime capacity", empty: EmptyRuntimeCapacity("no runtime").State, errorState: ErrorRuntimeCapacity(err).State, permission: PermissionRuntimeCapacity("forbidden").State},
		{name: "diagnosis evidence", empty: EmptyDiagnosisEvidence("no diagnosis").State, errorState: ErrorDiagnosisEvidence(err).State, permission: PermissionDiagnosisEvidence("forbidden").State},
		{name: "action plan", empty: EmptyActionPlan("no action").State, errorState: ErrorActionPlan(err).State, permission: PermissionActionPlan("forbidden").State},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.empty.Kind != StateEmpty || tc.empty.Message == "" {
				t.Fatalf("expected empty state with message, got %+v", tc.empty)
			}
			if tc.errorState.Kind != StateError || tc.errorState.Message != "api unavailable" {
				t.Fatalf("expected error state, got %+v", tc.errorState)
			}
			if tc.permission.Kind != StatePermission || tc.permission.Message != "forbidden" {
				t.Fatalf("expected permission state, got %+v", tc.permission)
			}
		})
	}
}

func TestNewOperationTimelineEmpty(t *testing.T) {
	t.Parallel()

	timeline := NewOperationTimeline(nil)
	if timeline.State.Kind != StateEmpty || len(timeline.Steps) != 0 || timeline.ActiveCount != 0 {
		t.Fatalf("unexpected empty timeline %+v", timeline)
	}
}

func TestPermissionErrorClassification(t *testing.T) {
	t.Parallel()

	if !IsPermissionError(PermissionDeniedError("workspace")) {
		t.Fatal("expected wrapped permission error to classify")
	}
	if !IsPermissionError(errors.New("request failed: 403 forbidden")) {
		t.Fatal("expected forbidden text to classify")
	}
	if IsPermissionError(errors.New("network unavailable")) {
		t.Fatal("did not expect generic error to classify as permission")
	}
}

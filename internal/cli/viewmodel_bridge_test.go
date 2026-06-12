package cli

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/cli/viewmodel"
	"fugue/internal/model"
)

func TestBuildAppOverviewViewModels(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:        "app_123",
		TenantID:  "tenant_123",
		ProjectID: "project_123",
		Name:      "web",
		Route:     &model.AppRoute{PublicURL: "https://web.example.com"},
		Spec:      model.AppSpec{RuntimeID: "runtime_a", Replicas: 1},
		Status:    model.AppStatus{Phase: "ready", CurrentReplicas: 1},
	}
	snapshot := appOverviewSnapshot{
		App:        app,
		Operations: []model.Operation{{ID: "op_running", AppID: app.ID, Type: model.OperationTypeDeploy, Status: model.OperationStatusRunning}},
		Diagnosis:  &appOverviewDiagnosis{Category: "deploy-runtime-failed", Summary: "deploy failed", Evidence: []string{"pod failed"}},
	}

	health := buildAppOverviewHealthView(snapshot)
	if health.State.Kind != viewmodel.StateReady || health.Name != "web" || health.Operations.ActiveCount != 1 {
		t.Fatalf("unexpected health view %+v", health)
	}
	diagnosis := buildAppOverviewDiagnosisEvidenceView(snapshot.Diagnosis)
	if diagnosis.State.Kind != viewmodel.StateReady || diagnosis.Category != "deploy-runtime-failed" || diagnosis.Tone != viewmodel.ToneDanger {
		t.Fatalf("unexpected diagnosis view %+v", diagnosis)
	}
}

func TestBuildAppDiagnosisEvidenceView(t *testing.T) {
	t.Parallel()

	view := buildAppDiagnosisEvidenceView(appDiagnosis{
		Category:       "volume-affinity-conflict",
		Summary:        "pod cannot schedule",
		Hint:           "inspect pods",
		Component:      "app",
		Namespace:      "tenant-123",
		Selector:       "app=web",
		ImplicatedNode: "node-a",
		Evidence:       []string{"node affinity conflict"},
	})
	if view.State.Kind != viewmodel.StateReady || view.Component != "app" || view.Scope != "tenant-123" || len(view.Evidence) != 1 {
		t.Fatalf("unexpected app diagnosis evidence view %+v", view)
	}
}

func TestBuildProjectStatusViewModels(t *testing.T) {
	t.Parallel()

	status := &projectStatusResponse{
		Services: []projectServiceStatus{
			{
				Service:           "web",
				AppID:             "app_123",
				AppName:           "web",
				Phase:             "degraded",
				Category:          "runtime-crash",
				Summary:           "runtime is crash looping",
				Build:             "completed",
				Runtime:           "failed",
				DeployOperationID: "op_deploy",
			},
		},
		Deletes: []projectDeleteStatus{{AppID: "app_old", AppName: "old", OperationID: "op_delete", Status: model.OperationStatusPending, Summary: "delete queued"}},
	}
	stages := buildProjectStatusServiceStageViews(status)
	if len(stages) != 1 || stages[0].Name != "web" || stages[0].Tone != viewmodel.ToneDanger {
		t.Fatalf("unexpected service stage views %+v", stages)
	}
	evidence := buildProjectStatusDiagnosisEvidenceViews(status)
	if len(evidence) != 2 || evidence[0].Scope != "web" || evidence[1].Scope != "old" {
		t.Fatalf("unexpected project diagnosis evidence %+v", evidence)
	}
}

func TestBuildProjectOverviewWorkbenchView(t *testing.T) {
	t.Parallel()

	detail := consoleProjectDetailResponse{
		Project:    &model.Project{ID: "project_123", TenantID: "tenant_123", Name: "production"},
		Apps:       []model.App{{ID: "app_123", Name: "web", Spec: model.AppSpec{Replicas: 1}, Status: model.AppStatus{Phase: "ready", CurrentReplicas: 1}}},
		Operations: []model.Operation{{ID: "op_deploy", AppID: "app_123", Type: model.OperationTypeDeploy, Status: model.OperationStatusCompleted, CreatedAt: time.Now()}},
	}
	view := buildProjectOverviewWorkbenchView(detail, []model.BackingService{{ID: "svc_123", ProjectID: "project_123", Name: "web-db", Type: "postgres", Status: "ready"}})
	if view.State.Kind != viewmodel.StateReady || view.Name != "production" || view.AppCount != 1 || view.ServiceCount != 1 || view.OperationCount != 1 {
		t.Fatalf("unexpected project workbench view %+v", view)
	}
}

func TestViewModelForError(t *testing.T) {
	t.Parallel()

	if got := viewModelForError(viewmodel.PermissionDeniedError("workspace")).Kind; got != viewmodel.StatePermission {
		t.Fatalf("expected permission state, got %s", got)
	}
	if got := viewModelForError(errors.New("api unavailable")).Kind; got != viewmodel.StateError {
		t.Fatalf("expected error state, got %s", got)
	}
}

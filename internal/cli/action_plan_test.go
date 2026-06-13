package cli

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"fugue/internal/cli/terminal"
	"fugue/internal/cli/ui"
	"fugue/internal/cli/viewmodel"
	"fugue/internal/model"
)

func TestActionPlansIncludeTargetScopeRiskAndConfirmation(t *testing.T) {
	app := model.App{
		ID:        "app_123",
		TenantID:  "tenant_123",
		ProjectID: "project_123",
		Name:      "web",
	}
	for _, plan := range []viewmodel.ActionPlanView{
		buildRestartActionPlan(app),
		buildRedeployActionPlan(app),
		buildCancelOperationActionPlan(model.Operation{ID: "op_123", TenantID: "tenant_123", AppID: "app_123", CreatedAt: time.Now()}),
	} {
		if plan.State.Kind != viewmodel.StateReady || plan.Target == "" || plan.Scope == "" || plan.Risk == "" || plan.RollbackHint == "" || plan.ConfirmText == "" {
			t.Fatalf("action plan missing required fields: %+v", plan)
		}
		if !actionPlanConfirmationMatches(plan, plan.ConfirmText) {
			t.Fatalf("expected confirm text to match plan %+v", plan)
		}
		if actionPlanConfirmationMatches(plan, "yes") {
			t.Fatalf("expected generic confirmation to be rejected for %+v", plan)
		}
	}
}

func TestDangerConfirmDialogSnapshot(t *testing.T) {
	plan := buildRestartActionPlan(model.App{ID: "app_123", TenantID: "tenant_123", ProjectID: "project_123", Name: "web"})
	out := ui.NewRenderer(72, terminal.Palette{}).DangerConfirmDialog(plan)
	for _, want := range []string{"Confirm restart", "action=restart", "target=web", "project_123", "POST /v1/apps/app_123/restart", "confirm=restart app web"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected dialog to contain %q, got %q", want, out)
		}
	}
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if utf8.RuneCountInString(line) > 72 {
			t.Fatalf("expected dialog line <= 72 cells, got %d %q\n%s", utf8.RuneCountInString(line), line, out)
		}
	}
}

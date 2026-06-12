package cli

import (
	"fmt"
	"strings"

	"fugue/internal/cli/viewmodel"
	"fugue/internal/model"
)

func buildRestartActionPlan(app model.App) viewmodel.ActionPlanView {
	target := firstNonEmptyTrimmed(app.Name, app.ID)
	scope := fmt.Sprintf("tenant=%s project=%s app=%s", firstNonEmptyTrimmed(app.TenantID, "-"), firstNonEmptyTrimmed(app.ProjectID, "-"), target)
	plan := buildActionPlanView("restart", target, scope, fmt.Sprintf("POST /v1/apps/%s/restart", app.ID), "restart", false)
	plan.Risk = "briefly restarts app replicas; existing route remains attached"
	plan.RollbackHint = "watch the returned operation; if restart fails, inspect logs and redeploy the previous desired spec"
	plan.ConfirmText = fmt.Sprintf("restart app %s in project %s", target, firstNonEmptyTrimmed(app.ProjectID, "-"))
	plan.NextCommands = []string{fmt.Sprintf("fugue operation watch <operation-id>"), fmt.Sprintf("fugue app logs runtime %s --follow", target)}
	return plan
}

func buildRedeployActionPlan(app model.App) viewmodel.ActionPlanView {
	target := firstNonEmptyTrimmed(app.Name, app.ID)
	scope := fmt.Sprintf("tenant=%s project=%s app=%s", firstNonEmptyTrimmed(app.TenantID, "-"), firstNonEmptyTrimmed(app.ProjectID, "-"), target)
	plan := buildActionPlanView("redeploy", target, scope, fmt.Sprintf("POST /v1/apps/%s/images/redeploy", app.ID), "deploy", false)
	plan.Risk = "queues a new deploy from the app desired image/config"
	plan.RollbackHint = "use fugue app rollback if the redeploy promotes an unhealthy release"
	plan.ConfirmText = fmt.Sprintf("redeploy app %s in project %s", target, firstNonEmptyTrimmed(app.ProjectID, "-"))
	plan.NextCommands = []string{fmt.Sprintf("fugue app redeploy %s", target), fmt.Sprintf("fugue operation watch <operation-id>")}
	return plan
}

func buildCancelOperationActionPlan(op model.Operation) viewmodel.ActionPlanView {
	target := firstNonEmptyTrimmed(op.ID, "operation")
	scope := fmt.Sprintf("tenant=%s app=%s operation=%s", firstNonEmptyTrimmed(op.TenantID, "-"), firstNonEmptyTrimmed(op.AppID, "-"), target)
	plan := buildActionPlanView("cancel operation", target, scope, "not available in current OpenAPI contract", "cancel", true)
	plan.Risk = "cancel semantics are not enabled until the control-plane API exposes a cancellable operation endpoint"
	plan.RollbackHint = "use fugue operation explain and app logs to decide whether to wait, restart, or redeploy"
	plan.ConfirmText = fmt.Sprintf("cancel operation %s", target)
	plan.NextCommands = []string{fmt.Sprintf("fugue operation explain %s", target), fmt.Sprintf("fugue operation watch %s", target)}
	return plan
}

func actionPlanConfirmationMatches(plan viewmodel.ActionPlanView, typed string) bool {
	want := strings.TrimSpace(plan.ConfirmText)
	return want != "" && strings.TrimSpace(typed) == want
}

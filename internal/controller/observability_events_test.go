package controller

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestControllerOperationAppEventFieldsAreRoutedAndRedacted(t *testing.T) {
	t.Parallel()
	op := model.Operation{
		ID:              "op_123",
		TenantID:        "tenant_123",
		Type:            model.OperationTypeDeploy,
		Status:          model.OperationStatusCompleted,
		ExecutionMode:   model.ExecutionModeManaged,
		RequestedByType: "user",
		AppID:           "app_123",
		TargetRuntimeID: "runtime_123",
		CreatedAt:       time.Date(2026, 6, 6, 1, 2, 3, 0, time.UTC),
	}
	app := model.App{
		ID:        "app_123",
		TenantID:  "tenant_123",
		ProjectID: "project_123",
		Status: model.AppStatus{
			CurrentRuntimeID: "runtime_current",
		},
	}
	fields := controllerOperationAppEventFields("completed", "info", op, app, "deployed", map[string]any{
		"token":      "secret",
		"elapsed_ms": int64(1200),
	})
	if fields["event_type"] != "deploy_event" || fields["operation_id"] != "op_123" || fields["project_id"] != "project_123" {
		t.Fatalf("unexpected operation event fields: %+v", fields)
	}
	attrs := fmt.Sprint(fields["attributes_json"])
	if strings.Contains(attrs, "secret") || strings.Contains(attrs, "token") {
		t.Fatalf("attributes leaked secret-like fields: %s", attrs)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(attrs), &decoded); err != nil {
		t.Fatalf("attributes_json was not JSON: %v", err)
	}
	if decoded["action"] != "completed" || decoded["operation_type"] != model.OperationTypeDeploy {
		t.Fatalf("unexpected decoded attrs: %+v", decoded)
	}
}

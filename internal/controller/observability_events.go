package controller

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
)

var controllerStructuredLogMu sync.Mutex

func (s *Service) logOperationAppEvent(action, severity string, op model.Operation, app model.App, message string, attrs map[string]any) {
	if s == nil || s.Logger == nil {
		return
	}
	fields := controllerOperationAppEventFields(action, severity, op, app, message, attrs)
	if len(fields) == 0 {
		return
	}
	body, err := json.Marshal(fields)
	if err != nil {
		return
	}
	controllerStructuredLogMu.Lock()
	defer controllerStructuredLogMu.Unlock()
	writeControllerStructuredLog(s.Logger.Writer(), body)
}

func writeControllerStructuredLog(w io.Writer, body []byte) {
	if w == nil || len(body) == 0 {
		return
	}
	_, _ = fmt.Fprintln(w, string(body))
}

func controllerOperationAppEventFields(action, severity string, op model.Operation, app model.App, message string, attrs map[string]any) map[string]any {
	action = strings.TrimSpace(action)
	if action == "" || strings.TrimSpace(op.ID) == "" {
		return nil
	}
	if strings.TrimSpace(severity) == "" {
		severity = "info"
	}
	eventType := "operation_event"
	if strings.EqualFold(strings.TrimSpace(op.Type), model.OperationTypeDeploy) && (action == "completed" || action == "failed") {
		eventType = "deploy_event"
	}
	if strings.EqualFold(strings.TrimSpace(op.Type), model.OperationTypeDelete) && (action == "completed" || action == "failed") {
		eventType = "runtime_event"
	}
	if strings.TrimSpace(message) == "" {
		message = "operation " + action
	}
	summary := map[string]any{
		"action":              action,
		"operation_type":      strings.TrimSpace(op.Type),
		"operation_status":    strings.TrimSpace(op.Status),
		"execution_mode":      strings.TrimSpace(op.ExecutionMode),
		"requested_by_type":   strings.TrimSpace(op.RequestedByType),
		"source_runtime_id":   strings.TrimSpace(op.SourceRuntimeID),
		"target_runtime_id":   strings.TrimSpace(op.TargetRuntimeID),
		"assigned_runtime_id": strings.TrimSpace(op.AssignedRuntimeID),
	}
	for key, value := range attrs {
		key = strings.TrimSpace(key)
		if key == "" || strings.Contains(strings.ToLower(key), "token") || strings.Contains(strings.ToLower(key), "secret") {
			continue
		}
		summary[key] = value
	}
	attributesJSON, _ := json.Marshal(summary)
	return map[string]any{
		"event_type":      eventType,
		"severity":        strings.TrimSpace(severity),
		"message":         strings.TrimSpace(message),
		"tenant_id":       firstNonEmptyString(strings.TrimSpace(app.TenantID), strings.TrimSpace(op.TenantID)),
		"project_id":      strings.TrimSpace(app.ProjectID),
		"app_id":          firstNonEmptyString(strings.TrimSpace(app.ID), strings.TrimSpace(op.AppID)),
		"operation_id":    strings.TrimSpace(op.ID),
		"runtime_id":      firstNonEmptyString(strings.TrimSpace(op.AssignedRuntimeID), strings.TrimSpace(op.TargetRuntimeID), strings.TrimSpace(app.Status.CurrentRuntimeID)),
		"attributes_json": string(attributesJSON),
	}
}

func (s *Service) appForOperationEvent(op model.Operation) model.App {
	if s == nil || s.Store == nil || strings.TrimSpace(op.AppID) == "" {
		return model.App{ID: op.AppID, TenantID: op.TenantID}
	}
	app, err := s.Store.GetApp(op.AppID)
	if err != nil {
		return model.App{ID: op.AppID, TenantID: op.TenantID}
	}
	return app
}

func operationElapsedMilliseconds(op model.Operation, now time.Time) int64 {
	if op.CreatedAt.IsZero() || now.IsZero() {
		return 0
	}
	if now.Before(op.CreatedAt) {
		return 0
	}
	return now.Sub(op.CreatedAt).Milliseconds()
}

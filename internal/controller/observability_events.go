package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
)

var controllerStructuredLogMu sync.Mutex

func (s *Service) logOperationAppEvent(action, severity string, op model.Operation, app model.App, message string, attrs map[string]any) {
	if s == nil {
		return
	}
	fields := controllerOperationAppEventFields(action, severity, op, app, message, attrs)
	if len(fields) == 0 {
		return
	}
	s.writeControllerStructuredEvent(context.Background(), fields)
}

func (s *Service) logControllerAppEvent(ctx context.Context, eventType, severity string, app model.App, message string, attrs map[string]any) {
	if s == nil {
		return
	}
	fields := controllerAppEventFields(eventType, severity, app, message, attrs)
	if len(fields) == 0 {
		return
	}
	s.writeControllerStructuredEvent(ctx, fields)
}

func (s *Service) writeControllerStructuredEvent(ctx context.Context, fields map[string]any) {
	if s == nil || len(fields) == 0 {
		return
	}
	body, err := json.Marshal(fields)
	if err == nil && s.Logger != nil {
		controllerStructuredLogMu.Lock()
		writeControllerStructuredLog(s.Logger.Writer(), body)
		controllerStructuredLogMu.Unlock()
	}
	s.postControllerStructuredEvent(ctx, fields)
}

func writeControllerStructuredLog(w io.Writer, body []byte) {
	if w == nil || len(body) == 0 {
		return
	}
	_, _ = fmt.Fprintln(w, string(body))
}

func (s *Service) postControllerStructuredEvent(ctx context.Context, fields map[string]any) {
	endpoint := controllerObservabilityLogsURL(s.Config.AppObservabilityEndpoint)
	if endpoint == "" || len(fields) == 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	body, err := json.Marshal(map[string]any{"events": []map[string]any{fields}})
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("controller observability event post failed: %v", err)
		}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if s.Logger != nil {
			s.Logger.Printf("controller observability event post returned status=%s", resp.Status)
		}
	}
}

func controllerObservabilityLogsURL(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || strings.TrimSpace(parsed.Host) == "" {
		return ""
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	if parsed.Path == "" {
		parsed.Path = "/v1/logs"
	} else if !strings.HasSuffix(parsed.Path, "/v1/logs") {
		parsed.Path += "/v1/logs"
	}
	return parsed.String()
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
		"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
		"kind":            "log",
		"fugue_table":     "app_events",
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

func controllerAppEventFields(eventType, severity string, app model.App, message string, attrs map[string]any) map[string]any {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" || strings.TrimSpace(app.ID) == "" {
		return nil
	}
	if strings.TrimSpace(severity) == "" {
		severity = "info"
	}
	if strings.TrimSpace(message) == "" {
		message = eventType
	}
	summary := map[string]any{}
	for key, value := range attrs {
		key = strings.TrimSpace(key)
		if key == "" || controllerEventAttributeSensitive(key) {
			continue
		}
		summary[key] = value
	}
	attributesJSON, _ := json.Marshal(summary)
	return map[string]any{
		"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
		"kind":            "log",
		"fugue_table":     "app_events",
		"event_type":      eventType,
		"severity":        strings.TrimSpace(severity),
		"message":         strings.TrimSpace(message),
		"tenant_id":       strings.TrimSpace(app.TenantID),
		"project_id":      strings.TrimSpace(app.ProjectID),
		"app_id":          strings.TrimSpace(app.ID),
		"runtime_id":      firstNonEmptyString(strings.TrimSpace(app.Spec.RuntimeID), strings.TrimSpace(app.Status.CurrentRuntimeID)),
		"operation_id":    stringFromAny(summary["operation_id"]),
		"deployment_id":   stringFromAny(summary["deployment_id"]),
		"attributes_json": string(attributesJSON),
	}
}

func controllerEventAttributeSensitive(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	return strings.Contains(key, "token") ||
		strings.Contains(key, "secret") ||
		strings.Contains(key, "password")
}

func stringFromAny(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return ""
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

package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
)

var apiRightSizingStructuredLogMu sync.Mutex

func (s *Server) logRightSizingDecision(app model.App, decision, message string, attrs map[string]any) {
	if s == nil || s.log == nil || strings.TrimSpace(app.ID) == "" {
		return
	}
	decision = strings.TrimSpace(decision)
	if decision == "" {
		decision = "unknown"
	}
	if strings.TrimSpace(message) == "" {
		message = "right-sizing auto apply decision"
	}
	summary := map[string]any{
		"decision": decision,
	}
	for key, value := range attrs {
		key = strings.TrimSpace(key)
		if key == "" || apiRightSizingAttributeSensitive(key) {
			continue
		}
		summary[key] = value
	}
	attributesJSON, _ := json.Marshal(summary)
	fields := map[string]any{
		"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
		"kind":            "log",
		"fugue_table":     "app_events",
		"event_type":      "right_sizing_decision",
		"severity":        rightSizingDecisionSeverity(decision),
		"message":         strings.TrimSpace(message),
		"tenant_id":       strings.TrimSpace(app.TenantID),
		"project_id":      strings.TrimSpace(app.ProjectID),
		"app_id":          strings.TrimSpace(app.ID),
		"runtime_id":      firstNonEmpty(strings.TrimSpace(app.Spec.RuntimeID), strings.TrimSpace(app.Status.CurrentRuntimeID)),
		"operation_id":    rightSizingStringFromAny(summary["operation_id"]),
		"attributes_json": string(attributesJSON),
	}
	body, err := json.Marshal(fields)
	if err != nil {
		return
	}
	apiRightSizingStructuredLogMu.Lock()
	defer apiRightSizingStructuredLogMu.Unlock()
	_, _ = fmt.Fprintln(s.log.Writer(), string(body))
}

func apiRightSizingAttributeSensitive(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	return strings.Contains(key, "token") ||
		strings.Contains(key, "secret") ||
		strings.Contains(key, "password")
}

func rightSizingDecisionSeverity(decision string) string {
	switch strings.TrimSpace(decision) {
	case "downtime_refused", "billing_cap_blocked", "queue_error":
		return "warning"
	default:
		return "info"
	}
}

func rightSizingStringFromAny(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return ""
	}
}

func rightSizingResourceSummary(resources *model.ResourceSpec) map[string]any {
	if resources == nil {
		return nil
	}
	return map[string]any{
		"cpu_milli_cores":        resources.CPUMilliCores,
		"memory_mebibytes":       resources.MemoryMebibytes,
		"cpu_limit_milli_cores":  resources.CPULimitMilliCores,
		"memory_limit_mebibytes": resources.MemoryLimitMebibytes,
	}
}

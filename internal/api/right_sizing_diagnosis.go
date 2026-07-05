package api

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/model"
)

func (s *Server) attachAppRightSizingEvidence(ctx context.Context, app model.App, diagnosis *appDiagnosis) {
	if s == nil || diagnosis == nil || strings.TrimSpace(app.ID) == "" {
		return
	}
	source := s.appObservabilitySourceStatus(app.ID, "logs", "right-sizing decision query backend is not wired yet")
	if source.Status == "disabled" || !observabilityExporterActive(source.ActiveExporters, "logs") {
		return
	}
	query := "SELECT ts, event_type, severity, operation_id, runtime_id, message, attributes_json " +
		"FROM app_events WHERE app_id = " + quoteClickHouseString(app.ID) +
		" AND event_type = 'right_sizing_decision'" +
		" ORDER BY ts DESC LIMIT 1 FORMAT JSONEachRow"
	rows, err := s.queryAppObservabilityClickHouse(ctx, query)
	if err != nil {
		diagnosis.Warnings = appendUniqueString(diagnosis.Warnings, "right-sizing diagnosis unavailable: "+err.Error())
		return
	}
	if len(rows) == 0 {
		return
	}
	row := rows[0]
	attrs := parseJSONMapField(row["attributes_json"])
	decision := strings.TrimSpace(stringField(attrs, "decision"))
	if decision == "" {
		decision = "unknown"
	}
	message := strings.TrimSpace(stringField(row, "message"))
	checkedAt := strings.TrimSpace(stringField(row, "ts"))
	evidence := fmt.Sprintf("right-sizing latest decision=%s checked_at=%s", decision, checkedAt)
	if message != "" {
		evidence += " message=" + message
	}
	if operationID := strings.TrimSpace(stringField(row, "operation_id")); operationID != "" {
		evidence += " operation_id=" + operationID
	}
	diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, evidence)
	switch decision {
	case "downtime_refused", "billing_cap_blocked", "queue_error":
		diagnosis.Warnings = appendUniqueString(diagnosis.Warnings, "right-sizing latest decision requires attention: "+decision)
	}
}

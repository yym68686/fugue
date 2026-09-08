package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/model"
)

// Request IDs and aggregate edge_perf IDs describe different objects. Never
// substitute a nearby performance sample or the current route for a request fact.
func (s *Server) explainRecordedRequest(ctx context.Context, id string, since, now time.Time) model.RequestExplainResponse {
	result := model.RequestExplainResponse{
		RequestID: id, ErrorClass: "not_observed", FailurePlane: "control_plane_observability",
		SecretSafe: true, GeneratedAt: now,
		Evidence: map[string]string{"since": since.Format(time.RFC3339Nano), "until": now.Format(time.RFC3339Nano), "lookup_status": "not_found", "source": "request_telemetry"},
	}
	if !s.observabilityConfig.Enabled || strings.TrimSpace(s.observabilityConfig.ClickHouseDSN) == "" {
		result.ErrorClass = "evidence_unavailable"
		result.Evidence["lookup_status"] = "not_configured"
		return result
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	rows, err := s.queryAppObservabilityClickHouse(ctx, recordedRequestQuery(id, since, now))
	if err != nil {
		// Backend errors can contain credentials, SQL and internal addresses.
		result.ErrorClass = "evidence_unavailable"
		result.Evidence["lookup_status"] = "backend_unavailable"
		return result
	}
	if len(rows) == 0 {
		result.Evidence["missing_evidence"] = "request_fact_in_window; collection and retention coverage unknown"
		return result
	}
	if len(rows) > 1 {
		result.ErrorClass = "ambiguous_request"
		result.Evidence["lookup_status"] = "ambiguous"
		return result
	}
	row := rows[0]
	summary := parseJSONMapField(row["summary_json"])
	if id != stringField(row, "request_id") && id != stringField(row, "trace_id") && id != stringField(summary, "edge_request_id") {
		result.ErrorClass = "evidence_unavailable"
		result.Evidence["lookup_status"] = "invalid_record"
		return result
	}
	observed, ok := parseAppObservabilityRequestTimestamp(stringField(row, "ts"))
	if !ok || observed.Before(since) || observed.After(now) {
		result.ErrorClass = "evidence_unavailable"
		result.Evidence["lookup_status"] = "invalid_record"
		return result
	}
	result.Found = true
	result.SampledAt = observed
	result.Evidence["lookup_status"] = "found"
	result.Evidence["source"] = stringField(row, "evidence_source")
	result.Evidence["attribution_kind"] = "observed_stages; root cause not inferred from durations"
	result.EdgeID = requestEvidenceToken(stringField(row, "edge_id"))
	result.EdgeGroupID = requestEvidenceToken(stringField(row, "edge_group_id"))
	result.Hostname = requestEvidenceToken(stringField(row, "hostname"))
	result.Method = requestEvidenceToken(stringField(row, "method"))
	result.PathPrefix = redactedAppObservabilityRequestPath(stringField(row, "path_template"))
	result.RouteGeneration = requestEvidenceToken(stringField(row, "route_generation"))
	for _, key := range []string{"trace_id", "request_id", "runtime_id", "app_id"} {
		if value := requestEvidenceToken(stringField(row, key)); value != "" {
			result.Evidence[key] = value
		}
	}
	if value := requestEvidenceToken(stringField(summary, "edge_request_id")); value != "" {
		result.Evidence["edge_request_id"] = value
	}
	status, _ := appObservabilitySummaryMilliseconds(row, "status_code")
	if status >= 100 && status <= 599 {
		result.StatusCode = int(status)
	}
	for key, target := range map[string]*int64{
		"request_content_length": &result.RequestBodyBytes, "request_body_read_bytes": &result.RequestBodyReadBytes,
		"body_read_block_ms": &result.BodyReadBlockMS, "avg_bps": &result.UploadEffectiveBPS,
		"min_window_bps": &result.MinWindowBPS, "max_read_gap_ms": &result.MaxReadGapMS,
		"origin_dns_ms": &result.OriginDNSMS, "origin_connect_ms": &result.OriginConnectMS,
		"origin_request_write_ms": &result.OriginRequestWriteMS, "origin_response_wait_ms": &result.OriginResponseWaitMS,
		"origin_ttfb_ms": &result.OriginTTFBMS, "origin_total_ms": &result.OriginTotalMS,
	} {
		*target, _ = appObservabilitySummaryMilliseconds(summary, key)
	}
	// An omitted measurement is not a measured zero or proof of an incomplete body.
	if complete, ok := summary["request_body_complete"].(bool); ok {
		result.Evidence["request_body_read_complete"] = fmt.Sprint(complete)
		if !complete {
			result.BodyIncompleteCount = 1
		}
	}
	result.ErrorClass, result.FailurePlane = "none", "none"
	switch {
	case stringField(summary, "request_body_read_error") != "":
		result.BodyReadErrorCount = 1
		result.ErrorClass, result.FailurePlane = "edge.body_read_error", "data_plane"
	case result.BodyIncompleteCount > 0:
		result.ErrorClass, result.FailurePlane = "edge.body_incomplete", "data_plane"
	case result.StatusCode == http.StatusRequestTimeout:
		result.ErrorClass, result.FailurePlane = "http.request_timeout", "unknown"
	case result.StatusCode >= 400:
		result.ErrorClass, result.FailurePlane = "http.error_response", "unknown"
	case result.StatusCode == 0:
		result.ErrorClass, result.FailurePlane = "incomplete_request_fact", "unknown"
	}
	result.Attribution = requestAttributionFromSample(model.EdgePerformanceSample{
		BodyReadErrorCount: result.BodyReadErrorCount, BodyIncompleteCount: result.BodyIncompleteCount,
		UploadEffectiveBPS: result.UploadEffectiveBPS, MaxReadGapMS: result.MaxReadGapMS,
		OriginDNSMS: result.OriginDNSMS, OriginConnectMS: result.OriginConnectMS,
		OriginTTFBMS: result.OriginTTFBMS, OriginResponseWaitMS: result.OriginResponseWaitMS,
	})
	result.FailureContracts = requestFailureContractsFromAttribution(result.Attribution)
	return result
}

func requestEvidenceToken(value string) string {
	if len(value) > 256 || strings.IndexFunc(value, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_.:-", r))
	}) >= 0 {
		return ""
	}
	return value
}

func recordedRequestQuery(id string, since, until time.Time) string {
	fields := []string{"request_id", "trace_id", "edge_id", "edge_group_id", "hostname", "path_template", "method", "route_generation", "runtime_id", "app_id"}
	eventFields := make([]string, 0, len(fields))
	for _, field := range fields {
		eventFields = append(eventFields, "JSONExtractString(attributes_json, '"+field+"') AS "+field)
	}
	window := "ts >= " + clickHouseDateTime64Literal(since) + " AND ts <= " + clickHouseDateTime64Literal(until)
	match := "(request_id = " + quoteClickHouseString(id) + " OR trace_id = " + quoteClickHouseString(id) + " OR JSONExtractString(summary_json, 'edge_request_id') = " + quoteClickHouseString(id) + ")"
	// Platform traffic can lack app_id and is stored as request_fact_incomplete.
	// Query both sources so trace IDs shared by multiple requests stay ambiguous.
	return "SELECT DISTINCT * FROM (" +
		"SELECT ts, " + strings.Join(fields, ", ") + ", status_code, summary_json, 'request_facts' AS evidence_source FROM request_facts WHERE " + window + " AND " + match +
		" UNION ALL SELECT ts, " + strings.Join(eventFields, ", ") + ", toUInt16OrZero(JSONExtractString(attributes_json, 'status_code')) AS status_code, JSONExtractString(attributes_json, 'summary_json') AS summary_json, 'app_events' AS evidence_source FROM app_events WHERE " + window + " AND event_type = 'request_fact_incomplete' AND " + match +
		") ORDER BY ts DESC LIMIT 2 SETTINGS max_threads = 1, max_memory_usage = 134217728 FORMAT JSONEachRow"
}

package observability

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func eventsFromOTLPJSON(kind EventKind, path string, contentType string, body []byte, receivedAt time.Time) ([]Event, int) {
	if kind != EventKindSpan || !strings.Contains(strings.ToLower(contentType), "json") || !json.Valid(body) {
		return nil, 0
	}
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, 0
	}
	resourceSpans, _ := root["resourceSpans"].([]any)
	if len(resourceSpans) == 0 {
		return nil, 0
	}
	var events []Event
	redacted := 0
	for _, rawResourceSpan := range resourceSpans {
		resourceSpan, _ := rawResourceSpan.(map[string]any)
		resourceAttrs, n := otlpAttributes(resourceFromOTLP(resourceSpan))
		redacted += n
		scopeSpans := otlpList(resourceSpan["scopeSpans"])
		if len(scopeSpans) == 0 {
			scopeSpans = otlpList(resourceSpan["instrumentationLibrarySpans"])
		}
		for _, rawScopeSpan := range scopeSpans {
			scopeSpan, _ := rawScopeSpan.(map[string]any)
			for _, rawSpan := range otlpList(scopeSpan["spans"]) {
				span, _ := rawSpan.(map[string]any)
				event, n, ok := eventFromOTLPSpan(span, resourceAttrs, path, receivedAt)
				redacted += n
				if ok {
					events = append(events, event)
				}
			}
		}
	}
	return events, redacted
}

func resourceFromOTLP(resourceSpan map[string]any) any {
	resource, _ := resourceSpan["resource"].(map[string]any)
	return resource["attributes"]
}

func eventFromOTLPSpan(span map[string]any, resourceAttrs map[string]string, path string, receivedAt time.Time) (Event, int, bool) {
	traceID := strings.TrimSpace(stringFromAny(span["traceId"]))
	spanID := strings.TrimSpace(stringFromAny(span["spanId"]))
	name := strings.TrimSpace(stringFromAny(span["name"]))
	if traceID == "" || spanID == "" {
		return Event{}, 0, false
	}
	attrs := make(map[string]string, len(resourceAttrs)+16)
	for key, value := range resourceAttrs {
		attrs[key] = value
	}
	spanAttrs, redacted := otlpAttributes(span["attributes"])
	for key, value := range spanAttrs {
		attrs[key] = value
	}
	attrs["event_type"] = "request_span"
	attrs["trace_id"] = traceID
	attrs["span_id"] = spanID
	if parentSpanID := strings.TrimSpace(stringFromAny(span["parentSpanId"])); parentSpanID != "" {
		attrs["parent_span_id"] = parentSpanID
	}
	if service := firstOTLPAttr(attrs, "service"); service != "" {
		attrs["service"] = service
	}
	if requestID := firstOTLPAttr(attrs, "request_id"); requestID != "" {
		attrs["request_id"] = requestID
	}
	if stage := firstOTLPAttr(attrs, "stage"); stage != "" {
		attrs["stage"] = stage
	} else if name != "" {
		attrs["stage"] = name
	}
	if name != "" {
		attrs["name"] = name
	}
	if statusCode := firstOTLPAttr(attrs, "status_code"); statusCode != "" {
		attrs["status_code"] = statusCode
	}
	if path != "" {
		attrs["otlp_path"] = path
	}
	startedAt := otlpUnixNanoTime(span["startTimeUnixNano"])
	endedAt := otlpUnixNanoTime(span["endTimeUnixNano"])
	if !startedAt.IsZero() && !endedAt.IsZero() && endedAt.After(startedAt) {
		attrs["stage_ms"] = strconv.FormatInt(endedAt.Sub(startedAt).Milliseconds(), 10)
	}
	if status, ok := span["status"].(map[string]any); ok {
		statusCode := strings.TrimSpace(stringFromAny(status["code"]))
		statusMessage := strings.TrimSpace(stringFromAny(status["message"]))
		if strings.EqualFold(statusCode, "STATUS_CODE_ERROR") || statusCode == "2" {
			if statusMessage == "" {
				statusMessage = "otel_status_error"
			}
			attrs["error_type"] = statusMessage
		}
	}
	timestamp := startedAt
	if timestamp.IsZero() {
		timestamp = receivedAt
	}
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	return Event{
		Timestamp:  timestamp.UTC(),
		Kind:       EventKindSpan,
		Source:     "otlp_http",
		Message:    "otlp span accepted",
		Attributes: attrs,
	}, redacted, true
}

func otlpAttributes(raw any) (map[string]string, int) {
	out := map[string]string{}
	redacted := 0
	for _, item := range otlpList(raw) {
		entry, _ := item.(map[string]any)
		key := strings.TrimSpace(stringFromAny(entry["key"]))
		if key == "" {
			continue
		}
		if IsSecretField(key) {
			redacted++
			continue
		}
		value := otlpAttributeValue(entry["value"])
		if value == "" {
			continue
		}
		if clean, changed := RedactText(value); changed {
			redacted++
			value = clean
		}
		normalized := normalizeOTLPAttributeKey(key)
		if normalized != "" {
			out[normalized] = value
		}
		if normalized != key {
			out[key] = value
		}
	}
	return out, redacted
}

func normalizeOTLPAttributeKey(key string) string {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "service.name":
		return "service"
	case "fugue.tenant_id", "tenant.id", "tenant_id":
		return "tenant_id"
	case "fugue.project_id", "project.id", "project_id":
		return "project_id"
	case "fugue.app_id", "app.id", "app_id":
		return "app_id"
	case "fugue.runtime_id", "runtime.id", "runtime_id":
		return "runtime_id"
	case "fugue.request_id", "request.id", "request_id":
		return "request_id"
	case "fugue.stage", "stage":
		return "stage"
	case "http.response.status_code", "http.status_code", "status_code":
		return "status_code"
	default:
		return strings.TrimSpace(key)
	}
}

func otlpAttributeValue(raw any) string {
	value, _ := raw.(map[string]any)
	if len(value) == 0 {
		return stringFromAny(raw)
	}
	for _, key := range []string{"stringValue", "intValue", "doubleValue", "boolValue"} {
		if out := stringFromAny(value[key]); out != "" {
			return out
		}
	}
	if bytes, ok := value["bytesValue"].(string); ok {
		return bytes
	}
	if arrayValue, ok := value["arrayValue"]; ok {
		body, err := json.Marshal(arrayValue)
		if err == nil {
			return string(body)
		}
	}
	if kvListValue, ok := value["kvlistValue"]; ok {
		body, err := json.Marshal(kvListValue)
		if err == nil {
			return string(body)
		}
	}
	return ""
}

func otlpList(raw any) []any {
	items, _ := raw.([]any)
	return items
}

func stringFromAny(raw any) string {
	switch value := raw.(type) {
	case string:
		return value
	case float64:
		if value == float64(int64(value)) {
			return strconv.FormatInt(int64(value), 10)
		}
		return fmt.Sprint(value)
	case bool:
		return strconv.FormatBool(value)
	case json.Number:
		return value.String()
	default:
		return ""
	}
}

func otlpUnixNanoTime(raw any) time.Time {
	value := strings.TrimSpace(stringFromAny(raw))
	if value == "" {
		return time.Time{}
	}
	nanos, err := strconv.ParseInt(value, 10, 64)
	if err != nil || nanos <= 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos).UTC()
}

func firstOTLPAttr(attrs map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(attrs[key]); value != "" {
			return value
		}
	}
	return ""
}

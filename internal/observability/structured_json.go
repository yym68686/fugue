package observability

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func eventsFromStructuredTelemetryJSON(defaultKind EventKind, path string, contentType string, body []byte, receivedAt time.Time) ([]Event, int) {
	if !strings.Contains(strings.ToLower(contentType), "json") || !json.Valid(body) {
		return nil, 0
	}
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, 0
	}
	objects := structuredTelemetryObjects(raw)
	if len(objects) == 0 {
		return nil, 0
	}
	events := make([]Event, 0, len(objects))
	redacted := 0
	for _, object := range objects {
		if !isStructuredTelemetryObject(object) {
			continue
		}
		event, n, ok := eventFromStructuredTelemetryObject(defaultKind, path, object, receivedAt)
		redacted += n
		if ok {
			events = append(events, event)
		}
	}
	return events, redacted
}

func structuredTelemetryObjects(raw any) []map[string]any {
	switch value := raw.(type) {
	case []any:
		objects := make([]map[string]any, 0, len(value))
		for _, item := range value {
			if object, ok := item.(map[string]any); ok {
				objects = append(objects, object)
			}
		}
		return objects
	case map[string]any:
		for _, key := range []string{"events", "logs", "metrics"} {
			items := otlpList(value[key])
			if len(items) == 0 {
				continue
			}
			objects := make([]map[string]any, 0, len(items))
			for _, item := range items {
				if object, ok := item.(map[string]any); ok {
					objects = append(objects, object)
				}
			}
			if len(objects) > 0 {
				return objects
			}
		}
		return []map[string]any{value}
	default:
		return nil
	}
}

func isStructuredTelemetryObject(object map[string]any) bool {
	for _, key := range []string{
		"kind",
		"event_kind",
		"event_type",
		"message",
		"msg",
		"attributes",
		"metric",
		"name",
		"value",
		"trace_id",
		"span_id",
		"stage",
		"summary",
		"summary_json",
	} {
		if _, ok := object[key]; ok {
			return true
		}
	}
	return false
}

func eventFromStructuredTelemetryObject(defaultKind EventKind, path string, object map[string]any, receivedAt time.Time) (Event, int, bool) {
	kind := defaultKind
	if structuredTelemetryKindExplicit(object) {
		detected := eventKindFromStructuredLog(object)
		kind = detected
	}
	if kind == "" {
		kind = EventKindLog
	}
	timestamp := structuredTelemetryTimestamp(object, receivedAt)
	message := strings.TrimSpace(firstStructuredString(object, "message", "msg"))
	if message == "" {
		message = firstStructuredString(object, "event_type", "metric", "name")
	}
	if message == "" {
		message = "structured telemetry accepted"
	}
	source := strings.TrimSpace(firstStructuredString(object, "source"))
	if source == "" {
		source = "structured_json"
	}

	attrs := map[string]string{}
	redacted := 0
	if rawAttrs, ok := object["attributes"].(map[string]any); ok {
		n := appendStructuredTelemetryAttributes(attrs, rawAttrs, "")
		redacted += n
	}
	if rawSummary, ok := object["summary"].(map[string]any); ok {
		n := appendStructuredTelemetryAttributes(attrs, rawSummary, "summary.")
		redacted += n
	}
	for key, value := range object {
		switch key {
		case "timestamp", "time", "ts", "kind", "event_kind", "message", "msg", "attributes", "summary":
			continue
		}
		if IsSecretField(key) {
			redacted++
			continue
		}
		attrKey := normalizeStructuredTelemetryKey(key)
		if attrKey == "" {
			continue
		}
		attrValue := structuredTelemetryScalar(value)
		if attrValue == "" {
			continue
		}
		if attrKey == "summary_json" && !json.Valid([]byte(attrValue)) {
			continue
		}
		if clean, changed := RedactText(attrValue); changed {
			redacted++
			attrValue = clean
		}
		attrs[attrKey] = attrValue
	}
	if path != "" && attrs["otlp_path"] == "" {
		attrs["otlp_path"] = path
	}
	return Event{
		Timestamp:  timestamp,
		Kind:       kind,
		Source:     source,
		Message:    message,
		Attributes: attrs,
	}, redacted, true
}

func structuredTelemetryKindExplicit(object map[string]any) bool {
	for _, key := range []string{"kind", "event_kind", "event_type"} {
		if _, ok := object[key]; ok {
			return true
		}
	}
	return false
}

func appendStructuredTelemetryAttributes(attrs map[string]string, raw map[string]any, prefix string) int {
	redacted := 0
	for key, value := range raw {
		if IsSecretField(key) {
			redacted++
			continue
		}
		attrKey := prefix + normalizeStructuredTelemetryKey(key)
		if strings.TrimSpace(attrKey) == "" {
			continue
		}
		attrValue := structuredTelemetryScalar(value)
		if attrValue == "" {
			continue
		}
		if clean, changed := RedactText(attrValue); changed {
			redacted++
			attrValue = clean
		}
		attrs[attrKey] = attrValue
	}
	return redacted
}

func normalizeStructuredTelemetryKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	normalized := normalizeOTLPAttributeKey(key)
	if normalized != "" {
		return normalized
	}
	return key
}

func structuredTelemetryScalar(raw any) string {
	switch value := raw.(type) {
	case string:
		return strings.TrimSpace(value)
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

func structuredTelemetryTimestamp(object map[string]any, fallback time.Time) time.Time {
	for _, key := range []string{"timestamp", "time", "ts"} {
		raw := strings.TrimSpace(structuredTelemetryScalar(object[key]))
		if raw == "" {
			continue
		}
		if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			return parsed.UTC()
		}
		if millis, err := strconv.ParseInt(raw, 10, 64); err == nil && millis > 0 {
			if millis > 1_000_000_000_000_000 {
				return time.Unix(0, millis).UTC()
			}
			if millis > 1_000_000_000 {
				return time.UnixMilli(millis).UTC()
			}
			return time.Unix(millis, 0).UTC()
		}
	}
	if fallback.IsZero() {
		return time.Now().UTC()
	}
	return fallback.UTC()
}

func firstStructuredString(object map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(structuredTelemetryScalar(object[key])); value != "" {
			return value
		}
	}
	return ""
}

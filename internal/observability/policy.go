package observability

import (
	"regexp"
	"sort"
	"strings"
)

const (
	DefaultSummaryMaxFields     = 16
	DefaultSummaryMaxKeyBytes   = 64
	DefaultSummaryMaxValueBytes = 256
)

var metricLabelAllowlist = map[string]struct{}{
	"tenant_id":    {},
	"project_id":   {},
	"app_id":       {},
	"route_id":     {},
	"runtime_id":   {},
	"region":       {},
	"status_class": {},
	"method":       {},
	"component":    {},
}

var highCardinalityLabels = map[string]struct{}{
	"trace_id":            {},
	"request_id":          {},
	"user_id":             {},
	"credential":          {},
	"credential_id":       {},
	"email":               {},
	"ip":                  {},
	"external_request_id": {},
	"session_id":          {},
}

var secretFieldTokens = []string{
	"authorization",
	"cookie",
	"set-cookie",
	"x-api-key",
	"password",
	"passwd",
	"secret",
	"token",
	"dsn",
	"database_url",
	"private_key",
	"credential",
}

var secretTextPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(authorization\s*[:=]\s*)([^,\s]+)`),
	regexp.MustCompile(`(?i)(cookie\s*[:=]\s*)([^,\s]+)`),
	regexp.MustCompile(`(?i)(set-cookie\s*[:=]\s*)([^,\s]+)`),
	regexp.MustCompile(`(?i)(x-api-key\s*[:=]\s*)([^,\s]+)`),
	regexp.MustCompile(`(?i)((?:access_|refresh_)?token\s*[:=]\s*)([^,\s]+)`),
	regexp.MustCompile(`(?i)(password\s*[:=]\s*)([^,\s]+)`),
	regexp.MustCompile(`(?i)(database_url\s*[:=]\s*)([^,\s]+)`),
}

type SummaryPolicy struct {
	MaxFields     int
	MaxKeyBytes   int
	MaxValueBytes int
}

func DefaultSummaryPolicy() SummaryPolicy {
	return SummaryPolicy{
		MaxFields:     DefaultSummaryMaxFields,
		MaxKeyBytes:   DefaultSummaryMaxKeyBytes,
		MaxValueBytes: DefaultSummaryMaxValueBytes,
	}
}

func IsAllowedMetricLabel(key string) bool {
	_, ok := metricLabelAllowlist[normalizeFieldKey(key)]
	return ok
}

func IsDeniedMetricLabel(key string) bool {
	key = normalizeFieldKey(key)
	if _, ok := highCardinalityLabels[key]; ok {
		return true
	}
	return IsSecretField(key)
}

func IsSecretField(key string) bool {
	normalized := normalizeFieldKey(key)
	if normalized == "" {
		return false
	}
	for _, token := range secretFieldTokens {
		if normalized == token || strings.Contains(normalized, token) {
			return true
		}
	}
	return false
}

func RedactFields(fields map[string]string) map[string]string {
	if len(fields) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(fields))
	for key, value := range fields {
		if IsSecretField(key) {
			out[key] = "[REDACTED]"
			continue
		}
		out[key] = value
	}
	return out
}

func RedactText(value string) (string, bool) {
	redacted := value
	changed := false
	for _, pattern := range secretTextPatterns {
		next := pattern.ReplaceAllString(redacted, "${1}[REDACTED]")
		if next != redacted {
			changed = true
			redacted = next
		}
	}
	return redacted, changed
}

func SanitizeSummaryFields(fields map[string]string, policy SummaryPolicy) (map[string]string, []string) {
	if policy.MaxFields <= 0 {
		policy.MaxFields = DefaultSummaryMaxFields
	}
	if policy.MaxKeyBytes <= 0 {
		policy.MaxKeyBytes = DefaultSummaryMaxKeyBytes
	}
	if policy.MaxValueBytes <= 0 {
		policy.MaxValueBytes = DefaultSummaryMaxValueBytes
	}
	if len(fields) == 0 {
		return map[string]string{}, nil
	}

	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := map[string]string{}
	warnings := []string{}
	for _, key := range keys {
		if len(out) >= policy.MaxFields {
			warnings = append(warnings, "summary field limit reached")
			break
		}
		normalized := strings.TrimSpace(key)
		if normalized == "" {
			continue
		}
		if IsSecretField(normalized) {
			warnings = append(warnings, "dropped secret summary field")
			continue
		}
		value := strings.TrimSpace(fields[key])
		if value == "" {
			continue
		}
		out[truncateBytes(normalized, policy.MaxKeyBytes)] = truncateBytes(value, policy.MaxValueBytes)
	}
	return out, dedupeWarnings(warnings)
}

func normalizeFieldKey(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.ReplaceAll(raw, "-", "_")
	raw = strings.ReplaceAll(raw, ".", "_")
	return raw
}

func truncateBytes(value string, maxBytes int) string {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	return value[:maxBytes]
}

func dedupeWarnings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

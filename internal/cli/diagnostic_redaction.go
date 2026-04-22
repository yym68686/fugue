package cli

import (
	"regexp"
	"strings"
)

var (
	redactAuthorizationHeaderPattern = regexp.MustCompile(`(?im)(authorization\s*[:=]\s*bearer\s+)([^\s,;]+)`)
	redactCookieHeaderPattern        = regexp.MustCompile(`(?im)(cookie\s*[:=]\s*)([^\r\n]+)`)
	redactSetCookiePattern           = regexp.MustCompile(`(?im)(set-cookie\s*:\s*[^=;,\r\n]+)=([^;\r\n]+)`)
	redactJSONSecretPattern          = regexp.MustCompile(`(?i)("(?:(?:access|refresh)_token|token|api[_-]?key|secret|password|authorization|cookie|session(?:_id)?)"\s*:\s*")([^"]*)(")`)
	redactQuerySecretPattern         = regexp.MustCompile(`(?i)\b((?:access|refresh)_token|token|api[_-]?key|secret|password|session(?:_id)?)=([^&\s]+)`)
)

func redactDiagnosticString(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return raw
	}
	redacted := redactAuthorizationHeaderPattern.ReplaceAllString(raw, `${1}[redacted]`)
	redacted = redactCookieHeaderPattern.ReplaceAllString(redacted, `${1}[redacted]`)
	redacted = redactSetCookiePattern.ReplaceAllString(redacted, `${1}=[redacted]`)
	redacted = redactJSONSecretPattern.ReplaceAllString(redacted, `${1}[redacted]${3}`)
	redacted = redactQuerySecretPattern.ReplaceAllString(redacted, `${1}=[redacted]`)
	return redacted
}

func redactDiagnosticHeaderValue(name, value string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	switch name {
	case "authorization", "proxy-authorization", "cookie", "x-api-key", "x-auth-token":
		if strings.TrimSpace(value) == "" {
			return value
		}
		return redactedSecretValue
	case "set-cookie":
		return redactDiagnosticString(value)
	default:
		return redactDiagnosticString(value)
	}
}

func redactDiagnosticHeaders(headers map[string][]string) map[string][]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string][]string, len(headers))
	for key, values := range headers {
		entries := make([]string, 0, len(values))
		for _, value := range values {
			entries = append(entries, redactDiagnosticHeaderValue(key, value))
		}
		out[key] = entries
	}
	return out
}

func redactDiagnosticStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, redactDiagnosticString(value))
	}
	return out
}

func redactDiagnosticStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		if diagnosticKeyLooksSensitive(key) {
			out[key] = redactedSecretValue
			continue
		}
		out[key] = redactDiagnosticString(value)
	}
	return out
}

func diagnosticKeyLooksSensitive(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	for _, needle := range []string{
		"authorization",
		"accesstoken",
		"refreshtoken",
		"token",
		"apikey",
		"secret",
		"password",
		"cookie",
		"sessionid",
		"session",
	} {
		if strings.Contains(normalized, needle) {
			return true
		}
	}
	return false
}

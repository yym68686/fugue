package cli

import (
	"encoding/json"
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
	redacted := redactDiagnosticScalarString(raw)
	if structured, ok := redactDiagnosticJSONText(redacted); ok {
		return structured
	}
	return redacted
}

func redactDiagnosticScalarString(raw string) string {
	redacted := redactAuthorizationHeaderPattern.ReplaceAllString(raw, `${1}[redacted]`)
	redacted = redactCookieHeaderPattern.ReplaceAllString(redacted, `${1}[redacted]`)
	redacted = redactSetCookiePattern.ReplaceAllString(redacted, `${1}=[redacted]`)
	redacted = redactJSONSecretPattern.ReplaceAllString(redacted, `${1}[redacted]${3}`)
	redacted = redactQuerySecretPattern.ReplaceAllString(redacted, `${1}=[redacted]`)
	return redacted
}

func redactDiagnosticJSONText(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !json.Valid([]byte(trimmed)) {
		return "", false
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", false
	}
	rendered, err := json.Marshal(redactDiagnosticJSONValue(value, false))
	if err != nil {
		return "", false
	}
	return string(rendered), true
}

func redactDiagnosticJSONValue(value any, force bool) any {
	if force {
		return redactDiagnosticJSONSecretValue(value)
	}
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			switch {
			case diagnosticKeyLooksSensitive(key):
				out[key] = redactDiagnosticJSONSecretValue(child)
			case diagnosticKeyRedactsChildren(key):
				out[key] = redactDiagnosticJSONValue(child, true)
			default:
				out[key] = redactDiagnosticJSONValue(child, false)
			}
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, child := range typed {
			out[index] = redactDiagnosticJSONValue(child, false)
		}
		return out
	case string:
		scalar := redactDiagnosticScalarString(typed)
		if structured, ok := redactDiagnosticJSONText(scalar); ok {
			return structured
		}
		return scalar
	default:
		return typed
	}
}

func redactDiagnosticJSONSecretValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			out[key] = redactDiagnosticJSONSecretValue(child)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, child := range typed {
			out[index] = redactDiagnosticJSONSecretValue(child)
		}
		return out
	case string:
		if typed == "" {
			return typed
		}
		return redactedSecretValue
	case nil:
		return nil
	default:
		return redactedSecretValue
	}
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
		"databaseurl",
		"dsn",
		"connectionstring",
		"connectionurl",
		"postgresurl",
		"postgresqlurl",
		"sessionid",
		"session",
	} {
		if strings.Contains(normalized, needle) {
			return true
		}
	}
	return false
}

func diagnosticKeyRedactsChildren(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	switch normalized {
	case "env", "environment", "environmentvariables":
		return true
	default:
		return false
	}
}

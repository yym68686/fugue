package cli

import (
	"encoding/json"
	"regexp"
	"strings"
)

var (
	redactAuthorizationHeaderPattern = regexp.MustCompile(`(?im)((?:proxy-)?authorization\s*[:=]\s*)(?:(?:bearer|basic|token)\s+)?([^\s,;]+)`)
	redactAPIHeaderPattern           = regexp.MustCompile(`(?im)((?:x-api-key|x-auth-token|x-access-token|api-key)\s*[:=]\s*)([^\s,;]+)`)
	redactCookieHeaderPattern        = regexp.MustCompile(`(?im)(cookie\s*[:=]\s*)([^\r\n]+)`)
	redactSetCookiePattern           = regexp.MustCompile(`(?im)(set-cookie\s*:\s*[^=;,\r\n]+)=([^;\r\n]+)`)
	redactJSONSecretPattern          = regexp.MustCompile(`(?i)("(?:(?:access|refresh|auth|id)_token|token|api[_-]?key|(?:client|webhook|signing)_secret|(?:aws_)?secret_access_key|private_key|secret|password|authorization|cookie|session(?:_id)?)"\s*:\s*")([^"]*)(")`)
	redactQuerySecretPattern         = regexp.MustCompile(`(?i)\b((?:access|refresh|auth|id)_token|token|api[_-]?key|(?:client|webhook)_secret|(?:aws_)?secret_access_key|password|session(?:_id)?)=([^&\s]+)`)
	redactURLUserInfoPattern         = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://[^\s/@:]+:)([^@\s/]+)(@)`)
	redactCredentialAssignment       = regexp.MustCompile(`(?im)(^|[\s;,])([a-z][a-z0-9_.-]*)(\s*[:=]\s*)([^\s,;]+)`)
)

func redactDiagnosticString(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return raw
	}
	if structured, ok := redactDiagnosticJSONText(raw); ok {
		return structured
	}
	if structured, ok := redactDiagnosticEmbeddedJSONText(raw); ok {
		return structured
	}
	return redactDiagnosticScalarString(raw)
}

func redactDiagnosticScalarString(raw string) string {
	redacted := redactAuthorizationHeaderPattern.ReplaceAllString(raw, `${1}[redacted]`)
	redacted = redactAPIHeaderPattern.ReplaceAllString(redacted, `${1}[redacted]`)
	redacted = redactCookieHeaderPattern.ReplaceAllString(redacted, `${1}[redacted]`)
	redacted = redactSetCookiePattern.ReplaceAllString(redacted, `${1}=[redacted]`)
	redacted = redactJSONSecretPattern.ReplaceAllString(redacted, `${1}[redacted]${3}`)
	redacted = redactQuerySecretPattern.ReplaceAllString(redacted, `${1}=[redacted]`)
	redacted = redactURLUserInfoPattern.ReplaceAllString(redacted, `${1}[redacted]${3}`)
	redacted = redactCredentialAssignments(redacted)
	return redacted
}

func redactCredentialAssignments(raw string) string {
	return redactCredentialAssignment.ReplaceAllStringFunc(raw, func(match string) string {
		parts := redactCredentialAssignment.FindStringSubmatch(match)
		if len(parts) != 5 || !diagnosticKeyLooksSensitive(parts[2]) {
			return match
		}
		return parts[1] + parts[2] + parts[3] + redactedSecretValue
	})
}

func redactDiagnosticEmbeddedJSONText(raw string) (string, bool) {
	for index := 0; index < len(raw); index++ {
		if raw[index] != '{' && raw[index] != '[' {
			continue
		}
		structured, ok := redactDiagnosticJSONText(strings.TrimSpace(raw[index:]))
		if !ok {
			continue
		}
		return redactDiagnosticScalarString(raw[:index]) + structured, true
	}
	return "", false
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
	normalized := normalizeDiagnosticSecretKey(key)
	switch normalized {
	case "session", "sessionid", "databaseurl", "dsn", "connectionstring", "connectionurl", "postgresurl", "postgresqlurl":
		return true
	}
	if diagnosticKeyIsNonSecretMetadata(normalized) {
		return false
	}
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
		"privatekey",
		"signingkey",
		"sessionid",
		"session",
	} {
		if strings.Contains(normalized, needle) {
			return true
		}
	}
	return false
}

func normalizeDiagnosticSecretKey(key string) string {
	normalized := strings.ToLower(strings.TrimSpace(key))
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	normalized = strings.ReplaceAll(normalized, ".", "")
	return normalized
}

func diagnosticKeyIsNonSecretMetadata(normalized string) bool {
	for _, suffix := range []string{
		"id",
		"ids",
		"identifier",
		"identifiers",
		"name",
		"names",
		"ref",
		"refs",
		"reference",
		"references",
		"prefix",
		"hash",
		"digest",
		"fingerprint",
		"type",
		"types",
		"endpoint",
		"path",
		"version",
	} {
		if strings.HasSuffix(normalized, suffix) {
			return true
		}
	}
	return false
}

func diagnosticKeyRedactsChildren(key string) bool {
	normalized := normalizeDiagnosticSecretKey(key)
	switch normalized {
	case "env", "environment", "environmentvariables":
		return true
	default:
		return false
	}
}

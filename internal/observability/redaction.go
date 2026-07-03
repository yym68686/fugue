package observability

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

type RedactionFinding struct {
	Kind           string `json:"kind"`
	OriginalLength int    `json:"original_length"`
	SHA256Prefix   string `json:"sha256_prefix,omitempty"`
}

type RedactionResult struct {
	Text     string             `json:"text"`
	Changed  bool               `json:"changed"`
	Findings []RedactionFinding `json:"findings,omitempty"`
}

var (
	secretAssignmentPattern    = regexp.MustCompile(`(?i)(^|[\s;])([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASSWD|PWD|KEY|CREDENTIAL|DATABASE_URL|DB_URL|DSN)[A-Z0-9_]*\s*[=:]\s*)([^\s&]+)`)
	bearerPattern              = regexp.MustCompile(`(?i)(Bearer\s+)([A-Za-z0-9._~+\-/]+=*)`)
	cookiePattern              = regexp.MustCompile(`(?i)((?:Cookie|Set-Cookie)\s*[:=]\s*)([^\r\n]+)`)
	authorizationHeaderPattern = regexp.MustCompile(`(?i)(Authorization\s*[:=]\s*(?:Bearer\s+)?)([^\r\n]+)`)
	emailPattern               = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	longDigitPattern           = regexp.MustCompile(`\b\d{12,19}\b`)
	urlSecretQueryPattern      = regexp.MustCompile(`(?i)([?&](?:token|key|secret|password|signature|credential)=)([^\s&]+)`)
	dsnCredentialPattern       = regexp.MustCompile(`(?i)(\b(?:postgres|postgresql|mysql|redis|mongodb)://[^\s:@/]+:)([^\s@]+)(@)`)
)

func RedactDiagnosticText(text string) RedactionResult {
	result := RedactionResult{Text: text}
	result.Text = replaceSensitiveMatches(result.Text, secretAssignmentPattern, "secret_like_assignment", 3, &result)
	result.Text = replaceSensitiveMatches(result.Text, authorizationHeaderPattern, "sensitive_header", 2, &result)
	result.Text = replaceSensitiveMatches(result.Text, bearerPattern, "authorization_bearer", 2, &result)
	result.Text = replaceSensitiveMatches(result.Text, cookiePattern, "sensitive_header", 2, &result)
	result.Text = replaceSensitiveMatches(result.Text, urlSecretQueryPattern, "secret_like_query", 2, &result)
	result.Text = replaceSensitiveMatches(result.Text, dsnCredentialPattern, "dsn_password", 2, &result)
	result.Text = replaceSensitiveMatches(result.Text, emailPattern, "email", 0, &result)
	result.Text = replaceSensitiveMatches(result.Text, longDigitPattern, "long_digit_identifier", 0, &result)
	result.Changed = result.Text != text
	return result
}

func replaceSensitiveMatches(text string, pattern *regexp.Regexp, kind string, valueGroup int, result *RedactionResult) string {
	matches := pattern.FindAllStringSubmatchIndex(text, -1)
	if len(matches) == 0 {
		return text
	}
	var b strings.Builder
	last := 0
	for _, match := range matches {
		start, end := match[0], match[1]
		valueStart, valueEnd := start, end
		if valueGroup > 0 && len(match) > valueGroup*2+1 && match[valueGroup*2] >= 0 {
			valueStart, valueEnd = match[valueGroup*2], match[valueGroup*2+1]
		}
		b.WriteString(text[last:valueStart])
		value := text[valueStart:valueEnd]
		b.WriteString("[REDACTED:")
		b.WriteString(kind)
		b.WriteString(":")
		b.WriteString(hashPrefix(value))
		b.WriteString("]")
		last = valueEnd
		if result != nil {
			result.Findings = append(result.Findings, RedactionFinding{
				Kind:           kind,
				OriginalLength: len(value),
				SHA256Prefix:   hashPrefix(value),
			})
		}
		_ = start
		_ = end
	}
	b.WriteString(text[last:])
	return b.String()
}

func hashPrefix(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:12]
}

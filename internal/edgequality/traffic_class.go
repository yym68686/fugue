package edgequality

import "strings"

// NormalizeTrafficClass is the closed taxonomy used by API sample ingestion.
// The worker adopts it in its own release; compatible binaries keep the same
// accepted values while API and worker are deployed independently.
func NormalizeTrafficClass(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "large_body_api", "small_api", "dynamic_api", "static_cacheable", "streaming", "sse", "websocket", "html_dynamic":
		return value
	default:
		return ""
	}
}

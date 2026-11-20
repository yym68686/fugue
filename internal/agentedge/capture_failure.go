package agentedge

import "net/http"

const CaptureFailureIDHeader = "X-Fugue-Agent-Capture-Failure-Id"

// CaptureFailureWitnesses holds at most one response ID per acquisition path.
// These values do not affect grant or transport decisions and are never persisted
// in the checkpoint.
type CaptureFailureWitnesses struct {
	SelectedEdge string
	Origin       string
}

// This only identifies a candidate join to the API's private receipt. A TLS
// intermediary may emit a header; header syntax alone is not proof of origin.
func captureFailureID(response *http.Response, request *http.Request) string {
	if response.StatusCode != http.StatusServiceUnavailable || request.URL.Scheme != "https" ||
		response.TLS == nil || len(response.TLS.VerifiedChains) == 0 {
		return ""
	}
	values := response.Header.Values(CaptureFailureIDHeader)
	if len(values) != 1 || len(values[0]) != 32 {
		return ""
	}
	for _, c := range values[0] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return ""
		}
	}
	return values[0]
}

package edge

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

var edgeStructuredLogMu sync.Mutex

func (s *Service) logProxyObservationFact(observed edgeProxyObservation) {
	if s == nil || s.Logger == nil {
		return
	}
	writeEdgeStructuredLog(s.Logger.Writer(), edgeProxyObservationRequestFactFields(observed, s.Config))
}

func writeEdgeStructuredLog(w io.Writer, fields map[string]any) {
	if w == nil || len(fields) == 0 {
		return
	}
	body, err := json.Marshal(fields)
	if err != nil {
		return
	}
	edgeStructuredLogMu.Lock()
	defer edgeStructuredLogMu.Unlock()
	_, _ = fmt.Fprintln(w, string(body))
}

func edgeProxyObservationRequestFactFields(observed edgeProxyObservation, cfg config.EdgeConfig) map[string]any {
	if observed.StatusCode == 0 {
		observed.StatusCode = http.StatusOK
	}
	if observed.TTFB <= 0 {
		observed.TTFB = observed.Duration
	}
	if observed.Upstream <= 0 && observed.Proxied {
		observed.Upstream = observed.Duration
	}
	route := observed.Route
	summary := map[string]any{
		"route_kind":            strings.TrimSpace(route.RouteKind),
		"route_generation":      strings.TrimSpace(route.RouteGeneration),
		"deployment_generation": strings.TrimSpace(route.DeploymentGeneration),
		"edge_group_id":         firstNonEmpty(route.EdgeGroupID, cfg.EdgeGroupID),
		"runtime_edge_group_id": firstNonEmpty(route.RuntimeEdgeGroupID, route.RuntimeEdgeGroup),
		"runtime_region":        edgeGroupRegion(firstNonEmpty(route.RuntimeEdgeGroupID, route.RuntimeEdgeGroup)),
		"fallback_hit":          observed.FallbackHit,
		"peer_fallback":         observed.PeerFallback,
		"websocket":             observed.WebSocket,
		"sse":                   observed.SSE,
		"upload":                observed.Upload,
		"cache_status":          firstNonEmpty(strings.TrimSpace(observed.CacheStatus), edgeCacheStatusBypass),
		"cache_policy_id":       strings.TrimSpace(observed.CachePolicyID),
		"asset_class":           strings.TrimSpace(observed.AssetClass),
		"path":                  strings.TrimSpace(observed.Path),
	}
	if strings.TrimSpace(observed.UpstreamError) != "" {
		summary["upstream_error"] = true
	}
	summaryJSON, _ := json.Marshal(summary)
	return map[string]any{
		"event_type":    "request_fact",
		"message":       "edge request",
		"tenant_id":     strings.TrimSpace(route.TenantID),
		"app_id":        strings.TrimSpace(route.AppID),
		"runtime_id":    strings.TrimSpace(route.RuntimeID),
		"edge_id":       strings.TrimSpace(cfg.EdgeID),
		"trace_id":      strings.TrimSpace(observed.TraceID),
		"request_id":    strings.TrimSpace(observed.RequestID),
		"route_id":      strings.TrimSpace(route.RouteGeneration),
		"hostname":      firstNonEmpty(route.Hostname, observed.Host),
		"path_template": model.NormalizeAppRoutePathPrefix(route.PathPrefix),
		"method":        strings.TrimSpace(observed.Method),
		"status_code":   observed.StatusCode,
		"status_class":  edgeStatusClass(observed.StatusCode),
		"duration_ms":   durationMilliseconds(observed.Duration),
		"ttfb_ms":       durationMilliseconds(observed.TTFB),
		"upstream_ms":   durationMilliseconds(observed.Upstream),
		"bytes_in":      nonNegativeInt64(observed.RequestBytes),
		"bytes_out":     nonNegativeInt64(observed.ResponseBytes),
		"streaming":     observed.Streaming,
		"error_type":    edgeObservationErrorType(observed),
		"summary_json":  string(summaryJSON),
	}
}

func edgeStatusClass(statusCode int) string {
	if statusCode <= 0 {
		return ""
	}
	return fmt.Sprintf("%dxx", statusCode/100)
}

func edgeObservationErrorType(observed edgeProxyObservation) string {
	if strings.TrimSpace(observed.UpstreamError) != "" {
		return "upstream_error"
	}
	switch {
	case observed.StatusCode >= 500:
		return "server_error"
	case observed.StatusCode >= 400:
		return "client_error"
	default:
		return ""
	}
}

func durationMilliseconds(value time.Duration) int64 {
	if value <= 0 {
		return 0
	}
	return value.Milliseconds()
}

func nonNegativeInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func edgeTraceIDFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	traceparent := strings.TrimSpace(r.Header.Get("traceparent"))
	parts := strings.Split(traceparent, "-")
	if len(parts) >= 4 && len(parts[1]) == 32 && !allZeroHex(parts[1]) {
		return strings.ToLower(parts[1])
	}
	return ""
}

func edgeRequestIDFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	for _, header := range []string{"X-Request-Id", "X-Request-ID", "X-Correlation-ID"} {
		if value := strings.TrimSpace(r.Header.Get(header)); value != "" {
			return value
		}
	}
	return ""
}

func allZeroHex(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return true
	}
	for _, r := range value {
		if r != '0' {
			return false
		}
	}
	return true
}

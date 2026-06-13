package edge

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

var edgeStructuredLogMu sync.Mutex
var edgeProxyRequestSequence uint64

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
		"edge_request_id":       strings.TrimSpace(observed.EdgeRequestID),
		"protocol":              strings.TrimSpace(observed.Protocol),
		"client_ip":             strings.TrimSpace(observed.ClientIP),
		"client_remote_addr":    strings.TrimSpace(observed.ClientRemoteAddr),
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
		"origin_got_conn":       observed.OriginGotConn,
		"origin_conn_reused":    observed.OriginConnectionReused,
		"origin_wrote_headers":  observed.OriginWroteHeaders,
		"origin_wrote_request":  observed.OriginWroteRequest,
		"origin_first_byte":     observed.OriginTTFB > 0,
		"request_body_eof":      observed.RequestBodyEOF,
	}
	if observed.RequestBytes > 0 {
		summary["request_content_length"] = observed.RequestBytes
		summary["request_body_read_bytes"] = nonNegativeInt64(observed.RequestBodyReadBytes)
		if observed.RequestBodyReadBytes < observed.RequestBytes {
			summary["request_body_complete"] = false
			summary["request_body_missing_bytes"] = observed.RequestBytes - observed.RequestBodyReadBytes
		} else {
			summary["request_body_complete"] = true
		}
	}
	if errText := logSafeValue(observed.RequestBodyReadError); errText != "-" {
		summary["request_body_read_error"] = errText
	}
	if observed.RequestBodyBuffered {
		summary["request_body_buffered"] = true
		summary["request_body_buffer_bytes"] = nonNegativeInt64(observed.RequestBodyBufferBytes)
		summary["request_body_buffer_ms"] = durationMilliseconds(observed.RequestBodyBuffer)
		summary["request_body_buffer_budget_bytes"] = nonNegativeInt64(observed.RequestBodyBufferBudget)
		summary["request_body_buffer_used_bytes"] = nonNegativeInt64(observed.RequestBodyBufferUsed)
		summary["request_body_buffer_active_requests"] = nonNegativeInt64(observed.RequestBodyBufferActive)
	}
	if errText := logSafeValue(observed.RequestBodyBufferError); errText != "-" {
		summary["request_body_buffer_error"] = errText
	}
	if observed.OriginDNS > 0 {
		summary["origin_dns_ms"] = durationMilliseconds(observed.OriginDNS)
	}
	if errText := logSafeValue(observed.OriginDNSError); errText != "-" {
		summary["origin_dns_error"] = errText
	}
	if observed.OriginConnect > 0 {
		summary["origin_connect_ms"] = durationMilliseconds(observed.OriginConnect)
	}
	if errText := logSafeValue(observed.OriginConnectError); errText != "-" {
		summary["origin_connect_error"] = errText
	}
	if addr := logSafeValue(observed.OriginRemoteAddr); addr != "-" {
		summary["origin_remote_addr"] = addr
	}
	if addr := logSafeValue(observed.OriginLocalAddr); addr != "-" {
		summary["origin_local_addr"] = addr
	}
	if observed.OriginRequestWrite > 0 {
		summary["origin_request_write_ms"] = durationMilliseconds(observed.OriginRequestWrite)
	}
	if errText := logSafeValue(observed.OriginRequestWriteErr); errText != "-" {
		summary["origin_request_write_error"] = errText
	}
	if wait := originResponseHeaderWait(observed); wait > 0 {
		summary["origin_response_wait_ms"] = durationMilliseconds(wait)
	}
	if observed.OriginTTFB > 0 {
		summary["origin_ttfb_ms"] = durationMilliseconds(observed.OriginTTFB)
	}
	if observed.OriginTotal > 0 {
		summary["origin_total_ms"] = durationMilliseconds(observed.OriginTotal)
	}
	if observed.ClientCanceled {
		summary["client_canceled"] = true
	} else if strings.TrimSpace(observed.UpstreamError) != "" {
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
	if observed.ClientCanceled {
		return "client_closed"
	}
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
	return edgeRequestIDFromHeader(r.Header)
}

func edgeRequestIDFromHeader(header http.Header) string {
	if header == nil {
		return ""
	}
	for _, headerName := range []string{"X-Request-Id", "X-Request-ID", "X-Correlation-ID"} {
		if value := strings.TrimSpace(header.Get(headerName)); value != "" {
			return value
		}
	}
	return ""
}

func edgeRequestIDForProxy(r *http.Request) string {
	if r != nil {
		if value := strings.TrimSpace(r.Header.Get(edgeRequestIDHeader)); value != "" {
			return value
		}
	}
	sequence := atomic.AddUint64(&edgeProxyRequestSequence, 1)
	return fmt.Sprintf("edge_%x_%x", time.Now().UnixNano(), sequence)
}

func edgeClientIPFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if forwarded := lastForwardedForValue(r.Header.Values("X-Forwarded-For")); forwarded != "" {
		return forwarded
	}
	if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); realIP != "" {
		return realIP
	}
	if host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr)); err == nil {
		return host
	}
	return strings.TrimSpace(r.RemoteAddr)
}

func edgeClientRemoteAddrFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if value := strings.TrimSpace(r.Header.Get(edgeClientRemoteAddrHeader)); value != "" {
		return value
	}
	return strings.TrimSpace(r.RemoteAddr)
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

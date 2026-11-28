// Package appdiagnosis turns application telemetry into evidence and next actions.
package appdiagnosis

import (
	"fmt"
	"math"
	"strings"

	"fugue/internal/queryrow"
)

type Diagnosis struct {
	Bottleneck  string   `json:"bottleneck"`
	Confidence  float64  `json:"confidence"`
	Evidence    []string `json:"evidence"`
	NextActions []string `json:"next_actions"`
}

func Unavailable(reason string) Diagnosis {
	return Diagnosis{
		Bottleneck: "unavailable",
		Confidence: 0,
		Evidence: []string{
			firstNonEmpty(reason, "observability diagnosis data is unavailable"),
		},
		NextActions: []string{
			"enable and verify Fugue Observability query backends before relying on automated bottleneck diagnosis",
		},
	}
}

func FromAggregate(row map[string]any) Diagnosis {
	bottleneck := firstNonEmpty(queryrow.String(row, "top_bottleneck_stage"), "unavailable")
	confidence := queryrow.Float(row, "top_bottleneck_confidence")
	evidence := []string{}
	for _, item := range []struct {
		key   string
		label string
		unit  string
	}{
		{key: "rpm", label: "rpm"},
		{key: "p95_ttfb_ms", label: "p95_ttfb_ms", unit: "ms"},
		{key: "p95_duration_ms", label: "p95_duration_ms", unit: "ms"},
		{key: "error_rate", label: "error_rate"},
	} {
		if value, ok := queryrow.OptionalFloat(row, item.key); ok {
			evidence = append(evidence, formatAppObservabilityEvidence(item.label, value, item.unit))
		}
	}
	if len(evidence) == 0 {
		evidence = append(evidence, "diagnosis window row did not contain aggregate evidence")
	}
	nextActions := []string{
		"inspect app requests, traces, logs, and runtime metrics for the same window before changing capacity",
	}
	if bottleneck == "unavailable" {
		nextActions = []string{
			"verify diagnosis window aggregation and app telemetry ingestion before changing capacity",
		}
	}
	return Diagnosis{
		Bottleneck:  bottleneck,
		Confidence:  confidence,
		Evidence:    evidence,
		NextActions: nextActions,
	}
}

func appObservabilityEdgeUpstreamErrorLikely(count float64, rate float64) bool {
	if count < 3 {
		return false
	}
	return rate >= 0.01 || count >= 10
}

func FromRules(requestRows []map[string]any, spanRows []map[string]any, peerTTFBRows []map[string]any, eventRows []map[string]any) Diagnosis {
	requestStats := map[string]any{}
	if len(requestRows) > 0 {
		requestStats = requestRows[0]
	}
	requestCount := queryrow.Float(requestStats, "request_count")
	error5xxCount := queryrow.Float(requestStats, "error_5xx_count")
	error4xxCount := queryrow.Float(requestStats, "error_4xx_count")
	notFoundCount := queryrow.Float(requestStats, "not_found_count")
	error5xxRate := queryrow.Float(requestStats, "error_5xx_rate")
	error4xxRate := queryrow.Float(requestStats, "error_4xx_rate")
	notFoundRate := queryrow.Float(requestStats, "not_found_rate")
	p95TTFB := queryrow.Float(requestStats, "p95_ttfb_ms")
	p95Duration := queryrow.Float(requestStats, "p95_duration_ms")
	maxDuration := queryrow.Float(requestStats, "max_duration_ms")
	streamCount := queryrow.Float(requestStats, "stream_count")
	edgeUpstreamErrorCount := queryrow.Float(requestStats, "edge_upstream_error_count")
	edgeUpstreamErrorRate := 0.0
	if requestCount > 0 {
		edgeUpstreamErrorRate = edgeUpstreamErrorCount / requestCount
	}
	if value, ok := queryrow.OptionalFloat(requestStats, "edge_upstream_error_rate"); ok {
		edgeUpstreamErrorRate = value
	}
	edgeFallbackCount := queryrow.Float(requestStats, "edge_fallback_count")
	peerFallbackCount := queryrow.Float(requestStats, "peer_fallback_count")
	actionableEdgeFallbackCount := edgeFallbackCount
	if value, ok := queryrow.OptionalFloat(requestStats, "actionable_edge_fallback_count"); ok {
		actionableEdgeFallbackCount = value
	}
	actionablePeerFallbackCount := peerFallbackCount
	if value, ok := queryrow.OptionalFloat(requestStats, "actionable_peer_fallback_count"); ok {
		actionablePeerFallbackCount = value
	}

	evidence := []string{
		formatAppObservabilityEvidence("request_count", requestCount, ""),
		formatAppObservabilityEvidence("error_5xx_count", error5xxCount, ""),
		formatAppObservabilityEvidence("p95_ttfb_ms", p95TTFB, "ms"),
		formatAppObservabilityEvidence("p95_duration_ms", p95Duration, "ms"),
	}
	if edgeFallbackCount > 0 || peerFallbackCount > 0 {
		evidence = append(evidence,
			formatAppObservabilityEvidence("edge_fallback_count", edgeFallbackCount, ""),
			formatAppObservabilityEvidence("peer_fallback_count", peerFallbackCount, ""),
		)
		if actionableEdgeFallbackCount != edgeFallbackCount || actionablePeerFallbackCount != peerFallbackCount {
			evidence = append(evidence,
				formatAppObservabilityEvidence("actionable_edge_fallback_count", actionableEdgeFallbackCount, ""),
				formatAppObservabilityEvidence("actionable_peer_fallback_count", actionablePeerFallbackCount, ""),
			)
		}
	}
	if streamCount > 0 {
		evidence = append(evidence, formatAppObservabilityEvidence("stream_count", streamCount, ""))
	}
	if edgeUpstreamErrorCount > 0 {
		evidence = append(evidence,
			formatAppObservabilityEvidence("edge_upstream_error_count", edgeUpstreamErrorCount, ""),
			formatAppObservabilityEvidence("edge_upstream_error_rate", edgeUpstreamErrorRate, ""),
		)
	}

	topSpan := appObservabilityTopSpanRuleRow(spanRows)
	if topSpan != nil {
		evidence = append(evidence,
			fmt.Sprintf("top_span=%s.%s", queryrow.String(topSpan, "service"), queryrow.String(topSpan, "stage")),
			formatAppObservabilityEvidence("top_span_p95_stage_ms", queryrow.Float(topSpan, "p95_stage_ms"), "ms"),
		)
	}
	peerTTFBStats := map[string]any{}
	if len(peerTTFBRows) > 0 {
		peerTTFBStats = peerTTFBRows[0]
	}
	if queryrow.Float(peerTTFBStats, "peer_correlated_trace_count") > 0 {
		evidence = append(evidence,
			formatAppObservabilityEvidence("peer_correlated_trace_count", queryrow.Float(peerTTFBStats, "peer_correlated_trace_count"), ""),
			formatAppObservabilityEvidence("peer_delta_trace_count", queryrow.Float(peerTTFBStats, "peer_delta_trace_count"), ""),
			formatAppObservabilityEvidence("peer_delta_trace_rate", queryrow.Float(peerTTFBStats, "peer_delta_trace_rate"), ""),
			formatAppObservabilityEvidence("current_peer_delta_p95_ttfb_ms", queryrow.Float(peerTTFBStats, "current_peer_delta_p95_ttfb_ms"), "ms"),
			formatAppObservabilityEvidence("peer_p95_ttfb_ms", queryrow.Float(peerTTFBStats, "peer_p95_ttfb_ms"), "ms"),
			formatAppObservabilityEvidence("peer_delta_p95_ms", queryrow.Float(peerTTFBStats, "peer_delta_p95_ms"), "ms"),
		)
		if peerAppID := strings.TrimSpace(queryrow.String(peerTTFBStats, "peer_sample_app_id")); peerAppID != "" {
			evidence = append(evidence, "peer_sample_app_id="+truncateAppObservabilityEvidence(peerAppID, 80))
		}
	}
	if event := appObservabilityFirstEventMatching(eventRows, appObservabilityEventLooksLikeError); event != nil {
		evidence = append(evidence,
			"recent_error_event="+firstNonEmpty(queryrow.String(event, "event_type"), "app_event"),
			"recent_error_message="+truncateAppObservabilityEvidence(queryrow.String(event, "message"), 160),
		)
	}

	switch {
	case requestCount == 0:
		return Diagnosis{
			Bottleneck: "no_recent_requests",
			Confidence: 0.35,
			Evidence:   evidence,
			NextActions: []string{
				"send a small canary request or widen the diagnosis window before changing capacity",
				"verify edge request_facts ingestion if traffic was expected in this window",
			},
		}
	case appObservabilityHasTracebackEvent(eventRows):
		return Diagnosis{
			Bottleneck: "traceback_error_burst",
			Confidence: appObservabilityConfidence(0.8+error5xxRate, 0.95),
			Evidence:   evidence,
			NextActions: []string{
				"inspect logs around the traceback burst before scaling workers",
				"compare the latest release events against the first error timestamp",
			},
		}
	case appObservabilityEdgeUpstreamErrorLikely(edgeUpstreamErrorCount, edgeUpstreamErrorRate):
		return Diagnosis{
			Bottleneck: "edge_upstream_error",
			Confidence: appObservabilityConfidence(0.68+edgeUpstreamErrorRate, 0.92),
			Evidence: append(evidence,
				formatAppObservabilityEvidence("error_5xx_rate", error5xxRate, ""),
			),
			NextActions: []string{
				"open edge request samples with upstream_error=true and compare runtime readiness, route generation, and upstream duration",
				"inspect app runtime logs around the edge 502 timestamps; avoid scaling the app until the edge-to-runtime failure mode is separated from app latency",
			},
		}
	case error5xxCount >= 3 && error5xxRate >= 0.05:
		return Diagnosis{
			Bottleneck: "error_burst",
			Confidence: appObservabilityConfidence(0.72+error5xxRate, 0.95),
			Evidence: append(evidence,
				formatAppObservabilityEvidence("error_5xx_rate", error5xxRate, ""),
			),
			NextActions: []string{
				"open request samples and trace waterfall for the 5xx window",
				"inspect recent app events and runtime logs before changing capacity",
			},
		}
	case notFoundCount >= 5 && notFoundRate >= 0.2:
		return Diagnosis{
			Bottleneck: "edge_routing_not_found",
			Confidence: appObservabilityConfidence(0.62+notFoundRate, 0.9),
			Evidence: append(evidence,
				formatAppObservabilityEvidence("not_found_count", notFoundCount, ""),
				formatAppObservabilityEvidence("not_found_rate", notFoundRate, ""),
			),
			NextActions: []string{
				"inspect route table, custom domains, and path prefixes for this app",
				"compare affected hostnames and path templates in request_facts",
			},
		}
	case requestCount > 0 && (actionableEdgeFallbackCount/requestCount >= 0.05 || actionablePeerFallbackCount/requestCount >= 0.05):
		return Diagnosis{
			Bottleneck: "edge_routing_fallback",
			Confidence: appObservabilityConfidence(0.62+(actionableEdgeFallbackCount+actionablePeerFallbackCount)/requestCount, 0.9),
			Evidence:   evidence,
			NextActions: []string{
				"inspect edge group placement and runtime availability for fallback traffic",
				"verify the active edge worker slot and route generation before scaling the app",
			},
		}
	case appObservabilityRecentDeployEvent(eventRows) != nil && (error4xxRate >= 0.15 || error5xxRate >= 0.03 || (p95Duration >= 3000 && topSpan == nil)):
		return Diagnosis{
			Bottleneck: "release_regression_candidate",
			Confidence: 0.72,
			Evidence: append(evidence,
				formatAppObservabilityEvidence("error_4xx_count", error4xxCount, ""),
				formatAppObservabilityEvidence("max_duration_ms", maxDuration, "ms"),
			),
			NextActions: []string{
				"compare request failures before and after the latest deploy_event",
				"inspect the latest operation logs and roll back only if the regression aligns with the deploy window",
			},
		}
	case appObservabilityPeerTTFBDeltaLikely(requestCount, p95TTFB, peerTTFBStats) && !appObservabilityTopSpanGivesSpecificLocalCause(topSpan):
		return Diagnosis{
			Bottleneck: "pre_peer_service_wait",
			Confidence: appObservabilityPeerTTFBDeltaConfidence(peerTTFBStats),
			Evidence:   evidence,
			NextActions: []string{
				"open trace waterfall for correlated trace_ids and compare caller spans against peer app TTFB",
				"inspect caller-side auth, routing, connection-pool, retry, and response-forwarding stages before scaling the peer app",
			},
		}
	case topSpan != nil:
		if appObservabilityIsResponseStreamEndStage(topSpan) {
			if requestCount > 0 && streamCount/requestCount >= 0.5 {
				return Diagnosis{
					Bottleneck:  "streaming_response_tail",
					Confidence:  appObservabilitySpanConfidence(topSpan, p95Duration),
					Evidence:    evidence,
					NextActions: appObservabilityStreamingTailNextActions(),
				}
			}
			return Diagnosis{
				Bottleneck:  "non_streaming_response_collection",
				Confidence:  appObservabilitySpanConfidence(topSpan, p95Duration),
				Evidence:    evidence,
				NextActions: appObservabilityNonStreamingCollectionNextActions(),
			}
		}
		bottleneck, nextActions := appObservabilityBottleneckFromTopSpan(topSpan)
		return Diagnosis{
			Bottleneck:  bottleneck,
			Confidence:  appObservabilitySpanConfidence(topSpan, p95Duration),
			Evidence:    evidence,
			NextActions: nextActions,
		}
	case p95TTFB >= 1000 || p95Duration >= 3000:
		return Diagnosis{
			Bottleneck: "app_latency",
			Confidence: 0.58,
			Evidence: append(evidence,
				formatAppObservabilityEvidence("max_duration_ms", maxDuration, "ms"),
			),
			NextActions: []string{
				"add or verify request_spans to split runtime, pool, database, and upstream wait",
				"inspect requests and runtime metrics in the same window before scaling",
			},
		}
	default:
		return Diagnosis{
			Bottleneck: "no_clear_bottleneck",
			Confidence: 0.45,
			Evidence:   evidence,
			NextActions: []string{
				"keep the current capacity unchanged until one signal dominates",
				"run a short stepped load test and watch requests, spans, logs, and metrics together",
			},
		}
	}
}

func appObservabilityPeerTTFBDeltaLikely(requestCount float64, p95TTFB float64, peerTTFBStats map[string]any) bool {
	if requestCount <= 0 || p95TTFB < 1000 {
		return false
	}
	correlatedCount := queryrow.Float(peerTTFBStats, "peer_correlated_trace_count")
	deltaCount := queryrow.Float(peerTTFBStats, "peer_delta_trace_count")
	deltaRate := queryrow.Float(peerTTFBStats, "peer_delta_trace_rate")
	peerP95 := queryrow.Float(peerTTFBStats, "peer_p95_ttfb_ms")
	deltaP95 := queryrow.Float(peerTTFBStats, "peer_delta_p95_ms")
	if correlatedCount < 3 || deltaCount < 2 {
		return false
	}
	return deltaRate >= 0.35 && peerP95 > 0 && peerP95 <= 1000 && deltaP95 >= 500
}

func appObservabilityPeerTTFBDeltaConfidence(peerTTFBStats map[string]any) float64 {
	deltaRate := queryrow.Float(peerTTFBStats, "peer_delta_trace_rate")
	deltaP95 := queryrow.Float(peerTTFBStats, "peer_delta_p95_ms")
	return appObservabilityConfidence(0.62+deltaRate*0.2+math.Min(deltaP95/5000, 0.12), 0.9)
}

func appObservabilityTopSpanGivesSpecificLocalCause(row map[string]any) bool {
	if row == nil {
		return false
	}
	bottleneck, _ := appObservabilityBottleneckFromTopSpan(row)
	switch bottleneck {
	case "auth_or_api_key_wait",
		"billing_or_balance_wait",
		"routing_or_provider_selection_wait",
		"db_lock_or_query_wait",
		"request_log_write_wait",
		"event_loop_lag",
		"upstream_retry_or_cooldown",
		"token_pool_wait",
		"upstream_network_connect_wait",
		"upstream_connection_pool_wait",
		"runtime_resource_wait":
		return true
	default:
		return false
	}
}

func appObservabilityTopSpanRuleRow(rows []map[string]any) map[string]any {
	var selected map[string]any
	var selectedP95 float64
	for _, row := range rows {
		p95 := queryrow.Float(row, "p95_stage_ms")
		if selected == nil || p95 > selectedP95 {
			selected = row
			selectedP95 = p95
		}
	}
	return selected
}

func appObservabilityIsResponseStreamEndStage(row map[string]any) bool {
	stage := strings.ToLower(strings.TrimSpace(queryrow.String(row, "stage")))
	return stage == "stream_end" ||
		stage == "response_stream_end" ||
		strings.HasSuffix(stage, "_stream_end")
}

func appObservabilityStreamingTailNextActions() []string {
	return []string{
		"treat the long tail as response lifetime, not first-byte or worker acquisition latency",
		"inspect upstream_headers_received and upstream_first_chunk spans before changing capacity",
	}
}

func appObservabilityNonStreamingCollectionNextActions() []string {
	return []string{
		"confirm whether these requests are intentionally non-streaming and wait for the full upstream response",
		"inspect upstream response duration before increasing worker or connection-pool capacity",
	}
}

func appObservabilityBottleneckFromTopSpan(row map[string]any) (string, []string) {
	service := strings.ToLower(queryrow.String(row, "service"))
	stage := strings.ToLower(queryrow.String(row, "stage"))
	key := strings.Trim(strings.Join([]string{service, stage}, "."), ".")

	switch {
	case strings.Contains(stage, "db") ||
		strings.Contains(stage, "postgres") ||
		strings.Contains(stage, "sql") ||
		strings.Contains(stage, "lock"):
		return "db_lock_or_query_wait", []string{
			"inspect database lock waiters, slow queries, and transaction scope",
			"do not scale app workers until database wait is separated from runtime wait",
		}
	case strings.Contains(stage, "request_log") ||
		strings.Contains(stage, "log_queue") ||
		strings.Contains(stage, "log_write") ||
		strings.Contains(stage, "usage_write") ||
		strings.Contains(stage, "usage_record"):
		return "request_log_write_wait", []string{
			"inspect request log queue depth, write latency, and dropped log counters",
			"move diagnostic writes off the request path before scaling application workers",
		}
	case strings.Contains(stage, "event_loop"):
		return "event_loop_lag", []string{
			"inspect event loop lag, blocking code paths, and worker saturation",
			"scale workers only after confirming the dominant wait is runtime execution and not upstream I/O",
		}
	case strings.Contains(stage, "retry") ||
		strings.Contains(stage, "cooldown"):
		return "upstream_retry_or_cooldown", []string{
			"inspect retry count, provider cooldowns, and upstream failure classes",
			"fix provider health or routing before increasing concurrency",
		}
	case strings.Contains(stage, "api_key") ||
		strings.Contains(stage, "auth"):
		return "auth_or_api_key_wait", []string{
			"inspect API key lookup, auth cache hit rate, and database pool wait",
			"separate auth storage latency from upstream latency before scaling workers",
		}
	case strings.Contains(stage, "balance") ||
		strings.Contains(stage, "billing") ||
		strings.Contains(stage, "quota") ||
		strings.Contains(stage, "credit"):
		return "billing_or_balance_wait", []string{
			"inspect balance checks, billing transactions, and lock waiters",
			"keep billing correctness ahead of throughput changes",
		}
	case strings.Contains(stage, "channel") ||
		strings.Contains(stage, "provider_selected") ||
		strings.Contains(stage, "provider_key_selected"):
		return "routing_or_provider_selection_wait", []string{
			"inspect route selection, provider selection, and configuration lookup latency",
			"cache or precompute selection state only after confirming this span dominates",
		}
	case strings.Contains(stage, "token_pool") ||
		strings.Contains(stage, "token_acquire") ||
		strings.Contains(stage, "token_acquired"):
		return "token_pool_wait", []string{
			"inspect token availability, account cooldowns, and token selection latency",
			"add tokens or split traffic only after confirming upstream connection wait is low",
		}
	case strings.Contains(stage, "http_connect") ||
		strings.Contains(stage, "connect_done") ||
		strings.Contains(stage, "http_tls") ||
		strings.Contains(stage, "tls_done") ||
		strings.Contains(stage, "dns"):
		return "upstream_network_connect_wait", []string{
			"inspect upstream DNS, connect, TLS, and regional network latency",
			"reuse connections or adjust upstream placement before increasing request concurrency",
		}
	case strings.Contains(stage, "pool") ||
		strings.Contains(stage, "acquire") ||
		strings.Contains(stage, "connection"):
		return "upstream_connection_pool_wait", []string{
			"inspect client pool size, pending acquire count, and upstream concurrency",
			"increase pool or replicas only after confirming event loop lag is low",
		}
	case strings.Contains(stage, "downstream") ||
		strings.Contains(stage, "response_start") ||
		strings.Contains(stage, "response_send"):
		return "downstream_response_wait", []string{
			"inspect downstream response writing, client disconnects, and backpressure",
			"compare downstream response wait against upstream wait before increasing worker capacity",
		}
	case strings.Contains(service, "edge") ||
		strings.Contains(stage, "route") ||
		strings.Contains(stage, "routing"):
		return "edge_routing_wait", []string{
			"inspect edge route generation, fallback count, and active edge slot",
			"verify runtime readiness before scaling application workers",
		}
	case strings.Contains(stage, "cpu") ||
		strings.Contains(stage, "memory") ||
		strings.Contains(stage, "event_loop") ||
		strings.Contains(stage, "worker"):
		return "runtime_resource_wait", []string{
			"inspect runtime CPU, memory, event loop lag, and worker saturation",
			"scale runtime capacity only if resource gauges align with this span evidence",
		}
	case strings.Contains(stage, "upstream") ||
		strings.Contains(stage, "headers_received") ||
		strings.Contains(stage, "first_chunk") ||
		strings.Contains(stage, "first_token"):
		return "upstream_response_wait", []string{
			"inspect upstream service latency, streaming behavior, and provider status",
			"compare upstream response wait against local pool and event loop wait before scaling",
		}
	default:
		if key == "" {
			key = "app_span_wait"
		}
		return key, []string{
			"inspect the dominant span stage before changing capacity",
			"add lower-level spans if this stage still hides multiple waits",
		}
	}
}

func appObservabilitySpanConfidence(row map[string]any, p95Duration float64) float64 {
	p95Stage := queryrow.Float(row, "p95_stage_ms")
	if p95Stage <= 0 {
		return 0.55
	}
	if p95Duration <= 0 {
		return 0.68
	}
	return appObservabilityConfidence(0.55+(p95Stage/p95Duration)*0.45, 0.92)
}

func appObservabilityFirstEventMatching(rows []map[string]any, match func(map[string]any) bool) map[string]any {
	for _, row := range rows {
		if match(row) {
			return row
		}
	}
	return nil
}

func appObservabilityHasTracebackEvent(rows []map[string]any) bool {
	return appObservabilityFirstEventMatching(rows, func(row map[string]any) bool {
		text := strings.ToLower(strings.Join([]string{
			queryrow.String(row, "message"),
			queryrow.String(row, "event_type"),
			queryrow.String(row, "severity"),
			queryrow.String(row, "attributes_json"),
		}, " "))
		return strings.Contains(text, "traceback") ||
			strings.Contains(text, "panic") ||
			strings.Contains(text, "exception")
	}) != nil
}

func appObservabilityEventLooksLikeError(row map[string]any) bool {
	text := strings.ToLower(strings.Join([]string{
		queryrow.String(row, "message"),
		queryrow.String(row, "event_type"),
		queryrow.String(row, "severity"),
		queryrow.String(row, "attributes_json"),
	}, " "))
	return strings.Contains(text, "error") ||
		strings.Contains(text, "failed") ||
		strings.Contains(text, "traceback") ||
		strings.Contains(text, "panic") ||
		strings.Contains(text, "exception")
}

func appObservabilityRecentDeployEvent(rows []map[string]any) map[string]any {
	return appObservabilityFirstEventMatching(rows, func(row map[string]any) bool {
		eventType := strings.ToLower(queryrow.String(row, "event_type"))
		message := strings.ToLower(queryrow.String(row, "message"))
		return strings.Contains(eventType, "deploy") || strings.Contains(message, "deploy")
	})
}

func appObservabilityConfidence(value float64, maxValue float64) float64 {
	if value < 0 {
		return 0
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func truncateAppObservabilityEvidence(value string, maxLength int) string {
	value = strings.TrimSpace(value)
	if maxLength <= 0 || len(value) <= maxLength {
		return value
	}
	return value[:maxLength] + "..."
}

func formatAppObservabilityEvidence(label string, value float64, unit string) string {
	if unit == "" {
		return fmt.Sprintf("%s=%.4g", label, value)
	}
	return fmt.Sprintf("%s=%.4g%s", label, value, unit)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

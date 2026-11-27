package appdiagnosis

import (
	"strings"
	"testing"
)

func fromRulesWithoutPeers(requestRows []map[string]any, spanRows []map[string]any, eventRows []map[string]any) Diagnosis {
	return FromRules(requestRows, spanRows, nil, eventRows)
}

func TestAppObservabilityBottleneckFromTopSpanClassifiesCommonStages(t *testing.T) {
	cases := []struct {
		name       string
		service    string
		stage      string
		bottleneck string
	}{
		{name: "db pool", service: "web-api", stage: "db_pool_acquire", bottleneck: "db_lock_or_query_wait"},
		{name: "api key", service: "api", stage: "api_key_checked", bottleneck: "auth_or_api_key_wait"},
		{name: "balance", service: "api", stage: "balance_checked", bottleneck: "billing_or_balance_wait"},
		{name: "channel", service: "api", stage: "channel_selected", bottleneck: "routing_or_provider_selection_wait"},
		{name: "event loop", service: "worker", stage: "event_loop_lag_ms", bottleneck: "event_loop_lag"},
		{name: "retry", service: "proxy", stage: "retry_started", bottleneck: "upstream_retry_or_cooldown"},
		{name: "cooldown", service: "proxy", stage: "provider_cooldown_wait", bottleneck: "upstream_retry_or_cooldown"},
		{name: "token pool", service: "gateway", stage: "token_acquired", bottleneck: "token_pool_wait"},
		{name: "http connect", service: "gateway", stage: "http_connect_done", bottleneck: "upstream_network_connect_wait"},
		{name: "http tls", service: "gateway", stage: "http_tls_done", bottleneck: "upstream_network_connect_wait"},
		{name: "client pool", service: "proxy", stage: "client_pool_acquired", bottleneck: "upstream_connection_pool_wait"},
		{name: "request log", service: "gateway", stage: "request_log_queue_wait_ms", bottleneck: "request_log_write_wait"},
		{name: "usage write", service: "api", stage: "usage_write_ms", bottleneck: "request_log_write_wait"},
		{name: "upstream response", service: "proxy", stage: "upstream_headers_received", bottleneck: "upstream_response_wait"},
		{name: "downstream response", service: "proxy", stage: "downstream_response_start", bottleneck: "downstream_response_wait"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, actions := appObservabilityBottleneckFromTopSpan(map[string]any{
				"service": tt.service,
				"stage":   tt.stage,
			})
			if got != tt.bottleneck {
				t.Fatalf("expected bottleneck %q, got %q", tt.bottleneck, got)
			}
			if len(actions) == 0 || strings.TrimSpace(actions[0]) == "" {
				t.Fatalf("expected next actions for %s.%s", tt.service, tt.stage)
			}
		})
	}
}

func TestAppObservabilityRuleDiagnosisDetectsPrePeerServiceWait(t *testing.T) {
	diagnosis := FromRules(
		[]map[string]any{{
			"request_count":                  12,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    2600,
			"p95_duration_ms":                3100,
			"max_duration_ms":                4600,
			"edge_fallback_count":            0,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   0,
		}},
		[]map[string]any{{
			"service":      "api",
			"stage":        "upstream_headers_received",
			"span_count":   11,
			"p95_stage_ms": 2400,
			"max_stage_ms": 4100,
			"error_count":  0,
		}},
		[]map[string]any{{
			"peer_correlated_trace_count":    10,
			"peer_delta_trace_count":         8,
			"peer_delta_trace_rate":          0.8,
			"current_peer_delta_p95_ttfb_ms": 2600,
			"peer_p95_ttfb_ms":               350,
			"peer_delta_p95_ms":              2250,
			"peer_delta_max_ms":              3100,
			"peer_sample_app_id":             "peer_app",
		}},
		nil,
	)
	if diagnosis.Bottleneck != "pre_peer_service_wait" {
		t.Fatalf("expected pre-peer service wait, got %+v", diagnosis)
	}
	joinedEvidence := strings.Join(diagnosis.Evidence, "\n")
	for _, want := range []string{"peer_correlated_trace_count=10", "peer_p95_ttfb_ms=350ms", "peer_delta_p95_ms=2250ms", "peer_sample_app_id=peer_app"} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence %q in %+v", want, diagnosis.Evidence)
		}
	}
}

func TestAppObservabilityRuleDiagnosisKeepsSpecificSpanOverPeerDelta(t *testing.T) {
	diagnosis := FromRules(
		[]map[string]any{{
			"request_count":                  12,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    2600,
			"p95_duration_ms":                3100,
			"max_duration_ms":                4600,
			"edge_fallback_count":            0,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   0,
		}},
		[]map[string]any{{
			"service":      "api",
			"stage":        "db_pool_acquire",
			"span_count":   11,
			"p95_stage_ms": 2400,
			"max_stage_ms": 4100,
			"error_count":  0,
		}},
		[]map[string]any{{
			"peer_correlated_trace_count":    10,
			"peer_delta_trace_count":         8,
			"peer_delta_trace_rate":          0.8,
			"current_peer_delta_p95_ttfb_ms": 2600,
			"peer_p95_ttfb_ms":               350,
			"peer_delta_p95_ms":              2250,
		}},
		nil,
	)
	if diagnosis.Bottleneck != "db_lock_or_query_wait" {
		t.Fatalf("expected specific DB span to win over peer delta, got %+v", diagnosis)
	}
}

func TestAppObservabilityRuleDiagnosisDetectsTracebackErrorBurst(t *testing.T) {
	diagnosis := fromRulesWithoutPeers(
		[]map[string]any{{
			"request_count":       50,
			"error_5xx_count":     10,
			"error_4xx_count":     12,
			"not_found_count":     0,
			"error_5xx_rate":      0.2,
			"error_4xx_rate":      0.24,
			"not_found_rate":      0,
			"p95_ttfb_ms":         320,
			"p95_duration_ms":     900,
			"max_duration_ms":     2000,
			"edge_fallback_count": 0,
			"peer_fallback_count": 0,
		}},
		nil,
		[]map[string]any{{
			"event_type": "runtime_event",
			"severity":   "error",
			"message":    "Traceback: connection refused",
		}},
	)
	if diagnosis.Bottleneck != "traceback_error_burst" {
		t.Fatalf("expected traceback bottleneck, got %+v", diagnosis)
	}
	if diagnosis.Confidence < 0.9 {
		t.Fatalf("expected high confidence, got %+v", diagnosis)
	}
}

func TestAppObservabilityRuleDiagnosisDetectsEdgeUpstreamErrorBeforeGenericErrorBurst(t *testing.T) {
	diagnosis := fromRulesWithoutPeers(
		[]map[string]any{{
			"request_count":                  100,
			"error_5xx_count":                5,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0.05,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    320,
			"p95_duration_ms":                900,
			"max_duration_ms":                2000,
			"edge_upstream_error_count":      5,
			"edge_upstream_error_rate":       0.05,
			"edge_fallback_count":            0,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   0,
		}},
		nil,
		nil,
	)
	if diagnosis.Bottleneck != "edge_upstream_error" {
		t.Fatalf("expected edge upstream error, got %+v", diagnosis)
	}
	joinedEvidence := strings.Join(diagnosis.Evidence, "\n")
	for _, want := range []string{"edge_upstream_error_count=5", "edge_upstream_error_rate=0.05", "error_5xx_rate=0.05"} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence %q in %+v", want, diagnosis.Evidence)
		}
	}
}

func TestAppObservabilityRuleDiagnosisKeepsSparseEdgeUpstreamErrorAsEvidence(t *testing.T) {
	diagnosis := fromRulesWithoutPeers(
		[]map[string]any{{
			"request_count":                  486,
			"error_5xx_count":                1,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0.002057613,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    2063,
			"p95_duration_ms":                7455,
			"max_duration_ms":                15000,
			"edge_upstream_error_count":      1,
			"edge_upstream_error_rate":       0.002057613,
			"edge_fallback_count":            0,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   0,
		}},
		[]map[string]any{{
			"service":      "api",
			"stage":        "response_stream_end",
			"span_count":   10,
			"p95_stage_ms": 7455,
			"max_stage_ms": 15000,
			"error_count":  0,
		}},
		nil,
	)
	if diagnosis.Bottleneck != "non_streaming_response_collection" {
		t.Fatalf("expected sparse edge error not to override span diagnosis, got %+v", diagnosis)
	}
	joinedEvidence := strings.Join(diagnosis.Evidence, "\n")
	for _, want := range []string{"edge_upstream_error_count=1", "edge_upstream_error_rate=0.002058", "top_span=api.response_stream_end"} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence %q in %+v", want, diagnosis.Evidence)
		}
	}
}

func TestAppObservabilityRuleDiagnosisIgnoresNonActionableFallback(t *testing.T) {
	diagnosis := fromRulesWithoutPeers(
		[]map[string]any{{
			"request_count":                  46,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    5155,
			"p95_duration_ms":                19820,
			"max_duration_ms":                34350,
			"edge_fallback_count":            10,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   20,
		}},
		nil,
		nil,
	)
	if diagnosis.Bottleneck == "edge_routing_fallback" {
		t.Fatalf("expected non-actionable fallback to be ignored, got %+v", diagnosis)
	}
	if diagnosis.Bottleneck != "app_latency" {
		t.Fatalf("expected latency diagnosis after ignoring static fallback, got %+v", diagnosis)
	}
	joinedEvidence := strings.Join(diagnosis.Evidence, "\n")
	for _, want := range []string{"edge_fallback_count=10", "actionable_edge_fallback_count=0"} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence %q in %+v", want, diagnosis.Evidence)
		}
	}
}

func TestAppObservabilityRuleDiagnosisDoesNotTreatStreamingTailAsReleaseRegression(t *testing.T) {
	diagnosis := fromRulesWithoutPeers(
		[]map[string]any{{
			"request_count":                  20,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    900,
			"p95_duration_ms":                40340,
			"max_duration_ms":                129800,
			"edge_fallback_count":            5,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   20,
		}},
		[]map[string]any{{
			"service":      "oaix",
			"stage":        "response_stream_end",
			"span_count":   20,
			"p95_stage_ms": 129800,
			"max_stage_ms": 129800,
			"error_count":  0,
		}},
		[]map[string]any{{
			"event_type": "deploy_event",
			"severity":   "info",
			"message":    "deploy completed",
		}},
	)
	if diagnosis.Bottleneck == "release_regression_candidate" {
		t.Fatalf("expected streaming tail not to be treated as release regression, got %+v", diagnosis)
	}
	if diagnosis.Bottleneck != "streaming_response_tail" {
		t.Fatalf("expected streaming tail diagnosis, got %+v", diagnosis)
	}
	joinedEvidence := strings.Join(diagnosis.Evidence, "\n")
	for _, want := range []string{"stream_count=20", "top_span=oaix.response_stream_end", "top_span_p95_stage_ms=1.298e+05ms"} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence %q in %+v", want, diagnosis.Evidence)
		}
	}
}

func TestAppObservabilityRuleDiagnosisDetectsNonStreamingResponseCollection(t *testing.T) {
	diagnosis := fromRulesWithoutPeers(
		[]map[string]any{{
			"request_count":                  40,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    34000,
			"p95_duration_ms":                35100,
			"max_duration_ms":                52000,
			"edge_fallback_count":            0,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   0,
		}},
		[]map[string]any{{
			"service":      "api",
			"stage":        "stream_end",
			"span_count":   40,
			"p95_stage_ms": 34200,
			"max_stage_ms": 52000,
			"error_count":  0,
		}},
		nil,
	)
	if diagnosis.Bottleneck != "non_streaming_response_collection" {
		t.Fatalf("expected non-streaming collection diagnosis, got %+v", diagnosis)
	}
	joinedActions := strings.Join(diagnosis.NextActions, "\n")
	if !strings.Contains(joinedActions, "non-streaming") {
		t.Fatalf("expected non-streaming next action, got %+v", diagnosis.NextActions)
	}
}

func TestAppObservabilityRuleDiagnosisPrefersTopSpanOverLatencyOnlyReleaseRegression(t *testing.T) {
	diagnosis := fromRulesWithoutPeers(
		[]map[string]any{{
			"request_count":                  17,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    28310,
			"p95_duration_ms":                28320,
			"max_duration_ms":                28320,
			"edge_fallback_count":            0,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   17,
		}},
		[]map[string]any{{
			"service":      "sample-api-ember",
			"stage":        "upstream_headers_received",
			"span_count":   17,
			"p95_stage_ms": 28040,
			"max_stage_ms": 28040,
			"error_count":  0,
		}},
		[]map[string]any{{
			"event_type": "deploy_event",
			"severity":   "info",
			"message":    "deploy completed",
		}},
	)
	if diagnosis.Bottleneck == "release_regression_candidate" {
		t.Fatalf("expected top span to take precedence over latency-only release regression, got %+v", diagnosis)
	}
	if diagnosis.Bottleneck != "upstream_response_wait" {
		t.Fatalf("expected upstream headers span diagnosis, got %+v", diagnosis)
	}
}

package observability

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEdgeRouteDecisionMissingLinkQueryIsBoundedAndOrderIndependent(t *testing.T) {
	t.Parallel()
	if EdgeRouteDecisionRetention < 7*24*time.Hour || EdgeRouteDecisionRetention > 30*24*time.Hour {
		t.Fatalf("decision evidence retention is outside the required bound: %s", EdgeRouteDecisionRetention)
	}
	query := edgeRouteDecisionMissingLinkQuery("fugue_observability")
	for _, want := range []string{
		"LEFT ANTI JOIN (SELECT tenant_id, app_id, decision_id, bundle_version, route_generation FROM fugue_observability.edge_route_decisions FINAL)",
		"LEFT ANTI JOIN (SELECT tenant_id, app_id, correlation_key, request_id FROM fugue_observability.edge_route_decision_missing_links FINAL)",
		"decision.tenant_id = rf.tenant_id",
		"decision.app_id = rf.app_id",
		"rf.ts <= now64(3) - INTERVAL 2 MINUTE",
		"rf.ts >= now64(3) - INTERVAL 30 DAY",
		"rf.status_code >= 500",
		"rf.platform_error_class IN ('route_unavailable', 'no_healthy', 'bundle_signature', 'invariant')",
		"rf.request_id != ''",
		"LIMIT 100",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("missing bounded/out-of-order-safe query clause %q:\n%s", want, query)
		}
	}
	for _, forbidden := range []string{"origin_dns", "origin_connect", "origin_unavailable", "evidence_unknown", "latency_regression", "origin_connected_application_5xx"} {
		if strings.Contains(query, "'"+forbidden+"'") {
			t.Fatalf("non-decision platform class %q entered the missing-decision join:\n%s", forbidden, query)
		}
	}
}

func TestPipelineMissingDecisionLinkCheckEmitsTypedAlert(t *testing.T) {
	t.Parallel()
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query().Get("query")
		_, _ = fmt.Fprintln(w, `{"ts":"2026-08-01T00:33:31.416Z","tenant_id":"tenant_123","app_id":"app_123","hostname":"platform.example.test","request_id":"req_123","edge_id":"edge_de","decision_id":"decision_123","bundle_version":"bundle_123","route_generation":"route_123","correlation_key":"[\"decision_123\",\"bundle_123\",\"route_123\"]","status_reason":"runtime invariant violation: current_image_mismatch, image_missing","platform_error_class":"invariant"}`)
	}))
	defer server.Close()
	pipeline := NewPipeline(Config{Enabled: true, ClickHouseDSN: server.URL, QueueSize: 4, MemoryLimitBytes: 4096, ExportTimeout: time.Second}, nil)
	pipeline.checkEdgeRouteDecisionLinks(context.Background())
	if !strings.Contains(query, "edge_route_decisions FINAL") {
		t.Fatalf("link check did not query persistent decision material: %s", query)
	}
	event := mustReadQueuedEvent(t, pipeline)
	if event.Attributes["event_type"] != "edge_route_decision_material_missing" || event.Attributes["correlation_key"] == "" || event.Attributes["platform_error_class"] != "decision_missing" {
		t.Fatalf("unexpected missing-link event: %+v", event)
	}
	snapshot := pipeline.Snapshot()
	if snapshot.EdgeRouteMissingLinks != 1 || snapshot.EdgeRouteLinkChecks != 1 || snapshot.EdgeRouteLinkAlerts != 1 || snapshot.EdgeRouteLinkErrors != 0 {
		t.Fatalf("unexpected link-check metrics: %+v", snapshot)
	}
}

func TestPipelineMissingDecisionLinkStorageFailureIsObservableNotFatal(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	pipeline := NewPipeline(Config{Enabled: true, ClickHouseDSN: server.URL, QueueSize: 1, MemoryLimitBytes: 4096, ExportTimeout: time.Second}, nil)
	pipeline.checkEdgeRouteDecisionLinks(context.Background())
	snapshot := pipeline.Snapshot()
	if snapshot.EdgeRouteLinkChecks != 1 || snapshot.EdgeRouteLinkErrors != 1 || snapshot.EdgeRouteLinkAlerts != 0 {
		t.Fatalf("storage failure was not fail-observable: %+v", snapshot)
	}
	if pipeline.Ingest(context.Background(), Event{Kind: EventKindLog, Message: "serving remains independent"}) != true {
		t.Fatal("link-check storage failure affected the serving-independent ingest path")
	}
}

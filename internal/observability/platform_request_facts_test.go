package observability

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPlatformRequestFactRetainsErrorsWithoutInventingTenantOwnership(t *testing.T) {
	var query string
	var row map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query().Get("query")
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &row); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	event := Event{Timestamp: time.Now().UTC(), Kind: EventKindLog, Attributes: map[string]string{
		"event_type": "request_fact", "trace_id": "trace-platform", "edge_id": "edge-a", "route_id": "platform-route-a",
		"hostname": "api.example.test", "path_template": "/healthz", "status_code": "503", "summary_json": `{"route_kind":"control-plane-api"}`,
	}}
	if err := NewClickHouseExporter(server.URL, server.Client()).Export(context.Background(), []Event{event}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, ".request_facts ") || row["status_code"] != float64(503) || row["app_id"] != "" || row["tenant_id"] != "" {
		t.Fatalf("platform fact lost/misattributed: %s %+v", query, row)
	}
	for _, kind := range []string{model.EdgeRouteKindControlPlaneAPI, model.EdgeRouteKindPlatformRoute, model.EdgeRouteKindPlatform, model.EdgeRouteKindCustomDomain, model.EdgeRouteKindPlatformDomain, "unknown"} {
		attrs := cloneEventAttributes(event.Attributes)
		attrs["summary_json"] = `{"route_kind":"` + kind + `"}`
		candidate := event
		candidate.Attributes = attrs
		want := kind == model.EdgeRouteKindControlPlaneAPI || kind == model.EdgeRouteKindPlatformRoute
		if requestFactEventComplete(candidate) != want {
			t.Fatalf("route kind %q has wrong ownership classification", kind)
		}
	}
	for _, key := range []string{"hostname", "edge_id", "route_id", "trace_id", "path_template", "status_code", "summary_json"} {
		attrs := cloneEventAttributes(event.Attributes)
		delete(attrs, key)
		incomplete := event
		incomplete.Attributes = attrs
		if requestFactEventComplete(incomplete) {
			t.Fatalf("accepted missing %s", key)
		}
	}
	for _, key := range []string{"tenant_id", "project_id"} {
		attrs := cloneEventAttributes(event.Attributes)
		attrs[key] = "tenant-owned"
		tenantEvent := event
		tenantEvent.Attributes = attrs
		if requestFactEventComplete(tenantEvent) {
			t.Fatalf("accepted tenant request with no app: %s", key)
		}
	}
}

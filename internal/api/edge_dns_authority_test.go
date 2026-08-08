package api

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type edgeDNSAuthorityRoundTripper func(*http.Request) (*http.Response, error)

func (fn edgeDNSAuthorityRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestEdgeDNSAuthorityReadyUsesOneGroupScopedService(t *testing.T) {
	now := time.Date(2026, 8, 8, 18, 0, 0, 0, time.UTC)
	server := &Server{controlPlaneNamespace: "fugue-system"}
	server.edgeDNSAuthorityHTTPClient = &http.Client{Transport: edgeDNSAuthorityRoundTripper(func(request *http.Request) (*http.Response, error) {
		want := "http://edge-control-jp.fugue-system.svc:8092/v1/authority/groups/edge-group-metro-jp/readyz"
		if request.URL.String() != want || request.Method != http.MethodGet {
			t.Fatalf("authority request = %s %s", request.Method, request.URL)
		}
		body := `{"edge_group_id":"edge-group-metro-jp","status":"ready","ready":true,"inventory_sequence":7,"inventory_producer_nodes":2,"inventory_heartbeat_at":"2026-08-08T17:59:40Z","publication_sequence":9,"publication_decision":"published","bundle_generation":"edgegroupbundle_test","published_bundle_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bundle_valid_until":"2026-08-08T18:10:00Z","lkg_state":"current"}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	groupID, nodes, err := server.edgeDNSAuthorityReady(context.Background(), "edge-control-jp", "edge-group-metro-jp", now)
	if err != nil || groupID != "edge-group-metro-jp" || len(nodes) != 2 {
		t.Fatalf("authority ready = %q %v, %v", groupID, nodes, err)
	}
}

func TestEdgeDNSAuthorityReadyFailsClosed(t *testing.T) {
	now := time.Date(2026, 8, 8, 18, 0, 0, 0, time.UTC)
	valid := edgeDNSAuthorityStatus{EdgeGroupID: "edge-group-country-de", Status: "ready", Ready: true, InventorySequence: 1, InventoryProducerNodes: 1, InventoryHeartbeatAt: now.Add(-time.Second), PublicationSequence: 1, PublicationDecision: "published", BundleGeneration: "bundle", PublishedBundleDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BundleValidUntil: now.Add(time.Minute), LKGState: "current"}
	for name, mutate := range map[string]func(*edgeDNSAuthorityStatus){
		"wrong group":     func(value *edgeDNSAuthorityStatus) { value.EdgeGroupID = "edge-group-country-us" },
		"not ready":       func(value *edgeDNSAuthorityStatus) { value.Ready = false },
		"stale inventory": func(value *edgeDNSAuthorityStatus) { value.InventoryHeartbeatAt = now.Add(-2 * time.Minute) },
		"expired bundle":  func(value *edgeDNSAuthorityStatus) { value.BundleValidUntil = now.Add(-time.Second) },
		"no producers":    func(value *edgeDNSAuthorityStatus) { value.InventoryProducerNodes = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if err := validateEdgeDNSAuthorityStatus(candidate, "edge-group-country-de", now); err == nil {
				t.Fatal("unsafe authority status accepted")
			}
		})
	}
}

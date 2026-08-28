package api

import (
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestEdgeRouteInventoryFallbackReasonIsFailClosedOutsideLegacyAuthority(t *testing.T) {
	tests := []struct {
		name       string
		inventory  error
		nodes      []model.EdgeNode
		authority  string
		wantReason string
	}{
		{name: "fencing not ready", inventory: store.ErrEdgeInstanceFencingNotReady, authority: model.EdgeRouteAuthorityLegacy, wantReason: edgeRouteInventoryFallbackFencingNotReady},
		{name: "empty active inventory", inventory: nil, authority: model.EdgeRouteAuthorityLegacy, wantReason: edgeRouteInventoryFallbackActiveEmpty},
		{name: "active authority", inventory: store.ErrEdgeInstanceFencingNotReady, authority: model.EdgeRouteAuthorityActiveEpoch},
		{name: "inventory error", inventory: assertRouteInventoryError{}, authority: model.EdgeRouteAuthorityLegacy},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := edgeRouteInventoryFallbackReason(test.inventory, test.nodes, model.EdgeActivationState{RouteAuthority: test.authority}, nil)
			if got != test.wantReason {
				t.Fatalf("fallback reason=%q, want %q", got, test.wantReason)
			}
		})
	}
}

func TestEdgeRouteInventoryFallbackMetricsAreLowCardinality(t *testing.T) {
	server := &Server{}
	server.recordEdgeRouteInventoryFallback(edgeRouteInventoryFallbackFencingNotReady)
	server.recordEdgeRouteInventoryFallback(edgeRouteInventoryFallbackActiveEmpty)
	server.recordEdgeRouteInventoryFallback("untrusted-input")
	var output strings.Builder
	server.writeEdgeRouteSourceMetrics(&output)
	metrics := output.String()
	for _, want := range []string{
		`fugue_edge_route_inventory_legacy_fallback_total{reason="fencing_not_ready"} 1.000000`,
		`fugue_edge_route_inventory_legacy_fallback_total{reason="active_inventory_empty"} 1.000000`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
	if strings.Contains(metrics, "untrusted-input") {
		t.Fatalf("metrics accepted an unbounded fallback reason: %s", metrics)
	}
}

type assertRouteInventoryError struct{}

func (assertRouteInventoryError) Error() string { return "inventory unavailable" }

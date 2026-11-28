package api

import (
	"testing"

	"fugue/internal/api/internal/routeobservation"
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
		{name: "fencing not ready", inventory: store.ErrEdgeInstanceFencingNotReady, authority: model.EdgeRouteAuthorityLegacy, wantReason: routeobservation.FallbackFencingNotReady},
		{name: "empty active inventory", inventory: nil, authority: model.EdgeRouteAuthorityLegacy, wantReason: routeobservation.FallbackActiveEmpty},
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

type assertRouteInventoryError struct{}

func (assertRouteInventoryError) Error() string { return "inventory unavailable" }

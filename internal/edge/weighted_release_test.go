package edge

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"fugue/internal/model"
)

func TestSelectWeightedEdgeRouteUpstreamIsSticky(t *testing.T) {
	t.Parallel()

	route := weightedReleaseTestRoute()
	req := httptest.NewRequest(http.MethodGet, "https://demo.fugue.pro/v1/models", nil)
	req.Header.Set("Authorization", "Bearer sk_test")

	selectedA, upstreamA := selectWeightedEdgeRouteUpstream(req, route, "demo.fugue.pro", "trace_a", "edge_req_a")
	selectedB, upstreamB := selectWeightedEdgeRouteUpstream(req, route, "demo.fugue.pro", "trace_b", "edge_req_b")
	if upstreamA.ReleaseID == "" || upstreamA.ReleaseID != upstreamB.ReleaseID {
		t.Fatalf("expected sticky release selection, got %q and %q", upstreamA.ReleaseID, upstreamB.ReleaseID)
	}
	if selectedA.UpstreamURL != upstreamA.UpstreamURL || selectedB.UpstreamURL != upstreamB.UpstreamURL {
		t.Fatalf("selected route did not adopt upstream URL: %+v %+v", selectedA, selectedB)
	}
}

func TestSelectWeightedEdgeRouteUpstreamDistribution(t *testing.T) {
	t.Parallel()

	route := weightedReleaseTestRoute()
	counts := map[string]int{}
	for i := 0; i < 1000; i++ {
		req := httptest.NewRequest(http.MethodGet, "https://demo.fugue.pro/v1/models", nil)
		req.Header.Set("X-API-Key", "key_"+strconv.Itoa(i))
		_, upstream := selectWeightedEdgeRouteUpstream(req, route, "demo.fugue.pro", "", "")
		counts[upstream.Role]++
	}
	if counts[model.AppReleaseRoleCandidate] < 50 || counts[model.AppReleaseRoleCandidate] > 150 {
		t.Fatalf("expected candidate distribution near 10%%, got %+v", counts)
	}
}

func weightedReleaseTestRoute() model.EdgeRouteBinding {
	return model.EdgeRouteBinding{
		Hostname:    "demo.fugue.pro",
		PathPrefix:  "/",
		AppID:       "app_demo",
		UpstreamURL: "http://stable.internal",
		Upstreams: []model.EdgeRouteUpstream{
			{Role: model.AppReleaseRoleStable, ReleaseID: "rel_stable", Weight: 90, UpstreamURL: "http://stable.internal", Status: model.EdgeRouteStatusActive},
			{Role: model.AppReleaseRoleCandidate, ReleaseID: "rel_candidate", Weight: 10, UpstreamURL: "http://candidate.internal", Status: model.EdgeRouteStatusActive},
		},
	}
}

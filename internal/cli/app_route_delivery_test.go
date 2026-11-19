package cli

import (
	"fugue/internal/httpx"
	"fugue/internal/model"
	"testing"
)

func TestPublicDeliveryRequiresMatchingTargetAndReachablePath(t *testing.T) {
	a := model.App{ID: "app", TenantID: "tenant", Route: &model.AppRoute{Hostname: "test.example", PathPrefix: "/"}}
	for _, tc := range []struct {
		status                        int
		release, tenant, reason, want string
	}{
		{503, "latest", "tenant", "", "public_unverified"},
		{200, "previous", "tenant", "", "inconclusive"},
		{200, "latest", "other", "", "inconclusive"},
		{200, "latest", "tenant", "published traffic consumers have not provided fresh matching serving evidence", "inconclusive"},
		{200, "latest", "tenant", "", "in_sync"},
	} {
		b := model.EdgeRouteBinding{AppID: a.ID, TenantID: tc.tenant, Hostname: a.Route.Hostname, PathPrefix: "/", Status: model.EdgeRouteStatusActive, Upstreams: []model.EdgeRouteUpstream{{ReleaseID: tc.release, Weight: 100}}}
		if tc.reason != "" {
			b.Status = model.EdgeRouteStatusUnavailable
			b.StatusReason = tc.reason
			b.Upstreams = nil
		}
		r := appRouteShowResult{ConclusionCode: "in_sync", Delivery: &routeDelivery{Workload: "in_sync", Stable: "latest"}}
		finishPublicDelivery(a, &r, model.RouteExplainResponse{Route: &b}, nil, httpx.PublicRouteObservation{StatusCode: tc.status, Reachable: tc.status == 200})
		if r.ConclusionCode != tc.want || r.Delivery.HTTP != tc.status {
			t.Fatalf("%+v: %+v", tc, r)
		}
	}
}

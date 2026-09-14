package routeproof

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDigestPreservesBehaviorAndIgnoresPublicationDiagnostics(t *testing.T) {
	route := model.EdgeRouteBinding{Hostname: "app.example.test", PathPrefix: "/api", AppID: "app-a", TenantID: "tenant-a", UpstreamURL: "http://private-origin:8080", Status: "active", ExcludedEdgeIDs: []string{"b", "a"}, Upstreams: []model.EdgeRouteUpstream{{ReleaseID: "stable", Weight: 80, UpstreamURL: "http://stable"}, {ReleaseID: "candidate", Weight: 20, UpstreamURL: "http://candidate"}}}
	digest, err := Digest(route)
	if err != nil {
		t.Fatal(err)
	}
	changed := route
	changed.Hostname = "APP.EXAMPLE.TEST."
	changed.RouteGeneration, changed.DecisionID, changed.StatusReason = "new", "decision", "diagnostic"
	changed.UpdatedAt = time.Now()
	changed.ExcludedEdgeIDs = []string{"a", "b", "a"}
	if got, _ := Digest(changed); got != digest {
		t.Fatal("bookkeeping changed proof")
	}
	if !reflect.DeepEqual(route.ExcludedEdgeIDs, []string{"b", "a"}) {
		t.Fatal("digest mutated caller")
	}
	for name, mutate := range map[string]func(*model.EdgeRouteBinding){
		"owner":    func(r *model.EdgeRouteBinding) { r.TenantID = "other" },
		"path":     func(r *model.EdgeRouteBinding) { r.PathPrefix = "/other" },
		"origin":   func(r *model.EdgeRouteBinding) { r.UpstreamURL = "http://other" },
		"tls":      func(r *model.EdgeRouteBinding) { r.TLSPolicy = "disabled" },
		"cache":    func(r *model.EdgeRouteBinding) { r.CachePolicyID = "assets" },
		"disabled": func(r *model.EdgeRouteBinding) { r.Status = "disabled" },
		"group":    func(r *model.EdgeRouteBinding) { r.EdgeGroupID = "other" },
		"upstream order": func(r *model.EdgeRouteBinding) {
			r.Upstreams = []model.EdgeRouteUpstream{r.Upstreams[1], r.Upstreams[0]}
		},
		"weight": func(r *model.EdgeRouteBinding) {
			r.Upstreams = append([]model.EdgeRouteUpstream(nil), r.Upstreams...)
			r.Upstreams[0].Weight = 100
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := route
			mutate(&r)
			if got, _ := Digest(r); got == digest {
				t.Fatal("behavior change retained proof")
			}
		})
	}
}

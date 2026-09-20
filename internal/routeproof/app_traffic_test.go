package routeproof

import (
	"reflect"
	"testing"

	"fugue/internal/model"
)

func appTrafficRoute() model.EdgeRouteBinding {
	return model.EdgeRouteBinding{Hostname: "app.example.test", PathPrefix: "/api", AppID: "app", TenantID: "tenant", Upstreams: []model.EdgeRouteUpstream{
		{Role: "stable", ReleaseID: "release-old", Weight: 80, UpstreamURL: "http://old:8080", RuntimeID: "runtime-a", DeploymentGeneration: "old-generation"},
		{Role: "candidate", ReleaseID: "release-new", Weight: 20, UpstreamURL: "http://new:8080", RuntimeID: "runtime-a", DeploymentGeneration: "new-generation"},
	}}
}

func TestAppTrafficDigestBindsActualReleasePartition(t *testing.T) {
	route := appTrafficRoute()
	want, err := AppTrafficDigest(route)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*model.EdgeRouteBinding){
		"owner":      func(r *model.EdgeRouteBinding) { r.TenantID = "other" },
		"app":        func(r *model.EdgeRouteBinding) { r.AppID = "other" },
		"hostname":   func(r *model.EdgeRouteBinding) { r.Hostname = "other.example.test" },
		"path":       func(r *model.EdgeRouteBinding) { r.PathPrefix = "/other" },
		"release":    func(r *model.EdgeRouteBinding) { r.Upstreams[1].ReleaseID = "different-release" },
		"role":       func(r *model.EdgeRouteBinding) { r.Upstreams[1].Role = "stable" },
		"weight":     func(r *model.EdgeRouteBinding) { r.Upstreams[0].Weight, r.Upstreams[1].Weight = 50, 50 },
		"upstream":   func(r *model.EdgeRouteBinding) { r.Upstreams[1].UpstreamURL = "http://different:8080" },
		"runtime":    func(r *model.EdgeRouteBinding) { r.Upstreams[1].RuntimeID = "runtime-b" },
		"deployment": func(r *model.EdgeRouteBinding) { r.Upstreams[1].DeploymentGeneration = "other-generation" },
		"port":       func(r *model.EdgeRouteBinding) { r.Upstreams[1].ServicePort = 8081 },
		"kind":       func(r *model.EdgeRouteBinding) { r.Upstreams[1].UpstreamKind = "mesh" },
		"scope":      func(r *model.EdgeRouteBinding) { r.Upstreams[1].UpstreamScope = "mesh" },
		"order":      func(r *model.EdgeRouteBinding) { r.Upstreams[0], r.Upstreams[1] = r.Upstreams[1], r.Upstreams[0] },
	} {
		t.Run(name, func(t *testing.T) {
			r := appTrafficRoute()
			change(&r)
			if got, err := AppTrafficDigest(r); err != nil || got == want {
				t.Fatalf("traffic change retained proof: %s %v", got, err)
			}
		})
	}
	canonical := appTrafficRoute()
	canonical.Hostname = "APP.EXAMPLE.TEST."
	canonical.EdgeGroupID, canonical.RouteGeneration, canonical.StatusReason = "other-group", "publication", "diagnostic"
	canonical.Upstreams[0].StatusReason = "ready"
	if got, err := AppTrafficDigest(canonical); err != nil || got != want {
		t.Fatal("publication changed app traffic proof", got, err)
	}
	if !reflect.DeepEqual(route, appTrafficRoute()) {
		t.Fatal("digest mutated input")
	}
}

func TestAppTrafficDigestRejectsAmbiguousOrIncompleteWeights(t *testing.T) {
	for name, change := range map[string]func(*model.EdgeRouteBinding){
		"no releases": func(r *model.EdgeRouteBinding) { r.Upstreams = nil },
		"no owner":    func(r *model.EdgeRouteBinding) { r.TenantID = "" },
		"no app":      func(r *model.EdgeRouteBinding) { r.AppID = "" },
		"no host":     func(r *model.EdgeRouteBinding) { r.Hostname = "" },
		"no identity": func(r *model.EdgeRouteBinding) { r.Upstreams[1].ReleaseID = "" },
		"duplicate":   func(r *model.EdgeRouteBinding) { r.Upstreams[1].ReleaseID = r.Upstreams[0].ReleaseID },
		"no target":   func(r *model.EdgeRouteBinding) { r.Upstreams[1].UpstreamURL = "" },
		"zero":        func(r *model.EdgeRouteBinding) { r.Upstreams[1].Weight = 0 },
		"incomplete":  func(r *model.EdgeRouteBinding) { r.Upstreams[1].Weight = 10 },
		"excess":      func(r *model.EdgeRouteBinding) { r.Upstreams[1].Weight = 101 },
	} {
		t.Run(name, func(t *testing.T) {
			r := appTrafficRoute()
			change(&r)
			if _, err := AppTrafficDigest(r); err == nil {
				t.Fatal("invalid traffic accepted")
			}
		})
	}
}

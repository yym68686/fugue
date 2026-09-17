package edge

import (
	"testing"

	"fugue/internal/model"
)

func TestCandidateProbeRejectsWrongPathOwnerOrBehavior(t *testing.T) {
	bundle := model.EdgeRouteBundle{Version: "candidate", EdgeGroupID: "edge-group-test", Routes: []model.EdgeRouteBinding{
		{Hostname: "shared.example.test", PathPrefix: "/", AppID: "root-owner", EdgeGroupID: "edge-group-test", RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive, UpstreamURL: "http://root"},
		{Hostname: "shared.example.test", PathPrefix: "/api", AppID: "child-owner", EdgeGroupID: "edge-group-test", RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive, UpstreamURL: "http://child", CachePolicyID: "assets", CacheNamespace: "child-v1"},
	}}
	for name, mutate := range map[string]func(*model.EdgeRouteBundle){
		"missing child falls back to root": func(b *model.EdgeRouteBundle) { b.Routes = b.Routes[:1] },
		"wrong path":                       func(b *model.EdgeRouteBundle) { b.Routes[1].PathPrefix = "/other" },
		"wrong owner":                      func(b *model.EdgeRouteBundle) { b.Routes[1].AppID = "other-owner" },
		"wrong upstream":                   func(b *model.EdgeRouteBundle) { b.Routes[1].UpstreamURL = "http://other" },
		"wrong group":                      func(b *model.EdgeRouteBundle) { b.Routes[1].EdgeGroupID = "edge-group-other" },
		"lost cache":                       func(b *model.EdgeRouteBundle) { b.Routes[1].CachePolicyID = "" },
		"wrong generation":                 func(b *model.EdgeRouteBundle) { b.Version = "old" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := bundle
			bad.Routes = append([]model.EdgeRouteBinding(nil), bundle.Routes...)
			mutate(&bad)
			index := buildEdgeRouteIndex(bad, bundle.EdgeGroupID, routePublicationMetadata{Candidate: true})
			if err := probePlatformCandidateIndex(bundle, index); err == nil {
				t.Fatal("invalid candidate lookup accepted")
			}
		})
	}
	index := buildEdgeRouteIndex(bundle, bundle.EdgeGroupID, routePublicationMetadata{Candidate: true})
	if err := probePlatformCandidateIndex(bundle, index); err != nil {
		t.Fatal(err)
	}
	if err := probePlatformCandidateIndex(bundle, nil); err == nil {
		t.Fatal("missing index accepted")
	}
}

package api

import (
	"encoding/json"
	"testing"

	pc "fugue/internal/platformconfig"
)

func TestHostedBaselineRefreshPreservesPendingPathOwners(t *testing.T) {
	const host, peer = "managed.example.test", "retained.example.test"
	prior := pc.PlatformIntent{Routes: []pc.RouteIntent{
		{Hostname: host, PathPrefix: "/", AppID: "origin"},
		{Hostname: peer, PathPrefix: "/", AppID: "retained"},
		{Hostname: peer, PathPrefix: "/api", AppID: "service"},
	}}
	reservation := pc.HostedAddressReservation{Record: pc.DNSIntent{Hostname: peer, RecordKind: "hosted"}}
	source := pc.HostedAddressSource{PriorIntent: prior, Inherited: []pc.HostedAddressReservation{reservation}}
	capture := &pc.HostedTransitionCapture{Hosts: []string{host}}
	candidate := func(routes ...pc.RouteIntent) pc.CompileResult {
		t.Helper()
		c := pc.CompileResult{InputSnapshot: pc.RuntimeSnapshot{HostedAddressReservations: []pc.HostedAddressReservation{reservation}}}
		raw, err := json.Marshal(pc.PlatformIntent{Routes: routes})
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &c.IntentArtifact.Content); err != nil {
			t.Fatal(err)
		}
		return c
	}
	fresh := append([]pc.RouteIntent(nil), prior.Routes...)
	fresh[0].DeploymentGeneration = "fresh-origin"
	if !canRefreshHostedBaseline(candidate(fresh...), source, capture) {
		t.Fatal("valid baseline with unchanged ownership and fresh runtime rejected")
	}
	if canRefreshHostedBaseline(candidate(append(fresh, pc.RouteIntent{Hostname: host, PathPrefix: "/v1", AppID: "new"})...), source, capture) {
		t.Fatal("forward path escaped its claim")
	}
	if canRefreshHostedBaseline(candidate(fresh[:2]...), source, capture) {
		t.Fatal("withdrawal of an inherited path escaped its claim")
	}
	if canRefreshHostedBaseline(candidate(fresh...), source, nil) {
		t.Fatal("unstaged desired state was treated as a baseline")
	}
}

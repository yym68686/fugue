package platformconfig

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func partitionCompileFixture(t *testing.T) (CompileRequest, CompiledRoute) {
	t.Helper()
	r := releaseCompileFixture()
	r.RuntimeSnapshot.IsolateTrafficAvailability = true
	r.Policy.TrafficConstraints[0].Mode = model.AppTrafficModeSingle
	r.Policy.TrafficConstraints[0].StableWeight, r.Policy.TrafficConstraints[0].CandidateWeight = 100, 0
	r.Policy.TrafficConstraints[0].CandidateReleaseID = ""
	r.Intent.Routes = append(r.Intent.Routes, RouteIntent{Hostname: "peer.example", AppID: "peer", TenantID: "tenant", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled, UpstreamURL: "http://peer:8080", ServicePort: 8080})
	r.Policy.TrafficConstraints = append(r.Policy.TrafficConstraints, TrafficPolicyConstraint{ID: "peer-traffic", AppID: "peer", TenantID: "tenant", Mode: model.AppTrafficModeSingle, StableReleaseID: "peer-release", StableWeight: 100})
	r.RuntimeSnapshot.Releases = append(r.RuntimeSnapshot.Releases, ReleaseObservation{ID: "peer-release", AppID: "peer", TenantID: "tenant", ObservedAt: *r.RuntimeSnapshot.CapturedAt, Status: model.EdgeRouteStatusActive, UpstreamURL: "http://peer-old:8080"})
	old, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	var previous CompiledRoute
	for _, route := range compiledReleaseRoutes(t, old) {
		if route.AppID == "app" {
			previous = route
		}
	}
	digest, err := TrafficPartitionInputDigest(r.Intent, r.Policy, "tenant", "app")
	if err != nil {
		t.Fatal(err)
	}
	r.RuntimeSnapshot.RetainedTrafficPartitions = []RetainedTrafficPartition{{AppID: "app", TenantID: "tenant", InputDigest: digest, Source: model.PlatformPublicationPrecondition{ArtifactID: "verified-parent", ContentHash: "sha256:" + strings.Repeat("a", 64), ReleaseID: "verified-full", FencingToken: 7}, RouteArtifactID: "verified-routes", RouteArtifactDigest: "sha256:" + strings.Repeat("b", 64), ObservedAt: *r.RuntimeSnapshot.CapturedAt, ExpiresAt: r.RuntimeSnapshot.CapturedAt.Add(30 * time.Second), Routes: []CompiledRoute{previous}}}
	r.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusUnavailable
	r.RuntimeSnapshot.Releases[0].StatusReason = "release endpoint identity differs: field=pod_release_key"
	r.RuntimeSnapshot.Releases[2].UpstreamURL = "http://peer-new:8080"
	r.Intent.Routes[1].DeploymentGeneration = "peer-v2"
	return r, previous
}

func TestTrafficPartitionKeepsExactVerifiedBindingWhilePeerAdvances(t *testing.T) {
	r, previous := partitionCompileFixture(t)
	before, _ := json.Marshal(r)
	got, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range compiledReleaseRoutes(t, got) {
		if route.AppID == "app" && !reflect.DeepEqual(route, previous) {
			t.Fatal("retained partition rewritten", route)
		}
		if route.AppID == "peer" && (len(route.Upstreams) != 1 || route.Upstreams[0].UpstreamURL != "http://peer-new:8080") {
			t.Fatal("healthy peer blocked", route)
		}
	}
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("compiler mutated pinned input")
	}
	if got.RouteArtifact.Metadata[RetainedTrafficPartitionsMetadata] == "" {
		t.Fatal("partition provenance missing from signed artifact")
	}
	// Without an authorized old binding only the failed partition closes.
	r.RuntimeSnapshot.RetainedTrafficPartitions = nil
	got, err = Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range compiledReleaseRoutes(t, got) {
		if route.AppID == "app" && (route.Status != model.EdgeRouteStatusUnavailable || len(route.Upstreams) > 0) {
			t.Fatal("unproven target received traffic", route)
		}
		if route.AppID == "peer" && len(route.Upstreams) != 1 {
			t.Fatal("peer lost route", route)
		}
	}
	r.RuntimeSnapshot.IsolateTrafficAvailability = false
	if _, err = Compile(r); err == nil {
		t.Fatal("legacy strict policy silently opted into isolation")
	}
}

func TestTrafficPartitionRejectsChangedAuthorityAndUntrustedEvidence(t *testing.T) {
	for name, change := range map[string]func(*CompileRequest){
		"withdrawn":             func(r *CompileRequest) { r.Intent.Routes[0].Enabled = false },
		"owner":                 func(r *CompileRequest) { r.Intent.Routes[0].TenantID = "other" },
		"executable generation": func(r *CompileRequest) { r.Intent.Routes[0].DeploymentGeneration = "new-executable" },
		"source binding":        func(r *CompileRequest) { r.Intent.Routes[0].UpstreamURL = "http://different:8080" },
		"traffic policy":        func(r *CompileRequest) { r.Policy.TrafficConstraints[0].StableReleaseID = "other" },
		"route policy": func(r *CompileRequest) {
			r.Policy.RouteConstraints = []RoutePolicyConstraint{{ID: "disabled", Hostname: r.Intent.Routes[0].Hostname, RoutePolicy: model.EdgeRoutePolicyRouteAOnly, Enabled: false}}
		},
		"expiry": func(r *CompileRequest) {
			r.RuntimeSnapshot.RetainedTrafficPartitions[0].ExpiresAt = *r.RuntimeSnapshot.CapturedAt
		},
		"consumer witness stale": func(r *CompileRequest) {
			r.RuntimeSnapshot.RetainedTrafficPartitions[0].ObservedAt = r.RuntimeSnapshot.CapturedAt.Add(-61 * time.Second)
		},
		"fence":           func(r *CompileRequest) { r.RuntimeSnapshot.RetainedTrafficPartitions[0].Source.FencingToken = 0 },
		"missing release": func(r *CompileRequest) { r.RuntimeSnapshot.Releases = r.RuntimeSnapshot.Releases[1:] },
		"ambiguous observation": func(r *CompileRequest) {
			r.RuntimeSnapshot.Releases = append(r.RuntimeSnapshot.Releases, r.RuntimeSnapshot.Releases[0])
		},
		"foreign release owner": func(r *CompileRequest) { r.RuntimeSnapshot.Releases[0].TenantID = "foreign" },
		"stale release": func(r *CompileRequest) {
			r.RuntimeSnapshot.Releases[0].ObservedAt = r.RuntimeSnapshot.CapturedAt.Add(-61 * time.Second)
		},
		"duplicate partition": func(r *CompileRequest) {
			r.RuntimeSnapshot.RetainedTrafficPartitions = append(r.RuntimeSnapshot.RetainedTrafficPartitions, r.RuntimeSnapshot.RetainedTrafficPartitions[0])
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, _ := partitionCompileFixture(t)
			change(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("untrusted old partition reused")
			}
		})
	}
}

func TestTrafficPartitionRetentionCannotOverrideNegativeOrigin(t *testing.T) {
	for _, status := range []string{model.EdgeRouteStatusDisabled, model.EdgeRouteStatusUnavailable} {
		t.Run(status, func(t *testing.T) {
			r := releaseCompileFixture()
			r.RuntimeSnapshot.IsolateTrafficAvailability = true
			r.Policy.TrafficConstraints[0].Mode = model.AppTrafficModeSingle
			r.Policy.TrafficConstraints[0].StableWeight, r.Policy.TrafficConstraints[0].CandidateWeight = 100, 0
			r.Policy.TrafficConstraints[0].CandidateReleaseID = ""
			r.Intent.Routes[0].RuntimeID, r.Intent.Routes[0].OriginRef = "runtime-a", "origin-a"
			r.RuntimeSnapshot.Origins = []OriginObservation{{Ref: "origin-a", RuntimeID: "runtime-a", Status: model.EdgeRouteStatusActive, ObservedAt: *r.RuntimeSnapshot.CapturedAt}}
			old, err := Compile(r)
			if err != nil {
				t.Fatal(err)
			}
			digest, _ := TrafficPartitionInputDigest(r.Intent, r.Policy, "tenant", "app")
			r.RuntimeSnapshot.RetainedTrafficPartitions = []RetainedTrafficPartition{{AppID: "app", TenantID: "tenant", InputDigest: digest, Source: model.PlatformPublicationPrecondition{ArtifactID: "verified-parent", ContentHash: "sha256:" + strings.Repeat("a", 64), ReleaseID: "verified-full", FencingToken: 7}, RouteArtifactID: "verified-routes", RouteArtifactDigest: "sha256:" + strings.Repeat("b", 64), ObservedAt: *r.RuntimeSnapshot.CapturedAt, ExpiresAt: r.RuntimeSnapshot.CapturedAt.Add(30 * time.Second), Routes: compiledReleaseRoutes(t, old)}}
			r.RuntimeSnapshot.Origins[0].Status = status
			r.RuntimeSnapshot.Origins[0].StatusReason = "origin independently unavailable"
			r.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusUnavailable
			got, err := Compile(r)
			if err != nil {
				t.Fatal(err)
			}
			for _, route := range compiledReleaseRoutes(t, got) {
				if route.Status != status || route.StatusReason != "origin independently unavailable" || len(route.Upstreams) != 0 {
					t.Fatal("retention bypassed the current origin gate", route)
				}
			}
		})
	}
}

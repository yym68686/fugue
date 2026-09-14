package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func releaseCompileFixture() CompileRequest {
	captured := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	return CompileRequest{
		Intent: PlatformIntent{Generation: "release-intent", Routes: []RouteIntent{{Hostname: "release.example", UpstreamURL: "http://fallback:8080", AppID: "app", TenantID: "tenant", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled, ServicePort: 8080}}},
		Policy: PolicySnapshot{Generation: "release-policy", MaxStaleSeconds: 60, TrafficConstraints: []TrafficPolicyConstraint{{ID: "traffic", AppID: "app", TenantID: "tenant", Mode: model.AppTrafficModeCanary, StableReleaseID: "stable", CandidateReleaseID: "candidate", StableWeight: 80, CandidateWeight: 20, UnavailableCandidate: "stable"}}},
		RuntimeSnapshot: RuntimeSnapshot{CapturedAt: &captured, Releases: []ReleaseObservation{
			{ID: "stable", AppID: "app", TenantID: "tenant", ObservedAt: captured.Add(-time.Second), Status: model.EdgeRouteStatusActive, UpstreamURL: "http://stable:8080", RuntimeID: "runtime-stable"},
			{ID: "candidate", AppID: "app", TenantID: "tenant", ObservedAt: captured.Add(-time.Second), Status: model.EdgeRouteStatusActive, UpstreamURL: "http://candidate:8080", RuntimeID: "runtime-candidate"},
		}},
	}
}

func compiledReleaseRoutes(t *testing.T, result CompileResult) []CompiledRoute {
	t.Helper()
	raw, err := json.Marshal(result.RouteArtifact.Content["routes"])
	if err != nil {
		t.Fatal(err)
	}
	var routes []CompiledRoute
	if err := json.Unmarshal(raw, &routes); err != nil {
		t.Fatal(err)
	}
	return routes
}

func TestReleaseCompilerSeparatesPolicyFromAvailability(t *testing.T) {
	request := releaseCompileFixture()
	before, _ := json.Marshal(request)
	ready, err := Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	routes := compiledReleaseRoutes(t, ready)
	if len(routes[0].Upstreams) != 2 || routes[0].Upstreams[0].Weight != 80 || routes[0].Upstreams[1].Weight != 20 || routes[0].Upstreams[1].RuntimeID != "runtime-candidate" {
		t.Fatalf("unexpected targets: %+v", routes)
	}
	after, _ := json.Marshal(request)
	if string(before) != string(after) {
		t.Fatal("compiler mutated caller input")
	}
	request.RuntimeSnapshot.Releases[0], request.RuntimeSnapshot.Releases[1] = request.RuntimeSnapshot.Releases[1], request.RuntimeSnapshot.Releases[0]
	request.CreatedAt = time.Now().Add(24 * time.Hour)
	replay, err := Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ready.RouteArtifact.Content, replay.RouteArtifact.Content) {
		t.Fatal("wall clock or observation order changed artifact")
	}
	request.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusUnavailable
	fallback, err := Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	routes = compiledReleaseRoutes(t, fallback)
	if len(routes[0].Upstreams) != 1 || routes[0].Upstreams[0].ReleaseID != "stable" || routes[0].Upstreams[0].Weight != 100 {
		t.Fatalf("bad fallback: %+v", routes)
	}
	if ready.Lineage.IntentDigest != fallback.Lineage.IntentDigest || ready.Lineage.PolicyDigest != fallback.Lineage.PolicyDigest || ready.Lineage.InputSnapshotDigest == fallback.Lineage.InputSnapshotDigest {
		t.Fatal("availability leaked into intent/policy or was absent from fact digest")
	}
	request.Policy.TrafficConstraints[0].StableWeight, request.Policy.TrafficConstraints[0].CandidateWeight = 0, 100
	fallback, err = Compile(request)
	if err != nil || compiledReleaseRoutes(t, fallback)[0].Upstreams[0].Weight != 100 {
		t.Fatal("100% candidate failed to use verified stable fallback", err)
	}
}

func TestReleaseCompilerRejectsUntrustedOrAmbiguousInputs(t *testing.T) {
	tests := map[string]func(*CompileRequest){
		"missing candidate": func(r *CompileRequest) { r.RuntimeSnapshot.Releases = r.RuntimeSnapshot.Releases[:1] },
		"missing stable":    func(r *CompileRequest) { r.RuntimeSnapshot.Releases = r.RuntimeSnapshot.Releases[1:] },
		"stale": func(r *CompileRequest) {
			r.RuntimeSnapshot.Releases[0].ObservedAt = r.RuntimeSnapshot.CapturedAt.Add(-61 * time.Second)
		},
		"future": func(r *CompileRequest) {
			r.RuntimeSnapshot.Releases[0].ObservedAt = r.RuntimeSnapshot.CapturedAt.Add(time.Second)
		},
		"missing timestamp":   func(r *CompileRequest) { r.RuntimeSnapshot.Releases[0].ObservedAt = time.Time{} },
		"missing captured at": func(r *CompileRequest) { r.RuntimeSnapshot.CapturedAt = nil },
		"duplicate": func(r *CompileRequest) {
			r.RuntimeSnapshot.Releases = append(r.RuntimeSnapshot.Releases, r.RuntimeSnapshot.Releases[0])
		},
		"wrong release tenant": func(r *CompileRequest) { r.RuntimeSnapshot.Releases[1].TenantID = "other" },
		"wrong release app":    func(r *CompileRequest) { r.RuntimeSnapshot.Releases[1].AppID = "other" },
		"wrong policy tenant":  func(r *CompileRequest) { r.Policy.TrafficConstraints[0].TenantID = "other" },
		"unknown app":          func(r *CompileRequest) { r.Policy.TrafficConstraints[0].AppID = "other" },
		"bad status":           func(r *CompileRequest) { r.RuntimeSnapshot.Releases[1].Status = "healthy-ish" },
		"credential URL":       func(r *CompileRequest) { r.RuntimeSnapshot.Releases[1].UpstreamURL = "http://user:secret@candidate" },
		"stable unavailable":   func(r *CompileRequest) { r.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusUnavailable },
		"unauthorized fallback": func(r *CompileRequest) {
			r.RuntimeSnapshot.Releases[1].Status = model.EdgeRouteStatusUnavailable
			r.Policy.TrafficConstraints[0].UnavailableCandidate = ""
		},
		"failed 100% candidate no stable": func(r *CompileRequest) {
			r.Policy.TrafficConstraints[0].StableWeight = 0
			r.Policy.TrafficConstraints[0].CandidateWeight = 100
			r.RuntimeSnapshot.Releases[1].Status = model.EdgeRouteStatusUnavailable
			r.RuntimeSnapshot.Releases = r.RuntimeSnapshot.Releases[1:]
		},
		"two sources of weights": func(r *CompileRequest) {
			r.Intent.Routes[0].Upstreams = []UpstreamIntent{{UpstreamURL: "http://other", Weight: 100}}
		},
		"unsupported sticky":        func(r *CompileRequest) { r.Policy.TrafficConstraints[0].StickyHeader = "X-Release" },
		"same stable and candidate": func(r *CompileRequest) { r.Policy.TrafficConstraints[0].CandidateReleaseID = "stable" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			r := releaseCompileFixture()
			mutate(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("unsafe release input accepted")
			}
		})
	}
}

func TestReleaseCompilerHonorsDisableAndMode(t *testing.T) {
	for _, mode := range []string{model.AppTrafficModeSingle, model.AppTrafficModePaused} {
		r := releaseCompileFixture()
		r.Policy.TrafficConstraints[0].Mode = mode
		compiled, err := Compile(r)
		if err != nil {
			t.Fatal(err)
		}
		upstreams := compiledReleaseRoutes(t, compiled)[0].Upstreams
		if len(upstreams) != 1 || upstreams[0].Weight != 100 {
			t.Fatalf("mode %s: %+v", mode, upstreams)
		}
	}
	for _, mutate := range []func(*CompileRequest){
		func(r *CompileRequest) { r.Intent.Routes[0].Enabled = false },
		func(r *CompileRequest) { r.Intent.Routes[0].Status = model.EdgeRouteStatusUnavailable },
		func(r *CompileRequest) {
			r.Policy.RouteConstraints = []RoutePolicyConstraint{{ID: "off", Hostname: "release.example", RoutePolicy: model.EdgeRoutePolicyRouteAOnly, Enabled: false}}
		},
	} {
		r := releaseCompileFixture()
		mutate(&r)
		compiled, err := Compile(r)
		if err != nil {
			t.Fatal(err)
		}
		if len(compiledReleaseRoutes(t, compiled)[0].Upstreams) != 0 {
			t.Fatal("release revived disabled or unavailable route")
		}
	}
}

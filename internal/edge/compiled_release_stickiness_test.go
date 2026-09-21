package edge

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/releaseflow"
	"fugue/internal/routeartifact"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
)

func TestCompiledDefaultCookieMatchesLegacyWeightedExecution(t *testing.T) {
	now := time.Now().UTC()
	const group = "edge-group-test-sticky"
	stableOrigin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("stable")) }))
	defer stableOrigin.Close()
	candidateOrigin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("candidate")) }))
	defer candidateOrigin.Close()
	stable := model.AppRelease{ID: "stable", AppID: "app", TenantID: "tenant", RuntimeID: "runtime", UpstreamURL: stableOrigin.URL, ResolvedImageRef: "image-stable", Status: model.AppReleaseStatusReady}
	candidate := stable
	candidate.ID, candidate.UpstreamURL, candidate.ResolvedImageRef = "candidate", candidateOrigin.URL, "image-candidate"
	request := platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "intent", Routes: []platformconfig.RouteIntent{{Hostname: "sticky.example.test", PathPrefix: "/api", AppID: "app", TenantID: "tenant", RuntimeID: "runtime", UpstreamURL: stable.UpstreamURL, UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService, ServicePort: 8080, Enabled: true}}},
		Policy: platformconfig.PolicySnapshot{Generation: "policy", MaxStaleSeconds: 120, TrafficConstraints: []platformconfig.TrafficPolicyConstraint{{ID: "policy-app", AppID: "app", TenantID: "tenant", Mode: model.AppTrafficModeCanary, StableReleaseID: stable.ID, CandidateReleaseID: candidate.ID, StableWeight: 80, CandidateWeight: 20, StickyCookie: "Fugue-Release-Stickiness"}}},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, Releases: []platformconfig.ReleaseObservation{
			{ID: stable.ID, AppID: stable.AppID, TenantID: stable.TenantID, RuntimeID: stable.RuntimeID, UpstreamURL: stable.UpstreamURL, DeploymentGeneration: stable.ResolvedImageRef, ObservedAt: now, Status: model.EdgeRouteStatusActive},
			{ID: candidate.ID, AppID: candidate.AppID, TenantID: candidate.TenantID, RuntimeID: candidate.RuntimeID, UpstreamURL: candidate.UpstreamURL, DeploymentGeneration: candidate.ResolvedImageRef, ObservedAt: now, Status: model.EdgeRouteStatusActive},
		}},
	}
	compiled, err := platformconfig.Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := platformconfig.Compile(request)
	if err != nil || !reflect.DeepEqual(compiled.RouteArtifact.Content, replayed.RouteArtifact.Content) {
		t.Fatal("default cookie replay changed", err)
	}
	bundle, err := routeartifact.MaterializeForGroup(compiled.RouteArtifact, group)
	if err != nil || len(bundle.Routes) != 1 {
		t.Fatal("materialize", err)
	}
	route := bundle.Routes[0]
	legacy := route
	legacy.Upstreams = nil
	legacy = releaseflow.ApplyAppReleaseTraffic(legacy, map[string]model.AppTrafficPolicy{"app": {AppID: "app", Mode: model.AppTrafficModeCanary, StableReleaseID: stable.ID, CandidateReleaseID: candidate.ID, StableWeight: 80, CandidateWeight: 20, StickyCookie: "Fugue-Release-Stickiness"}}, map[string]model.AppRelease{stable.ID: stable, candidate.ID: candidate})
	if !reflect.DeepEqual(route.Upstreams, legacy.Upstreams) {
		t.Fatal("compiled release targets differ from legacy execution")
	}
	counts := map[string]int{}
	for i := range 1000 {
		value := "session-" + strconv.Itoa(i)
		for _, source := range []string{"cookie", "header", "api-key", "authorization", "anonymous"} {
			req := httptest.NewRequest(http.MethodGet, "https://sticky.example.test/api/items", nil)
			switch source {
			case "cookie":
				req.AddCookie(&http.Cookie{Name: "Fugue-Release-Stickiness", Value: value})
				req.Header.Set("X-Fugue-Release-Stickiness", "lower-priority")
			case "header":
				req.Header.Set("X-Fugue-Release-Stickiness", value)
				req.Header.Set("X-API-Key", "lower-priority")
			case "api-key":
				req.Header.Set("X-API-Key", value)
				req.Header.Set("Authorization", "lower-priority")
			case "authorization":
				req.Header.Set("Authorization", value)
			}
			_, want := selectWeightedEdgeRouteUpstream(req, legacy, route.Hostname, value, value)
			_, got := selectWeightedEdgeRouteUpstream(req, route, route.Hostname, value, value)
			if got.ReleaseID != want.ReleaseID {
				t.Fatal("compiled sticky partition differs", source)
			}
			if source != "anonymous" {
				_, repeated := selectWeightedEdgeRouteUpstream(req, route, route.Hostname, "another-trace", "another-request")
				if repeated.ReleaseID != got.ReleaseID {
					t.Fatal("sticky release changed across requests", source)
				}
			}
			if source == "cookie" {
				counts[got.ReleaseID]++
			}
		}
	}
	if counts[candidate.ID] < 140 || counts[candidate.ID] > 260 {
		t.Fatalf("unexpected 80/20 distribution: %v", counts)
	}
	// Policy spelling is still part of lineage even where executor behavior is
	// identical to its built-in default. It is never copied into runtime facts.
	request.Policy.TrafficConstraints[0].StickyCookie = ""
	implicit, err := platformconfig.Compile(request)
	if err != nil || implicit.Lineage.PolicyDigest == compiled.Lineage.PolicyDigest || implicit.Lineage.IntentDigest != compiled.Lineage.IntentDigest || implicit.Lineage.InputSnapshotDigest != compiled.Lineage.InputSnapshotDigest {
		t.Fatal("sticky policy leaked out of policy lineage", err)
	}
	for _, weight := range []int{20, 50, 100, 0} {
		rule := &request.Policy.TrafficConstraints[0]
		rule.StickyCookie = "Fugue-Release-Stickiness"
		rule.StableWeight, rule.CandidateWeight = 100-weight, weight
		compiled, err := platformconfig.Compile(request)
		if err != nil {
			t.Fatal(err)
		}
		materialized, err := routeartifact.MaterializeForGroup(compiled.RouteArtifact, group)
		if err != nil {
			t.Fatal(err)
		}
		actual := materialized.Routes[0]
		digest, err := routeproof.AppTrafficDigest(actual)
		if err != nil {
			t.Fatal("release proof", weight, err)
		}
		service := NewService(config.EdgeConfig{}, nil)
		service.Config.EdgeID, service.Config.EdgeGroupID, service.Config.CaddyEnabled = "edge-sticky", group, true
		materialized.Version, materialized.ValidUntil = "applied-"+strconv.Itoa(weight), now.Add(time.Minute)
		service.recordSyncSuccess(materialized, materialized.Version, now, false)
		service.mu.Lock()
		service.snapshot.Healthy, service.snapshot.CaddyAppliedVersion = true, materialized.Version
		service.mu.Unlock()
		req := httptest.NewRequest(http.MethodHead, "https://sticky.example.test/api", nil)
		const nonce = "0123456789abcdef0123456789abcdef"
		req.Header.Set(routeproof.RequestHeader, "1")
		req.Header.Set(routeproof.NonceHeader, nonce)
		w := httptest.NewRecorder()
		service.ProxyHandler().ServeHTTP(w, req)
		proof, err := routeprobe.ParseResponse(w.Result(), nonce, now)
		if err != nil || proof.AppTrafficDigest != digest {
			t.Fatalf("compiled %d%% route not proved: %+v %v", weight, proof, err)
		}
		for session := range 50 {
			r := httptest.NewRequest(http.MethodGet, "https://sticky.example.test/api/items", nil)
			r.AddCookie(&http.Cookie{Name: "Fugue-Release-Stickiness", Value: "live-session-" + strconv.Itoa(session)})
			_, selected := selectWeightedEdgeRouteUpstream(r, actual, actual.Hostname, "", "")
			response := httptest.NewRecorder()
			service.ProxyHandler().ServeHTTP(response, r)
			if response.Code != http.StatusOK || response.Body.String() != selected.ReleaseID {
				t.Fatalf("compiled %d%% route proxied to unexpected release: %d %q want %s", weight, response.Code, response.Body.String(), selected.ReleaseID)
			}
		}
	}
}

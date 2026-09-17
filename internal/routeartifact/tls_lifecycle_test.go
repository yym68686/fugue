package routeartifact

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func tlsLifecycleFixture() platformconfig.CompileRequest {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	verified := now.Add(-180 * 24 * time.Hour)
	return platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "tls-intent", Routes: []platformconfig.RouteIntent{
		{Hostname: "domain.example.test", AppID: "app", TenantID: "tenant", UpstreamURL: "http://origin", Enabled: true, TLSPolicy: model.EdgeRouteTLSPolicyCustomDomain, EdgeGroupMode: model.PlatformRouteEdgeGroupModePinned, EdgeGroupID: "edge-group-a"},
		{Hostname: "plain.example.test", UpstreamURL: "http://plain", Enabled: true},
	}, TLS: []platformconfig.TLSIntent{{Hostname: "domain.example.test", Policy: model.EdgeRouteTLSPolicyCustomDomain, DomainRef: "domain-ref", AppID: "app", TenantID: "tenant"}}},
		Policy: platformconfig.PolicySnapshot{Generation: "tls-policy", MaxStaleSeconds: 60}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now,
			TLSDomains: []platformconfig.TLSDomainObservation{{Ref: "domain-ref", Hostname: "domain.example.test", AppID: "app", TenantID: "tenant", Status: model.AppDomainStatusVerified, TLSStatus: model.AppDomainTLSStatusReady, VerifiedAt: &verified, TLSReadyAt: &verified}},
		}}
}

func TestTLSDomainLifecycleBindsArtifactsWithoutRenewingEvidence(t *testing.T) {
	request := tlsLifecycleFixture()
	before, _ := json.Marshal(request)
	compiled, err := platformconfig.Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(compiled.RouteArtifact.Content["tls_allowlist"], compiled.TLSArtifact.Content["tls_allowlist"]) {
		t.Fatal("route and TLS artifacts disagree on domain authorization")
	}
	var states []platformconfig.TLSDomainObservation
	raw, _ := json.Marshal(compiled.TLSArtifact.Content["domain_states"])
	if json.Unmarshal(raw, &states) != nil || !reflect.DeepEqual(states, request.RuntimeSnapshot.TLSDomains) || states[0].TLSLastCheckedAt != nil {
		t.Fatal("original TLS lifecycle timestamps were renewed or omitted")
	}
	projection, err := Project(compiled.RouteArtifact)
	if err != nil || len(projection.TLSAllowlist) != 1 || projection.TLSAllowlist[0].TLSStatus != model.AppDomainTLSStatusReady {
		t.Fatal("domain lifecycle projection missing", err)
	}
	for _, group := range []string{"edge-group-a", "edge-group-b"} {
		bundle, err := MaterializeForGroup(compiled.RouteArtifact, group)
		want := 0
		if group == "edge-group-a" {
			want = 1
		}
		if err != nil || len(bundle.TLSAllowlist) != want {
			t.Fatal("group materialization lost or leaked domain authorization", group, err)
		}
	}
	after, _ := json.Marshal(request)
	if string(before) != string(after) {
		t.Fatal("compilation mutated desired intent or historical facts")
	}
	request.CreatedAt = request.RuntimeSnapshot.CapturedAt.Add(24 * time.Hour)
	replay, err := platformconfig.Compile(request)
	if err != nil || !reflect.DeepEqual(compiled.RouteArtifact.Content, replay.RouteArtifact.Content) || !reflect.DeepEqual(compiled.TLSArtifact.Content, replay.TLSArtifact.Content) {
		t.Fatal("wall clock changed domain lifecycle compilation", err)
	}
	request.RuntimeSnapshot.TLSDomains[0].TLSStatus = model.AppDomainTLSStatusPending
	pending, err := platformconfig.Compile(request)
	if err != nil || pending.Lineage.IntentDigest != compiled.Lineage.IntentDigest || pending.Lineage.PolicyDigest != compiled.Lineage.PolicyDigest || pending.Lineage.InputSnapshotDigest == compiled.Lineage.InputSnapshotDigest {
		t.Fatal("TLS observation mutated desired intent or lost lineage", err)
	}
	// Lifecycle ready cannot authorize DNS without the independent placement
	// facts required for the same app and compiled route.
	request = tlsLifecycleFixture()
	request.Intent.DNS = []platformconfig.DNSIntent{{Hostname: "domain.example.test", AppID: "app", TenantID: "tenant", Type: "FUGUE_APP", Values: []string{"app"}, TTL: 60,
		Application: &platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}
	if _, err := platformconfig.Compile(request); err == nil {
		t.Fatal("historical TLS ready bypassed independent DNS placement")
	}
}

func TestTLSDomainLifecycleRejectsUnboundOrFabricatedFacts(t *testing.T) {
	for name, mutate := range map[string]func(*platformconfig.CompileRequest){
		"missing capture": func(r *platformconfig.CompileRequest) { r.RuntimeSnapshot.CapturedAt = nil },
		"missing fact":    func(r *platformconfig.CompileRequest) { r.RuntimeSnapshot.TLSDomains = nil },
		"unreferenced fact": func(r *platformconfig.CompileRequest) {
			r.Intent.TLS[0].DomainRef, r.Intent.TLS[0].AppID, r.Intent.TLS[0].TenantID = "", "", ""
		},
		"duplicate fact": func(r *platformconfig.CompileRequest) {
			r.RuntimeSnapshot.TLSDomains = append(r.RuntimeSnapshot.TLSDomains, r.RuntimeSnapshot.TLSDomains[0])
		},
		"wrong fact owner":  func(r *platformconfig.CompileRequest) { r.RuntimeSnapshot.TLSDomains[0].AppID = "other" },
		"wrong fact tenant": func(r *platformconfig.CompileRequest) { r.RuntimeSnapshot.TLSDomains[0].TenantID = "other" },
		"wrong host": func(r *platformconfig.CompileRequest) {
			r.RuntimeSnapshot.TLSDomains[0].Hostname = "other.example.test"
		},
		"missing verification time": func(r *platformconfig.CompileRequest) { r.RuntimeSnapshot.TLSDomains[0].VerifiedAt = nil },
		"missing ready time":        func(r *platformconfig.CompileRequest) { r.RuntimeSnapshot.TLSDomains[0].TLSReadyAt = nil },
		"future event": func(r *platformconfig.CompileRequest) {
			at := r.RuntimeSnapshot.CapturedAt.Add(time.Second)
			r.RuntimeSnapshot.TLSDomains[0].TLSLastCheckedAt = &at
		},
		"unverified ready": func(r *platformconfig.CompileRequest) {
			r.RuntimeSnapshot.TLSDomains[0].Status = model.AppDomainStatusPending
		},
		"invalid TLS state":    func(r *platformconfig.CompileRequest) { r.RuntimeSnapshot.TLSDomains[0].TLSStatus = "invented" },
		"foreign route tenant": func(r *platformconfig.CompileRequest) { r.Intent.Routes[0].TenantID = "other" },
		"unknown route owner":  func(r *platformconfig.CompileRequest) { r.Intent.TLS[0].AppID = "other" },
		"duplicate TLS intent": func(r *platformconfig.CompileRequest) { r.Intent.TLS = append(r.Intent.TLS, r.Intent.TLS[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			r := tlsLifecycleFixture()
			mutate(&r)
			if _, err := platformconfig.Compile(r); err == nil {
				t.Fatal("invalid domain lifecycle accepted")
			}
		})
	}
	compiled, err := platformconfig.Compile(tlsLifecycleFixture())
	if err != nil {
		t.Fatal(err)
	}
	for name, entries := range map[string][]model.EdgeTLSAllowlistEntry{
		"foreign owner":    {{Hostname: "domain.example.test", AppID: "other", TenantID: "tenant", Status: "verified", TLSStatus: "ready"}},
		"foreign tenant":   {{Hostname: "domain.example.test", AppID: "app", TenantID: "other", Status: "verified", TLSStatus: "ready"}},
		"absent route":     {{Hostname: "absent.example.test", AppID: "app", TenantID: "tenant", Status: "verified"}},
		"unverified ready": {{Hostname: "domain.example.test", AppID: "app", TenantID: "tenant", Status: "pending", TLSStatus: "ready"}},
	} {
		t.Run("projection "+name, func(t *testing.T) {
			compiled.RouteArtifact.Content["tls_allowlist"] = entries
			if _, err := Project(compiled.RouteArtifact); err == nil {
				t.Fatal("executor accepted invalid signed allowlist semantics")
			}
		})
	}
}

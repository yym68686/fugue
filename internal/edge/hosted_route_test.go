package edge

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
	"fugue/internal/trafficbinding"
)

// The group and host use different keys. Only the group is installed in
// the ordinary route cache. HTTP requests and explicit nonce proofs cross
// a real TLS socket; the Caddy apply status below is a fixture, not Caddy.
func TestHostedWorkerIndependentSelectionOnWireAndRestart(t *testing.T) {
	const a, b = "a.fixture.test", "b.fixture.test"
	now := time.Now().UTC().Truncate(time.Microsecond)
	digest := func(v any) string {
		d, e := pc.Digest(v)
		if e != nil {
			t.Fatal(e)
		}
		return d
	}
	d := digest("base")
	cfg := config.EdgeConfig{EdgeID: "edge-fixture", EdgeGroupID: testCacheEdgeGroupID, CachePath: filepath.Join(t.TempDir(), "group.json"), CaddyEnabled: true, BundleSigningKey: "separate-host-fixture-signing-key", BundleSigningKeyID: "host-key"}
	keys := bundleauth.NewKeyring(cfg.BundleSigningKey, cfg.BundleSigningKeyID, "", "", nil)
	who := platformcontrol.PlatformComponentIdentityClaims{NodeID: cfg.EdgeID, Component: model.PlatformConsumerComponentEdgeWorker, ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle}, WorkerEnrollmentID: strings.Repeat("a", 32)}
	sel := model.PlatformConsumerAssignment{ExpectedConsumerSetID: "set-fixture", ReleaseSetID: "parent", ArtifactReleaseID: "full-release", ReleaseChannel: "full", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ArtifactID: "routes", ContentHash: d, Revision: 3, FencingToken: 9, ScopeKey: "global"}
	group := testEdgeControlCacheFile("group", 1, 0, now, time.Hour)
	gr := &group.Bundle
	gr.TrafficRelease = &model.TrafficReleaseBinding{Schema: trafficbinding.Schema, ReleaseSetID: sel.ReleaseSetID, ReleaseSetDigest: d, ReleaseSetGeneration: "full", RouteArtifactID: sel.ArtifactID, RouteArtifactDigest: d, RouteArtifactGeneration: "route", RouteArtifactSequence: 1, ReleaseID: sel.ArtifactReleaseID, ReleaseChannel: "full", FencingToken: 9, ScopeKey: "global", IntentDigest: d, PolicyDigest: d, InputSnapshotDigest: d, ProjectionDigest: d, CompilerVersion: "test"}
	origin := func(name string) string {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, name+" "+r.URL.Path) }))
		t.Cleanup(s.Close)
		return s.URL
	}
	oldA, newA, oldB, newB := origin("old-a"), origin("new-a"), origin("shared-v1"), origin("shared-v2")
	route := func(host, path, up string) model.EdgeRouteBinding {
		r := group.Bundle.Routes[0]
		r.Hostname, r.PathPrefix, r.UpstreamURL, r.AppID = host, path, up, "shared"
		r.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
		return r
	}
	firstA, firstB := route(a, "/", oldA), route(b, "/", oldB)
	gr.Routes = []model.EdgeRouteBinding{firstA, firstB}
	gr.TLSAllowlist = nil
	for _, host := range []string{a, b} {
		gr.TLSAllowlist = append(gr.TLSAllowlist, model.EdgeTLSAllowlistEntry{Hostname: host, AppID: "shared", TenantID: firstA.TenantID, Status: model.AppDomainStatusVerified})
	}
	*gr = bundleauth.SignEdgeRouteBundle(*gr, testCacheKey, testCacheKeyID, time.Hour)
	original := digest(*gr)
	start := func() *Service {
		s := newTestEdgeControlCacheService(t, cfg, nil)
		s.recordSyncSuccess(*gr, "", now, false)
		s.recordCaddyApply(gr.Version, 2, "fixture", nil)
		if e := s.reserveHostedRoutesOnRestart(); e != nil {
			t.Fatal(e)
		}
		return s
	}
	s := start()
	makePhases := func(host string, base model.EdgeRouteBinding, next []model.EdgeRouteBinding) []model.PlatformArtifact {
		r := routeartifact.HostedRecipient{Component: who.Component, Kind: sel.ArtifactKind, Node: who.NodeID, Group: cfg.EdgeGroupID}
		out := make([]model.PlatformArtifact, 0, 3)
		var prev, prepare, oldPart string
		for i, phase := range []string{"prepare", "forward", "withdraw"} {
			rows := []model.EdgeRouteBinding{base}
			if i == 1 {
				rows = next
			}
			in := routeartifact.HostedRecipientInput{Routes: rows}
			part := routeartifact.HostedDeliveryPart{Recipient: r, Content: in, ContentDigest: digest([]any{"fugue.hosted-recipient-part/v1", host, r, in})}
			v := routeartifact.HostedDelivery{Schema: "fugue.hosted-recipient-input/v1", Host: host, DraftDigest: d, ImageDigest: d, Phase: phase, Member: who.ConsumerID(), MembershipSet: sel.ExpectedConsumerSetID, MembershipRelease: sel.ArtifactReleaseID, MembershipRevision: sel.Revision, MembershipFence: sel.FencingToken, Parts: []routeartifact.HostedDeliveryPart{part}}
			p := routeartifact.HostedSelectedPacket{Scope: "host:" + host, Project: "fixture", Draft: "draft-" + host, Offer: 1, Fence: int64(i + 1), Decision: digest([]string{host, phase}), Previous: prev, Authority: d, Business: d, Parent: sel.ReleaseSetID, Child: sel.ArtifactID, ChildDigest: d, SelectedAt: now.Add(-time.Second), ValidUntil: now.Add(time.Minute), Delivery: v}
			if i == 0 {
				prepare = p.Decision
			} else {
				k, e := v.PartKey(part)
				if e != nil {
					t.Fatal(e)
				}
				p.PriorParts = map[string]string{k: oldPart}
			}
			if i == 2 {
				p.SkippedForwardFrom = prepare
			}
			wire, e := routeartifact.SignHostedPacket(p, keys, now)
			if e != nil {
				t.Fatal(e)
			}
			out = append(out, wire)
			prev, oldPart = p.Decision, part.ContentDigest
		}
		return out
	}
	phasesA := makePhases(a, firstA, []model.EdgeRouteBinding{route(a, "/", newA), route(a, "/one", oldB), route(a, "/two", oldB), route(a, "/three", oldB)})
	phasesB := makePhases(b, firstB, []model.EdgeRouteBinding{route(b, "/", newB)})
	apply := func(host string, cmd model.PlatformArtifact) {
		t.Helper()
		if e := s.applyHostedRoute(cmd, host, who, sel, *gr); e != nil {
			t.Fatal(host, cmd.GenerationSequence, e)
		}
	}
	pem, key := testCaddyTLSKeyPair(t, "*.fixture.test")
	cert, e := tls.X509KeyPair([]byte(pem), []byte(key))
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(pem))
	var live http.Handler = s.ProxyHandler()
	// No handler is replaced while a request is in flight.
	var endpoint *httptest.Server
	var cl *http.Client
	listen := func() {
		endpoint = httptest.NewUnstartedServer(live)
		endpoint.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
		endpoint.StartTLS()
		t.Cleanup(endpoint.Close)
		addr := endpoint.Listener.Addr().String()
		tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}
		t.Cleanup(tr.CloseIdleConnections)
		cl = &http.Client{Transport: tr, Timeout: 3 * time.Second}
	}
	listen()
	get := func(host, path, want string, code int) {
		t.Helper()
		resp, e := cl.Get("https://" + host + path)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != code || (code == 200 && string(body) != want+" "+path) || len(resp.TLS.VerifiedChains) == 0 || !reflect.DeepEqual(resp.TLS.PeerCertificates[0].Raw, cert.Certificate[0]) {
			t.Fatalf("%s%s = %d %q", host, path, resp.StatusCode, body)
		}
	}
	check := func(root, sub, peer string) {
		get(a, "/", root, 200)
		for _, path := range []string{"/one", "/two", "/three"} {
			get(a, path, sub, 200)
		}
		get(b, "/", peer, 200)
	}
	check("old-a", "old-a", "shared-v1")
	if e := os.MkdirAll(filepath.Join(cfg.CachePath+".hosted-routes", a), 0700); e != nil {
		t.Fatal(e)
	}
	if e := s.reserveHostedRoutesOnRestart(); e != nil {
		t.Fatal(e)
	}
	get(a, "/", "", 404)
	get(b, "/", "shared-v1", 200)
	if e := s.applyHostedRoute(phasesA[1], a, who, sel, *gr); e == nil {
		t.Fatal("interrupted initial write accepted a forward without prepare")
	}
	apply(a, phasesA[0])
	apply(b, phasesB[0])
	apply(b, phasesB[1])
	check("old-a", "old-a", "shared-v2")
	apply(a, phasesA[1])
	check("new-a", "shared-v1", "shared-v2")
	proof := func(host, decision string) *http.Response {
		req, _ := http.NewRequest("HEAD", "https://"+host+"/one", nil)
		req.Header.Set(routeproof.RequestHeader, "1")
		req.Header.Set(routeproof.NonceHeader, strings.Repeat("b", 32))
		if decision != "" {
			req.Header.Set(routeproof.HostedDecisionHeader, decision)
		}
		resp, e := cl.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		resp.Body.Close()
		return resp
	}
	for _, q := range []string{"", phasesB[1].Generation} {
		if r := proof(a, q); r.StatusCode == 204 {
			t.Fatal("wrong proof class")
		}
	}
	resp := proof(a, phasesA[1].Generation)
	if _, e := routeprobe.ParseResponse(resp, strings.Repeat("b", 32), now); e == nil {
		t.Fatal("host proof counted as global")
	}
	rp, e := routeprobe.ParseHostedResponse(resp, strings.Repeat("b", 32), phasesA[1].Generation, time.Now())
	want, _ := routeproof.Digest(route(a, "/one", oldB))
	if e != nil || rp.Digest != want || rp.TrafficRelease != nil || rp.Kind != "hosted-route" {
		t.Fatal("scoped proof", e, rp)
	}
	s.Config.CaddyListenAddr = endpoint.Listener.Addr().String()
	payload := platformTLSCandidatePayload{Certificates: []pc.TLSIntent{{Hostname: a, AppID: "shared", TenantID: firstA.TenantID, Policy: model.EdgeRouteTLSPolicyCustomDomain}}, Policy: pc.PolicySnapshot{TLSReadiness: &pc.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 2, FactFreshnessSeconds: 60, MaxProbes: 16, MaxConcurrency: 2}}}
	p := s.hostedRoutes[a].packet
	c := p
	c.Child = "tls-child"
	c.Delivery.Parts = append([]routeartifact.HostedDeliveryPart(nil), p.Delivery.Parts...)
	tlsPart := &c.Delivery.Parts[0]
	tlsPart.Recipient.Kind = model.PlatformArtifactKindCaddyRouteConfig
	tlsPart.Content = routeartifact.HostedRecipientInput{TLS: []model.EdgeTLSAllowlistEntry{gr.TLSAllowlist[0]}}
	tlsPart.ContentDigest = digest([]any{"fugue.hosted-recipient-part/v1", a, tlsPart.Recipient, tlsPart.Content})
	for _, packet := range []routeartifact.HostedSelectedPacket{p, c} {
		started := time.Now().UTC()
		samples, e := s.witnessHostedWorker(t.Context(), p, packet, payload, roots)
		if e != nil {
			t.Fatal("local route/TLS witness failed", packet.Child, e)
		}
		o := routeartifact.HostedObservation{Host: a, Member: p.Delivery.Member, Fence: p.Fence, Decision: p.Decision, StartedAt: started, Samples: samples}
		if rows, e := o.Evaluate(packet, time.Now().UTC()); e != nil || len(rows) != 1 || !rows[0].Passing {
			t.Fatal("server rejected exact route or TLS receipt", e)
		}
		if packet.Child == p.Child && len(samples) != 4 || packet.Child == c.Child && (len(samples) != 1 || samples[0].CertificateDigest == "" || samples[0].OwnerDigest == "") {
			t.Fatal("incomplete physical evidence", packet.Child)
		}
		if _, e := o.Evaluate(s.hostedRoutes[b].packet, time.Now().UTC()); e == nil {
			t.Fatal("B borrowed A receipt")
		}
	}
	badTLS := c
	badTLS.Delivery.Parts = append([]routeartifact.HostedDeliveryPart(nil), c.Delivery.Parts...)
	badTLS.Delivery.Parts[0].Content.TLS = []model.EdgeTLSAllowlistEntry{gr.TLSAllowlist[1]}
	if _, e := s.witnessHostedWorker(t.Context(), p, badTLS, payload, roots); e == nil {
		t.Fatal("foreign owner accepted despite sharing the certificate")
	}
	endpoint.Close()
	s = start()
	live = s.ProxyHandler()
	listen()
	get(a, "/", "", 404)
	get(b, "/", "", 404)
	apply(b, phasesB[1])
	get(b, "/", "shared-v2", 200)
	get(a, "/", "", 404)
	apply(a, phasesA[2])
	check("old-a", "old-a", "shared-v2")
	if e := s.applyHostedRoute(phasesA[1], a, who, sel, *gr); e == nil {
		t.Fatal("replayed A")
	}
	check("old-a", "old-a", "shared-v2")
	got, _ := s.Bundle()
	if digest(got) != original {
		t.Fatal("rewrote signed group or TLS ownership")
	}
}

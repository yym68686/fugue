package platformconfig

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"fugue/internal/model"
)

func testHostSuccessorInterleaving(t *testing.T, old, edit CompileRequest, src HostedAddressSource, fixtures ...func(HostedAddressSource, HostedScopeProposal)) {
	b := DNSIntent{Hostname: "independent.example.test", Type: "A", Values: []string{"192.0.2.10"}, TTL: 90}
	old.Intent.DNS = append(slices.Clone(old.Intent.DNS), b)
	// A separate route under B changes deployment while A is in flight.
	// Its origin fact is freshly captured and its DNS address stays put.
	foreign := old.Intent.Routes[0]
	// B is a different host, not a different app: one of A's four paths
	// addresses this same application. App ID alone is not a DNS boundary.
	foreign.Hostname, foreign.DeploymentGeneration = b.Hostname, "deployment-old"
	foreign.RecordOriginForTest(t, "/", "independent-runtime")
	observation := old.RuntimeSnapshot.Origins[0]
	observation.Ref, observation.RuntimeID = foreign.OriginRef, foreign.RuntimeID
	old.Intent.Routes = append(slices.Clone(old.Intent.Routes), foreign)
	old.RuntimeSnapshot.Origins = append(slices.Clone(old.RuntimeSnapshot.Origins), observation)
	edit.Intent.DNS = slices.Clone(edit.Intent.DNS)
	edit.Intent.Routes = slices.Clone(edit.Intent.Routes)
	for _, path := range []string{"/rpc", "/health"} {
		r := edit.Intent.Routes[1]
		r.RecordOriginForTest(t, path, r.RuntimeID)
		edit.Intent.Routes = append(edit.Intent.Routes, r)
		f := edit.RuntimeSnapshot.Origins[1]
		f.Ref = r.OriginRef
		edit.RuntimeSnapshot.Origins = append(slices.Clone(edit.RuntimeSnapshot.Origins), f)
		for i, d := range edit.Intent.DNS {
			ptr := *d.Route
			ptr.Bindings = append(slices.Clone(ptr.Bindings), DNSRouteBinding{Hostname: r.Hostname, PathPrefix: path, AppID: r.AppID})
			edit.Intent.DNS[i].Route = &ptr
		}
	}
	pending := b
	pending.TTL = 300
	edit.Intent.DNS = append(edit.Intent.DNS, pending)
	newForeign := foreign
	newForeign.DeploymentGeneration = "deployment-new"
	edit.Intent.Routes = append(edit.Intent.Routes, newForeign)
	edit.RuntimeSnapshot.Origins = append(slices.Clone(edit.RuntimeSnapshot.Origins), observation)
	bTLS := TLSIntent{Hostname: b.Hostname, AppID: foreign.AppID, TenantID: foreign.TenantID, DomainRef: "domain-independent", Policy: "automatic"}
	edit.Intent.TLS = append(slices.Clone(edit.Intent.TLS), bTLS)
	bFact := TLSDomainObservation{Hostname: b.Hostname, AppID: bTLS.AppID, TenantID: bTLS.TenantID, Ref: bTLS.DomainRef, Status: "verified", VerifiedAt: edit.RuntimeSnapshot.CapturedAt}
	edit.RuntimeSnapshot.TLSDomains = append(slices.Clone(edit.RuntimeSnapshot.TLSDomains), bFact)
	if len(fixtures) > 0 {
		shared := old.Intent.Routes[0]
		rule := TrafficPolicyConstraint{ID: "shared-rule", TenantID: shared.TenantID, AppID: shared.AppID, Mode: model.AppTrafficModeSingle, StableReleaseID: "host-stable-v1", StableWeight: 100}
		for _, req := range []*CompileRequest{&old, &edit} {
			req.Policy.TrafficConstraints = []TrafficPolicyConstraint{rule}
			req.RuntimeSnapshot.Releases = []ReleaseObservation{{ID: rule.StableReleaseID, AppID: shared.AppID, TenantID: shared.TenantID, RuntimeID: shared.RuntimeID, Status: "active", UpstreamURL: "http://host-release-v1.example.test", ObservedAt: *req.RuntimeSnapshot.CapturedAt}}
		}
	}
	setBase := func(c CompileResult, token int64) {
		src.Publication.FencingToken = token
		src.Publication.ReleaseID = "selected-" + c.ReleaseArtifact.Generation
		src.Publication.ReleaseSetID = c.ReleaseArtifact.Generation
		src.Publication.RouteArtifactID, src.Publication.TLSArtifactID, src.Publication.DNSArtifactID = c.RouteArtifact.Generation, c.TLSArtifact.Generation, c.DNSArtifact.Generation
		src.Publication.ReleaseSetDigest, _ = Digest(c.ReleaseArtifact.Content)
		src.Publication.RouteArtifactDigest, _ = Digest(c.RouteArtifact.Content)
		src.Publication.TLSArtifactDigest, _ = Digest(c.TLSArtifact.Content)
		src.Publication.DNSArtifactDigest, _ = Digest(c.DNSArtifact.Content)
		route, tls := c.RouteArtifact, c.TLSArtifact
		route.ScopeKey, tls.ScopeKey = GlobalScopeKey, GlobalScopeKey
		route.ID, route.ContentHash = src.Publication.RouteArtifactID, src.Publication.RouteArtifactDigest
		tls.ID, tls.ContentHash = src.Publication.TLSArtifactID, src.Publication.TLSArtifactDigest
		src.PriorRoute, src.PriorTLS = &route, &tls
		read := func(v any, dest any) {
			raw, e := json.Marshal(v)
			if e != nil {
				t.Fatal(e)
			}
			if e = json.Unmarshal(raw, dest); e != nil {
				t.Fatal(e)
			}
		}
		src.PriorIntent = PlatformIntent{}
		read(c.IntentArtifact.Content, &src.PriorIntent)
		src.Prior = DNSArtifactPayload{}
		read(c.DNSArtifact.Content, &src.Prior)
		src.Inherited = c.InputSnapshot.HostedAddressReservations
		src.PriorDNSSelections = c.InputSnapshot.DNSSelections
	}
	base, e := Compile(old)
	if e != nil {
		t.Fatal(e)
	}
	setBase(base, 1)
	stage, cap, e := CompileHostedPathWithCapture(edit, src)
	if e != nil {
		t.Fatal(e)
	}
	ps, e := BuildHostedScopeProposals(src, stage, cap)
	if e != nil || len(ps) != 1 {
		t.Fatal("offer", e)
	}
	p := ps[0]
	if len(fixtures) > 0 {
		for _, c := range fixtures {
			c(src, p)
		}
		return
	}
	t.Run("foreign measured selector needs a fresh baseline", func(t *testing.T) {
		saved := src
		defer func() { src = saved }()
		peer := "measured.example.test"
		measured := physicalQueryFixture()
		addPeer := func(req CompileRequest, narrow bool) CompileRequest {
			route := old.Intent.Routes[0]
			route.Hostname = peer
			route.RecordOriginForTest(t, "/", route.RuntimeID)
			origin := old.RuntimeSnapshot.Origins[0]
			origin.Ref = route.OriginRef
			req.Intent.Routes = append(slices.Clone(req.Intent.Routes), route)
			req.RuntimeSnapshot.Origins = append(slices.Clone(req.RuntimeSnapshot.Origins), origin)
			dns := req.Intent.DNS[0]
			dns.Hostname, dns.AppID, dns.RecordKind = peer, route.AppID, "custom-domain-target"
			dns.Type, dns.Route = "FUGUE_ROUTE", &DNSRouteIntent{
				Hostnames: []string{peer}, DNSApplicationIntent: *measured.Intent.DNS[0].Application,
			}
			dns.Application, dns.Values = nil, []string{}
			req.Intent.DNS = append(slices.Clone(req.Intent.DNS), dns)
			rule, fact := measured.Policy.DNSAnswerRules[0], measured.RuntimeSnapshot.DNSSelections[0]
			rule.Hostname, fact.Hostname = peer, peer
			fact.ObservedAt = *req.RuntimeSnapshot.CapturedAt
			fact.PhysicalSelection = model.CloneDNSPhysicalSelection(fact.PhysicalSelection)
			fact.PhysicalSelection.CapturedAt = fact.ObservedAt
			if narrow {
				fact.Candidates = fact.Candidates[:1]
				fact.PhysicalSelection.PrimaryEdgeID = fact.Candidates[0].EdgeID
				fact.PhysicalSelection.OrderedEdgeIDs = []string{fact.Candidates[0].EdgeID}
			}
			req.Policy.DNSAnswerRules = append(slices.Clone(req.Policy.DNSAnswerRules), rule)
			req.RuntimeSnapshot.DNSSelections = append(slices.Clone(req.RuntimeSnapshot.DNSSelections), fact)
			return req
		}
		withPeer := addPeer(old, false)
		before, err := Compile(withPeer)
		if err != nil {
			t.Fatal("compile selected baseline:", err)
		}
		setBase(before, 1)
		next := addPeer(edit, true)
		stage, capture, err := CompileHostedPathWithCapture(next, src)
		if err != nil {
			t.Fatal("compile baseline with fresh evidence:", err)
		}
		proposals, err := BuildHostedScopeProposals(src, stage, capture)
		if err != nil || len(proposals) != 1 {
			t.Fatal("build host offer:", err)
		}
		_, err = CompileHostedSuccessor(next, src, proposals[0], "prepare")
		if !errors.Is(err, ErrHostedSuccessorNeedsBaseline) || !strings.Contains(err.Error(), peer) {
			t.Fatalf("expected foreign selector to defer the scoped successor, got %v", err)
		}
		seen := false
		for _, view := range compiledQueryViews(t, stage) {
			for _, record := range view.Records {
				if record.Name == peer {
					seen = true
					if !slices.Equal(record.AnswerPolicy.PhysicalSelection.OrderedEdgeIDs, []string{"edge-a"}) ||
						len(record.Candidates) != 1 || record.Candidates[0].EdgeID != "edge-a" {
						t.Fatal("fresh baseline did not use exactly the authorized endpoint")
					}
				}
			}
		}
		if !seen {
			t.Fatal("baseline omitted the independently compiled peer")
		}
	})
	t.Run("unselected dynamic hostname must not enter the scoped DNS compiler", func(t *testing.T) {
		// A newly enrolled sibling is valid in the complete business capture
		// but absent in the selected full. Its rule and observation used to
		// reach CompileDNSQueryViews after its declaration had been excluded.
		other := "new-sibling.example.test"
		next := edit
		route := old.Intent.Routes[0]
		route.Hostname = other
		route.RecordOriginForTest(t, "/", route.RuntimeID)
		origin := old.RuntimeSnapshot.Origins[0]
		origin.Ref = route.OriginRef
		next.Intent.Routes = append(slices.Clone(next.Intent.Routes), route)
		next.RuntimeSnapshot.Origins = append(slices.Clone(next.RuntimeSnapshot.Origins), origin)
		record := old.Intent.DNS[0]
		record.Hostname = other
		next.Intent.DNS = append(slices.Clone(next.Intent.DNS), record)
		rule, fact := edit.Policy.DNSAnswerRules[0], edit.RuntimeSnapshot.DNSSelections[0]
		rule.Hostname, fact.Hostname = other, other
		next.Policy.DNSAnswerRules = append(slices.Clone(next.Policy.DNSAnswerRules), rule)
		next.RuntimeSnapshot.DNSSelections = append(slices.Clone(next.RuntimeSnapshot.DNSSelections), fact)
		if _, err := Compile(next); err != nil {
			t.Fatal("invalid complete business fixture", err)
		}
		scoped, err := CompileHostedSuccessor(next, src, p, "prepare")
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range scoped.InputSnapshot.DNSSelections {
			if v.Hostname == other {
				t.Fatal("foreign DNS observation entered A's input")
			}
		}
		payload, err := globalProposalArtifact(scoped.DNSArtifact, model.PlatformArtifactKindDNSAnswerBundle)
		if err != nil {
			t.Fatal(err)
		}
		dns, err := DecodeDNSArtifact(payload)
		if err != nil || !reflect.DeepEqual(src.Prior.Records, dns.Records) {
			t.Fatal("selected wire records not preserved", err)
		}
	})
	check := func(c CompileResult, count, ttl int) {
		t.Helper()
		q, e := ProjectRouteArtifact(c.RouteArtifact)
		wantTLS := 1
		if ttl == 300 {
			wantTLS = 2
			if !slices.ContainsFunc(q.TLSAllowlist, func(v model.EdgeTLSAllowlistEntry) bool {
				return v.Hostname == b.Hostname && v.AppID == bTLS.AppID && v.TenantID == bTLS.TenantID && v.Status == bFact.Status
			}) {
				t.Fatal("A altered B's separately published certificate ownership")
			}
		}
		if e != nil || len(q.Routes) != count+1 || len(q.TLSAllowlist) != wantTLS || !slices.Contains(q.TLSAllowlist, p.Before.Owner) {
			t.Fatal("dispatch/TLS", e)
		}
		expect := foreign.DeploymentGeneration
		if ttl == 300 {
			expect = newForeign.DeploymentGeneration
		}
		if !slices.ContainsFunc(q.Routes, func(r model.EdgeRouteIntent) bool {
			return r.Hostname == b.Hostname && r.DeploymentGeneration == expect
		}) {
			t.Fatal("B's independent route was overwritten")
		}
		a, e := globalProposalArtifact(c.DNSArtifact, c.DNSArtifact.ArtifactKind)
		if e != nil {
			t.Fatal(e)
		}
		d, e := DecodeDNSArtifact(a)
		if e != nil {
			t.Fatal(e)
		}
		expected := slices.Clone(src.Prior.Records)
		found := false
		for i := range expected {
			if expected[i].Hostname == b.Hostname {
				expected[i].TTL = ttl
				found = true
			}
		}
		if !found || !reflect.DeepEqual(expected, d.Records) {
			t.Fatal("foreign address/TTL or hosted wire identity changed", d.Records)
		}
	}
	mid, e := CompileHostedSuccessor(edit, src, p, "prepare")
	if e != nil {
		t.Fatal("prepare", e)
	}
	check(mid, 1, 90)
	setBase(mid, 2)
	src.PriorHolds = src.EffectiveHolds
	active, e := CompileHostedSuccessor(edit, src, p, "forward")
	if e != nil {
		t.Fatal("forward", e)
	}
	check(active, 4, 90)
	setBase(active, 3)
	// B's separate ordinary publication changes its real selected TTL.
	newB, staged, e := CompileHostedPathTransition(edit, src)
	if e != nil || staged {
		t.Fatal("independent update", e)
	}
	check(newB, 4, 300)
	setBase(newB, 4)
	// A stale view of B in the inverse request must never replace the newer full.
	inverse := edit
	inverse.Intent = PlatformIntent{}
	raw, _ := json.Marshal(mid.IntentArtifact.Content)
	if e = json.Unmarshal(raw, &inverse.Intent); e != nil {
		t.Fatal(e)
	}
	inverse.RuntimeSnapshot = mid.InputSnapshot
	inverse.RuntimeSnapshot.TLSDomains = newB.InputSnapshot.TLSDomains
	inverse.RuntimeSnapshot.HostedAddressReservations = nil
	back, e := CompileHostedSuccessor(inverse, src, p, "withdraw")
	if e != nil {
		t.Fatal("withdraw", e)
	}
	check(back, 1, 300)
	inverse.RuntimeSnapshot.TLSDomains = mid.InputSnapshot.TLSDomains
	if _, e := CompileHostedSuccessor(inverse, src, p, "withdraw"); e == nil {
		t.Fatal("withdrawal fabricated current evidence for B's retained certificate")
	}
	if _, e := CompileHostedSuccessor(inverse, src, p, "prepare"); e == nil {
		t.Fatal("stale prepare accepted")
	}
}

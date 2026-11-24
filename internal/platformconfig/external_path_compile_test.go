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

func externalFourRouteEdit(t *testing.T) (HostedAddressSource, CompileRequest) {
	return externalFourRouteEditWithThirdEndpoint(t, false)
}

func externalFourRouteEditWithThirdEndpoint(t *testing.T, extra bool) (HostedAddressSource, CompileRequest) {
	t.Helper()
	const h = "outside.example.org"
	r := consumerPlacementFixture()
	if extra {
		endpoint := r.RuntimeSnapshot.DNSEdgeEndpoints[1]
		endpoint.EdgeID, endpoint.A, endpoint.AAAA = "edge-c", []string{"93.184.216.36"}, nil
		r.RuntimeSnapshot.DNSEdgeEndpoints = append(r.RuntimeSnapshot.DNSEdgeEndpoints, endpoint)
		for i := range r.RuntimeSnapshot.DNSSelections {
			sel := &r.RuntimeSnapshot.DNSSelections[i]
			candidate := sel.Candidates[1]
			candidate.EdgeID, candidate.IP = endpoint.EdgeID, endpoint.A[0]
			sel.Candidates = append(sel.Candidates, candidate)
			for j := range sel.ScopedCandidates {
				scope := &sel.ScopedCandidates[j]
				scope.Candidates = append(scope.Candidates, candidate)
			}
		}
	}
	r.Policy.MinimumHealthyEdges = 1
	r.Intent.Scope, r.Policy.Scope = GlobalScopeKey, GlobalScopeKey
	d := &r.Intent.DNS[0]
	d.Type, d.RecordKind, d.Values = "FUGUE_ROUTE", model.EdgeDNSRecordKindCustomDomainTarget, []string{}
	d.Route = &DNSRouteIntent{DNSApplicationIntent: *d.Application, Hostnames: []string{h}}
	d.Application = nil
	root := &r.Intent.Routes[0]
	root.Hostname = h
	root.RecordOriginForTest(t, "/", "runtime-a")
	now := *r.RuntimeSnapshot.CapturedAt
	r.Intent.TLS = []TLSIntent{{Hostname: h, TenantID: d.TenantID, AppID: d.AppID, DomainRef: "test-cert", Policy: "automatic"}}
	r.RuntimeSnapshot.TLSDomains = []TLSDomainObservation{{Ref: "test-cert", Hostname: h, TenantID: d.TenantID, AppID: d.AppID, Status: "verified", VerifiedAt: &now}}
	r.RuntimeSnapshot.Origins = []OriginObservation{{Ref: root.OriginRef, ObservedAt: now, Status: "active", RuntimeID: root.RuntimeID}}
	prior, err := Compile(r)
	if err != nil {
		t.Fatal("prior", err)
	}
	decode := func(a model.PlatformArtifact, out any) {
		t.Helper()
		b, e := json.Marshal(a.Content)
		if e != nil || json.Unmarshal(b, out) != nil {
			t.Fatal("invalid compiler output", e)
		}
	}
	hash := "sha256:" + strings.Repeat("a", 64)
	src := HostedAddressSource{Publication: PreviousTrafficPublicationReference{ReleaseSetID: "parent", ReleaseSetDigest: hash, ReleaseID: "published", FencingToken: 1,
		RouteArtifactID: "routes", RouteArtifactDigest: hash, TLSArtifactID: "certs", TLSArtifactDigest: hash, DNSArtifactID: "dns", DNSArtifactDigest: hash}}
	decode(prior.IntentArtifact, &src.PriorIntent)
	decode(prior.DNSArtifact, &src.Prior)
	src.PriorRoute, src.PriorTLS = &prior.RouteArtifact, &prior.TLSArtifact
	src.PriorRoute.ID, src.PriorRoute.ContentHash, src.PriorRoute.ScopeKey = src.Publication.RouteArtifactID, hash, GlobalScopeKey
	src.PriorTLS.ID, src.PriorTLS.ContentHash, src.PriorTLS.ScopeKey = src.Publication.TLSArtifactID, hash, GlobalScopeKey
	edit := r
	edit.Intent.Routes = slices.Clone(r.Intent.Routes)
	edit.RuntimeSnapshot.Origins = slices.Clone(r.RuntimeSnapshot.Origins)
	edit.Intent.DNS = slices.Clone(r.Intent.DNS)
	route := *d.Route
	edit.Intent.DNS[0].Route = &route
	for _, path := range []string{"/one", "/two", "/three"} {
		extra := *root
		extra.RecordOriginForTest(t, path, root.RuntimeID)
		edit.Intent.Routes = append(edit.Intent.Routes, extra)
		fact := edit.RuntimeSnapshot.Origins[0]
		fact.Ref = extra.OriginRef
		edit.RuntimeSnapshot.Origins = append(edit.RuntimeSnapshot.Origins, fact)
	}
	for _, rt := range edit.Intent.Routes {
		route.Bindings = append(route.Bindings, DNSRouteBinding{Hostname: h, PathPrefix: rt.PathPrefix, AppID: rt.AppID})
	}
	edit.Intent.Generation = "new-routes"
	edit.RuntimeSnapshot.IntentGeneration = edit.Intent.Generation
	return src, edit
}

func TestExternalPathThreeAddressReplaySeparatesDeclarationAndEligibility(t *testing.T) {
	source, edit := externalFourRouteEditWithThirdEndpoint(t, true)
	target := edit.Intent.DNS[0].Hostname
	held := HostedAddressHold{"edge-c", "edge-group-b", "93.184.216.36"}
	edit.RuntimeSnapshot.DNSEdgeEndpoints = edit.RuntimeSnapshot.DNSEdgeEndpoints[:2]
	for i := range edit.RuntimeSnapshot.DNSSelections {
		sel := &edit.RuntimeSnapshot.DNSSelections[i]
		sel.Candidates = sel.Candidates[:2]
		for j := range sel.ScopedCandidates {
			s := &sel.ScopedCandidates[j]
			s.Candidates = s.Candidates[:2]
		}
	}
	stage, capture, err := CompileHostedPathWithCapture(edit, source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = BuildHostedScopeProposals(source, stage, capture)
	var conflict *ExternalTargetConflict
	if !errors.As(err, &conflict) || conflict.Target != target || conflict.RecordType != "A" ||
		conflict.Field != "addresses" || conflict.ExpectedAddresses != 3 || conflict.CandidateAddresses != 2 {
		t.Fatalf("unexplained address removal lacks a precise refusal: %v", err)
	}
	source.EffectiveHolds = []HostedAddressHold{held}
	proposals, err := BuildHostedScopeProposals(source, stage, capture)
	if err != nil || len(proposals) != 1 {
		t.Fatal("same effective candidate with a declared hold must remain publishable", err)
	}
	p := proposals[0]
	if len(p.Physical) != 3 || len(p.Eligible) != 2 || len(p.Records) != 1 || len(p.Records[0].Values) != 3 ||
		!slices.Contains(p.Records[0].Values, held.Address) || !slices.Equal(p.ExternalPath.Held, []HostedAddressHold{held}) {
		t.Fatalf("expected three declared addresses and two eligible: %+v", p)
	}
}

// Keep the physical RR in the real selected compiler output. Derive the
// candidate with a declared hold and one fewer eligible endpoint.
func ExternalHeldFixtureForTest(t *testing.T) (HostedAddressSource, HostedScopeProposal) {
	t.Helper()
	bound, partial := externalFourRouteEdit(t)
	bound.EffectiveHolds = []HostedAddressHold{{"edge-b", "edge-group-b", "93.184.216.35"}}
	partial.RuntimeSnapshot.DNSEdgeEndpoints = slices.Clone(partial.RuntimeSnapshot.DNSEdgeEndpoints[:1])
	for i := range partial.RuntimeSnapshot.DNSSelections {
		s := &partial.RuntimeSnapshot.DNSSelections[i]
		s.Candidates = slices.Clone(s.Candidates[:1])
		s.SelectedEdgeGroupID = s.Candidates[0].EdgeGroupID
		for j := range s.ScopedCandidates {
			s.ScopedCandidates[j].Candidates = slices.Clone(s.ScopedCandidates[j].Candidates[:1])
		}
	}
	compiled, cap, e := CompileHostedPathWithCapture(partial, bound)
	if e != nil {
		t.Fatal(e)
	}
	ps, e := BuildHostedScopeProposals(bound, compiled, cap)
	if e != nil || len(ps) != 1 {
		t.Fatal("missing external held proposal", e)
	}
	return bound, ps[0]
}

func TestCompileExternalPathProducesReversibleProposalWithoutAddressHold(t *testing.T) {
	src, edit := externalFourRouteEdit(t)
	d := &edit.Intent.DNS[0]
	h := edit.Intent.Routes[0].Hostname
	stage, captured, err := CompileHostedPathWithCapture(edit, src)
	if err != nil || captured == nil || !slices.Equal(captured.ExternalHosts, []string{h}) || len(captured.Hosts) != 0 {
		t.Fatal("compiler failed to discover external path edit", err)
	}
	t.Run("retain only the changed hostname on a shared target", func(t *testing.T) {
		const peer = "peer.example.org"
		withPeer := func(in PlatformIntent, extra bool) PlatformIntent {
			in.DNS = slices.Clone(in.DNS)
			r := *in.DNS[0].Route
			r.Hostnames = append(slices.Clone(r.Hostnames), peer)
			r.Bindings = slices.Clone(r.Bindings)
			in.DNS[0].Route = &r
			base := in.Routes[0]
			base.Hostname = peer
			base.RecordOriginForTest(t, "/", base.RuntimeID)
			in.Routes = append(slices.Clone(in.Routes), base)
			if extra {
				r.Bindings = append(r.Bindings, DNSRouteBinding{Hostname: peer, PathPrefix: "/", AppID: base.AppID})
				base.RecordOriginForTest(t, "/peer", base.RuntimeID)
				in.Routes = append(in.Routes, base)
				r.Bindings = append(r.Bindings, DNSRouteBinding{Hostname: peer, PathPrefix: "/peer", AppID: base.AppID})
			}
			return in
		}
		fresh := edit
		fresh.Intent = withPeer(fresh.Intent, true)
		staged, e := stageBaselineExternalPaths(withPeer(src.PriorIntent, false), fresh, []string{h})
		if e != nil {
			t.Fatal(e)
		}
		got := staged.Intent.DNS[0].Route.Bindings
		for _, b := range []DNSRouteBinding{{Hostname: h, PathPrefix: "/", AppID: d.AppID}, {Hostname: peer, PathPrefix: "/", AppID: d.AppID}, {Hostname: peer, PathPrefix: "/peer", AppID: d.AppID}} {
			if !slices.Contains(got, b) {
				t.Fatal("missing staged or current peer binding", b, got)
			}
		}
		if len(got) != 3 {
			t.Fatal("pending paths escaped", got)
		}
	})
	t.Run("ordinary DNS refresh does not apply pending external paths", func(t *testing.T) {
		fresh := edit
		fresh.Intent.Routes = slices.Clone(fresh.Intent.Routes)
		fresh.Intent.Routes[0].DeploymentGeneration = "current-origin"
		staged, err := stageBaselineExternalPaths(src.PriorIntent, fresh, captured.ExternalHosts)
		if err != nil {
			t.Fatal(err)
		}
		if len(staged.Intent.Routes) != 1 || staged.Intent.Routes[0].PathPrefix != "/" ||
			staged.Intent.Routes[0].DeploymentGeneration != "current-origin" || len(staged.Intent.DNS[0].Route.Bindings) != 0 ||
			len(fresh.Intent.Routes) != 4 || len(fresh.Intent.DNS[0].Route.Bindings) != 4 {
			t.Fatal("external baseline replaced live origin or retained a pending binding")
		}
		if _, err := Compile(staged); err != nil {
			t.Fatal("ordinary baseline cannot compile", err)
		}
		fresh.Intent.Routes[0].AppID = "different-owner"
		if _, err := stageBaselineExternalPaths(src.PriorIntent, fresh, captured.ExternalHosts); err == nil {
			t.Fatal("ordinary baseline revived a previous path with no current owner binding")
		}
	})
	ps, err := BuildHostedScopeProposals(src, stage, captured)
	if err != nil || len(ps) != 1 {
		t.Fatal("could not build proposal", err)
	}
	p := ps[0]
	t.Run("declared hold preserves physical RR and survives path withdrawal", func(t *testing.T) {
		bound := src
		suspended := HostedAddressHold{"edge-b", "edge-group-b", "93.184.216.35"}
		bound.EffectiveHolds = []HostedAddressHold{suspended}
		partial := edit
		partial.RuntimeSnapshot.DNSEdgeEndpoints = slices.Clone(edit.RuntimeSnapshot.DNSEdgeEndpoints[:1])
		partial.RuntimeSnapshot.DNSSelections = slices.Clone(edit.RuntimeSnapshot.DNSSelections)
		for i := range partial.RuntimeSnapshot.DNSSelections {
			s := &partial.RuntimeSnapshot.DNSSelections[i]
			s.Candidates = slices.Clone(s.Candidates[:1])
			s.SelectedEdgeGroupID = s.Candidates[0].EdgeGroupID
			s.ScopedCandidates = slices.Clone(s.ScopedCandidates)
			for j := range s.ScopedCandidates {
				s.ScopedCandidates[j].Candidates = slices.Clone(s.ScopedCandidates[j].Candidates[:1])
			}
		}
		compiled, cap, e := CompileHostedPathWithCapture(partial, bound)
		if e != nil {
			t.Fatal(e)
		}
		ps, e := BuildHostedScopeProposals(bound, compiled, cap)
		if e != nil || len(ps) != 1 {
			t.Fatal(e)
		}
		pr := ps[0]
		if len(pr.Physical) != 2 || len(pr.Eligible) != 1 || len(pr.Records) != 1 || !reflect.DeepEqual(pr.Records, p.Records) || !slices.Equal(pr.ExternalPath.Held, []HostedAddressHold{suspended}) {
			t.Fatal("lost physical declaration or hold")
		}
		view := bound.Prior
		var prepared HostedDNSContent
		for _, phase := range []string{"prepare", "forward", "withdraw"} {
			result, e := ComposeHostedDNSPhase(view, pr, phase)
			if e != nil {
				t.Fatal(phase, e)
			}
			if !reflect.DeepEqual(result.Views, bound.Prior.QueryViews) || !slices.Equal(result.Withheld, pr.ExternalPath.Held) {
				t.Fatal("wire declaration or veto changed")
			}
			for _, probe := range result.Plan.Probes {
				if probe.Hostname == h && probe.Address == suspended.Address {
					t.Fatal("disallowed readiness probe")
				}
			}
			if phase == "prepare" {
				prepared = result
			}
			if phase == "withdraw" && !reflect.DeepEqual(prepared, result) {
				t.Fatal("withdrawal lifted hold")
			}
			view.ReadinessPlan = &result.Plan
		}
		if _, e := ComposeHostedDNSPhase(bound.Prior, pr, "forward"); e == nil {
			t.Fatal("skipped hold preparation")
		}
		for _, holds := range [][]HostedAddressHold{nil, {{suspended.EdgeID, "edge-group-other", suspended.Address}}, {{suspended.EdgeID, suspended.EdgeGroupID, "93.184.216.36"}}} {
			wrong := bound
			wrong.EffectiveHolds = holds
			if _, e := BuildHostedScopeProposals(wrong, compiled, cap); e == nil {
				t.Fatal("unexplained target loss accepted")
			}
		}
	})
	t.Run("ineligible sibling does not suppress valid host", func(t *testing.T) {
		mixed := *captured
		mixed.Hosts = []string{"unowned.example.test"}
		partial, rejected := BuildIndependentHostedScopeProposals(src, stage, &mixed)
		if rejected == nil || !strings.Contains(rejected.Error(), mixed.Hosts[0]) || !reflect.DeepEqual(partial, ps) {
			t.Fatal("valid host changed or sibling refusal was lost", rejected)
		}
		if all, err := BuildHostedScopeProposals(src, stage, &mixed); err == nil || len(all) != 0 {
			t.Fatal("all-or-nothing caller accepted a partial result")
		}
		mixed.Hosts = []string{h}
		if ambiguous, err := BuildIndependentHostedScopeProposals(src, stage, &mixed); err == nil || len(ambiguous) != 0 {
			t.Fatal("duplicate ownership returned a draft")
		}
	})
	if p.ExternalPath == nil || p.ExternalPath.Target.Target != d.Hostname || len(p.Before.Routes) != 1 || len(p.After.Routes) != 4 ||
		len(p.Hold) != 0 || !reflect.DeepEqual(p.Physical, p.Eligible) {
		t.Fatal("incorrect scope or address reservation")
	}
	dns := src.Prior
	scoped, e := HostedDNSProgramSource(dns, p)
	if e != nil || len(scoped.Records) != len(p.Records) {
		t.Fatal("external target could not be resolved from the compiled wire records", e)
	}
	if _, e := ComposeHostedDNSPhase(scoped, p, "forward"); e != nil {
		t.Fatal("scoped target cannot execute the proposal", e)
	}
	for _, phase := range []string{"prepare", "forward", "withdraw"} {
		result, e := ComposeHostedDNSPhase(dns, p, phase)
		if e != nil || !reflect.DeepEqual(result.Views, src.Prior.QueryViews) || !reflect.DeepEqual(result.Rules, src.Prior.Policy.DNSAnswerRules) {
			t.Fatal(phase, e)
		}
		dns.ReadinessPlan = &result.Plan
	}
	if !reflect.DeepEqual(dns.ReadinessPlan, src.Prior.ReadinessPlan) {
		t.Fatal("withdrawal did not restore the original requirements")
	}
}

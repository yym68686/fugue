package platformconfig

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestCompileExternalPathProducesReversibleProposalWithoutAddressHold(t *testing.T) {
	const h = "outside.example.org"
	r := consumerPlacementFixture()
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
	stage, captured, err := CompileHostedPathWithCapture(edit, src)
	if err != nil || captured == nil || !slices.Equal(captured.ExternalHosts, []string{h}) || len(captured.Hosts) != 0 {
		t.Fatal("compiler failed to discover external path edit", err)
	}
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

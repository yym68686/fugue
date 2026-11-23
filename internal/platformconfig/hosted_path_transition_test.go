package platformconfig

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestHostedPathTransitionSeparatesAddressHoldFromForwardAndReverse(t *testing.T) {
	testHostedPathTransition(t, false)
}

func TestHostedPathAliasRetainsOnlySameOwnerSharedPhysicalAddress(t *testing.T) {
	testHostedPathTransition(t, true)
}

// Exercise the actual compiler in downstream executor tests without serializing
// production data or duplicating a handwritten proposal on each side.
func HostedTransitionFixtureForTest(t *testing.T, withAlias bool, check func(HostedAddressSource, HostedScopeProposal)) {
	t.Helper()
	testHostedPathTransition(t, withAlias, check)
}

func testHostedPathTransition(t *testing.T, withAlias bool, checks ...func(HostedAddressSource, HostedScopeProposal)) {
	r := consumerPlacementFixture()
	r.Intent.Scope, r.Policy.Scope = GlobalScopeKey, GlobalScopeKey
	r.Policy.MinimumHealthyEdges = 1
	root := &r.Intent.Routes[0]
	root.RecordOriginForTest(t, "/", "runtime-a")
	d := &r.Intent.DNS[0]
	d.RecordKind = "hosted"
	r.Intent.TLS = []TLSIntent{{Hostname: d.Hostname, AppID: d.AppID, TenantID: d.TenantID, DomainRef: "domain-a", Policy: "automatic"}}
	now := *r.RuntimeSnapshot.CapturedAt
	r.RuntimeSnapshot.TLSDomains = []TLSDomainObservation{{Ref: "domain-a", Hostname: d.Hostname, AppID: d.AppID, TenantID: d.TenantID, Status: "verified", VerifiedAt: &now}}
	fact := func(v RouteIntent) OriginObservation {
		return OriginObservation{Ref: v.OriginRef, ObservedAt: now, Status: "active", RuntimeID: v.RuntimeID}
	}
	r.RuntimeSnapshot.Origins = []OriginObservation{fact(*root)}
	r.RuntimeSnapshot.IntentGeneration = r.Intent.Generation
	var alias DNSIntent
	if withAlias {
		alias = *d
		alias.Hostname = "edge.example.test"
		alias.RecordKind = "custom-domain-target"
		alias.Type = "FUGUE_ROUTE"
		alias.Values = []string{}
		alias.Route = &DNSRouteIntent{Hostnames: []string{d.Hostname}, DNSApplicationIntent: *d.Application}
		alias.Application = nil
		r.Intent.DNS = append(slices.Clone(r.Intent.DNS), alias)
		rule := r.Policy.DNSAnswerRules[0]
		rule.Hostname = alias.Hostname
		r.Policy.DNSAnswerRules = append(slices.Clone(r.Policy.DNSAnswerRules), rule)
		observation := r.RuntimeSnapshot.DNSSelections[0]
		observation.Hostname = alias.Hostname
		observation.ScopedCandidates = slices.Clone(observation.ScopedCandidates)
		r.RuntimeSnapshot.DNSSelections = append(slices.Clone(r.RuntimeSnapshot.DNSSelections), observation)
	}
	prior, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	payload := func(c CompileResult) DNSArtifactPayload {
		t.Helper()
		var v DNSArtifactPayload
		b, e := json.Marshal(c.DNSArtifact.Content)
		if e != nil {
			t.Fatal(e)
		}
		if e = json.Unmarshal(b, &v); e != nil {
			t.Fatal(e)
		}
		return v
	}
	pin := func(id string, n int64) PreviousTrafficPublicationReference {
		dig := "sha256:" + strings.Repeat("a", 64)
		return PreviousTrafficPublicationReference{ReleaseSetID: "parent-" + id, ReleaseSetDigest: dig, ReleaseID: "full-" + id, FencingToken: n, RouteArtifactID: "routes-" + id, RouteArtifactDigest: dig, TLSArtifactID: "tls-" + id, TLSArtifactDigest: dig, DNSArtifactID: "dns-" + id, DNSArtifactDigest: dig}
	}
	hold := []HostedAddressHold{{"edge-b", "edge-group-b", "93.184.216.35"}}
	src := HostedAddressSource{Publication: pin("old", 1), PriorIntent: r.Intent, Prior: payload(prior), EffectiveHolds: hold}
	next := r
	next.Intent.Generation = "split-routes"
	a, b := *root, *root
	a.AppID = "web-app"
	a.RecordOriginForTest(t, "/", "runtime-web")
	b.RecordOriginForTest(t, "/api", "runtime-a")
	b.CacheNamespace = "new-cache"
	b.DeploymentGeneration = "new-api-deploy"
	next.Intent.Routes = []RouteIntent{a, b}
	rr := *d
	rr.Type = "FUGUE_ROUTE"
	rr.Values = []string{}
	rr.Route = &DNSRouteIntent{DNSApplicationIntent: *d.Application, Hostnames: []string{d.Hostname}, ManagedHost: true, Bindings: []DNSRouteBinding{{Hostname: d.Hostname, PathPrefix: "/", AppID: a.AppID}, {Hostname: d.Hostname, PathPrefix: "/api", AppID: b.AppID}}}
	rr.Application = nil
	next.Intent.DNS = []DNSIntent{rr}
	if withAlias {
		shared := *alias.Route
		shared.Bindings = slices.Clone(rr.Route.Bindings)
		alias.Route = &shared
		next.Intent.DNS = append(next.Intent.DNS, alias)
	}
	next.RuntimeSnapshot.IntentGeneration = next.Intent.Generation
	next.RuntimeSnapshot.Origins = []OriginObservation{fact(a), fact(b)}
	next.RuntimeSnapshot.DNSEdgeEndpoints = next.RuntimeSnapshot.DNSEdgeEndpoints[:1]
	for i := range next.RuntimeSnapshot.DNSSelections {
		s := &next.RuntimeSnapshot.DNSSelections[i]
		s.Candidates = s.Candidates[:1]
		s.SelectedEdgeGroupID = s.Candidates[0].EdgeGroupID
		s.ScopedCandidates[0].Candidates = s.ScopedCandidates[0].Candidates[:1]
	}
	intermediate, capture, err := CompileHostedPathWithCapture(next, src)
	if err != nil || capture == nil {
		t.Fatal("missing address-only baseline", err)
	}
	pr, pt := prior.RouteArtifact, prior.TLSArtifact
	pr.ID, pr.ContentHash = src.Publication.RouteArtifactID, src.Publication.RouteArtifactDigest
	pt.ID, pt.ContentHash = src.Publication.TLSArtifactID, src.Publication.TLSArtifactDigest
	src.PriorRoute, src.PriorTLS = &pr, &pt
	pr.ScopeKey, pt.ScopeKey = GlobalScopeKey, GlobalScopeKey
	proposals, e := BuildHostedScopeProposals(src, intermediate, capture)
	if e != nil || len(proposals) != 1 {
		t.Fatal("global scoped capture", e)
	}
	proposal := proposals[0]
	if withAlias && len(checks) == 0 {
		t.Run("compile four paths across another publication", func(t *testing.T) {
			testHostSuccessorInterleaving(t, r, next, src)
		})
	}
	testHostedSuccessorWire(t, pr, intermediate.RouteArtifact, capture.Desired.RouteArtifact, proposal)
	for _, check := range checks {
		check(src, proposal)
	}
	original, _ := Digest(src.Prior)
	held, e := ComposeHostedDNSPhase(src.Prior, proposal, "prepare")
	if e != nil {
		t.Fatal("isolated DNS prepare", e)
	}
	prepared := src.Prior
	prepared.ReadinessPlan, prepared.QueryViews = &held.Plan, held.Views
	advanced, e := ComposeHostedDNSPhase(prepared, proposal, "forward")
	if e != nil {
		t.Fatal("isolated DNS paths", e)
	}
	active := prepared
	active.ReadinessPlan, active.QueryViews = &advanced.Plan, advanced.Views
	undone, e := ComposeHostedDNSPhase(active, proposal, "withdraw")
	if e != nil || !reflect.DeepEqual(undone, held) {
		t.Fatal("withdraw changed the prepared DNS selection", e)
	}
	if _, e := ComposeHostedDNSPhase(src.Prior, proposal, "forward"); e == nil {
		t.Fatal("skipped address hold")
	}
	if _, e := ComposeHostedDNSPhase(active, proposal, "prepare"); e == nil {
		t.Fatal("skipped withdrawal")
	}
	if current, _ := Digest(src.Prior); current != original {
		t.Fatal("changed source artifact during execution")
	}
	for _, stage := range []struct {
		base      DNSArtifactPayload
		direction string
	}{{prepared, "forward"}, {active, "withdraw"}} {
		drift := stage.base
		plan := *drift.ReadinessPlan
		plan.Records = slices.Clone(plan.Records)
		for i := range plan.Records {
			if plan.Records[i].Hostname == proposal.Host {
				plan.Records[i].Targets = slices.Clone(plan.Records[i].Targets)
				target := &plan.Records[i].Targets[0]
				target.RequireSinglePublication = !target.RequireSinglePublication
			}
		}
		drift.ReadinessPlan = &plan
		if _, err := ComposeHostedDNSPhase(drift, proposal, stage.direction); err == nil {
			t.Fatal("rebound a changed publication requirement", stage.direction)
		}
		changedOwner := proposal
		changedOwner.Records = slices.Clone(proposal.Records)
		changedOwner.Records[0].TTL++
		if _, err := ComposeHostedDNSPhase(stage.base, changedOwner, stage.direction); err == nil {
			t.Fatal("borrowed a different DNS record", stage.direction)
		}
	}
	if len(proposal.Physical) != 2 || len(proposal.Eligible) != 1 || len(proposal.Before.Routes) != 1 || len(proposal.After.Routes) != 2 ||
		len(proposal.BaseProbes) != 2 || len(proposal.PathProbes) != 2 || proposal.After.Owner != proposal.Before.Owner || proposal.Base != src.Publication {
		t.Fatal("wrong physical, route, TLS or probe coverage")
	}
	foreign := *capture
	foreign.Desired.RouteArtifact.Scope.Key = AuthorityCellScope("cell-example")
	if _, e := BuildHostedScopeProposals(src, intermediate, &foreign); e == nil {
		t.Fatal("global address borrowed another cell's route authority")
	}
	baseline := payload(intermediate)
	needed := 1
	if withAlias {
		needed = 2
	}
	requirements, e := SelectHostedHoldRequirements(d.Hostname, src.Prior, baseline, hold)
	if e != nil || len(requirements) != needed {
		t.Fatal("cannot isolate physical hold prerequisite", e, len(requirements))
	}
	if _, e = SelectHostedHoldRequirements(d.Hostname, src.Prior, baseline, []HostedAddressHold{{"edge-other", "edge-group-b", "93.184.216.35"}}); e == nil {
		t.Fatal("wrong physical owner authorized removal")
	}
	if withAlias {
		broken := payload(intermediate)
		previous := payload(prior)
		unrelated := DNSQueryView{NodeID: "another-reader", EdgeGroupID: "edge-group-other", Zone: "another.test"}
		previous.QueryViews = append(previous.QueryViews, unrelated)
		broken.QueryViews = append(broken.QueryViews, unrelated)
		if _, e = SelectHostedHoldRequirements(d.Hostname, previous, broken, hold); e != nil {
			t.Fatal("unrelated zone incorrectly had to publish this host", e)
		}
		broken.QueryViews[0].NodeID = "replacement-reader"
		if _, e = SelectHostedHoldRequirements(d.Hostname, previous, broken, hold); e == nil {
			t.Fatal("unapproved reader accepted")
		}
		broken = payload(intermediate)
		for i := range broken.Records {
			if broken.Records[i].RecordKind == "custom-domain-target" {
				broken.Records[i].TenantID = "another-tenant"
			}
		}
		if _, e = SelectHostedHoldRequirements(d.Hostname, src.Prior, broken, hold); e == nil {
			t.Fatal("foreign alias included in scoped hold")
		}
		broken = payload(intermediate)
		for vi := range broken.QueryViews {
			for ri := range broken.QueryViews[vi].Records {
				q := &broken.QueryViews[vi].Records[ri]
				if q.Name == alias.Hostname && len(q.Candidates) > 0 {
					q.Candidates[0].IP = hold[0].Address
				}
			}
		}
		if _, e = SelectHostedHoldRequirements(d.Hostname, src.Prior, broken, hold); e == nil {
			t.Fatal("held IP still selected in a consumer view")
		}
	}
	if withAlias {
		if len(intermediate.InputSnapshot.HostedAddressReservations) != 2 || !reflect.DeepEqual(src.Prior.Records, baseline.Records) {
			t.Fatal("same-owner reverse dependency lost its original wire declaration")
		}
	}
	if len(baseline.ReadinessPlan.Probes) != 1 || baseline.ReadinessPlan.Probes[0].Path != "/" || !reflect.DeepEqual(baseline.Records, src.Prior.Records) {
		t.Fatal("address-only baseline changed routes or RR")
	}
	src.Publication = pin("baseline", 2)
	src.Prior, src.PriorHolds, src.Inherited = baseline, hold, intermediate.InputSnapshot.HostedAddressReservations
	forward, staged, err := CompileHostedPathTransition(next, src)
	if err != nil || staged {
		t.Fatal("did not advance from verified address-only baseline", err)
	}
	after := payload(forward)
	if len(after.ReadinessPlan.Probes) != 2 || !reflect.DeepEqual(after.Records, baseline.Records) || !reflect.DeepEqual(after.QueryViews, baseline.QueryViews) {
		t.Fatal("path-only release changed DNS or lost an exact path")
	}
	reverse := r
	// Reverse only the host binding, retaining the current owner's deploy and
	// cache namespace. Do not roll the application back with the route table.
	body, err := json.Marshal(intermediate.IntentArtifact.Content)
	if err != nil {
		t.Fatal(err)
	}
	reverse.Intent = PlatformIntent{}
	if err = json.Unmarshal(body, &reverse.Intent); err != nil || len(reverse.Intent.Routes) != 1 || reverse.Intent.Routes[0].DeploymentGeneration != b.DeploymentGeneration {
		t.Fatal("reverse rewound the app deployment", err)
	}
	reverse.RuntimeSnapshot = intermediate.InputSnapshot
	reverse.RuntimeSnapshot.IntentGeneration = reverse.Intent.Generation
	reverse.RuntimeSnapshot.HostedAddressReservations = nil
	src.Publication = pin("forward", 3)
	src.Prior, src.Inherited = after, forward.InputSnapshot.HostedAddressReservations
	back, staged, err := CompileHostedPathTransition(reverse, src)
	if err != nil || staged {
		t.Fatal("reverse cannot retain the address-only baseline", err)
	}
	if got := payload(back); !reflect.DeepEqual(got.Records, baseline.Records) || !reflect.DeepEqual(got.QueryViews, baseline.QueryViews) || len(got.ReadinessPlan.Probes) != 1 {
		t.Fatal("reverse offered held address or did not restore root")
	}
	// The intermediate and path-only phases are record-isolated, but retaining
	// the same address for the same paths is not an indefinite global freeze.
	intermediate.DNSArtifact.ScopeKey, forward.DNSArtifact.ScopeKey, back.DNSArtifact.ScopeKey = GlobalScopeKey, GlobalScopeKey, GlobalScopeKey
	for _, pair := range [][2]CompileResult{{prior, intermediate}, {intermediate, forward}, {forward, back}, {forward, forward}} {
		if e := ValidateScopedHostedPublicationTransition(pair[0].DNSArtifact, pair[1].DNSArtifact); e != nil {
			t.Fatal("exact path transition", e)
		}
	}
	// Make a real candidate, including the consumer query view and lineage,
	// exactly as an independently authorized TXT update would be captured.
	src.PriorIntent = next.Intent
	next.Intent.DNS = append(slices.Clone(next.Intent.DNS), DNSIntent{Hostname: "note.example.test", Type: "TXT", Values: []string{"new"}, TTL: 60})
	next.Intent.Generation = "independent-txt"
	next.RuntimeSnapshot.IntentGeneration = next.Intent.Generation
	withTXT, _, err := CompileHostedPathTransition(next, src)
	if err != nil {
		t.Fatal(err)
	}
	nextForward := withTXT.DNSArtifact
	nextForward.ScopeKey = GlobalScopeKey
	if e := ValidateScopedHostedPublicationTransition(intermediate.DNSArtifact, nextForward); e == nil {
		t.Fatal("path transaction included an unrelated DNS edit")
	}
	if e := ValidateScopedHostedPublicationTransition(forward.DNSArtifact, nextForward); e != nil {
		t.Fatal("steady reservation blocked an unrelated normal change", e)
	}
}

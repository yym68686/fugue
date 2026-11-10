package dnsroutesource

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func TestPublicationProjectionReuseReappliesCurrentSelectionAndOwnsResults(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, true)
	dns := celldns.Compile(t, req).DNSArtifact
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, req, dns, policies, activations, now)
	if err := VerifySnapshot(snapshot, dns, celldns.Keys(), now); err != nil {
		t.Fatal(err)
	}
	var cache publicationProjectionCache
	first, err := buildVerified(dns, snapshot, &cache)
	want, ordinaryErr := buildVerified(dns, snapshot)
	if err != nil || ordinaryErr != nil || !reflect.DeepEqual(first, want) || cache.misses == 0 {
		t.Fatal("initial pure publication cache differs", err, ordinaryErr)
	}
	misses := cache.misses
	// A fresh lane version changes SelectionDigest but no publication's pure
	// route/cell plan. Two distinct compilers can share this preparation.
	snapshot.Scopes[0].Lanes[0].Version++
	snapshot.SelectionDigest, _ = SelectionDigest(snapshot)
	if err := VerifySnapshot(snapshot, dns, celldns.Keys(), now); err != nil {
		t.Fatal(err)
	}
	got, err := buildVerified(dns, snapshot, &cache)
	want, ordinaryErr = buildVerified(dns, snapshot)
	if err != nil || ordinaryErr != nil || !reflect.DeepEqual(got, want) || cache.misses != misses || cache.hits == 0 {
		t.Fatal("lane-only change rebuilt immutable publications", err, ordinaryErr)
	}
	for _, entry := range cache.entries {
		copy := cloneReadinessPlan(entry.plan)
		for i := range copy.Probes {
			copy.Probes[i].RouteDigest = "caller mutation"
		}
	}
	for _, binding := range got.Bindings {
		binding.ReleaseID = "caller mutation"
	}
	for i := range got.records {
		for j := range got.records[i].Targets {
			got.records[i].Targets[j].ProbeIDs[0] = "caller mutation"
		}
	}
	again, err := buildVerified(dns, snapshot, &cache)
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatal("caller changed shared pure projection", err)
	}
	// Bound a stream of distinct bindings without retaining any source bodies.
	pub := snapshot.Scopes[0].Publications[0]
	var payload struct {
		Source platformconfig.CellDNSPlanSource `json:"cell_dns_source"`
		Policy platformconfig.PolicySnapshot    `json:"policy"`
	}
	raw, _ := json.Marshal(dns.Content)
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	cell := payload.Source.Intent.EdgeTopology.Cells[0].ID
	for i := range 70 {
		pub.Release.FencingToken += int64(i + 1)
		if _, _, err := cache.buildVerified(dns, payload.Source, payload.Policy, pub, cell); err != nil {
			t.Fatal(err)
		}
	}
	if len(cache.entries) > 64 || len(cache.order) > 64 || cache.bytes > 8<<20 {
		t.Fatal("publication cache exceeded retention budget")
	}
}

func TestPublicationProjectionHitStillAppliesFreshHardRestrictions(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, false)
	dns := celldns.Compile(t, req).DNSArtifact
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, req, dns, policies, activations, now)
	if err := VerifySnapshot(snapshot, dns, celldns.Keys(), now); err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Source platformconfig.CellDNSPlanSource `json:"cell_dns_source"`
		Policy platformconfig.PolicySnapshot    `json:"policy"`
	}
	raw, _ := json.Marshal(dns.Content)
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	pub := snapshot.Scopes[0].Publications[0]
	cell := payload.Source.Intent.EdgeTopology.Cells[0].ID
	var cache publicationProjectionCache
	base, _, err := cache.buildVerified(dns, payload.Source, payload.Policy, pub, cell)
	if err != nil || len(base.Records) == 0 || len(base.Records[0].Targets) == 0 {
		t.Fatal("positive source fixture missing", err)
	}
	// A selected publication can tighten policy while an older positive source
	// keeps the same cache identity. The cached base never owns this verdict.
	var current model.PlatformDNSRouteSourcePublication
	raw, _ = json.Marshal(pub)
	if err := json.Unmarshal(raw, &current); err != nil {
		t.Fatal(err)
	}
	var policy platformconfig.PolicySnapshot
	raw, _ = json.Marshal(current.Route.Content["policy"])
	if err := json.Unmarshal(raw, &policy); err != nil {
		t.Fatal(err)
	}
	policy.RouteConstraints = []platformconfig.RoutePolicyConstraint{{ID: "constraint", Hostname: "app.example.test", TenantID: "tenant-a", AppID: "app-a", Enabled: true, RoutePolicy: "direct", ExcludedEdgeIDs: []string{base.Records[0].Targets[0].EdgeID}}}
	current.Route.Content["policy"] = policy
	fresh, err := platformconfig.PrepareDNSRuntimeSourceRestrictions(payload.Source, []model.PlatformDNSRouteSourcePublication{current})
	if err != nil {
		t.Fatal(err)
	}
	plan, _, err := cache.buildVerified(dns, payload.Source, payload.Policy, pub, cell)
	if err != nil || cache.hits != 1 {
		t.Fatal("immutable publication was not reused", err)
	}
	if err := fresh.Restrict(plan, pub); err != nil || len(plan.Records[0].Targets) != 0 || len(plan.Probes) != 0 {
		t.Fatal("cache bypassed fresh exclusion", err)
	}
	again, _, err := cache.buildVerified(dns, payload.Source, payload.Policy, pub, cell)
	if err != nil || !reflect.DeepEqual(again, base) {
		t.Fatal("fresh restriction mutated the shared base", err)
	}
}

func TestCloneReadinessPlanOwnsStaticAndPreviousProbePointers(t *testing.T) {
	plan := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{{StaticEdge: &platformconfig.StaticEdgeDNSProbe{StatusCode: 200}, PreviousAuthority: &platformconfig.DNSPreviousAuthority{RouteDigest: "original"}}}}
	copy := cloneReadinessPlan(plan)
	copy.Probes[0].StaticEdge.StatusCode = 503
	copy.Probes[0].PreviousAuthority.RouteDigest = "changed"
	if plan.Probes[0].StaticEdge.StatusCode != 200 || plan.Probes[0].PreviousAuthority.RouteDigest != "original" {
		t.Fatal("pure probe pointers escaped to mutable caller")
	}
}

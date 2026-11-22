package dnsserver

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

func TestPhysicalCheckpointScanRequiresTwoContiguousExactPasses(t *testing.T) {
	const group = "edge-group-a"
	parent, candidate := dnsServingFixtureWithIntent(t, group, "global", func(r *platformconfig.CompileRequest) {
		r.Policy.DNSReadiness.FactFreshnessSeconds = 120
		route, record := r.Intent.Routes[0], r.Intent.DNS[0]
		route.Hostname, route.AppID = "second.example.test", "second"
		record.Hostname, record.AppID, record.Values = route.Hostname, route.AppID, []string{route.AppID}
		r.Intent.Routes = append(r.Intent.Routes, route)
		r.Intent.DNS = append(r.Intent.DNS[:1], record)
		rule, choice := r.Policy.DNSAnswerRules[0], r.RuntimeSnapshot.DNSSelections[0]
		rule.Hostname, choice.Hostname = route.Hostname, route.Hostname
		r.Policy.DNSAnswerRules = append(r.Policy.DNSAnswerRules, rule)
		r.RuntimeSnapshot.DNSSelections = append(r.RuntimeSnapshot.DNSSelections, choice)
	}, true)
	s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: group, Zone: "example.test",
		BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}, nil)
	p, routeID, err := s.verifyDNSServingRelease(parent, candidate)
	if err != nil || len(p.Plan.Probes) != 2 {
		t.Fatalf("want two independent signed requirements: %v", err)
	}
	epoch := time.Now().UTC()
	second := epoch.Add(30 * time.Second)
	base := epoch.Add(-25 * time.Hour)
	a := candidate.Assignment
	sample := func(i int, start time.Time, observation string) dnsReadinessFact {
		req := p.Plan.Probes[i]
		proof := routeprobe.Proof{Version: "serving", Digest: req.RouteDigest, State: req.State, EdgeID: req.EdgeID, GroupID: req.EdgeGroupID,
			CheckedAt: start.Add(time.Second), ValidUntil: start.Add(100 * time.Second),
			TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash,
				RouteArtifactID: routeID, PolicyDigest: p.Lineage.PolicyDigest, IntentDigest: p.Lineage.IntentDigest,
				InputSnapshotDigest: p.Lineage.InputSnapshotDigest, ReleaseID: candidate.Release.ID, ReleaseChannel: candidate.Release.ReleaseChannel, FencingToken: candidate.Release.FencingToken, ScopeKey: "global"}}
		f := evaluateDNSReadinessFact(req, p.Policy.DNSReadiness, proof, nil, start.Add(2*time.Second))
		f.ObservationID = observation
		return f
	}
	firstFact, secondFact := sample(0, epoch, "first"), sample(1, second, "second")
	missing := func(id string) dnsReadinessFact { return dnsReadinessFact{ProbeID: id, Reason: "not_observed"} }
	rawA := []dnsReadinessFact{firstFact, missing(secondFact.ProbeID)}
	rawB := []dnsReadinessFact{missing(firstFact.ProbeID), secondFact}
	remember := func(raw []dnsReadinessFact) *dnsCheckpointScan {
		return newDNSCheckpointScan(a, parent, candidate, routeID, p, base, epoch, epoch.Add(3*time.Second), raw)
	}
	scan := remember(rawA)
	prior := scan.prior(a, parent, routeID, base, p, second)
	end := second.Add(3 * time.Second)
	got, ok := completeDNSCheckpointScan(p, parent, candidate, routeID, prior, rawB, second, end)
	if !ok || !reflect.DeepEqual(got, []dnsReadinessFact{firstFact, secondFact}) {
		t.Fatalf("exact observation identity/times must be preserved: %v", got)
	}
	if !reflect.DeepEqual(rawB[0], missing(firstFact.ProbeID)) {
		t.Fatal("rewrote scheduler output")
	}
	changed := a
	changed.FencingToken++
	for _, rejected := range []map[string]dnsReadinessFact{
		scan.prior(changed, parent, routeID, base, p, second), scan.prior(a, parent, routeID+"x", base, p, second),
		scan.prior(a, parent, routeID, base.Add(time.Second), p, second), scan.prior(a, parent, routeID, base, p, epoch.Add(3*time.Minute)),
		(*dnsCheckpointScan)(nil).prior(a, parent, routeID, base, p, second),
	} {
		if len(rejected) != 0 {
			t.Fatal("crossed a fence, restart, or expired deadline")
		}
	}
	for _, reason := range []string{"probe_unavailable", "probe_failed", "route_digest_mismatch", "retained_valid_proof"} {
		b := slices.Clone(rawB)
		b[0].Reason = reason
		if _, ok := completeDNSCheckpointScan(p, parent, candidate, routeID, prior, b, second, end); ok {
			t.Fatal("overwrote actual observation", reason)
		}
	}
	for _, change := range []func(*dnsReadinessFact){
		func(f *dnsReadinessFact) { f.Reason = "retained_valid_proof" },
		func(f *dnsReadinessFact) { f.probeObservedAt = time.Time{} },
		func(f *dnsReadinessFact) { f.ObservationID = "" },
		func(f *dnsReadinessFact) { f.Proof.CheckedAt = epoch.Add(-time.Second) },
		func(f *dnsReadinessFact) { f.Proof.ValidUntil = epoch },
		func(f *dnsReadinessFact) {
			binding := *f.Proof.TrafficRelease
			binding.FencingToken++
			f.Proof.TrafficRelease = &binding
		},
	} {
		b := slices.Clone(rawA)
		change(&b[0])
		if remember(b) != nil {
			t.Fatal("remembered nonphysical or out-of-scope fact")
		}
	}
	if remember([]dnsReadinessFact{firstFact, firstFact}) != nil {
		t.Fatal("duplicate ID accepted")
	}
	if _, ok := completeDNSCheckpointScan(p, parent, candidate, routeID, prior, rawB, second, firstFact.Proof.ValidUntil); ok {
		t.Fatal("old proof expired during second sweep")
	}
	// A third pass cannot get the first pass's evidence from the second.
	next := newDNSCheckpointScan(a, parent, candidate, routeID, p, base, second, end, rawB)
	if len(next.prior(a, parent, routeID, base, p, end)) != 1 {
		t.Fatal("second pass carried a previous batch forward")
	}
}

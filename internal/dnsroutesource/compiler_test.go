package dnsroutesource_test

import (
	"maps"
	"reflect"
	"sync"
	"testing"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/testfixture/celldns"
)

func TestCompilerReusesOnlyPureProjectionAndReturnsOwnedPlans(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, true)
	dns := celldns.Compile(t, req).DNSArtifact
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, req, dns, policies, activations, now)
	var compiler dnsroutesource.Compiler
	first, err := compiler.Build(dns, snapshot, celldns.Keys(), now)
	if err != nil {
		t.Fatal(err)
	}
	want, err := dnsroutesource.Build(dns, snapshot, celldns.Keys(), now)
	if err != nil || !reflect.DeepEqual(first, want) {
		t.Fatal("cached compilation differs from ordinary builder", err)
	}
	for id, p := range first.Probes {
		p.RouteDigest = "mutated"
		if p.PreviousAuthority != nil {
			p.PreviousAuthority.RouteDigest = "mutated"
		}
		first.Probes[id] = p
	}
	for _, b := range first.Bindings {
		b.ReleaseID = "mutated"
	}
	chosen := first.Choose(func(string) bool { return true })
	plan, err := first.Replay(chosen)
	if err != nil {
		t.Fatal(err)
	}
	for i := range plan.Records {
		if plan.Records[i].MinDistinctDomains != nil {
			plan.Records[i].MinDistinctDomains["host"] = 99
		}
		for j := range plan.Records[i].Targets {
			plan.Records[i].Targets[j].ProbeIDs[0] = "mutated"
		}
	}
	snapshot.ObservedAt = now.Add(time.Second)
	next, err := compiler.Build(dns, snapshot, celldns.Keys(), now.Add(time.Second))
	want.Snapshot = snapshot
	if err != nil || !reflect.DeepEqual(next, want) {
		t.Fatal("caller mutation contaminated cache or observation clock was cached", err)
	}
	// A changed release must recompile, even when the DNS configuration is stable.
	changed := clone(snapshot)
	pub := &changed.Scopes[0].Publications[0]
	pub.Release.ID += "-next"
	pub.Release.FencingToken++
	changed.Scopes[0].Lanes[0].ActiveReleaseID = pub.Release.ID
	changed.Scopes[0].Lanes[0].FencingToken = pub.Release.FencingToken
	changed.Scopes[0].Lanes[0].Version++
	changed.SelectionDigest, _ = dnsroutesource.SelectionDigest(changed)
	got, err := compiler.Build(dns, changed, celldns.Keys(), now.Add(time.Second))
	want, ordinaryErr := dnsroutesource.Build(dns, changed, celldns.Keys(), now.Add(time.Second))
	if err != nil || ordinaryErr != nil || !reflect.DeepEqual(got, want) || reflect.DeepEqual(got.Bindings, next.Bindings) {
		t.Fatal("release change did not invalidate pure projection", err, ordinaryErr)
	}
}

func TestWarmCompilerStillRejectsRevocationCorruptionAndLedgerMismatch(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, true)
	dns := celldns.Compile(t, req).DNSArtifact
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, req, dns, policies, activations, now)
	var compiler dnsroutesource.Compiler
	if _, err := compiler.Build(dns, snapshot, celldns.Keys(), now); err != nil {
		t.Fatal(err)
	}
	keys := celldns.Keys()
	keys.RevokedKeyIDs = map[string]struct{}{keys.PrimaryKeyID: {}}
	if _, err := compiler.Build(dns, snapshot, keys, now); err == nil {
		t.Fatal("cache bypassed key revocation")
	}
	bad := clone(snapshot)
	bad.Scopes[0].Publications[0].Route.Content["schema_version"] = "corrupted"
	if _, err := compiler.Build(dns, bad, celldns.Keys(), now); err == nil {
		t.Fatal("cache trusted claimed content hash instead of actual bytes")
	}
	badDNS := dns
	badDNS.Content = maps.Clone(dns.Content)
	badDNS.Content["schema_version"] = "corrupted"
	if _, err := compiler.Build(badDNS, snapshot, celldns.Keys(), now); err == nil {
		t.Fatal("cache trusted DNS claimed digest instead of actual content")
	}
	bad = clone(snapshot)
	bad.Scopes[0].Lanes[0].FencingToken++
	if _, err := compiler.Build(dns, bad, celldns.Keys(), now); err == nil {
		t.Fatal("cache bypassed mutable ledger binding")
	}
	beforeRelease := snapshot.Scopes[0].Publications[0].Release.ReleasedAt.Add(-time.Second)
	if _, err := compiler.Build(dns, snapshot, celldns.Keys(), beforeRelease); err == nil {
		t.Fatal("cache bypassed publication time validation")
	}
}

func TestCompilerConcurrentCallersOwnTheirPlans(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, true)
	dns := celldns.Compile(t, req).DNSArtifact
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, req, dns, policies, activations, now)
	var compiler dnsroutesource.Compiler
	var callers sync.WaitGroup
	for range 8 {
		callers.Go(func() {
			plans, err := compiler.Build(dns, snapshot, celldns.Keys(), now)
			if err != nil {
				t.Error(err)
				return
			}
			for id, probe := range plans.Probes {
				probe.RouteDigest = "caller-owned"
				plans.Probes[id] = probe
			}
		})
	}
	callers.Wait()
	got, err := compiler.Build(dns, snapshot, celldns.Keys(), now)
	want, ordinaryErr := dnsroutesource.Build(dns, snapshot, celldns.Keys(), now)
	if err != nil || ordinaryErr != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("concurrent caller contaminated projection", err, ordinaryErr)
	}
}

func BenchmarkSourceCompiler(b *testing.B) {
	req, policies, activations := celldns.SourceAuthorizedRequest(b, true)
	dns := celldns.Compile(b, req).DNSArtifact
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(b, req, dns, policies, activations, now)
	for _, cached := range []bool{false, true} {
		name := "uncached"
		if cached {
			name = "warm"
		}
		b.Run(name, func(b *testing.B) {
			var compiler dnsroutesource.Compiler
			if _, err := compiler.Build(dns, snapshot, celldns.Keys(), now); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var err error
				if cached {
					_, err = compiler.Build(dns, snapshot, celldns.Keys(), now)
				} else {
					_, err = dnsroutesource.Build(dns, snapshot, celldns.Keys(), now)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

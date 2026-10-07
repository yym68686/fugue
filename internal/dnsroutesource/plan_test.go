package dnsroutesource_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/routeprobe"
	"fugue/internal/testfixture/celldns"
)

func TestSourcePlansPreserveDNSOwnershipAndFollowAuthorizedReleases(t *testing.T) {
	for _, transition := range []bool{false, true} {
		t.Run(map[bool]string{false: "cells", true: "transition"}[transition], func(t *testing.T) {
			req, policies, activations := celldns.SourceAuthorizedRequest(t, transition)
			compiled := celldns.Compile(t, req)
			now := time.Now().UTC()
			snapshot := celldns.SourceSnapshot(t, req, compiled.DNSArtifact, policies, activations, now)
			original, _ := json.Marshal(snapshot)
			plans, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, celldns.Keys(), now)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(snapshot)
			if string(original) != string(after) {
				t.Fatal("projection mutated signed source")
			}
			chosen := plans.Choose(func(string) bool { return true })
			plan, err := plans.Replay(chosen)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Records) == 0 || len(plan.Probes) == 0 {
				t.Fatal("lost requirements")
			}
			for _, r := range plan.Records {
				if r.MinimumHealthyEdges < 2 || r.MinDistinctCells < 2 || len(r.Targets) != 2 {
					t.Fatal("DNS ownership/quorum broadened", r)
				}
			}
			for _, p := range plan.Probes {
				proof := routeprobe.Proof{EdgeID: p.EdgeID, GroupID: p.EdgeGroupID, Digest: p.RouteDigest, TrafficRelease: plans.Bindings[p.CellPublicationDigest]}
				if !plans.Matches(p, proof) {
					t.Fatal("exact authorized proof rejected")
				}
				proof.EdgeID = "foreign"
				if plans.Matches(p, proof) {
					t.Fatal("foreign edge admitted")
				}
			}
			// A new full fence from the same approved producer requires neither a DNS
			// config edit nor a DNS code release, but the old fence no longer matches.
			next := clone(snapshot)
			for i := range next.Scopes {
				scope := &next.Scopes[i]
				pub := &scope.Publications[0]
				pub.Release.FencingToken++
				pub.Release.ID += "-next"
				scope.Lanes[0].Version++
				scope.Lanes[0].FencingToken = pub.Release.FencingToken
				scope.Lanes[0].ActiveReleaseID = pub.Release.ID
			}
			next.SelectionDigest, _ = dnsroutesource.SelectionDigest(next)
			newer, err := dnsroutesource.Build(compiled.DNSArtifact, next, celldns.Keys(), now)
			if err != nil {
				t.Fatal(err)
			}
			if !dnsroutesource.Monotonic(snapshot, next) || dnsroutesource.Monotonic(next, snapshot) {
				t.Fatal("fence monotonicity violated")
			}
			if reflect.DeepEqual(plans.Bindings, newer.Bindings) {
				t.Fatal("new release not bound")
			}
			forged := clone(next)
			forged.Scopes[0].Publications[0].ProducerPolicy.Provenance.Signature = "forged"
			forged.SelectionDigest, _ = dnsroutesource.SelectionDigest(forged)
			if _, err := dnsroutesource.Build(compiled.DNSArtifact, forged, celldns.Keys(), now); err == nil {
				t.Fatal("forged policy accepted")
			}
			changed := *chosen
			changed.Selections = map[string]string{"foreign": "foreign"}
			if _, err := plans.Replay(&changed); err == nil {
				t.Fatal("unapproved target selected")
			}
			for _, r := range plan.Records {
				if !platformconfig.DNSReadinessQuorum(r, func(platformconfig.DNSReadinessTarget) bool { return true }) {
					t.Fatal("physical quorum not preserved")
				}
			}
		})
	}
}
func clone(in model.PlatformDNSRouteSourceSnapshot) model.PlatformDNSRouteSourceSnapshot {
	raw, _ := json.Marshal(in)
	var out model.PlatformDNSRouteSourceSnapshot
	json.Unmarshal(raw, &out)
	return out
}

func TestSourceLKGRequiresVerifiedUnexpiredFullAndCoherentTarget(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, false)
	compiled := celldns.Compile(t, req)
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, req, compiled.DNSArtifact, policies, activations, now)
	scope := &snapshot.Scopes[0]
	old := scope.Publications[0]
	a := old.Parent
	old.Selections = []string{"lkg"}
	old.Release.Status = model.PlatformArtifactReleaseStatusSuperseded
	old.Release.VerificationState = model.PlatformArtifactVerificationStateVerified
	old.Release.VerifiedLKGGeneration = a.Generation
	lkg, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{ID: "source-lkg", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, Scope: a.Scope, ScopeKey: a.ScopeKey, SchemaVersion: a.SchemaVersion, Generation: a.Generation, GenerationSequence: a.GenerationSequence, ContentHash: a.ContentHash, ArtifactProvenance: a.Provenance, VerifiedByReleaseID: old.Release.ID, VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64), ExpiresAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now}, celldns.Keys())
	if err != nil {
		t.Fatal(err)
	}
	old.LKG = &lkg
	next := scope.Publications[0]
	next.Release.ID += "-next"
	next.Release.FencingToken++
	scope.Publications = []model.PlatformDNSRouteSourcePublication{next, old}
	scope.Lanes[0].ActiveReleaseID = next.Release.ID
	scope.Lanes[0].FencingToken = next.Release.FencingToken
	scope.Lanes[0].Version++
	snapshot.SelectionDigest, _ = dnsroutesource.SelectionDigest(snapshot)
	plans, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, celldns.Keys(), now)
	if err != nil {
		t.Fatal(err)
	}
	chosen := plans.Choose(func(id string) bool {
		p := plans.Probes[id]
		return plans.Bindings[p.CellPublicationDigest].ReleaseID != next.Release.ID
	})
	plan, err := plans.Replay(chosen)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range plan.Probes {
		if p.EdgeID == "edge-a" && plans.Bindings[p.CellPublicationDigest].ReleaseID != old.Release.ID {
			t.Fatal("verified LKG not retained while candidate applies")
		}
	}
	if _, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, celldns.Keys(), now.Add(2*time.Minute)); err == nil {
		t.Fatal("expired LKG revived")
	}
	bad := clone(snapshot)
	bad.Scopes[0].Publications[1].Release.VerificationState = model.PlatformArtifactVerificationStateFailed
	bad.SelectionDigest, _ = dnsroutesource.SelectionDigest(bad)
	if _, err := dnsroutesource.Build(compiled.DNSArtifact, bad, celldns.Keys(), now); err == nil {
		t.Fatal("failed candidate used as fallback")
	}
}

func TestApplicationGenerationChangeUsesNewAuthorizedRouteDigest(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, false)
	compiled := celldns.Compile(t, req)
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, req, compiled.DNSArtifact, policies, activations, now)
	before, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, celldns.Keys(), now)
	if err != nil {
		t.Fatal(err)
	}
	next := clone(snapshot)
	pub := &next.Scopes[0].Publications[0]
	var routes []platformconfig.CompiledRoute
	raw, _ := json.Marshal(pub.Route.Content["routes"])
	json.Unmarshal(raw, &routes)
	for i := range routes {
		routes[i].DeploymentGeneration += "-next"
	}
	pub.Route.Content["routes"] = routes
	pub.Route = celldns.Sign(t, pub.Route, pub.Route.ID+"-deployment", pub.Route.GenerationSequence+1)
	ids := pub.Parent.Content["artifact_ids"].([]any)
	for i, id := range ids {
		if id == snapshot.Scopes[0].Publications[0].Route.ID {
			ids[i] = pub.Route.ID
		}
	}
	pub.Parent = celldns.Sign(t, pub.Parent, pub.Parent.ID+"-deployment", pub.Parent.GenerationSequence+1)
	pub.Release.ID += "-deployment"
	pub.Release.ArtifactID = pub.Parent.ID
	pub.Release.FencingToken++
	next.Scopes[0].Lanes[0].Version++
	next.Scopes[0].Lanes[0].FencingToken = pub.Release.FencingToken
	next.Scopes[0].Lanes[0].ActiveReleaseID = pub.Release.ID
	next.SelectionDigest, _ = dnsroutesource.SelectionDigest(next)
	after, err := dnsroutesource.Build(compiled.DNSArtifact, next, celldns.Keys(), now)
	if err != nil {
		t.Fatal(err)
	}
	changed := false
	for _, p := range before.Probes {
		if p.EdgeID != "edge-a" {
			continue
		}
		for _, n := range after.Probes {
			if n.EdgeID == p.EdgeID && n.Hostname == p.Hostname && n.Path == p.Path {
				changed = changed || n.RouteDigest != p.RouteDigest
				old := routeprobe.Proof{EdgeID: p.EdgeID, GroupID: p.EdgeGroupID, Digest: p.RouteDigest, TrafficRelease: before.Bindings[p.CellPublicationDigest]}
				if after.Matches(n, old) {
					t.Fatal("old deployment proof accepted for new generation")
				}
			}
		}
	}
	if !changed {
		t.Fatal("application deployment generation did not change required digest")
	}
}

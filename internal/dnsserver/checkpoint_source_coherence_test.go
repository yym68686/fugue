package dnsserver

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func TestRetainedProofKeepsNewSourcePlanAndOriginalCheckpointDeadline(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, true)
	compiled := celldns.Compile(t, req)
	keys := celldns.Keys()
	now := time.Now().UTC()
	s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: platformconfig.AuthorityCellScope("cell-dns"), Zone: "example.test", BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
	release := model.PlatformArtifactRelease{ID: "dns-full", ArtifactID: compiled.ReleaseArtifact.ID, ArtifactKind: compiled.ReleaseArtifact.ArtifactKind, ScopeKey: compiled.ReleaseArtifact.ScopeKey, Generation: compiled.ReleaseArtifact.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now}
	assignment := model.PlatformConsumerAssignment{ArtifactID: compiled.DNSArtifact.ID, ArtifactKind: compiled.DNSArtifact.ArtifactKind, ArtifactReleaseID: release.ID, ReleaseSetID: compiled.ReleaseArtifact.ID, ExpectedConsumerSetID: "dns-expected", Revision: 1, ScopeKey: compiled.DNSArtifact.ScopeKey, ExpectedGeneration: compiled.DNSArtifact.Generation, GenerationSequence: compiled.DNSArtifact.GenerationSequence, ContentHash: compiled.DNSArtifact.ContentHash, FencingToken: 1, ReleaseChannel: "full"}
	candidate := dnsPlatformCandidate{Artifact: compiled.DNSArtifact, Assignment: assignment, Release: release}
	payload, _, err := s.verifyDNSServingRelease(compiled.ReleaseArtifact, candidate)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := celldns.SourceSnapshot(t, req, compiled.DNSArtifact, policies, activations, now)
	choose := func(snapshot model.PlatformDNSRouteSourceSnapshot) (*dnsroutesource.Context, *platformconfig.DNSReadinessPlan) {
		plans, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, keys, now)
		if err != nil {
			t.Fatal(err)
		}
		context := plans.Choose(func(id string) bool {
			p := plans.Probes[id]
			return (p.PreviousAuthority != nil) == (p.EdgeID == "edge-a")
		})
		plan, err := plans.Replay(context)
		if err != nil {
			t.Fatal(err)
		}
		return context, plan
	}
	oldContext, oldPlan := choose(snapshot)
	var updated model.PlatformDNSRouteSourceSnapshot
	raw, _ := json.Marshal(snapshot)
	json.Unmarshal(raw, &updated)
	for i := range updated.Scopes {
		scope := &updated.Scopes[i]
		if scope.ScopeKey != "global" {
			continue
		}
		pub := &scope.Publications[0]
		pub.Release.ID += "-renewed"
		pub.Release.FencingToken++
		scope.Lanes[0].ActiveReleaseID = pub.Release.ID
		scope.Lanes[0].FencingToken = pub.Release.FencingToken
		scope.Lanes[0].Version++
	}
	updated.SelectionDigest, _ = dnsroutesource.SelectionDigest(updated)
	newContext, newPlan := choose(updated)
	if reflect.DeepEqual(oldPlan, newPlan) {
		t.Fatal("fixture did not change effective plan")
	}
	oldCheckpoint := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: "dns-a", GroupID: "cell-dns", Parent: compiled.ReleaseArtifact, Candidate: candidate, AppliedAt: now.Add(-time.Minute), Positive: true, RouteSources: oldContext}
	old := &dnsServingState{record: oldCheckpoint, payload: payload}
	old.payload.Plan = oldPlan
	next := oldCheckpoint
	next.RouteSources = newContext
	next.AppliedAt = now
	retained := preserveDNSCheckpointAge(next, old, []dnsReadinessFact{{Reason: "retained_valid_proof"}})
	restored, _, err := s.verifiedDNSCheckpointPayload(retained)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.Plan, newPlan) {
		t.Fatal("checkpoint replays old source plan while runtime serves new plan")
	}
	if !retained.AppliedAt.Equal(oldCheckpoint.AppliedAt) {
		t.Fatal("retained proof renewed checkpoint deadline")
	}
	if !reflect.DeepEqual(retained.RouteSources, newContext) {
		t.Fatal("current source selection was replaced")
	}
	if got := preserveDNSCheckpointAge(next, old, nil); !got.AppliedAt.Equal(now) {
		t.Fatal("fresh observation failed to advance checkpoint")
	}
}

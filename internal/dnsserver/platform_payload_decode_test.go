package dnsserver

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func sharedDNSPayloadFixture(t testing.TB, records int) (*Service, model.PlatformArtifact, dnsPlatformCandidate) {
	req := celldns.TransitionRequest(t)
	for i := 0; i < records; i++ {
		req.Intent.DNS = append(req.Intent.DNS, platformconfig.DNSIntent{Hostname: fmt.Sprintf("static-%d.example.test", i), Type: "TXT", TTL: 60, Values: []string{"synthetic-value"}})
	}
	c := celldns.Compile(t, req)
	keys := celldns.Keys()
	s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: platformconfig.AuthorityCellScope("cell-dns"), Zone: "example.test", BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
	release := model.PlatformArtifactRelease{ID: "dns-full", ArtifactID: c.ReleaseArtifact.ID, ArtifactKind: c.ReleaseArtifact.ArtifactKind, ScopeKey: c.ReleaseArtifact.ScopeKey, Generation: c.ReleaseArtifact.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: time.Now()}
	assignment := model.PlatformConsumerAssignment{ArtifactID: c.DNSArtifact.ID, ArtifactKind: c.DNSArtifact.ArtifactKind, ArtifactReleaseID: release.ID, ReleaseSetID: c.ReleaseArtifact.ID, ExpectedConsumerSetID: "dns-expected", Revision: 1, ScopeKey: c.DNSArtifact.ScopeKey, ExpectedGeneration: c.DNSArtifact.Generation, GenerationSequence: c.DNSArtifact.GenerationSequence, ContentHash: c.DNSArtifact.ContentHash, FencingToken: 1, ReleaseChannel: "full"}
	candidate := dnsPlatformCandidate{Artifact: c.DNSArtifact, Assignment: assignment, Release: release}
	return s, c.ReleaseArtifact, candidate
}

func TestSharedDNSPayloadDecodeKeepsStrictEmbeddedFields(t *testing.T) {
	s, parent, candidate := sharedDNSPayloadFixture(t, 0)
	if _, _, err := s.verifyDNSServingRelease(parent, candidate); err != nil {
		t.Fatal(err)
	}
	// The historical typed candidate decoder rejected unknown nested envelope
	// fields. Reusing its result must retain that rejection after pure cache hits.
	candidate.Artifact.Content["previous_traffic_publication"].(map[string]any)["route"].(map[string]any)["unknown_envelope_field"] = true
	candidate.Artifact = celldns.Sign(t, candidate.Artifact, candidate.Artifact.ID, 1)
	candidate.Assignment.ContentHash = candidate.Artifact.ContentHash
	if _, _, err := s.verifyDNSServingRelease(parent, candidate); err == nil || !strings.Contains(err.Error(), "schema invalid") {
		t.Fatal("strict embedded decoder changed", err)
	}
}

func TestSharedDNSPayloadDecodeRechecksEmbeddedTrust(t *testing.T) {
	for _, change := range []string{"revoked", "cell signature", "previous signature", "previous declaration"} {
		t.Run(change, func(t *testing.T) {
			s, parent, candidate := sharedDNSPayloadFixture(t, 0)
			if _, _, err := s.verifyDNSServingRelease(parent, candidate); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "revoked":
				s.Config.BundleRevokedKeyIDs = []string{s.Config.BundleSigningKeyID}
			case "cell signature":
				candidate.Artifact.Content["cell_route_publications"].([]any)[0].(map[string]any)["route"].(map[string]any)["provenance"].(map[string]any)["signature"] = "forged"
			case "previous signature":
				candidate.Artifact.Content["previous_traffic_publication"].(map[string]any)["route"].(map[string]any)["provenance"].(map[string]any)["signature"] = "forged"
			case "previous declaration":
				delete(candidate.Artifact.Content, "previous_traffic_publication")
			}
			if change != "revoked" {
				candidate.Artifact = celldns.Sign(t, candidate.Artifact, candidate.Artifact.ID, 1)
				candidate.Assignment.ContentHash = candidate.Artifact.ContentHash
			}
			if _, _, err := s.verifyDNSServingRelease(parent, candidate); err == nil {
				t.Fatal("accepted changed embedded authority after a warm validation")
			}
		})
	}
}

func TestSameCallDNSReleaseValidationPreservesBinding(t *testing.T) {
	s, parent, candidate := sharedDNSPayloadFixture(t, 0)
	candidate.Assignment.ReleaseChannel, candidate.Release.ReleaseChannel = "shadow", "shadow"
	_, payload, err := s.decodePlatformDNSCandidate(candidate, candidate.Assignment)
	if err != nil {
		t.Fatal(err)
	}
	want, wantRoute, err := s.verifyDNSRelease(parent, candidate, false)
	if err != nil {
		t.Fatal(err)
	}
	got, route, err := s.verifyDecodedDNSRelease(parent, candidate, false, payload)
	if err != nil || route != wantRoute || !reflect.DeepEqual(got, want) {
		t.Fatal("same-call validation changed release semantics", err)
	}
	parent.Provenance.Signature = "forged"
	if _, _, err = s.verifyDecodedDNSRelease(parent, candidate, false, payload); err == nil {
		t.Fatal("decoded child bypassed parent signature")
	}
}

func BenchmarkDNSVerifiedPayloadDecode(b *testing.B) {
	s, parent, candidate := sharedDNSPayloadFixture(b, 512)
	if _, _, err := s.verifyDNSServingRelease(parent, candidate); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := s.verifyDNSServingRelease(parent, candidate); err != nil {
			b.Fatal(err)
		}
	}
}

func TestDNSCheckpointReturnsExactVerifiedPayload(t *testing.T) {
	s, parent, candidate := sharedDNSPayloadFixture(t, 0)
	record := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: s.Config.DNSNodeID, GroupID: s.Config.EdgeGroupID, Parent: parent, Candidate: candidate, AppliedAt: time.Now().UTC(), Positive: true}
	if err := s.signDNSCheckpoint(&record); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, got, route, err := s.decodeDNSCheckpointPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	want, wantRoute, err := s.verifiedDNSCheckpointPayload(decoded)
	if err != nil || route != wantRoute || !reflect.DeepEqual(got, want) {
		t.Fatal("checkpoint payload differs after content compaction", err)
	}
	s.Config.BundleRevokedKeyIDs = []string{s.Config.BundleSigningKeyID}
	if _, _, _, err := s.decodeDNSCheckpointPayload(raw); err == nil {
		t.Fatal("checkpoint decode reused revoked trust")
	}
}

func TestDNSQueryReusesOnlyTransientBoundPayload(t *testing.T) {
	view, plan, policy, facts, _ := queryExecutionFixture()
	s := &Service{Config: config.DNSConfig{DNSNodeID: view.NodeID, EdgeGroupID: view.EdgeGroupID}}
	payload := dnsServingPayload{Queries: []platformconfig.DNSQueryView{view}, Plan: &plan, Policy: platformconfig.PolicySnapshot{DNSReadiness: &policy}}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]any
	if err = json.Unmarshal(raw, &content); err != nil {
		t.Fatal(err)
	}
	candidate := dnsPlatformCandidate{Artifact: model.PlatformArtifact{ID: "artifact", ContentHash: "digest", Content: content}}
	a := model.PlatformConsumerAssignment{ReleaseSetID: "release", ExpectedConsumerSetID: "expected", FencingToken: 7}
	r := dnsReadinessReceipt{ArtifactID: "artifact", ArtifactDigest: "digest", ReleaseSetID: a.ReleaseSetID, ExpectedConsumerSetID: a.ExpectedConsumerSetID, FencingToken: a.FencingToken, NodeID: view.NodeID, Facts: facts}
	want, err := s.evaluatePlatformDNSQueries(candidate, a, &r)
	if err != nil {
		t.Fatal(err)
	}
	r.payload = &payload
	got, err := s.evaluatePlatformDNSQueries(candidate, a, &r)
	if err != nil || got.Status.ViewDigest != want.Status.ViewDigest || got.Status.Questions != want.Status.Questions || got.Status.Answers != want.Status.Answers || !reflect.DeepEqual(got.Zones, want.Zones) {
		t.Fatal("transient payload changed query behavior", err)
	}
	r.FencingToken++
	if _, err = s.evaluatePlatformDNSQueries(candidate, a, &r); err == nil {
		t.Fatal("transient payload bypassed receipt fence")
	}
}

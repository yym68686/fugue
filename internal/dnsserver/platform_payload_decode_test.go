package dnsserver

import (
	"fmt"
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

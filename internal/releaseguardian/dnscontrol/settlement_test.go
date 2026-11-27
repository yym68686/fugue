package dnscontrol

import (
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/productioncontrol"
)

func TestServingCheckpointSeparatesSelectionFromConvergence(t *testing.T) {
	now := time.Now().UTC()
	s, _, _, _ := testSources(t, now)
	approved, _, err := ValidateSources(s, now)
	if err != nil {
		t.Fatal(err)
	}
	old := servingSelection{s.Producer, s.Full, s.LKG}
	check, dnsID, err := verifyServingCheckpoint(old, now)
	if err != nil || dnsID != "dns-child" {
		t.Fatalf("verified predecessor: %s %v", dnsID, err)
	}
	if outcome, err := checkpointOutcome(check, old, approved); err != nil || outcome != productioncontrol.NotApplied {
		t.Fatalf("unchanged predecessor: %s %v", outcome, err)
	}
	newProducer := s.Producer
	newProducer.Artifact = s.NextProducer
	newProducer.Release.ID = "policy-after"
	newProducer.Release.ArtifactID = s.NextProducer.ID
	newProducer.Release.Generation = s.NextProducer.Generation
	newProducer.Release.FencingToken++
	latest := servingSelection{newProducer, s.Full, s.LKG}
	if _, _, err := verifyServingCheckpoint(latest, now); err == nil {
		t.Fatal("selected policy with an unchanged Full was treated as converged")
	}
	latest.full.Artifact = testArtifact(t, "verified-full-2", model.PlatformArtifactKindReleaseSet, "global", "full-2", 7,
		platformconfig.ReleaseSet{Scope: "global", Generation: "full-2", ArtifactIDs: []string{"route-child-2", "dns-child-2", "caddy-child-2"}, ArtifactKinds: platformconfig.PublicationArtifactKinds("")})
	latest.full.Artifact.Metadata = map[string]string{
		platformproducer.PolicyReleaseMetadata: newProducer.Release.ID, platformproducer.DNSPolicyIDMetadata: s.NextDNS.ID,
		platformproducer.DNSPolicyDigestMetadata: s.NextDNS.ContentHash, platformproducer.StaticIntentIDMetadata: s.Static.ID,
		platformproducer.StaticIntentDigestMetadata: s.Static.ContentHash,
	}
	latest.full.Release.ID = "full-release-2"
	latest.full.Release.FencingToken++
	latest.full.Release.ArtifactID = latest.full.Artifact.ID
	latest.full.Release.Generation = latest.full.Artifact.Generation
	latest.full.Release.VerifiedLKGGeneration = latest.full.Artifact.Generation
	latest.lkg.ID = "lkg-b"
	latest.lkg.ArtifactID, latest.lkg.Generation = latest.full.Artifact.ID, latest.full.Artifact.Generation
	latest.lkg.ContentHash, latest.lkg.GenerationSequence = latest.full.Artifact.ContentHash, latest.full.Artifact.GenerationSequence
	latest.lkg.ArtifactProvenance, latest.lkg.VerifiedByReleaseID = latest.full.Artifact.Provenance, latest.full.Release.ID
	check, dnsID, err = verifyServingCheckpoint(latest, now)
	if err != nil || dnsID != "dns-child-2" {
		t.Fatalf("missing successor DNS member: %s %v", dnsID, err)
	}
	if outcome, err := checkpointOutcome(check, latest, approved); err != nil || outcome != productioncontrol.Applied || check.Version().Digest != s.NextProducer.ContentHash {
		t.Fatalf("did not bind exact signed change: %s %v", outcome, err)
	}
	latest.lkg.ExpiresAt = now
	if _, _, err := verifyServingCheckpoint(latest, now); err == nil {
		t.Fatal("expired successor LKG treated as serving evidence")
	}
}

func TestServingEvidenceDoesNotAcceptCheckpointAlone(t *testing.T) {
	checkpoint, _ := platformconfig.Digest("checkpoint")
	coverage, _ := platformconfig.Digest("listener coverage")
	answers, _ := platformconfig.Digest("independent answers")
	if _, _, err := EncodeServingEvidence(ServingEvidence{CheckpointDigest: checkpoint}); err == nil {
		t.Fatal("checkpoint alone was sufficient")
	}
	if _, _, err := EncodeServingEvidence(ServingEvidence{CheckpointDigest: checkpoint, AuthoritativeDNSDigest: answers}); err == nil {
		t.Fatal("missing listener coverage was accepted")
	}
	if _, _, err := EncodeServingEvidence(ServingEvidence{checkpoint, coverage, checkpoint}); err == nil {
		t.Fatal("checkpoint was accepted as authoritative DNS report")
	}
	if raw, digest, err := EncodeServingEvidence(ServingEvidence{checkpoint, coverage, answers}); err != nil || len(raw) == 0 || digest == checkpoint || digest == coverage || digest == answers {
		t.Fatalf("could not bind distinct observation records: %s %v", digest, err)
	}
}

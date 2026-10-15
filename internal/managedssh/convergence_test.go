package managedssh

import (
	"encoding/json"
	"testing"
	"time"
)

func TestKnownPreviousArtifactDoesNotVerifyDesiredAndFailsClosed(test *testing.T) {
	configuration, _, _ := testConfiguration(test)
	now := time.Now().UTC()
	configuration.ValidUntil = now.Add(5 * time.Minute)
	configuration.Keys = []KeyGrant{{ID: "key", PublicKey: "public", ExpiresAt: configuration.ValidUntil}}
	old := Artifact{Digest: "old", Configuration: configuration}
	slot := ServingSlot{Artifact: old}
	evidence := CandidateEvidence{ArtifactDigest: old.Digest, IdentityID: configuration.Identity.ID, Fingerprint: configuration.Identity.Fingerprint, ServerPodUID: "server", TargetPodUID: configuration.Target.PodUID, TargetContainerID: configuration.Target.ContainerID, KeyExchangeVerified: true, RuntimeVerified: true, ObservedAt: now, ExpiresAt: now.Add(time.Minute)}
	if reason := slot.AcceptEvidence(evidence, now); reason != "" {
		test.Fatal(reason)
	}
	desired := old
	desired.Digest, desired.Configuration.Generation = "new", old.Configuration.Generation+1
	slot.BeginApply(desired, now)
	if !slot.Serving(now) || slot.Verified(now) || slot.PublicStatus(now).ConvergenceState != "pending_apply" {
		test.Fatal("old receipt verified new publication")
	}
	for _, scenario := range []struct {
		name, reason string
		mutate       func(*ServingSlot)
	}{
		{"unknown digest", "unknown_loaded_digest", func(s *ServingSlot) { s.Evidence.ArtifactDigest = "unknown" }},
		{"identity", "host_identity_mismatch", func(s *ServingSlot) { s.Evidence.IdentityID = "foreign" }},
		{"target", "target_identity_mismatch", func(s *ServingSlot) { s.Evidence.TargetPodUID = "foreign" }},
		{"revoked key", "loaded_authorization_revoked_or_expired", func(s *ServingSlot) { s.Artifact.Configuration.Keys = nil }},
		{"expired key", "loaded_authorization_revoked_or_expired", func(s *ServingSlot) { s.LoadedArtifact.Configuration.Keys[0].ExpiresAt = now }},
		{"expired desired authorization", "loaded_authorization_revoked_or_expired", func(s *ServingSlot) { s.Artifact.Configuration.ValidUntil = now }},
		{"expired loaded authorization", "observation_exceeds_authorization", func(s *ServingSlot) { s.LoadedArtifact.Configuration.ValidUntil = now }},
		{"expired receipt", "observation_expired", func(s *ServingSlot) { s.Evidence.ExpiresAt = now }},
		{"deadline", "apply_deadline_exceeded", func(s *ServingSlot) { s.PendingApply.Deadline = now }},
		{"tightened policy", "loaded_authorization_revoked_or_expired", func(s *ServingSlot) { s.Artifact.Configuration.AllowPTY = !s.Artifact.Configuration.AllowPTY }},
		{"missing observation", "evidence_missing", func(s *ServingSlot) { s.Evidence = nil }},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			raw, _ := json.Marshal(slot)
			var copy ServingSlot
			if err := json.Unmarshal(raw, &copy); err != nil {
				test.Fatal(err)
			}
			scenario.mutate(&copy)
			if copy.ServingFailure(now) != scenario.reason || copy.Serving(now) || copy.Verified(now) {
				test.Fatal("unsafe old artifact accepted", copy.ServingFailure(now))
			}
			if copy.LastValidEvidence == nil {
				test.Fatal("historical evidence lost")
			}
		})
	}
	// Even a late exact receipt can recover, but it must prove the desired digest.
	evidence.ArtifactDigest = desired.Digest
	if reason := slot.AcceptEvidence(evidence, now); reason != "" || !slot.Verified(now) || slot.PendingApply != nil {
		test.Fatal("exact confirmation did not complete", reason)
	}
}

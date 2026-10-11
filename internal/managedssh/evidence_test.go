package managedssh

import (
	"testing"
	"time"
)

func TestVerificationFailureIdentifiesExactRejectedEvidence(test *testing.T) {
	configuration, _, _ := testConfiguration(test)
	now := time.Now().UTC()
	slot := ServingSlot{Artifact: Artifact{Digest: "expected", Configuration: configuration}}
	evidence := CandidateEvidence{ArtifactDigest: "expected", IdentityID: configuration.Identity.ID, Fingerprint: configuration.Identity.Fingerprint, ServerPodUID: "server", TargetPodUID: configuration.Target.PodUID, TargetContainerID: configuration.Target.ContainerID, KeyExchangeVerified: true, RuntimeVerified: true, ObservedAt: now, ExpiresAt: now.Add(time.Minute)}
	slot.Evidence = &evidence
	if !slot.Verified(now) || slot.VerificationFailure(now) != "" {
		test.Fatal("valid evidence rejected")
	}
	for _, scenario := range []struct {
		reason string
		mutate func(*ServingSlot)
	}{
		{"evidence_missing", func(value *ServingSlot) { value.Evidence = nil }},
		{"artifact_digest_mismatch", func(value *ServingSlot) { value.Evidence.ArtifactDigest = "different" }},
		{"host_identity_mismatch", func(value *ServingSlot) { value.Evidence.Fingerprint = "different" }},
		{"server_pod_missing", func(value *ServingSlot) { value.Evidence.ServerPodUID = "" }},
		{"target_identity_mismatch", func(value *ServingSlot) { value.Evidence.TargetContainerID = "different" }},
		{"key_exchange_unverified", func(value *ServingSlot) { value.Evidence.KeyExchangeVerified = false }},
		{"runtime_unverified", func(value *ServingSlot) { value.Evidence.RuntimeVerified = false }},
		{"observation_time_missing", func(value *ServingSlot) { value.Evidence.ObservedAt = time.Time{} }},
		{"observation_after_verification_clock", func(value *ServingSlot) { value.Evidence.ObservedAt = now.Add(6 * time.Second) }},
		{"observation_expired", func(value *ServingSlot) { value.Evidence.ExpiresAt = now }},
		{"observation_lifetime_invalid", func(value *ServingSlot) { value.Evidence.ExpiresAt = now.Add(3 * time.Minute) }},
		{"observation_exceeds_authorization", func(value *ServingSlot) { value.Artifact.Configuration.ValidUntil = value.Evidence.ExpiresAt }},
	} {
		test.Run(scenario.reason, func(test *testing.T) {
			candidate := slot
			copied := evidence
			candidate.Evidence = &copied
			scenario.mutate(&candidate)
			if candidate.Verified(now) || candidate.VerificationFailure(now) != scenario.reason {
				test.Fatal("incorrect evidence rejection diagnostic", candidate.VerificationFailure(now))
			}
		})
	}
}

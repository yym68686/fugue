package platformcontrol

import (
	"encoding/hex"
	"strings"

	"fugue/internal/model"
)

// ConsumerReleaseBinding comes from verified artifacts and the authoritative
// active release, never from the reporting consumer. It is evaluation context,
// not a mutable field added to an immutable expected set or runtime receipt.
type ConsumerReleaseBinding struct {
	ReleaseSetID       string
	ArtifactReleaseID  string
	ArtifactKind       string
	ScopeKey           string
	Generation         string
	FencingToken       int64
	GenerationSequence int64
}

func assessConsumerReleaseBinding(set model.PlatformExpectedConsumerSet, consumer model.PlatformConsumerInstance, binding *ConsumerReleaseBinding, assessment *model.PlatformConsumerEvidenceAssessment) {
	if set.ReleaseSetID == "" && set.ArtifactReleaseID == "" {
		return
	}
	add := func(state, reason string) {
		assessment.State = aggregateEvidenceState(assessment.State, state)
		assessment.Reasons = append(assessment.Reasons, reason)
	}
	if binding == nil || set.ReleaseSetID == "" || set.ArtifactReleaseID == "" || binding.ReleaseSetID != set.ReleaseSetID || binding.ArtifactReleaseID != set.ArtifactReleaseID || binding.ArtifactKind != set.ArtifactKind || binding.ScopeKey != set.ScopeKey || binding.Generation != set.ExpectedGeneration || binding.FencingToken <= 0 || binding.GenerationSequence <= 0 {
		add(model.InvariantEvidenceStateUnknown, "authoritative release and artifact binding unavailable")
		return
	}
	if assessment.Observed == nil {
		return
	}
	if !consumer.IdentityVerified || consumer.CredentialID == "" || consumer.TokenID == "" {
		add(model.InvariantEvidenceStateFail, "consumer identity is not verified")
	}
	if consumer.ExpectedConsumerSetID != set.ID || consumer.ReleaseSetID != set.ReleaseSetID {
		add(model.InvariantEvidenceStateFail, "consumer receipt belongs to another expectation or ReleaseSet")
	}
	if consumer.FencingToken != binding.FencingToken || consumer.GenerationSequence != binding.GenerationSequence {
		add(model.InvariantEvidenceStateFail, "consumer receipt fence or artifact sequence does not match the active release")
	}
	hash, err := hex.DecodeString(strings.TrimPrefix(consumer.EvidenceHash, "sha256:"))
	if consumer.Sequence <= 0 || consumer.IssuedAt == nil || consumer.IssuedAt.IsZero() || consumer.Nonce == "" || err != nil || len(hash) != 32 || len(consumer.EvidenceHash) != 71 || !strings.HasPrefix(consumer.EvidenceHash, "sha256:") {
		add(model.InvariantEvidenceStateUnknown, "consumer receipt lacks trusted sequence or evidence identity")
	}
}

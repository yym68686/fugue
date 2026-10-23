package store

import (
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

// A rollback's new fence is not a new positive verification. Configuration
// recovery may use the exact original signed LKG, but must not promote that
// rollback, reset consumer evidence or change the serving lane as a side effect.
func availabilityVerifiedRollbackBaseline(state *model.State, parent model.PlatformArtifact, current model.PlatformArtifactRelease, keys bundleauth.Keyring, now time.Time) bool {
	lkg := verifiedPlatformLKGSnapshotFromState(state, model.PlatformArtifactKindReleaseSet, parent.ScopeKey, now, keys)
	if lkg == nil || lkg.ArtifactID != parent.ID || lkg.ContentHash != parent.ContentHash || lkg.Generation != parent.Generation ||
		current.VerificationState != model.PlatformArtifactVerificationStateServingUnverified || current.PinnedRollbackGeneration != parent.Generation || current.RollbackTargetGeneration != parent.Generation || current.VerifiedLKGGeneration != parent.Generation ||
		current.OverrideMode != "" || len(current.BypassedInvariants) != 0 || !producerOwnsPublication(current) || current.ReleasedAt.IsZero() || current.ReleasedAt.After(now) {
		return false
	}
	index := platformArtifactReleaseIndex(state.PlatformArtifactReleases, lkg.VerifiedByReleaseID)
	if index < 0 {
		return false
	}
	verified := state.PlatformArtifactReleases[index]
	if verified.ID == current.ID || verified.ArtifactID != parent.ID || verified.ArtifactKind != parent.ArtifactKind || verified.ScopeKey != parent.ScopeKey || verified.Generation != parent.Generation ||
		verified.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || verified.VerificationState != model.PlatformArtifactVerificationStateVerified || verified.VerifiedLKGGeneration != parent.Generation ||
		verified.OverrideMode != "" || len(verified.BypassedInvariants) != 0 || !producerOwnsPublication(verified) || verified.FencingToken <= 0 || current.FencingToken <= verified.FencingToken || !verified.ReleasedAt.Before(current.ReleasedAt) {
		return false
	}
	for _, message := range state.PlatformReleaseMessages {
		if message.ReleaseID == current.ID && message.MessageType == model.PlatformReleaseMessageTypeRollback && message.ArtifactID == parent.ID && message.ArtifactKind == parent.ArtifactKind && message.ScopeKey == parent.ScopeKey && message.Generation == parent.Generation && message.ReleaseChannel == model.PlatformArtifactReleaseChannelFull && message.CreatedAt.Equal(current.ReleasedAt) {
			return true
		}
	}
	return false
}

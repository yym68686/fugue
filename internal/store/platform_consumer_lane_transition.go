package store

import (
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

// A lane transition changes only the fence comparison domain. All replay,
// issued-at and artifact sequence constraints remain intact. Callers load
// these release records and lock the new lane in the consumer transaction.
func consumerCursorForLaneTransition(previous model.PlatformConsumerInstance, cursor *platformcontrol.PlatformConsumerHeartbeatCursor, oldSet, nextSet model.PlatformExpectedConsumerSet, oldRelease, nextRelease model.PlatformArtifactRelease, lane model.PlatformReleaseLane, nextFence int64) (*platformcontrol.PlatformConsumerHeartbeatCursor, error) {
	if cursor == nil || nextFence > cursor.FencingToken || nextFence == cursor.FencingToken && oldSet.ArtifactReleaseID == nextSet.ArtifactReleaseID {
		return cursor, nil
	}
	validSet := func(set model.PlatformExpectedConsumerSet, r model.PlatformArtifactRelease) bool {
		return set.ArtifactReleaseID == r.ID && set.ReleaseSetID == r.ArtifactID && r.ArtifactKind == model.PlatformArtifactKindReleaseSet && set.ScopeKey == r.ScopeKey && r.LaneKey == platformsafety.ReleaseLaneKey(r.ArtifactKind, r.ScopeKey, r.ReleaseChannel)
	}
	// A later gray candidate may follow a full rollback whose lane counter is
	// already larger. The counter belongs to its lane, not to the parent.
	// The unchanged artifact sequence below still prevents generation rollback;
	// moving to an older artifact requires the separate verified-LKG proof.
	serving := nextRelease.ReleaseChannel == model.PlatformArtifactReleaseChannelGray || nextRelease.ReleaseChannel == model.PlatformArtifactReleaseChannelFull
	if !previous.IdentityVerified || previous.ExpectedConsumerSetID != oldSet.ID || previous.ReleaseSetID != oldSet.ReleaseSetID || !validSet(oldSet, oldRelease) || !validSet(nextSet, nextRelease) || oldSet.ScopeKey != nextSet.ScopeKey || oldSet.ArtifactKind != nextSet.ArtifactKind || (oldSet.ReleaseSetID != nextSet.ReleaseSetID && !serving) || oldRelease.LaneKey == nextRelease.LaneKey || oldRelease.FencingToken != previous.FencingToken || nextRelease.FencingToken != nextFence || nextFence <= 0 || oldRelease.ReleasedAt.IsZero() || !nextRelease.ReleasedAt.After(oldRelease.ReleasedAt) || nextRelease.Status != model.PlatformArtifactReleaseStatusActive || lane.Frozen || lane.LaneKey != nextRelease.LaneKey || lane.ActiveReleaseID != nextRelease.ID || lane.FencingToken != nextFence {
		return nil, platformcontrol.ErrPlatformConsumerHeartbeatFencingBack
	}
	copy := *cursor
	copy.FencingToken = nextFence
	return &copy, nil
}

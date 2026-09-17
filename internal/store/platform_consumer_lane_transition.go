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
	if cursor == nil || nextFence >= cursor.FencingToken {
		return cursor, nil
	}
	validSet := func(set model.PlatformExpectedConsumerSet, r model.PlatformArtifactRelease) bool {
		return set.ArtifactReleaseID == r.ID && set.ReleaseSetID == r.ArtifactID && r.ArtifactKind == model.PlatformArtifactKindReleaseSet && set.ScopeKey == r.ScopeKey && r.LaneKey == platformsafety.ReleaseLaneKey(r.ArtifactKind, r.ScopeKey, r.ReleaseChannel)
	}
	if !previous.IdentityVerified || previous.ExpectedConsumerSetID != oldSet.ID || previous.ReleaseSetID != oldSet.ReleaseSetID || !validSet(oldSet, oldRelease) || !validSet(nextSet, nextRelease) || oldSet.ScopeKey != nextSet.ScopeKey || oldSet.ArtifactKind != nextSet.ArtifactKind || oldSet.ReleaseSetID != nextSet.ReleaseSetID || oldRelease.LaneKey == nextRelease.LaneKey || oldRelease.FencingToken != previous.FencingToken || nextRelease.FencingToken != nextFence || nextFence <= 0 || oldRelease.ReleasedAt.IsZero() || !nextRelease.ReleasedAt.After(oldRelease.ReleasedAt) || nextRelease.Status != model.PlatformArtifactReleaseStatusActive || lane.Frozen || lane.LaneKey != nextRelease.LaneKey || lane.ActiveReleaseID != nextRelease.ID || lane.FencingToken != nextFence {
		return nil, platformcontrol.ErrPlatformConsumerHeartbeatFencingBack
	}
	copy := *cursor
	copy.FencingToken = nextFence
	return &copy, nil
}

func consumerLaneTransitionInState(state *model.State, previous model.PlatformConsumerInstance, cursor *platformcontrol.PlatformConsumerHeartbeatCursor, nextSet model.PlatformExpectedConsumerSet, nextFence int64) (*platformcontrol.PlatformConsumerHeartbeatCursor, error) {
	var oldSet model.PlatformExpectedConsumerSet
	for _, s := range state.ExpectedConsumerSets {
		if s.ID == previous.ExpectedConsumerSetID {
			oldSet = s
			break
		}
	}
	var oldRelease, nextRelease model.PlatformArtifactRelease
	for _, r := range state.PlatformArtifactReleases {
		if r.ID == oldSet.ArtifactReleaseID {
			oldRelease = r
		}
		if r.ID == nextSet.ArtifactReleaseID {
			nextRelease = r
		}
	}
	lane, _ := platformReleaseLaneByKey(state.PlatformReleaseLanes, nextRelease.LaneKey)
	return consumerCursorForLaneTransition(previous, cursor, oldSet, nextSet, oldRelease, nextRelease, lane, nextFence)
}

package store

import (
	"fmt"
	"math"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

// Automatic promotion carries the complete prepared topology from the previous
// phase into the same transaction as its new release. These are expectations,
// never runtime evidence: every new release still needs fresh bound heartbeats.
func producedReleaseExpectations(state *model.State, parent model.PlatformArtifact, release model.PlatformArtifactRelease, guard *platformProducerReleaseGuard, now time.Time) ([]model.PlatformExpectedConsumerSet, error) {
	if guard == nil || (guard.Phase != "gray" && guard.Phase != "full") {
		return nil, nil
	}
	fail := func(reason string) ([]model.PlatformExpectedConsumerSet, error) {
		return nil, fmt.Errorf("%w: automatic promotion requires complete prior-phase consumer expectations: %s", ErrConflict, reason)
	}
	if state == nil || release.ArtifactID != parent.ID || release.ScopeKey != parent.ScopeKey || release.Generation != parent.Generation || release.ReleaseChannel != guard.Phase {
		return fail("new release binding")
	}
	channel := model.PlatformArtifactReleaseChannelShadow
	if guard.Phase == "full" {
		channel = model.PlatformArtifactReleaseChannelGray
	}
	lane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, platformsafety.ReleaseLaneKey(parent.ArtifactKind, parent.ScopeKey, channel))
	if !ok || lane.Frozen || lane.ActiveReleaseID == "" {
		return fail("prior phase lane")
	}
	index := platformArtifactReleaseIndex(state.PlatformArtifactReleases, lane.ActiveReleaseID)
	if index < 0 {
		return fail("prior phase release absent")
	}
	source := state.PlatformArtifactReleases[index]
	if source.ArtifactID != parent.ID || source.ScopeKey != parent.ScopeKey || source.Generation != parent.Generation || source.ReleaseChannel != channel || source.Status != model.PlatformArtifactReleaseStatusActive || source.FencingToken != lane.FencingToken || source.VerificationState == model.PlatformArtifactVerificationStateFailed {
		return fail("prior phase authority")
	}
	kinds, err := platformconfig.ValidateReleaseComposition(parent)
	if err != nil {
		return fail("release composition")
	}
	latest := map[string]model.PlatformExpectedConsumerSet{}
	var revision int64
	for _, set := range state.ExpectedConsumerSets {
		if set.ReleaseSetID != parent.ID || set.ScopeKey != parent.ScopeKey {
			continue
		}
		revision = max(revision, set.Revision)
		if set.ArtifactReleaseID == source.ID && set.Revision > latest[set.ArtifactKind].Revision {
			latest[set.ArtifactKind] = set
		}
	}
	sets := make([]model.PlatformExpectedConsumerSet, 0, len(kinds))
	for _, kind := range kinds {
		prior, ok := latest[kind]
		if !ok || prior.CreatedAt.IsZero() || revision == math.MaxInt64 || platformcontrol.ValidateDeclaredTrafficConsumerSet(parent, prior) != nil {
			return fail("prior expected membership")
		}
		validChild := false
		for _, child := range state.PlatformArtifacts {
			if child.ArtifactKind == kind && child.ScopeKey == parent.ScopeKey && child.Generation == prior.ExpectedGeneration && child.Metadata["release_set_generation"] == parent.Generation {
				validChild = true
			}
		}
		if !validChild || prior.HeartbeatDeadline.Before(prior.CreatedAt) || prior.ConvergenceDeadline.Before(prior.HeartbeatDeadline) {
			return fail("prior child or observation window")
		}
		revision++
		next, err := platformcontrol.RebindExpectedConsumerSet(prior, release.ID, revision, now)
		if err != nil {
			return fail("expectation rebind")
		}
		_, err = normalizePlatformExpectedConsumerSetForStore(next)
		if err != nil {
			return fail("normalized expectation: " + err.Error())
		}
		if err = platformcontrol.ValidateDeclaredTrafficConsumerSet(parent, next); err != nil {
			return fail("rebound membership: " + err.Error())
		}
		sets = append(sets, next)
	}
	return sets, nil
}

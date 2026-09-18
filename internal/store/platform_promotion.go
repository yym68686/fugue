package store

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

// Evaluate only immutable declared expectations and facts loaded in the same
// store transaction as publication. Live inventory projection is a preflight;
// it cannot silently rewrite the topology being authorized by this transaction.
func validateFullReleaseSetInState(state *model.State, parent model.PlatformArtifact, keyring bundleauth.Keyring, now time.Time) error {
	fail := func(reason string) error {
		return fmt.Errorf("%w: release set atomic convergence: %s", ErrConflict, reason)
	}
	if parent.ArtifactKind != model.PlatformArtifactKindReleaseSet {
		return nil
	}
	if parent.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(parent, keyring).Pass {
		return fail("untrusted parent")
	}
	var latest *model.PlatformArtifactRelease
	for _, r := range state.PlatformArtifactReleases {
		if r.ArtifactID != parent.ID || r.ArtifactKind != parent.ArtifactKind || r.ScopeKey != parent.ScopeKey || r.Status != model.PlatformArtifactReleaseStatusActive {
			continue
		}
		lane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, r.LaneKey)
		if !ok || lane.Frozen || lane.ActiveReleaseID != r.ID || lane.FencingToken != r.FencingToken || r.FencingToken <= 0 || r.ReleasedAt.IsZero() || r.Generation != parent.Generation {
			return fail("active publication authority unavailable")
		}
		if latest != nil && r.ReleasedAt.Equal(latest.ReleasedAt) && r.ID != latest.ID {
			return fail("publication ordering is ambiguous")
		}
		if latest == nil || r.ReleasedAt.After(latest.ReleasedAt) {
			copy := r
			latest = &copy
		}
	}
	if latest == nil {
		return fail("no active consumer publication")
	}
	ids, okIDs := parent.Content["artifact_ids"].([]any)
	kinds, okKinds := parent.Content["artifact_kinds"].([]any)
	if !okIDs || !okKinds || len(ids) == 0 || len(ids) > 64 || len(ids) != len(kinds) {
		return fail("members missing or malformed")
	}
	seenKinds, seenIDs := map[string]bool{}, map[string]bool{}
	for i, raw := range ids {
		id, ok := raw.(string)
		kind, kindOK := kinds[i].(string)
		if !ok || !kindOK || strings.TrimSpace(id) == "" || kind == "" || seenIDs[id] || seenKinds[kind] {
			return fail("ambiguous child membership")
		}
		seenIDs[id], seenKinds[kind] = true, true
		index := platformArtifactIndex(state.PlatformArtifacts, id)
		if index < 0 {
			return fail("child missing")
		}
		child := state.PlatformArtifacts[index]
		if child.ID != id || child.ArtifactKind != kind || child.ScopeKey != parent.ScopeKey || child.Status != model.PlatformArtifactStatusValidated || child.GenerationSequence <= 0 || !platformsafety.EvaluateArtifactIntegrity(child, keyring).Pass || child.Metadata["release_set_generation"] != parent.Generation {
			return fail("child integrity or ownership invalid")
		}
		for _, key := range []string{"intent_digest", "policy_digest", "compiler_version", "input_snapshot_digest", "intent_generation", "policy_generation"} {
			if parent.Metadata[key] == "" || child.Metadata[key] != parent.Metadata[key] {
				return fail("child lineage differs")
			}
		}
		var latestSet *model.PlatformExpectedConsumerSet
		for _, set := range state.ExpectedConsumerSets {
			if set.ReleaseSetID != parent.ID || set.ArtifactReleaseID != latest.ID || set.ArtifactKind != kind || set.ScopeKey != parent.ScopeKey {
				continue
			}
			if latestSet != nil && set.Revision == latestSet.Revision && set.ID != latestSet.ID {
				return fail("expectation revision is ambiguous")
			}
			if latestSet == nil || set.Revision > latestSet.Revision {
				copy := set
				latestSet = &copy
			}
		}
		if latestSet == nil || !latestSet.RequiresConsumers || latestSet.ExpectedGeneration != child.Generation {
			return fail("latest member expectation missing")
		}
		consumers := []model.PlatformConsumerInstance{}
		for _, consumer := range state.PlatformConsumerInstances {
			if consumer.ArtifactKind == kind && consumer.ScopeKey == parent.ScopeKey {
				consumers = append(consumers, consumer)
			}
		}
		binding := &platformcontrol.ConsumerReleaseBinding{ReleaseSetID: parent.ID, ArtifactReleaseID: latest.ID, ArtifactKind: kind, ScopeKey: parent.ScopeKey, Generation: child.Generation, FencingToken: latest.FencingToken, GenerationSequence: child.GenerationSequence}
		status := platformcontrol.EvaluateConsumerConvergence(*latestSet, consumers, now, binding)
		if !status.Pass {
			return fail("required consumers are not currently applied and probed for " + kind)
		}
	}
	return nil
}

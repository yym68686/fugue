package store

import (
	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

// A rollback changes the desired artifact, not the authenticated replay stream.
// The caller holds the configuration scope transaction through fact persistence.
func consumerCursorForTrafficRollback(state *model.State, previous model.PlatformConsumerInstance, cursor *platformcontrol.PlatformConsumerHeartbeatCursor, set model.PlatformExpectedConsumerSet, heartbeat platformcontrol.PlatformConsumerHeartbeatEnvelope, keys bundleauth.Keyring) (*platformcontrol.PlatformConsumerHeartbeatCursor, error) {
	fail := func() (*platformcontrol.PlatformConsumerHeartbeatCursor, error) {
		return nil, platformcontrol.ErrPlatformConsumerHeartbeatGenerationBack
	}
	if cursor == nil || !previous.IdentityVerified || heartbeat.GenerationSequence <= 0 || heartbeat.GenerationSequence >= cursor.GenerationSequence {
		return fail()
	}
	var oldSet model.PlatformExpectedConsumerSet
	for _, s := range state.ExpectedConsumerSets {
		if s.ID == previous.ExpectedConsumerSetID {
			oldSet = s
		}
	}
	var old, next model.PlatformArtifactRelease
	for _, r := range state.PlatformArtifactReleases {
		if r.ID == oldSet.ArtifactReleaseID {
			old = r
		}
		if r.ID == set.ArtifactReleaseID {
			next = r
		}
	}
	if old.ID == "" || next.ID == "" || old.ID == next.ID || old.ArtifactID != previous.ReleaseSetID || oldSet.ReleaseSetID != previous.ReleaseSetID || old.ArtifactKind != model.PlatformArtifactKindReleaseSet || old.ScopeKey != set.ScopeKey || old.FencingToken != previous.FencingToken || old.ReleasedAt.IsZero() || !next.ReleasedAt.After(old.ReleasedAt) || next.Status != model.PlatformArtifactReleaseStatusActive || next.ArtifactKind != model.PlatformArtifactKindReleaseSet || next.ArtifactID != set.ReleaseSetID || next.ScopeKey != set.ScopeKey || next.FencingToken != heartbeat.FencingToken || next.FencingToken <= 0 || next.LaneKey != platformsafety.ReleaseLaneKey(next.ArtifactKind, next.ScopeKey, next.ReleaseChannel) {
		return fail()
	}
	if next.ReleaseChannel != model.PlatformArtifactReleaseChannelGray && next.ReleaseChannel != model.PlatformArtifactReleaseChannelFull {
		return fail()
	}
	if next.LaneKey == old.LaneKey && next.FencingToken <= old.FencingToken {
		return fail()
	}
	lane, found := platformReleaseLaneByKey(state.PlatformReleaseLanes, next.LaneKey)
	if !found || lane.Frozen || lane.ActiveReleaseID != next.ID || lane.FencingToken != next.FencingToken {
		return fail()
	}
	explicit := false
	for _, m := range state.PlatformReleaseMessages {
		if m.ReleaseID == next.ID && m.MessageType == model.PlatformReleaseMessageTypeRollback && m.ArtifactID == next.ArtifactID && m.ArtifactKind == next.ArtifactKind && m.ScopeKey == next.ScopeKey && m.Generation == next.Generation && m.ReleaseChannel == next.ReleaseChannel {
			explicit = true
		}
	}
	if !explicit {
		return fail()
	}
	index := platformArtifactIndex(state.PlatformArtifacts, next.ArtifactID)
	if index < 0 {
		return fail()
	}
	parent := state.PlatformArtifacts[index]
	if parent.Generation != next.Generation || parent.ScopeKey != next.ScopeKey || parent.ArtifactKind != next.ArtifactKind || parent.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(parent, keys).Pass {
		return fail()
	}
	ids, ok := parent.Content["artifact_ids"].([]any)
	kinds, kindsOK := parent.Content["artifact_kinds"].([]any)
	if !ok || !kindsOK || len(ids) != 3 || len(kinds) != 3 {
		return fail()
	}
	var child model.PlatformArtifact
	seen := map[string]bool{}
	for i, raw := range ids {
		id, ok := raw.(string)
		if !ok {
			return fail()
		}
		index := platformArtifactIndex(state.PlatformArtifacts, id)
		if index < 0 {
			return fail()
		}
		a := state.PlatformArtifacts[index]
		if seen[a.ArtifactKind] || a.ArtifactKind != kinds[i] || a.ScopeKey != parent.ScopeKey || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass || a.Metadata["release_set_generation"] != parent.Generation {
			return fail()
		}
		switch a.ArtifactKind {
		case model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig:
		default:
			return fail()
		}
		seen[a.ArtifactKind] = true
		for _, key := range []string{"intent_digest", "policy_digest", "compiler_version", "input_snapshot_digest", "intent_generation", "policy_generation"} {
			if parent.Metadata[key] == "" || a.Metadata[key] != parent.Metadata[key] {
				return fail()
			}
		}
		if platformconfig.ValidateTrafficCohortProjection(parent, a) != nil {
			return fail()
		}
		if a.ArtifactKind == set.ArtifactKind {
			child = a
		}
	}
	if child.ID == "" || child.Generation != set.ExpectedGeneration || heartbeat.GenerationSequence != child.GenerationSequence || heartbeat.DesiredGeneration != child.Generation {
		return fail()
	}
	for _, s := range state.ExpectedConsumerSets {
		if s.ArtifactReleaseID == next.ID && s.ReleaseSetID == parent.ID && s.ArtifactKind == set.ArtifactKind && s.ScopeKey == set.ScopeKey && s.ID != set.ID && s.Revision >= set.Revision {
			return fail()
		}
	}
	group := ""
	for _, c := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
		if c.ConsumerID == heartbeat.ConsumerID && c.Component == heartbeat.Component && c.NodeID == heartbeat.NodeID && c.ArtifactKind == heartbeat.ArtifactKind && c.ScopeKey == heartbeat.ScopeKey {
			if group != "" && group != c.Cohort {
				return fail()
			}
			group = c.Cohort
		}
	}
	if group == "" {
		return fail()
	}
	if next.ReleaseChannel == model.PlatformArtifactReleaseChannelGray {
		groups, err := platformconfig.ResolveTrafficCanary(parent, next.CanaryRuleRef)
		if err != nil || !platformconfig.TrafficCanaryContains(groups, group) {
			return fail()
		}
	}
	// A live newer publication in another channel can supersede this lane for
	// this group. Shadow never grants serving authority.
	for _, r := range state.PlatformArtifactReleases {
		if r.ID == next.ID || r.ArtifactKind != next.ArtifactKind || r.ScopeKey != next.ScopeKey || r.Status != model.PlatformArtifactReleaseStatusActive || r.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow || r.ReleasedAt.Before(next.ReleasedAt) {
			continue
		}
		if r.ReleaseChannel == model.PlatformArtifactReleaseChannelFull {
			return fail()
		}
		index := platformArtifactIndex(state.PlatformArtifacts, r.ArtifactID)
		if index < 0 || !platformsafety.EvaluateArtifactIntegrity(state.PlatformArtifacts[index], keys).Pass {
			return fail()
		}
		groups, err := platformconfig.ResolveTrafficCanary(state.PlatformArtifacts[index], r.CanaryRuleRef)
		if err != nil || platformconfig.TrafficCanaryContains(groups, group) {
			return fail()
		}
	}
	copy := *cursor
	copy.GenerationSequence = heartbeat.GenerationSequence
	copy.FencingToken = heartbeat.FencingToken
	return &copy, nil
}

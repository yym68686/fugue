package store

import (
	"fmt"
	"slices"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

// Capability is evidence about executable support, not success of the candidate.
// In particular, recovery may use fresh negative facts from the currently
// serving release. The caller holds the publication transaction through commit.
func validateLeasedTrafficAdmission(state *model.State, parent model.PlatformArtifact, channel, canaryRef string, keys bundleauth.Keyring, now time.Time) error {
	if channel == model.PlatformArtifactReleaseChannelShadow {
		return nil
	}
	fail := func(reason string) error {
		return fmt.Errorf("%w: leased traffic compatibility: %s", ErrConflict, reason)
	}
	if parent.ArtifactKind != model.PlatformArtifactKindReleaseSet {
		if platformconfig.DNSArtifactHasValueExpirations(parent) {
			return fail("DNS value expiration requires a complete traffic ReleaseSet")
		}
		return nil
	}
	ids, _ := parent.Content["artifact_ids"].([]any)
	leased := false
	for _, id := range ids {
		value, ok := id.(string)
		if !ok {
			return fail("invalid member reference")
		}
		index := platformArtifactIndex(state.PlatformArtifacts, value)
		if index < 0 {
			return fail("member unavailable")
		}
		leased = leased || platformconfig.DNSArtifactHasValueExpirations(state.PlatformArtifacts[index])
	}
	if !leased {
		return nil
	}
	kinds, _ := parent.Content["artifact_kinds"].([]any)
	if len(ids) != 3 || len(kinds) != 3 || parent.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(parent, keys).Pass {
		return fail("complete signed traffic parent required")
	}
	var groups []string
	if channel == model.PlatformArtifactReleaseChannelGray {
		var err error
		groups, err = platformconfig.ResolveTrafficCanary(parent, canaryRef)
		if err != nil {
			return fail("signed cohort unavailable")
		}
	} else if channel != model.PlatformArtifactReleaseChannelFull {
		return fail("unknown serving channel")
	}
	seen := map[string]bool{}
	for i, raw := range ids {
		child := state.PlatformArtifacts[platformArtifactIndex(state.PlatformArtifacts, raw.(string))]
		component := model.PlatformConsumerComponentEdgeWorker
		switch child.ArtifactKind {
		case model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig:
		case model.PlatformArtifactKindDNSAnswerBundle:
			component = model.PlatformConsumerComponentDNSServer
		default:
			return fail("unsupported member")
		}
		if seen[child.ArtifactKind] || kinds[i] != child.ArtifactKind || child.ScopeKey != parent.ScopeKey || child.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(child, keys).Pass || child.Metadata["release_set_generation"] != parent.Generation {
			return fail("member integrity or ownership invalid")
		}
		seen[child.ArtifactKind] = true
		for _, key := range []string{"intent_digest", "policy_digest", "compiler_version", "input_snapshot_digest", "intent_generation", "policy_generation"} {
			if parent.Metadata[key] == "" || child.Metadata[key] != parent.Metadata[key] {
				return fail("member lineage differs")
			}
		}
		if platformconfig.ValidateTrafficCohortProjection(parent, child) != nil {
			return fail("member cohort policy differs")
		}
		var latest *model.PlatformExpectedConsumerSet
		for _, set := range state.ExpectedConsumerSets {
			if set.ReleaseSetID != parent.ID || set.ArtifactKind != child.ArtifactKind || set.ScopeKey != parent.ScopeKey {
				continue
			}
			if latest != nil && set.Revision == latest.Revision && set.ID != latest.ID {
				return fail("prepared topology revision ambiguous")
			}
			if latest == nil || set.Revision > latest.Revision {
				copy := set
				latest = &copy
			}
		}
		if latest == nil || !latest.RequiresConsumers || latest.ExpectedGeneration != child.Generation || latest.TopologyRevision == "" || latest.Revision <= 0 {
			return fail("prepare target consumer topology before serving publication")
		}
		count, cohorts := 0, map[string]bool{}
		for _, expected := range platformcontrol.ProjectExpectedConsumerOwners(*latest).Consumers {
			if !expected.Required || len(groups) > 0 && !platformconfig.TrafficCanaryContains(groups, expected.Cohort) {
				continue
			}
			if expected.Component != component || expected.ConsumerID != component+":"+expected.NodeID || expected.ArtifactKind != child.ArtifactKind || expected.ScopeKey != parent.ScopeKey || expected.ExpectedGeneration != child.Generation || expected.Cohort == "" {
				return fail("required consumer ownership invalid")
			}
			found := false
			for _, fact := range state.PlatformConsumerInstances {
				if fact.ConsumerID != expected.ConsumerID || fact.ArtifactKind != child.ArtifactKind || fact.ScopeKey != parent.ScopeKey {
					continue
				}
				if found || !trafficCapabilityFactFresh(expected, fact, now) {
					return fail("fresh authenticated traffic capability required for " + expected.ConsumerID + "/" + child.ArtifactKind)
				}
				found = true
			}
			if !found {
				return fail("required capability fact missing for " + expected.ConsumerID + "/" + child.ArtifactKind)
			}
			count++
			cohorts[expected.Cohort] = true
		}
		if count == 0 {
			return fail("required member consumer set is empty")
		}
		for _, group := range groups {
			if !cohorts[group] {
				return fail("selected cohort has no required member consumer")
			}
		}
	}
	return nil
}

func trafficCapabilityFactFresh(expected model.PlatformExpectedConsumer, fact model.PlatformConsumerInstance, now time.Time) bool {
	freshness := time.Duration(expected.HeartbeatFreshnessSeconds) * time.Second
	if freshness <= 0 {
		freshness = 90 * time.Second
	}
	return fact.IdentityVerified && fact.Component == expected.Component && fact.NodeID == expected.NodeID &&
		fact.CredentialID != "" && fact.TokenID != "" && fact.EvidenceHash != "" && fact.ExpectedConsumerSetID != "" && fact.ReleaseSetID != "" &&
		fact.Sequence > 0 && fact.GenerationSequence > 0 && fact.FencingToken > 0 && fact.Nonce != "" &&
		fact.ProtocolVersion == "v1" && fact.SchemaVersion == "v1" && slices.Contains(fact.CompatibilityCapabilities, platformcontrol.TrafficReleaseCapabilityV1) &&
		fact.IssuedAt != nil && !fact.IssuedAt.IsZero() && !fact.IssuedAt.After(now.Add(30*time.Second)) && fact.IssuedAt.Add(freshness).After(now) &&
		!fact.LastHeartbeatAt.IsZero() && !fact.LastHeartbeatAt.After(now) && fact.LastHeartbeatAt.Add(freshness).After(now)
}

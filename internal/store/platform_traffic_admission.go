package store

import (
	"encoding/json"
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
	return validateLeasedTrafficAdmissionWithRecovery(state, parent, channel, canaryRef, keys, now, false)
}

func validateVerifiedTrafficRecoveryAdmission(state *model.State, parent model.PlatformArtifact, channel, canaryRef string, keys bundleauth.Keyring, now time.Time, lkg *model.PlatformLKGSnapshot, guard *platformProducerReleaseGuard) error {
	recovery := guard != nil && guard.Phase == "rollback" && guard.BaselineArtifactID == parent.ID &&
		channel == model.PlatformArtifactReleaseChannelFull && parent.ArtifactKind == model.PlatformArtifactKindReleaseSet && lkg != nil &&
		lkg.ArtifactID == parent.ID && lkg.ContentHash == parent.ContentHash && lkg.Generation == parent.Generation &&
		lkg.ScopeKey == parent.ScopeKey && lkg.ArtifactKind == parent.ArtifactKind && lkg.VerifiedByReleaseID != "" &&
		lkg.VerificationEvidenceHash != "" && lkg.ExpiresAt.After(now)
	return validateLeasedTrafficAdmissionWithRecovery(state, parent, channel, canaryRef, keys, now, recovery)
}

func validateLeasedTrafficAdmissionWithRecovery(state *model.State, parent model.PlatformArtifact, channel, canaryRef string, keys bundleauth.Keyring, now time.Time, verifiedRecovery bool) error {
	if channel == model.PlatformArtifactReleaseChannelShadow {
		return nil
	}
	fail := func(reason string) error {
		return fmt.Errorf("%w: leased traffic compatibility: %s", ErrConflict, reason)
	}
	if parent.ArtifactKind != model.PlatformArtifactKindReleaseSet {
		if platformconfig.DNSArtifactRequiresTrafficRelease(parent) {
			return fail("DNS value expiration requires a complete traffic ReleaseSet")
		}
		return nil
	}
	ids, _ := parent.Content["artifact_ids"].([]any)
	role, _ := parent.Content["publication_role"].(string)
	cellRoutes := role == platformconfig.PublicationRoleCellRoutes
	cellDNS := role == platformconfig.PublicationRoleCellDNS
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
		leased = leased || platformconfig.DNSArtifactRequiresTrafficRelease(state.PlatformArtifacts[index])
	}
	if !leased && !cellRoutes && !cellDNS {
		return nil
	}
	kinds, _ := parent.Content["artifact_kinds"].([]any)
	_, compositionErr := platformconfig.ValidateReleaseComposition(parent)
	if compositionErr != nil || len(ids) != len(kinds) || parent.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(parent, keys).Pass {
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
			if err := validateCellDNSReferencesInState(state, child, keys); err != nil {
				return err
			}
		default:
			return fail("unsupported member")
		}
		if seen[child.ArtifactKind] || kinds[i] != child.ArtifactKind || child.ScopeKey != parent.ScopeKey || child.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(child, keys).Pass || child.Metadata["release_set_generation"] != parent.Generation {
			return fail("member integrity or ownership invalid")
		}
		seen[child.ArtifactKind] = true
		authorityTransition := child.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle && child.Content["previous_traffic_publication"] != nil
		sources, sourceErr := platformconfig.DNSRouteSourceAuthorizations(child)
		if sourceErr != nil {
			return fail("DNS routing source authorization invalid")
		}
		physicalRequired, err := physicalNetworkCapabilityRequired(child)
		if err != nil {
			return fail("physical network policy cannot be decoded")
		}
		orderRequired, err := physicalOrderCapabilityRequired(child)
		if err != nil {
			return fail("physical order policy cannot be decoded")
		}
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
		if platformcontrol.ValidateDeclaredTrafficConsumerSet(parent, *latest) != nil {
			return fail("prepared capability membership differs from signed topology")
		}
		count, cohorts := 0, map[string]bool{}
		for _, expected := range platformcontrol.ProjectExpectedConsumerOwners(*latest).Consumers {
			if !expected.Required || len(groups) > 0 && !platformconfig.TrafficCanaryContains(groups, expected.Cohort) {
				continue
			}
			claims := platformcontrol.PlatformComponentIdentityClaims{Component: component, NodeID: expected.NodeID, AuthorityID: expected.AuthorityID}
			if !platformcontrol.ExpectedConsumerIdentityMatches(expected, claims) || expected.ArtifactKind != child.ArtifactKind || expected.ScopeKey != parent.ScopeKey || expected.ExpectedGeneration != child.Generation || expected.Cohort == "" {
				return fail("required consumer ownership invalid")
			}
			if verifiedRecovery {
				count++
				cohorts[expected.Cohort] = true
				continue
			}
			found := false
			for _, fact := range state.PlatformConsumerInstances {
				if fact.ConsumerID != expected.ConsumerID || fact.ArtifactKind != child.ArtifactKind || fact.ScopeKey != parent.ScopeKey {
					continue
				}
				if physicalRequired && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.PhysicalNetworkBoundedCapabilityV3) {
					return fail("physical network v3 capability required for " + expected.ConsumerID + "/" + child.ArtifactKind)
				}
				if orderRequired && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.PhysicalOrderCapabilityV1) {
					return fail("physical order capability required for " + expected.ConsumerID + "/" + child.ArtifactKind)
				}
				if found || !trafficCapabilityFactFresh(expected, fact, now) || cellRoutes && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.CellRoutesCapabilityV1) || cellDNS && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.CellDNSCapabilityV1) || authorityTransition && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.DNSAuthorityTransitionCapabilityV1) || len(sources) > 0 && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.DNSRouteSourcesCapabilityV1) {
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

func physicalNetworkCapabilityRequired(artifact model.PlatformArtifact) (bool, error) {
	var payload struct {
		Policy struct {
			DNSQueryPolicy *platformconfig.DNSQueryPolicy `json:"dns_query_policy"`
		} `json:"policy"`
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false, err
	}
	if payload.Policy.DNSQueryPolicy != nil {
		for _, route := range payload.Policy.DNSQueryPolicy.PhysicalRoutes {
			if route.Policy.Version == model.PhysicalBoundedNetworkPolicyVersion {
				return true, nil
			}
		}
	}
	return false, nil
}

func physicalOrderCapabilityRequired(artifact model.PlatformArtifact) (bool, error) {
	var payload struct {
		Policy struct {
			Rules []platformconfig.DNSAnswerRule `json:"dns_answer_rules"`
		} `json:"policy"`
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false, err
	}
	for _, rule := range payload.Policy.Rules {
		if rule.SelectionMode == model.DNSAnswerPolicyKindPhysicalOrder {
			return true, nil
		}
	}
	return false, nil
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

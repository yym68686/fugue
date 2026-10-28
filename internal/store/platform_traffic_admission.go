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
		required, err := physicalCapabilitiesRequired(child)
		if err != nil {
			return fail("physical network policy cannot be decoded")
		}
		physicalRequired, deliveryRequired, failureAwareRequired, comparableDeliveryRequired, orderRequired, projectionRequired := required.network, required.delivery, required.failureAware, required.comparableDelivery, required.order, required.projection
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
			for _, fact := range trafficCapabilityObservations(state, parent, child, *latest, now) {
				if fact.ConsumerID != expected.ConsumerID || fact.ArtifactKind != child.ArtifactKind || fact.ScopeKey != parent.ScopeKey {
					continue
				}
				if deliveryRequired && (!slices.Contains(fact.CompatibilityCapabilities, platformcontrol.PhysicalNetworkDeliveryCapabilityV4) || !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.PhysicalDynamicQualityCapabilityV1)) {
					return fail("required consumer lacks physical network delivery v4 capability")
				}
				if failureAwareRequired && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.PhysicalNetworkFailureAwareCapabilityV5) {
					return fail("required consumer lacks physical network failure-aware v5 capability")
				}
				if comparableDeliveryRequired && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.PhysicalComparableDeliveryCapabilityV6) {
					return fail("required consumer lacks physical comparable delivery v6 capability")
				}
				if required.business && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.BusinessEdgeExplorationCapabilityV1) {
					return fail("required consumer lacks business edge exploration v1 capability")
				}
				if required.businessDNSBaseline && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.BusinessDNSBaselineCapabilityV1) {
					return fail("required consumer lacks business DNS baseline v1 capability")
				}
				if required.availability && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.DNSAvailabilityCapabilityV1) {
					return fail("required consumer lacks DNS availability v1 capability")
				}
				if required.redundantAvailability && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.DNSAvailabilityRedundantCapabilityV2) {
					return fail("required consumer lacks redundant DNS availability v2 capability")
				}
				if required.retainedRoutes && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.DNSRetainedRoutesCapabilityV1) {
					return fail("retained route DNS capability required for " + expected.ConsumerID + "/" + child.ArtifactKind)
				}
				if required.businessUnified && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.BusinessUnifiedCapabilityV2) {
					return fail("required consumer lacks unified business decision v2 capability")
				}
				if physicalRequired && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.PhysicalNetworkBoundedCapabilityV3) {
					return fail("physical network v3 capability required for " + expected.ConsumerID + "/" + child.ArtifactKind)
				}
				if orderRequired && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.PhysicalOrderCapabilityV1) {
					return fail("physical order capability required for " + expected.ConsumerID + "/" + child.ArtifactKind)
				}
				if projectionRequired && !slices.Contains(fact.CompatibilityCapabilities, platformcontrol.PhysicalOrderProjectionCapabilityV1) {
					return fail("physical order projection capability required for " + expected.ConsumerID + "/" + child.ArtifactKind)
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

type physicalCapabilities struct{ network, delivery, failureAware, comparableDelivery, order, projection, business, businessDNSBaseline, businessUnified, availability, redundantAvailability, retainedRoutes bool }

func physicalCapabilitiesRequired(artifact model.PlatformArtifact) (physicalCapabilities, error) {
	var out physicalCapabilities
	var payload struct {
		Source struct {
			Intent struct {
				Transition *struct {
					ReferenceMode string `json:"reference_mode"`
				} `json:"route_authority_transition"`
			} `json:"intent"`
		} `json:"cell_dns_source"`
		Policy struct {
			Query *platformconfig.DNSQueryPolicy `json:"dns_query_policy"`
			Rules []platformconfig.DNSAnswerRule `json:"dns_answer_rules"`
		} `json:"policy"`
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return out, err
	}
	if err = json.Unmarshal(raw, &payload); err != nil {
		return out, err
	}
	out.retainedRoutes = payload.Source.Intent.Transition != nil && payload.Source.Intent.Transition.ReferenceMode == platformconfig.DNSRetainedGlobalReferenceV1
	if query := payload.Policy.Query; query != nil {
		out.availability = len(query.AvailabilityRoutes) > 0
		for _, route := range query.AvailabilityRoutes {
			out.redundantAvailability = out.redundantAvailability || route.Policy.Version == model.DNSAvailabilityRedundantVersion
		}
		out.projection = query.OrderedProjection != nil
		if query.DynamicQuality != nil {
			out.network = true
			out.delivery = true
			out.failureAware = query.DynamicQuality.Policy.Version == model.PhysicalFailureAwareNetworkPolicyVersion
			out.comparableDelivery = query.DynamicQuality.Policy.Version == model.PhysicalComparableDeliveryPolicyVersion
		}
		for _, route := range query.PhysicalRoutes {
			out.business = out.business || route.BusinessExploration != nil
			if route.BusinessExploration != nil {
				out.businessUnified = out.businessUnified || route.BusinessExploration.Version == model.BusinessEdgeUnifiedVersion
				for _, anchor := range route.BusinessExploration.InitialDNSPublications {
					out.businessDNSBaseline = out.businessDNSBaseline || anchor.BaselineEdgeID != ""
				}
			}
			out.network = out.network || model.IsBoundedPhysicalNetworkPolicy(route.Policy.Version)
			out.delivery = out.delivery || model.IsDeliveryPhysicalNetworkPolicy(route.Policy.Version)
			out.failureAware = out.failureAware || route.Policy.Version == model.PhysicalFailureAwareNetworkPolicyVersion
			out.comparableDelivery = out.comparableDelivery || route.Policy.Version == model.PhysicalComparableDeliveryPolicyVersion
		}
	}
	for _, rule := range payload.Policy.Rules {
		out.order = out.order || rule.SelectionMode == model.DNSAnswerPolicyKindPhysicalOrder
	}
	return out, nil
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
		if payload.Policy.DNSQueryPolicy.DynamicQuality != nil {
			return true, nil
		}
		for _, route := range payload.Policy.DNSQueryPolicy.PhysicalRoutes {
			if model.IsBoundedPhysicalNetworkPolicy(route.Policy.Version) {
				return true, nil
			}
		}
	}
	return false, nil
}

func physicalDeliveryCapabilityRequired(artifact model.PlatformArtifact) (bool, error) {
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
	if query := payload.Policy.DNSQueryPolicy; query != nil {
		if query.DynamicQuality != nil {
			return true, nil
		}
		for _, route := range query.PhysicalRoutes {
			if model.IsDeliveryPhysicalNetworkPolicy(route.Policy.Version) {
				return true, nil
			}
		}
	}
	return false, nil
}

func physicalOrderProjectionCapabilityRequired(artifact model.PlatformArtifact) (bool, error) {
	var payload struct {
		Policy struct {
			Query *platformconfig.DNSQueryPolicy `json:"dns_query_policy"`
		} `json:"policy"`
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false, err
	}
	return payload.Policy.Query != nil && payload.Policy.Query.OrderedProjection != nil, nil
}

func physicalFailureAwareCapabilityRequired(artifact model.PlatformArtifact) (bool, error) {
	return physicalNetworkVersionCapabilityRequired(artifact, model.PhysicalFailureAwareNetworkPolicyVersion)
}

func physicalComparableDeliveryCapabilityRequired(artifact model.PlatformArtifact) (bool, error) {
	return physicalNetworkVersionCapabilityRequired(artifact, model.PhysicalComparableDeliveryPolicyVersion)
}

func physicalNetworkVersionCapabilityRequired(artifact model.PlatformArtifact, version string) (bool, error) {
	var payload struct {
		Policy struct {
			Query *platformconfig.DNSQueryPolicy `json:"dns_query_policy"`
		} `json:"policy"`
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false, err
	}
	if query := payload.Policy.Query; query != nil {
		if query.DynamicQuality != nil && query.DynamicQuality.Policy.Version == version {
			return true, nil
		}
		for _, route := range query.PhysicalRoutes {
			if route.Policy.Version == version {
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

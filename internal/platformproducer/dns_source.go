package platformproducer

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

const DNSProducerInputSource = "verified-global-dns"

// This transitional source follows only one explicitly approved producer. It
// cannot select transport or substitute DNS receipts for another authority.
type DNSProducerSource struct {
	ProducerPolicy   model.PlatformPublicationPrecondition `json:"producer_policy"`
	PreviousTopology edgetopology.Intent                   `json:"previous_topology"`
	DNSNodeIDs       []string                              `json:"dns_node_ids"`
}

func validateDNSProducerPolicy(p Policy) error {
	if p.InputSource != DNSProducerInputSource || p.DNSSource == nil || p.PublicationRole != platformconfig.PublicationRoleCellDNS ||
		p.AuthorityCellID == "" || p.TargetScope != platformconfig.AuthorityCellScope(p.AuthorityCellID) ||
		platformconfig.ValidatePublicationRole(p.PublicationRole, p.AuthorityCellID, p.TargetScope) != nil ||
		p.RoutePlacementTransition != nil || p.StaticIntentArtifactID != "" || p.StaticIntentDigest != "" ||
		p.DNSPolicyArtifactID != "" || p.DNSPolicyDigest != "" || p.RequireApplicationDomains || p.RequireRouteDefaults || p.RequireDNSQueryPolicy || len(p.HostedZoneTemplates) != 0 {
		return fmt.Errorf("DNS producer requires only its explicit verified global source")
	}
	if p.Mode != "paused" && p.Mode != "shadow" && p.Mode != "serving" || p.IntervalSeconds < 30 || p.IntervalSeconds > 900 || p.RefreshSeconds < 120 || p.RefreshSeconds > 3600 || p.RefreshSeconds < p.IntervalSeconds {
		return fmt.Errorf("DNS producer schedule or mode invalid")
	}
	if p.Mode == "serving" && p.Serving == nil {
		return fmt.Errorf("DNS producer requires explicit serving gates")
	}
	if v := p.Serving; v != nil && (v.SinglePublication || !servingCohortRef.MatchString(v.CanaryRuleRef) || v.GrayMinSeconds < 1 || v.GrayMinSeconds > 1800 || v.FullMinSeconds < 1 || v.FullMinSeconds > 1800 || v.RolloutTimeoutSeconds < 30 || v.RolloutTimeoutSeconds > 3600 || v.RolloutTimeoutSeconds < max(v.GrayMinSeconds, v.FullMinSeconds)+p.IntervalSeconds) {
		return fmt.Errorf("DNS producer serving gates invalid")
	}
	source := p.DNSSource
	ref := source.ProducerPolicy
	validID := func(v string) bool { return v != "" && len(v) <= 256 && !strings.ContainsAny(v, " /\t\r\n") }
	if !validID(ref.ArtifactID) || !validID(ref.ReleaseID) || !ValidDigest(ref.ContentHash) || ref.FencingToken <= 0 || source.PreviousTopology.Validate() != nil || len(source.PreviousTopology.Cells) > 16 {
		return fmt.Errorf("DNS producer source identity or topology invalid")
	}
	seen := map[string]bool{}
	for _, cell := range source.PreviousTopology.Cells {
		if cell.ID == p.AuthorityCellID || cell.LegacyGroupID == "" || seen[cell.LegacyGroupID] {
			return fmt.Errorf("DNS producer requires distinct explicit routing aliases")
		}
		seen[cell.LegacyGroupID] = true
	}
	nodes := source.DNSNodeIDs
	if len(nodes) < 1 || len(nodes) > 16 || !slices.IsSorted(nodes) {
		return fmt.Errorf("DNS producer requires sorted bounded physical membership")
	}
	for i, node := range nodes {
		if !validID(node) || i > 0 && node == nodes[i-1] {
			return fmt.Errorf("DNS producer physical membership invalid")
		}
	}
	return nil
}

// ComposeDNSProjection is a pure projection of authenticated stored inputs.
// Callers authenticate the source and current selection; compiler and publication
// admission independently verify the retained source, full behavior and fences.
// No wall clock, network observation or runtime success is manufactured here.
func ComposeDNSProjection(p Policy, previous platformconfig.PreviousTrafficPublicationInput, intent platformconfig.PlatformIntent, policy platformconfig.PolicySnapshot, snapshot platformconfig.RuntimeSnapshot) (platformconfig.CompileRequest, error) {
	var out platformconfig.CompileRequest
	if err := validateDNSProducerPolicy(p); err != nil {
		return out, err
	}
	if previous.ProducerPolicy == nil || *previous.ProducerPolicy != p.DNSSource.ProducerPolicy || previous.Parent.ScopeKey != "global" || previous.Parent.Metadata[PolicyReleaseMetadata] != previous.ProducerPolicy.ReleaseID || intent.Scope != "global" || policy.Scope != "global" || intent.PublicationRole != "" || policy.PublicationRole != "" || intent.Generation != previous.Parent.Metadata["intent_generation"] || policy.Generation != previous.Parent.Metadata["policy_generation"] {
		return out, fmt.Errorf("DNS producer source input lineage differs")
	}
	if d, err := platformconfig.Digest(intent); err != nil || d != previous.Parent.Metadata["intent_digest"] {
		return out, fmt.Errorf("DNS producer source intent differs")
	}
	if d, err := platformconfig.Digest(policy); err != nil || d != previous.Parent.Metadata["policy_digest"] {
		return out, fmt.Errorf("DNS producer source policy differs")
	}
	// Detach nested data before removing only fields outside DNS authority.
	raw, err := json.Marshal(platformconfig.CompileRequest{Intent: intent, Policy: policy, RuntimeSnapshot: snapshot, PreviousTrafficPublication: &previous})
	if err != nil || json.Unmarshal(raw, &out) != nil {
		return out, fmt.Errorf("DNS producer input cannot be copied")
	}
	in, pol, facts := &out.Intent, &out.Policy, &out.RuntimeSnapshot
	topology := p.DNSSource.PreviousTopology.Clone()
	aliases, members := map[string]string{}, map[string]string{}
	for i, c := range topology.Cells {
		aliases[c.LegacyGroupID] = c.ID
		topology.Cells[i].LegacyGroupID = ""
	}
	for _, edge := range topology.Edges {
		members[edge.ID] = edge.AuthorityCellID
	}
	remap := func(v string) string {
		if next, ok := aliases[v]; ok {
			return next
		}
		return v
	}
	in.PublicationRole, in.AuthorityCellID, in.Scope = p.PublicationRole, p.AuthorityCellID, p.TargetScope
	in.Routes, in.TLS, in.CachePolicies, in.ApplicationDomains = nil, nil, nil, nil
	in.EdgeTopology, in.CellRoutePublications = &topology, nil
	in.DNSRouteSources = []platformconfig.DNSRouteSourceAuthorization{{ScopeKey: "global", PolicyArtifactID: previous.ProducerPolicy.ArtifactID, PolicyDigest: previous.ProducerPolicy.ContentHash}}
	in.RouteAuthorityTransition = &platformconfig.RouteAuthorityTransition{ReferenceMode: platformconfig.DNSRetainedGlobalReferenceV1, PreviousTopology: p.DNSSource.PreviousTopology.Clone(), PreviousPublication: previous.Reference}
	nodes := []string{}
	for i := range in.DNSConsumers {
		nodes = append(nodes, in.DNSConsumers[i].NodeID)
		in.DNSConsumers[i].EdgeGroupID = p.AuthorityCellID
	}
	slices.Sort(nodes)
	if !reflect.DeepEqual(nodes, p.DNSSource.DNSNodeIDs) {
		return out, fmt.Errorf("DNS producer source membership changed")
	}
	for i := range in.DNS {
		in.DNS[i].EdgeGroupID = remap(in.DNS[i].EdgeGroupID)
		in.DNS[i].FallbackEdgeGroupID = remap(in.DNS[i].FallbackEdgeGroupID)
	}
	projectedPolicy, err := projectDNSProducerPolicy(p, *in, *pol)
	if err != nil {
		return out, err
	}
	*pol = projectedPolicy
	facts.Origins, facts.Releases, facts.TLSDomains, facts.DNSPlacements, facts.Facts = nil, nil, nil, nil, nil
	for i := range facts.DNSEdgeEndpoints {
		e := &facts.DNSEdgeEndpoints[i]
		if members[e.EdgeID] == "" || members[e.EdgeID] != remap(e.EdgeGroupID) {
			return out, fmt.Errorf("DNS producer endpoint membership changed")
		}
		e.EdgeGroupID = members[e.EdgeID]
	}
	for i := range facts.DNSConsumers {
		if !slices.Contains(nodes, facts.DNSConsumers[i].NodeID) {
			return out, fmt.Errorf("DNS producer observation has an undeclared member")
		}
		facts.DNSConsumers[i].EdgeGroupID = p.AuthorityCellID
	}
	mapCandidates := func(candidates []platformconfig.DNSSelectionCandidate) error {
		for i := range candidates {
			c := &candidates[i]
			if members[c.EdgeID] == "" || members[c.EdgeID] != remap(c.EdgeGroupID) {
				return fmt.Errorf("DNS producer selection membership changed")
			}
			c.EdgeGroupID = members[c.EdgeID]
		}
		return nil
	}
	for i := range facts.DNSSelections {
		s := &facts.DNSSelections[i]
		s.PhysicalEvidence = nil
		s.SelectedEdgeGroupID, s.ShadowSelectedEdgeGroupID = remap(s.SelectedEdgeGroupID), remap(s.ShadowSelectedEdgeGroupID)
		if err := mapCandidates(s.Candidates); err != nil {
			return out, err
		}
		for j := range s.ScopedCandidates {
			sc := &s.ScopedCandidates[j]
			sc.SelectedEdgeGroupID = remap(sc.SelectedEdgeGroupID)
			if err := mapCandidates(sc.Candidates); err != nil {
				return out, err
			}
		}
	}
	// Generated identities bind the explicit configuration and exact source.
	seed, err := platformconfig.Digest(struct {
		Policy Policy
		Source platformconfig.PreviousTrafficPublicationReference
	}{p, previous.Reference})
	if err != nil {
		return out, err
	}
	in.Generation, pol.Generation = "dns-producer-intent-"+seed[7:], "dns-producer-policy-"+seed[7:]
	facts.IntentGeneration, facts.PolicyGeneration = in.Generation, pol.Generation
	return out, nil
}

// Project only the declared authority fields. The publication guard uses the
// same transformation on the retained signed policy so proof freshness, quorum,
// maximum stale time and every other constraint cannot be weakened by a caller.
func projectDNSProducerPolicy(p Policy, intent platformconfig.PlatformIntent, source platformconfig.PolicySnapshot) (platformconfig.PolicySnapshot, error) {
	raw, err := json.Marshal(source)
	var pol platformconfig.PolicySnapshot
	if err != nil || json.Unmarshal(raw, &pol) != nil {
		return pol, fmt.Errorf("DNS source policy invalid")
	}
	pol.PublicationRole, pol.AuthorityCellID, pol.Scope = p.PublicationRole, p.AuthorityCellID, p.TargetScope
	pol.RouteConstraints, pol.TrafficConstraints, pol.TLSReadiness = nil, nil, nil
	pol.TrafficRolloutCohorts = []platformconfig.TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{p.AuthorityCellID}}}
	topology, err := platformconfig.TrafficConsumerTopologyFromIntent(intent)
	if err != nil {
		return pol, err
	}
	pol.ConsumerTopologyDigest, err = platformconfig.Digest(topology)
	if err != nil {
		return pol, err
	}
	aliases := map[string]string{}
	for _, cell := range p.DNSSource.PreviousTopology.Cells {
		aliases[cell.LegacyGroupID] = cell.ID
	}
	remap := func(v string) string {
		if next, ok := aliases[v]; ok {
			return next
		}
		return v
	}
	for i := range pol.DNSAnswerRules {
		for j, group := range pol.DNSAnswerRules[i].PreferredEdgeGroups {
			pol.DNSAnswerRules[i].PreferredEdgeGroups[j] = remap(group)
		}
		for j, group := range pol.DNSAnswerRules[i].FallbackEdgeGroups {
			pol.DNSAnswerRules[i].FallbackEdgeGroups[j] = remap(group)
		}
	}
	return pol, nil
}

// ValidateDNSProducerOutput is also run inside the publication transaction. A
// valid DNS artifact is insufficient: every node must preserve the approved
// predecessor's complete DNS behavior, not merely its selected winning answer.
func ValidateDNSProducerOutput(p Policy, child model.PlatformArtifact) error {
	if err := validateDNSProducerPolicy(p); err != nil {
		return err
	}
	// Compiler output has a typed scope before persistence materializes ScopeKey.
	if child.ScopeKey == "" {
		child.ScopeKey = child.Scope.Key
	}
	payload, err := platformconfig.DecodeDNSArtifact(child)
	if err != nil {
		return fmt.Errorf("DNS producer output: %w", err)
	}
	if payload.CellDNSSource == nil || payload.PreviousTrafficPublication == nil {
		return fmt.Errorf("DNS producer output lacks retained source")
	}
	in, previous := payload.CellDNSSource.Intent, payload.PreviousTrafficPublication
	if child.ScopeKey != p.TargetScope || in.AuthorityCellID != p.AuthorityCellID || !platformconfig.DNSUsesRetainedRoutes(in) || !reflect.DeepEqual(in.RouteAuthorityTransition.PreviousTopology, p.DNSSource.PreviousTopology) || previous.ProducerPolicy == nil || *previous.ProducerPolicy != p.DNSSource.ProducerPolicy || previous.Parent.Metadata[PolicyReleaseMetadata] != previous.ProducerPolicy.ReleaseID {
		return fmt.Errorf("DNS producer output authority differs")
	}
	old, err := platformconfig.DecodeDNSArtifact(previous.DNS)
	if err != nil {
		return err
	}
	expected, err := projectDNSProducerPolicy(p, in, old.Policy)
	if err != nil {
		return err
	}
	expected.Generation = payload.Policy.Generation
	if !reflect.DeepEqual(platformconfig.NormalizePolicySnapshot(expected), payload.Policy) {
		return fmt.Errorf("DNS producer changed signed policy constraints")
	}
	nodes := []string{}
	for _, c := range in.DNSConsumers {
		nodes = append(nodes, c.NodeID)
	}
	slices.Sort(nodes)
	if !reflect.DeepEqual(nodes, p.DNSSource.DNSNodeIDs) {
		return fmt.Errorf("DNS producer output membership differs")
	}
	for _, node := range nodes {
		if err := platformconfig.ValidateDNSAuthorityHandoff(previous.DNS, child, node); err != nil {
			return err
		}
	}
	return nil
}

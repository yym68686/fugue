package dnsserver

import (
	"encoding/json"
	"errors"
	"reflect"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/routeprobe"
	"slices"
)

type dnsServingPayload struct {
	CellRoutePublications []platformconfig.CellRoutePublicationInput `json:"cell_route_publications,omitempty"`
	CellDNSSource         *platformconfig.CellDNSPlanSource          `json:"cell_dns_source,omitempty"`
	cellBindings          map[string]*model.TrafficReleaseBinding
	cellNodes             map[string]string
	Schema                string                           `json:"schema_version"`
	Generation            string                           `json:"generation"`
	Records               []platformconfig.DNSIntent       `json:"records"`
	Views                 []platformconfig.DNSConsumerView `json:"consumer_views"`
	Plan                  *platformconfig.DNSReadinessPlan `json:"readiness_plan"`
	Queries               []platformconfig.DNSQueryView    `json:"query_views"`
	Policy                platformconfig.PolicySnapshot    `json:"policy"`
	Lineage               platformconfig.Lineage           `json:"lineage"`
}

func (s *Service) verifyDNSServingRelease(parent model.PlatformArtifact, c dnsPlatformCandidate) (dnsServingPayload, string, error) {
	return s.verifyDNSRelease(parent, c, true)
}

func (s *Service) verifyDNSRelease(parent model.PlatformArtifact, c dnsPlatformCandidate, serving bool) (dnsServingPayload, string, error) {
	fail := func() (dnsServingPayload, string, error) {
		return dnsServingPayload{}, "", errors.New("DNS serving ReleaseSet binding invalid")
	}
	a, child, r := c.Assignment, c.Artifact, c.Release
	if a.ReleaseChannel != "gray" && a.ReleaseChannel != "full" && (serving || a.ReleaseChannel != "shadow") {
		return fail()
	}
	if _, err := s.verifyPlatformDNSCandidate(c, a); err != nil {
		return dnsServingPayload{}, "", err
	}
	keys := s.platformDNSKeys()
	if parent.ArtifactKind != model.PlatformArtifactKindReleaseSet || parent.ID != a.ReleaseSetID || parent.Status != model.PlatformArtifactStatusValidated || parent.ScopeKey != a.ScopeKey || parent.ScopeKey != r.ScopeKey || parent.Generation != r.Generation || !platformsafety.EvaluateArtifactIntegrity(parent, keys).Pass {
		return fail()
	}
	var set platformconfig.ReleaseSet
	raw, _ := json.Marshal(parent.Content)
	_, compositionErr := platformconfig.ValidateReleaseComposition(parent)
	if json.Unmarshal(raw, &set) != nil || set.SchemaVersion != platformconfig.SchemaVersion || set.Generation != parent.Generation || set.Scope != parent.ScopeKey || compositionErr != nil {
		return fail()
	}
	routeID := ""
	seen := map[string]bool{}
	ids := map[string]bool{}
	member := false
	for i, id := range set.ArtifactIDs {
		kind := set.ArtifactKinds[i]
		if id == "" || ids[id] || seen[kind] {
			return fail()
		}
		ids[id], seen[kind] = true, true
		switch kind {
		case model.PlatformArtifactKindEdgeRouteBundle:
			routeID = id
		case child.ArtifactKind:
			member = id == child.ID
		case model.PlatformArtifactKindCaddyRouteConfig:
		default:
			return fail()
		}
	}
	if (set.PublicationRole != platformconfig.PublicationRoleCellDNS && (routeID == "" || !seen[model.PlatformArtifactKindCaddyRouteConfig])) || !member || !reflect.DeepEqual(set.Lineage, platformconfig.LineageFromArtifact(child)) || !reflect.DeepEqual(set.Lineage, platformconfig.LineageFromArtifact(parent)) || platformconfig.ValidateTrafficCohortProjection(parent, child) != nil {
		return fail()
	}
	if topology, err := platformconfig.TrafficConsumersFromRelease(parent); err != nil || topology != nil && (topology.AuthorityCellID != s.Config.EdgeGroupID || !slices.Contains(topology.DNSNodeIDs, s.Config.DNSNodeID)) {
		return fail()
	}
	if r.ReleaseChannel == "gray" {
		groups, err := platformconfig.ResolveTrafficCanary(parent, r.CanaryRuleRef)
		if err != nil || !platformconfig.TrafficCanaryContains(groups, s.Config.EdgeGroupID) {
			return fail()
		}
	} else if r.CanaryRuleRef != "" {
		return fail()
	}
	var p dnsServingPayload
	raw, _ = json.Marshal(child.Content)
	if json.Unmarshal(raw, &p) != nil || !reflect.DeepEqual(p.Lineage, set.Lineage) || p.Plan == nil || p.Policy.DNSReadiness == nil || len(p.Queries) == 0 || len(p.Policy.DNSAuthorities) == 0 {
		return fail()
	}
	if err := populateDNSCellBindings(&p); err != nil {
		return fail()
	}
	// Independent DNS has already replayed its entire plan from signed Cell
	// routes in verifyPlatformDNSCandidate. That replay validates active quorum
	// and explicit inactive omissions. The legacy approximation below cannot
	// distinguish a valid omission from a missing required target.
	if set.PublicationRole != platformconfig.PublicationRoleCellDNS && !dnsEdgeSelectionRequirementsComplete(p.Plan, p.Policy.EdgeSelectionConstraints) {
		return fail()
	}
	consumers := map[string]*platformconfig.DNSConsumerIntent{}
	for _, v := range p.Views {
		c := consumers[v.NodeID]
		if c == nil {
			c = &platformconfig.DNSConsumerIntent{NodeID: v.NodeID}
			consumers[v.NodeID] = c
		}
		c.Zones = append(c.Zones, v.Zone)
	}
	declared := []platformconfig.DNSConsumerIntent{}
	for _, c := range consumers {
		declared = append(declared, *c)
	}
	if platformconfig.ValidateDNSAuthorityOwnership(p.Policy.DNSAuthorities, declared) != nil {
		return fail()
	}
	return p, routeID, nil
}

func dnsEdgeSelectionRequirementsComplete(plan *platformconfig.DNSReadinessPlan, constraints []platformconfig.EdgeSelectionConstraint) bool {
	if len(constraints) == 0 {
		return true
	}
	if plan == nil {
		return false
	}
	for _, constraint := range constraints {
		edges := make(map[string]bool)
		probes := make(map[string]platformconfig.DNSReadinessProbe)
		for _, probe := range plan.Probes {
			if probe.Hostname == constraint.Hostname {
				probes[probe.ID] = probe
			}
		}
		for _, record := range plan.Records {
			for _, target := range record.Targets {
				for _, id := range target.ProbeIDs {
					if probe, ok := probes[id]; ok && probe.EdgeID == target.EdgeID && probe.EdgeGroupID == target.EdgeGroupID && probe.Address == target.Address {
						edges[target.EdgeID] = true
					}
				}
			}
		}
		if len(edges) < constraint.MinCandidates {
			return false
		}
	}
	return true
}
func (s *Service) platformDNSKeys() bundleauth.Keyring {
	return bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)
}

func dnsProofMatchesRelease(proof routeprobe.Proof, parent model.PlatformArtifact, c dnsPlatformCandidate, routeID string, payload ...dnsServingPayload) bool {
	if parent.Content["publication_role"] == platformconfig.PublicationRoleCellDNS {
		if len(payload) != 1 || payload[0].cellNodes[proof.EdgeID] != proof.GroupID {
			return false
		}
		b := payload[0].cellBindings[proof.GroupID]
		return b != nil && reflect.DeepEqual(b, proof.TrafficRelease)
	}
	b := proof.TrafficRelease
	return b != nil && b.ReleaseSetID == parent.ID && b.ReleaseSetDigest == parent.ContentHash && b.RouteArtifactID == routeID && b.PolicyDigest == c.Artifact.Metadata["policy_digest"] && b.IntentDigest == c.Artifact.Metadata["intent_digest"] && b.InputSnapshotDigest == c.Artifact.Metadata["input_snapshot_digest"] && b.ReleaseID == c.Release.ID && b.ReleaseChannel == c.Release.ReleaseChannel && b.FencingToken == c.Release.FencingToken && b.ScopeKey == c.Assignment.ScopeKey
}

func populateDNSCellBindings(p *dnsServingPayload) error {
	if len(p.CellRoutePublications) == 0 {
		return nil
	}
	p.cellBindings = map[string]*model.TrafficReleaseBinding{}
	p.cellNodes = map[string]string{}
	for _, input := range p.CellRoutePublications {
		b, err := platformconfig.CellRoutePublicationBinding(input)
		if err != nil {
			return err
		}
		p.cellBindings[input.Reference.AuthorityCellID] = b
		t, err := platformconfig.TrafficConsumersFromRelease(input.Parent)
		if err != nil {
			return err
		}
		for _, node := range t.EdgeNodeIDs {
			p.cellNodes[node] = t.AuthorityCellID
		}
	}
	return nil
}

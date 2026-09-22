package api

import (
	"errors"
	"slices"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// Delegation is derived from the same pinned declarations that produced the
// verified serving configuration. Current drafts, workload env and inventory
// cannot choose nameserver membership or rename glue records.
func (s *Server) verifiedDNSDelegationHints(zone string) (dnsDelegationPlanHint, error) {
	fail := func() (dnsDelegationPlanHint, error) {
		return dnsDelegationPlanHint{}, errors.New("verified DNS delegation configuration unavailable")
	}
	parent, found, err := s.verifiedPlatformArtifactForScope(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || !found || !s.validateReleaseSetReferences(parent).Pass {
		return fail()
	}
	policyReleaseID := parent.Metadata[platformproducer.PolicyReleaseMetadata]
	release, err := s.store.GetPlatformArtifactRelease(policyReleaseID)
	if err != nil || release.ID != policyReleaseID {
		return fail()
	}
	authority, err := s.store.GetPlatformArtifact(release.ArtifactID)
	if err != nil || authority.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(authority, s.bundleKeyring()).Pass {
		return fail()
	}
	producer, err := platformproducer.Decode(authority)
	if err != nil || producer.InputSource != "business-static-intent" || producer.StaticIntentArtifactID != parent.Metadata[platformproducer.StaticIntentIDMetadata] || producer.StaticIntentDigest != parent.Metadata[platformproducer.StaticIntentDigestMetadata] || producer.DNSPolicyArtifactID != parent.Metadata[platformproducer.DNSPolicyIDMetadata] || producer.DNSPolicyDigest != parent.Metadata[platformproducer.DNSPolicyDigestMetadata] {
		return fail()
	}
	intent, err := s.loadStaticPlatformIntent(producer.StaticIntentArtifactID, producer.StaticIntentDigest)
	if err != nil {
		return fail()
	}
	policyArtifact, err := s.store.GetPlatformArtifact(producer.DNSPolicyArtifactID)
	if err != nil || policyArtifact.ContentHash != producer.DNSPolicyDigest || policyArtifact.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(policyArtifact, s.bundleKeyring()).Pass {
		return fail()
	}
	policy, err := platformproducer.DecodeProjectionPolicy(policyArtifact, intent.Consumers, producer.HostedZoneTemplates)
	if err != nil {
		return fail()
	}
	hint, err := dnsDelegationHintsFromPinned(zone, intent, policy, producer.HostedZoneTemplates, time.Now().UTC())
	if err != nil {
		return fail()
	}
	current, found, err := s.verifiedPlatformArtifactForScope(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || !found || current.ID != parent.ID || current.ContentHash != parent.ContentHash {
		return fail()
	}
	return hint, nil
}

func dnsDelegationHintsFromPinned(zone string, intent platformproducer.StaticIntentInput, policy platformproducer.ProjectionPolicyInput, templates []platformproducer.HostedZoneTemplate, now time.Time) (dnsDelegationPlanHint, error) {
	zone = normalizeExternalAppDomain(zone)
	if zone == "" {
		return dnsDelegationPlanHint{}, errors.New("delegation zone required")
	}
	templateByNode := map[string]string{}
	for _, t := range templates {
		if templateByNode[t.NodeID] != "" {
			return dnsDelegationPlanHint{}, errors.New("ambiguous delegation template")
		}
		templateByNode[t.NodeID] = t.TemplateZone
	}
	names := []string{}
	for _, consumer := range intent.Consumers {
		sourceZone := zone
		if !slices.Contains(consumer.Zones, zone) {
			sourceZone = templateByNode[consumer.NodeID]
		}
		if sourceZone == "" {
			return dnsDelegationPlanHint{}, errors.New("hosted zone lacks explicit authority template")
		}
		found := false
		for _, a := range policy.Authorities {
			if a.NodeID == consumer.NodeID && a.Zone == sourceZone {
				if found {
					return dnsDelegationPlanHint{}, errors.New("ambiguous authority")
				}
				found = true
				names = append(names, a.Nameservers...)
			}
		}
		if !found {
			return dnsDelegationPlanHint{}, errors.New("delegation authority missing")
		}
	}
	names = uniqueSortedStrings(names)
	if len(names) == 0 {
		return dnsDelegationPlanHint{}, errors.New("delegation nameservers absent")
	}
	// Use only declared nameserver A records. Static apex NS records cannot
	// override the signed authority policy, and expired glue cannot be revived.
	records := []model.EdgeDNSRecord{}
	for _, r := range intent.DNS {
		if r.Type != "A" || !slices.Contains(names, normalizeExternalAppDomain(r.Name)) || r.Status != "" && r.Status != model.EdgeRouteStatusActive {
			continue
		}
		copy := r
		copy.Values = nil
		for _, v := range r.Values {
			expires, leased := r.ValueExpirations[v]
			if !leased || now.Before(expires) {
				copy.Values = append(copy.Values, v)
			}
		}
		if len(copy.Values) == 0 {
			return dnsDelegationPlanHint{}, errors.New("declared nameserver glue has expired")
		}
		records = append(records, copy)
	}
	hint := dnsDelegationPlanHints(zone, records, names)
	hint.ConsumerGroups = map[string]string{}
	for _, consumer := range intent.Consumers {
		hint.ConsumerGroups[consumer.NodeID] = consumer.EdgeGroupID
	}
	return hint, nil
}

// Multi-zone consumers report one physical serving snapshot. Project that
// fresh fact onto a declared zone; stale legacy aliases cannot shadow it.
func dnsNodesForVerifiedDelegation(nodes []model.DNSNode, zone string, owners map[string]string) []model.DNSNode {
	byNode := map[string]model.DNSNode{}
	for _, node := range nodes {
		physical := dnsNodePolicyLookupID(node)
		if owners[physical] == "" || owners[physical] != node.EdgeGroupID {
			continue
		}
		if node.ID != physical && normalizeExternalAppDomain(node.Zone) != zone {
			continue
		}
		if prior, exists := byNode[physical]; exists && (prior.ID == physical || node.ID != physical) {
			continue
		}
		node.PhysicalNodeID, node.Zone = physical, zone
		byNode[physical] = node
	}
	out := make([]model.DNSNode, 0, len(byNode))
	for _, node := range byNode {
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EdgeGroupID != out[j].EdgeGroupID {
			return out[i].EdgeGroupID < out[j].EdgeGroupID
		}
		return out[i].ID < out[j].ID
	})
	return out
}

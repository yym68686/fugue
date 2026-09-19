package api

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func (s *Server) capturePlatformIntentForProducer(ctx context.Context, principal model.Principal, policy platformproducer.Policy) (platformIntentProjectionResponse, error) {
	if policy.InputSource == "business-migration" {
		return s.capturePlatformIntent(ctx, principal)
	}
	static, err := s.loadStaticPlatformIntent(policy.StaticIntentArtifactID, policy.StaticIntentDigest)
	if err != nil {
		return platformIntentProjectionResponse{}, err
	}
	var dns *platformproducer.DNSPolicyInput
	if policy.DNSPolicyArtifactID != "" {
		a, err := s.store.GetPlatformArtifact(policy.DNSPolicyArtifactID)
		if err != nil {
			return platformIntentProjectionResponse{}, err
		}
		if a.ID != policy.DNSPolicyArtifactID || a.ContentHash != policy.DNSPolicyDigest || a.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(a) != nil || !platformsafety.EvaluateArtifactIntegrity(a, s.bundleKeyring()).Pass {
			return platformIntentProjectionResponse{}, fmt.Errorf("DNS policy reference is not trusted")
		}
		p, err := platformproducer.DecodeDNSInputs(a, static.Consumers, policy.HostedZoneTemplates)
		if err != nil {
			return platformIntentProjectionResponse{}, err
		}
		dns = &p
	}
	return s.capturePlatformIntentWithInputs(ctx, principal, static, dns, policy.HostedZoneTemplates)
}

func projectPinnedDNSInputs(result *platformIntentProjectionResponse, declared []platformconfig.DNSConsumerIntent, p platformproducer.DNSPolicyInput, templates []platformproducer.HostedZoneTemplate, nodes []model.DNSNode, hosted []model.HostedZone, now time.Time) error {
	consumers := platformconfig.NormalizePlatformIntent(platformconfig.PlatformIntent{DNSConsumers: declared}).DNSConsumers
	authority := append([]platformconfig.DNSAuthorityPolicy(nil), p.Authorities...)
	zones := edgeDNSPublishableHostedZoneNames(hosted)
	for _, t := range templates {
		var template platformconfig.DNSAuthorityPolicy
		for _, a := range p.Authorities {
			if a.NodeID == t.NodeID && a.Zone == t.TemplateZone {
				template = a
			}
		}
		if template.NodeID == "" {
			return fmt.Errorf("hosted zone template absent")
		}
		for i, c := range consumers {
			if c.NodeID != t.NodeID {
				continue
			}
			for _, zone := range zones {
				if slices.Contains(consumers[i].Zones, zone) {
					continue
				}
				consumers[i].Zones = append(consumers[i].Zones, zone)
				a := template
				a.Zone = zone
				authority = append(authority, a)
			}
		}
	}
	byNode := map[string]platformconfig.DNSConsumerIntent{}
	for _, c := range consumers {
		byNode[c.NodeID] = c
	}
	observations := map[string]platformconfig.DNSConsumerObservation{}
	for _, node := range nodes {
		id := firstNonEmpty(node.PhysicalNodeID, node.ID)
		c, ok := byNode[id]
		if !ok || c.EdgeGroupID != node.EdgeGroupID {
			return fmt.Errorf("DNS runtime inventory has undeclared owner")
		}
		if !slices.Contains(c.Zones, node.Zone) {
			continue
		}
		fact := platformconfig.DNSConsumerObservation{NodeID: id, EdgeGroupID: c.EdgeGroupID, ObservedAt: now}
		if node.PublicIPv4 != "" {
			fact.A = []string{node.PublicIPv4}
		}
		if node.PublicIPv6 != "" {
			fact.AAAA = []string{node.PublicIPv6}
		}
		if old, found := observations[id]; found && !reflect.DeepEqual(old, fact) {
			return fmt.Errorf("DNS runtime aliases disagree on endpoint")
		}
		observations[id] = fact
	}
	facts := []platformconfig.DNSConsumerObservation{}
	for _, c := range consumers {
		f, ok := observations[c.NodeID]
		if !ok {
			return fmt.Errorf("declared DNS consumer endpoint unavailable")
		}
		facts = append(facts, f)
	}
	draft := *result
	draft.Intent = platformconfig.NormalizePlatformIntent(result.Intent)
	draft.Intent.DNSConsumers = consumers
	draft.RuntimeSnapshot = result.RuntimeSnapshot
	draft.RuntimeSnapshot.DNSConsumers = facts
	draft.CapturedAt = now
	draft.RuntimeSnapshot.CapturedAt = &draft.CapturedAt
	if _, err := platformconfig.CompileDNSConsumerViews(consumers, draft.RuntimeSnapshot, draft.Intent.DNS); err != nil {
		return err
	}
	if err := platformconfig.ValidateDNSAuthorityOwnership(authority, consumers); err != nil {
		return err
	}
	if err := platformconfig.ValidateDNSClientPolicyOwnership(p.Clients, consumers); err != nil {
		return err
	}
	draft.Intent = platformconfig.NormalizePlatformIntent(draft.Intent)
	gen, err := platformconfig.PlatformIntentGeneration(draft.Intent)
	if err != nil {
		return err
	}
	draft.Intent.Generation = gen
	draft.RuntimeSnapshot.IntentGeneration = gen
	draft.Policy.DNSAuthorities = authority
	draft.Policy.DNSClientPolicies = p.Clients
	draft.Policy = platformconfig.NormalizePolicySnapshot(draft.Policy)
	gen, err = platformconfig.PolicySnapshotGeneration(draft.Policy)
	if err != nil {
		return err
	}
	draft.Policy.Generation = gen
	draft.RuntimeSnapshot.PolicyGeneration = gen
	*result = draft
	return nil
}

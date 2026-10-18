package api

import (
	"encoding/json"
	"fmt"
	"net"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func (s *Server) applyGlobalOfflineConsumerIntent(parent model.PlatformArtifact, topology platformcontrol.ExpectedConsumerTopology) (platformcontrol.ExpectedConsumerTopology, error) {
	trusted := func(artifact model.PlatformArtifact) bool {
		return artifact.Status == model.PlatformArtifactStatusValidated && s.store.VerifyPlatformArtifactIntegrity(artifact) == nil && platformsafety.EvaluateArtifactIntegrity(artifact, s.bundleKeyring()).Pass
	}
	fail := func() (platformcontrol.ExpectedConsumerTopology, error) {
		return platformcontrol.ExpectedConsumerTopology{}, fmt.Errorf("global offline consumer intent lacks exact signed source or candidate binding")
	}
	if parent.ScopeKey != "global" || parent.Metadata[platformproducer.DNSPolicyIDMetadata] == "" {
		return topology, nil
	}
	source, err := s.store.GetPlatformArtifact(parent.Metadata[platformproducer.DNSPolicyIDMetadata])
	if err != nil || !trusted(source) || source.ContentHash != parent.Metadata[platformproducer.DNSPolicyDigestMetadata] {
		return fail()
	}
	if source.Content["offline_edges"] == nil {
		return topology, nil
	}
	if parent.CreatedByType != model.ActorTypeBootstrap || parent.CreatedByID != platformproducer.Actor || parent.Content["consumer_topology"] != nil || parent.Content["publication_role"] != nil {
		return fail()
	}
	release, err := s.store.GetPlatformArtifactRelease(parent.Metadata[platformproducer.PolicyReleaseMetadata])
	if err != nil || release.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || release.ScopeKey != platformproducer.Scope || release.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow || release.FencingToken <= 0 {
		return fail()
	}
	authority, err := s.store.GetPlatformArtifact(release.ArtifactID)
	if err != nil || !trusted(authority) || authority.Generation != release.Generation {
		return fail()
	}
	producer, err := platformproducer.Decode(authority)
	if err != nil || producer.TargetScope != "global" || producer.PublicationRole != "" || producer.Mode != "serving" || producer.DNSPolicyArtifactID != source.ID || producer.DNSPolicyDigest != source.ContentHash || producer.StaticIntentArtifactID != parent.Metadata[platformproducer.StaticIntentIDMetadata] || producer.StaticIntentDigest != parent.Metadata[platformproducer.StaticIntentDigestMetadata] {
		return fail()
	}
	intent, err := s.loadStaticPlatformIntent(producer.StaticIntentArtifactID, producer.StaticIntentDigest)
	if err != nil {
		return fail()
	}
	policy, err := platformproducer.DecodeProjectionPolicy(source, intent.Consumers, producer.HostedZoneTemplates)
	if err != nil {
		return fail()
	}
	if len(policy.OfflineEdges) == 0 {
		return topology, nil
	}
	if _, err := platformproducer.EffectiveOfflineHolds(topology.EdgeNodes, policy.OfflineEdges, nil, parent.Provenance.SignedAt); err != nil {
		return fail()
	}
	policy.OfflineEdges, err = platformproducer.PublicationOfflineHolds(parent, policy.OfflineEdges)
	if err != nil {
		return fail()
	}
	identities, ok := parent.Content["artifact_ids"].([]any)
	kinds, kindsOK := parent.Content["artifact_kinds"].([]any)
	if !ok || !kindsOK || len(identities) != len(kinds) {
		return fail()
	}
	var dns *model.PlatformArtifact
	for index, identity := range identities {
		if kinds[index] != model.PlatformArtifactKindDNSAnswerBundle {
			continue
		}
		identifier, ok := identity.(string)
		if !ok {
			return fail()
		}
		child, err := s.store.GetPlatformArtifact(identifier)
		if err != nil {
			return fail()
		}
		if child.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || dns != nil || !trusted(child) {
			return fail()
		}
		dns = &child
	}
	if dns == nil || validateOfflineDNSCandidate(*dns, policy.OfflineEdges) != nil {
		return fail()
	}
	selected, err := platformproducer.SelectDNSCandidateEndpoints(topology.EdgeNodes, policy.OfflineEdges)
	if err != nil {
		return fail()
	}
	topology.EdgeNodes = selected
	return topology, nil
}

func validateOfflineDNSCandidate(artifact model.PlatformArtifact, holds []platformproducer.OfflineEdge) error {
	var plan *platformconfig.DNSReadinessPlan
	raw, err := json.Marshal(artifact.Content["readiness_plan"])
	if err != nil || json.Unmarshal(raw, &plan) != nil || plan == nil || len(plan.Records) == 0 || len(plan.Probes) == 0 || platformproducer.ValidateOfflineEdges(holds) != nil {
		return fmt.Errorf("complete offline candidate plan required")
	}
	for _, hold := range holds {
		for _, probe := range plan.Probes {
			address, _, _ := net.SplitHostPort(probe.Address)
			if probe.EdgeID == hold.EdgeID || address == hold.PublicIPv4 || probe.Address == hold.PublicIPv4 {
				return fmt.Errorf("held endpoint remains in candidate probe plan")
			}
		}
		for _, record := range plan.Records {
			for _, target := range record.Targets {
				address, _, _ := net.SplitHostPort(target.Address)
				if target.EdgeID == hold.EdgeID || address == hold.PublicIPv4 || target.Address == hold.PublicIPv4 {
					return fmt.Errorf("held endpoint remains in candidate record")
				}
			}
		}
	}
	return nil
}

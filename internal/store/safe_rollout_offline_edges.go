package store

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// SelectSafeRolloutServingEdges follows the exact verified serving publication.
// A draft or merely published producer hold cannot remove a rollout observer.
// The caller still proves fresh application traffic and zero old-Pod connections.
func (s *Store) SelectSafeRolloutServingEdges(nodes []model.EdgeNode) ([]model.EdgeNode, error) {
	parent, release, found, err := s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", model.PlatformArtifactReleaseChannelFull)
	if err != nil || !found {
		return nodes, err
	}
	sourceID := parent.Metadata[platformproducer.DNSPolicyIDMetadata]
	if sourceID == "" {
		return nodes, nil
	}
	source, err := s.GetPlatformArtifact(sourceID)
	if err != nil {
		return nil, err
	}
	if source.Content["offline_edges"] == nil {
		return nodes, nil
	}
	fail := func() ([]model.EdgeNode, error) {
		return nil, fmt.Errorf("offline rollout membership lacks verified serving publication evidence")
	}
	trusted := func(a model.PlatformArtifact) bool {
		return a.Status == model.PlatformArtifactStatusValidated && s.VerifyPlatformArtifactIntegrity(a) == nil
	}
	lkg, err := s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil {
		return nil, err
	}
	if lkg == nil || !trusted(parent) || !trusted(source) || source.ContentHash != parent.Metadata[platformproducer.DNSPolicyDigestMetadata] ||
		release.Status != model.PlatformArtifactReleaseStatusActive || release.VerificationState != model.PlatformArtifactVerificationStateVerified || release.ArtifactID != parent.ID || release.Generation != parent.Generation || release.FencingToken <= 0 ||
		lkg.VerifiedByReleaseID != release.ID || !platformsafety.EvaluatePlatformLKGSnapshot(*lkg, parent, s.platformArtifactSigningKeyring(), time.Now().UTC()).Pass ||
		parent.CreatedByType != model.ActorTypeBootstrap || parent.CreatedByID != platformproducer.Actor || parent.Content["consumer_topology"] != nil || parent.Content["publication_role"] != nil {
		return fail()
	}
	authorityRelease, err := s.GetPlatformArtifactRelease(parent.Metadata[platformproducer.PolicyReleaseMetadata])
	if err != nil {
		return fail()
	}
	authority, err := s.GetPlatformArtifact(authorityRelease.ArtifactID)
	if err != nil || !trusted(authority) || authorityRelease.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || authorityRelease.ScopeKey != platformproducer.Scope || authorityRelease.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow || authorityRelease.FencingToken <= 0 || authority.Generation != authorityRelease.Generation {
		return fail()
	}
	producer, err := platformproducer.Decode(authority)
	if err != nil || producer.TargetScope != "global" || producer.PublicationRole != "" || producer.Mode != "serving" || producer.DNSPolicyArtifactID != source.ID || producer.DNSPolicyDigest != source.ContentHash || producer.StaticIntentArtifactID != parent.Metadata[platformproducer.StaticIntentIDMetadata] || producer.StaticIntentDigest != parent.Metadata[platformproducer.StaticIntentDigestMetadata] {
		return fail()
	}
	static, err := s.GetPlatformArtifact(producer.StaticIntentArtifactID)
	if err != nil || !trusted(static) || static.ContentHash != producer.StaticIntentDigest {
		return fail()
	}
	intent, err := platformproducer.DecodeStaticIntent(static)
	if err != nil {
		return fail()
	}
	policy, err := platformproducer.DecodeProjectionPolicy(source, intent.Consumers, producer.HostedZoneTemplates)
	if err != nil {
		return fail()
	}
	if len(policy.OfflineEdges) == 0 {
		return nodes, nil
	}
	ids, ok := parent.Content["artifact_ids"].([]any)
	kinds, kindsOK := parent.Content["artifact_kinds"].([]any)
	if !ok || !kindsOK || len(ids) != len(kinds) {
		return fail()
	}
	dnsFound := false
	for i, id := range ids {
		if kinds[i] != model.PlatformArtifactKindDNSAnswerBundle {
			continue
		}
		id, ok := id.(string)
		if !ok || dnsFound {
			return fail()
		}
		dns, err := s.GetPlatformArtifact(id)
		if err != nil || dns.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || dns.ScopeKey != "global" || !trusted(dns) {
			return fail()
		}
		if !safeRolloutDNSExcludesOfflineEndpoints(dns, policy.OfflineEdges) {
			return fail()
		}
		dnsFound = true
	}
	if !dnsFound {
		return fail()
	}
	selected, err := platformproducer.SelectDNSCandidateEndpoints(nodes, policy.OfflineEdges)
	if err != nil {
		return nil, err
	}
	// Do not return evidence assembled across a concurrent publication switch.
	_, current, found, err := s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", model.PlatformArtifactReleaseChannelFull)
	if err != nil || !found || current.ID != release.ID || current.FencingToken != release.FencingToken || current.Status != release.Status || current.VerificationState != release.VerificationState {
		return fail()
	}
	return selected, nil
}

func safeRolloutDNSExcludesOfflineEndpoints(artifact model.PlatformArtifact, holds []platformproducer.OfflineEdge) bool {
	var plan *platformconfig.DNSReadinessPlan
	raw, err := json.Marshal(artifact.Content["readiness_plan"])
	if err != nil || json.Unmarshal(raw, &plan) != nil || plan == nil || len(plan.Records) == 0 || len(plan.Probes) == 0 || platformproducer.ValidateOfflineEdges(holds) != nil {
		return false
	}
	for _, hold := range holds {
		matches := func(id, address string) bool {
			host, _, _ := net.SplitHostPort(address)
			return id == hold.EdgeID || address == hold.PublicIPv4 || host == hold.PublicIPv4
		}
		for _, probe := range plan.Probes {
			if matches(probe.EdgeID, probe.Address) {
				return false
			}
		}
		for _, record := range plan.Records {
			for _, target := range record.Targets {
				if matches(target.EdgeID, target.Address) {
					return false
				}
			}
		}
	}
	return true
}

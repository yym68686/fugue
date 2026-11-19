package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// ErrSafeRolloutServingPublicationPending means membership cannot yet be proved
// across an in-progress publication. Callers may wait, but must not use an
// unverified hold or an older publication to exclude an observer.
var ErrSafeRolloutServingPublicationPending = errors.New("offline rollout membership is waiting for verified serving publication")

// SafeRolloutServingMembershipError identifies the failed check without
// exposing artifact contents or foreign tenant traffic data in release logs.
type SafeRolloutServingMembershipError struct {
	Check string
}

func (e *SafeRolloutServingMembershipError) Error() string {
	return "offline rollout membership lacks verified serving publication evidence: " + e.Check
}

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
		if errors.Is(err, ErrNotFound) {
			return nil, &SafeRolloutServingMembershipError{Check: "dns_policy_unavailable"}
		}
		return nil, fmt.Errorf("read offline rollout DNS policy: %w", err)
	}
	if source.Content["offline_edges"] == nil {
		return nodes, nil
	}
	fail := func(check string) ([]model.EdgeNode, error) {
		return nil, &SafeRolloutServingMembershipError{Check: check}
	}
	trusted := func(a model.PlatformArtifact) bool {
		return a.Status == model.PlatformArtifactStatusValidated && s.VerifyPlatformArtifactIntegrity(a) == nil
	}
	lkg, err := s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil {
		return nil, err
	}
	if !trusted(parent) {
		return fail("release_set_integrity")
	}
	if !trusted(source) || source.ContentHash != parent.Metadata[platformproducer.DNSPolicyDigestMetadata] {
		return fail("dns_policy_integrity")
	}
	if release.Status != model.PlatformArtifactReleaseStatusActive || release.ArtifactID != parent.ID || release.Generation != parent.Generation || release.FencingToken <= 0 ||
		parent.CreatedByType != model.ActorTypeBootstrap || parent.CreatedByID != platformproducer.Actor || parent.Content["consumer_topology"] != nil || parent.Content["publication_role"] != nil {
		return fail("global_publication_identity")
	}
	// A new full publication becomes active before consumer ACKs establish its
	// LKG. This is expected during each canary step, not permission to omit any
	// observer. Keep waiting within the caller's existing deadline.
	if release.VerificationState == model.PlatformArtifactVerificationStateServingUnverified {
		return nil, fmt.Errorf("%w: active release %s is awaiting verification", ErrSafeRolloutServingPublicationPending, release.ID)
	}
	if release.VerificationState != model.PlatformArtifactVerificationStateVerified {
		return fail("publication_verification_state")
	}
	if lkg == nil || lkg.VerifiedByReleaseID != release.ID || lkg.ArtifactID != parent.ID {
		return nil, fmt.Errorf("%w: active release %s and LKG have not converged", ErrSafeRolloutServingPublicationPending, release.ID)
	}
	if decision := platformsafety.EvaluatePlatformLKGSnapshot(*lkg, parent, s.platformArtifactSigningKeyring(), time.Now().UTC()); !decision.Pass {
		checks := make([]string, 0, len(decision.Violations))
		for _, v := range decision.Violations {
			checks = append(checks, string(v.Invariant))
		}
		return fail("lkg_integrity/" + strings.Join(checks, ","))
	}
	authorityRelease, err := s.GetPlatformArtifactRelease(parent.Metadata[platformproducer.PolicyReleaseMetadata])
	if err != nil {
		return fail("producer_policy_release_unavailable")
	}
	authority, err := s.GetPlatformArtifact(authorityRelease.ArtifactID)
	if err != nil || !trusted(authority) || authorityRelease.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || authorityRelease.ScopeKey != platformproducer.Scope || authorityRelease.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow || authorityRelease.FencingToken <= 0 || authority.Generation != authorityRelease.Generation {
		return fail("producer_policy_release_integrity")
	}
	producer, err := platformproducer.Decode(authority)
	if err != nil || producer.TargetScope != "global" || producer.PublicationRole != "" || producer.Mode != "serving" || producer.DNSPolicyArtifactID != source.ID || producer.DNSPolicyDigest != source.ContentHash || producer.StaticIntentArtifactID != parent.Metadata[platformproducer.StaticIntentIDMetadata] || producer.StaticIntentDigest != parent.Metadata[platformproducer.StaticIntentDigestMetadata] {
		return fail("producer_policy_binding")
	}
	static, err := s.GetPlatformArtifact(producer.StaticIntentArtifactID)
	if err != nil || !trusted(static) || static.ContentHash != producer.StaticIntentDigest {
		return fail("static_intent_integrity")
	}
	intent, err := platformproducer.DecodeStaticIntent(static)
	if err != nil {
		return fail("static_intent_decode")
	}
	policy, err := platformproducer.DecodeProjectionPolicy(source, intent.Consumers, producer.HostedZoneTemplates)
	if err != nil {
		return fail("dns_projection_policy_decode")
	}
	if len(policy.OfflineEdges) == 0 {
		return nodes, nil
	}
	if _, err := platformproducer.EffectiveOfflineHolds(nodes, policy.OfflineEdges, nil, parent.Provenance.SignedAt); err != nil {
		return fail("offline_inventory_identity")
	}
	policy.OfflineEdges, err = platformproducer.PublicationOfflineHolds(parent, policy.OfflineEdges)
	if err != nil {
		return fail("pinned_offline_recovery")
	}
	ids, ok := parent.Content["artifact_ids"].([]any)
	kinds, kindsOK := parent.Content["artifact_kinds"].([]any)
	if !ok || !kindsOK || len(ids) != len(kinds) {
		return fail("publication_child_list")
	}
	dnsFound := false
	for i, id := range ids {
		if kinds[i] != model.PlatformArtifactKindDNSAnswerBundle {
			continue
		}
		id, ok := id.(string)
		if !ok || dnsFound {
			return fail("dns_bundle_reference")
		}
		dns, err := s.GetPlatformArtifact(id)
		if err != nil || dns.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || dns.ScopeKey != "global" || !trusted(dns) {
			return fail("dns_bundle_integrity")
		}
		if check := safeRolloutDNSOfflineExclusionCheck(dns, policy.OfflineEdges); check != "" {
			return fail(check)
		}
		dnsFound = true
	}
	if !dnsFound {
		return fail("dns_bundle_missing")
	}
	selected, err := platformproducer.SelectDNSCandidateEndpoints(nodes, policy.OfflineEdges)
	if err != nil {
		return nil, err
	}
	// Do not return evidence assembled across a concurrent publication switch.
	_, current, found, err := s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", model.PlatformArtifactReleaseChannelFull)
	if err != nil {
		return nil, err
	}
	if !found || current.ID != release.ID || current.FencingToken != release.FencingToken || current.Status != release.Status || current.VerificationState != release.VerificationState {
		return nil, fmt.Errorf("%w: active publication changed while verifying membership", ErrSafeRolloutServingPublicationPending)
	}
	return selected, nil
}

func safeRolloutDNSOfflineExclusionCheck(artifact model.PlatformArtifact, holds []platformproducer.OfflineEdge) string {
	var plan *platformconfig.DNSReadinessPlan
	raw, err := json.Marshal(artifact.Content["readiness_plan"])
	if err != nil || json.Unmarshal(raw, &plan) != nil || plan == nil || len(plan.Records) == 0 || len(plan.Probes) == 0 {
		return "dns_readiness_plan_unavailable"
	}
	if platformproducer.ValidateOfflineEdges(holds) != nil {
		return "offline_declaration_invalid"
	}
	for _, hold := range holds {
		matches := func(id, address string) bool {
			host, _, _ := net.SplitHostPort(address)
			return id == hold.EdgeID || address == hold.PublicIPv4 || host == hold.PublicIPv4
		}
		for _, probe := range plan.Probes {
			if matches(probe.EdgeID, probe.Address) {
				return "dns_readiness_offline_exclusion"
			}
		}
		for _, record := range plan.Records {
			for _, target := range record.Targets {
				if matches(target.EdgeID, target.Address) {
					return "dns_readiness_offline_exclusion"
				}
			}
		}
	}
	return ""
}

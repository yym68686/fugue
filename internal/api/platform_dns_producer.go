package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// Read only signed stored inputs. In particular, this independent worker never
// waits on an Edge health request or takes the global producer election lock.
func (s *Server) captureDNSProducer(ctx context.Context, p platformproducer.Policy) (platformconfig.CompileRequest, error) {
	fail := func() (platformconfig.CompileRequest, error) {
		return platformconfig.CompileRequest{}, fmt.Errorf("DNS producer verified source unavailable")
	}
	if err := ctx.Err(); err != nil {
		return platformconfig.CompileRequest{}, err
	}
	if p.DNSSource == nil {
		return fail()
	}
	pin := p.DNSSource.ProducerPolicy
	producer, activation, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil || !found || producer.ID != pin.ArtifactID || producer.ContentHash != pin.ContentHash || activation.ID != pin.ReleaseID || activation.FencingToken != pin.FencingToken || producer.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(producer) != nil {
		return fail()
	}
	global, err := platformproducer.Decode(producer)
	if err != nil || global.TargetScope != "global" || global.PublicationRole != "" || global.Mode != "serving" || global.Serving == nil || global.Serving.SinglePublication {
		return fail()
	}
	parent, release, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
	if err != nil || !found || parent.Metadata[platformproducer.PolicyReleaseMetadata] != pin.ReleaseID || parent.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(parent) != nil || release.VerificationState != model.PlatformArtifactVerificationStateVerified || release.VerifiedLKGGeneration != parent.Generation {
		return fail()
	}
	lkg, err := s.store.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || lkg == nil || lkg.ArtifactID != parent.ID || lkg.ContentHash != parent.ContentHash || lkg.VerifiedByReleaseID != release.ID || !lkg.ExpiresAt.After(time.Now()) || !platformsafety.EvaluatePlatformLKGSnapshot(*lkg, parent, s.bundleKeyring(), time.Now().UTC()).Pass {
		return fail()
	}
	children := map[string]model.PlatformArtifact{}
	for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindDNSAnswerBundle} {
		child, err := s.consumerAssignmentChild(parent, kind)
		if err != nil || child.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(child) != nil {
			return fail()
		}
		children[kind] = child
	}
	route, tls, dns := children[model.PlatformArtifactKindEdgeRouteBundle], children[model.PlatformArtifactKindCaddyRouteConfig], children[model.PlatformArtifactKindDNSAnswerBundle]
	previous := platformconfig.PreviousTrafficPublicationInput{ProducerPolicy: &pin, Parent: parent, Route: route, TLS: tls, DNS: dns,
		Reference: platformconfig.PreviousTrafficPublicationReference{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, ReleaseID: release.ID, FencingToken: release.FencingToken, RouteArtifactID: route.ID, RouteArtifactDigest: route.ContentHash, TLSArtifactID: tls.ID, TLSArtifactDigest: tls.ContentHash, DNSArtifactID: dns.ID, DNSArtifactDigest: dns.ContentHash}}
	var intent platformconfig.PlatformIntent
	var policy platformconfig.PolicySnapshot
	for _, input := range []struct {
		kind, generation string
		target           any
	}{
		{model.PlatformArtifactKindPlatformIntent, parent.Metadata["intent_generation"], &intent},
		{model.PlatformArtifactKindPolicySnapshot, parent.Metadata["policy_generation"], &policy},
	} {
		a, err := s.store.GetPlatformArtifactByIdentity(input.kind, "global", input.generation)
		if err != nil || a.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(a) != nil {
			return fail()
		}
		raw, err := json.Marshal(a.Content)
		if err != nil || json.Unmarshal(raw, input.target) != nil {
			return fail()
		}
	}
	snapshot, err := s.store.GetPlatformCompilerInput(parent.Metadata["input_snapshot_digest"])
	if err != nil || snapshot.IntentGeneration != intent.Generation || snapshot.PolicyGeneration != policy.Generation {
		return fail()
	}
	request, err := platformproducer.ComposeDNSProjection(p, previous, intent, policy, dnsConfigurationInput(snapshot))
	if err != nil {
		return request, err
	}
	// A coherent selection read is rechecked before returning; admission repeats
	// reference validation under source/target authority locks before any write.
	_, after, have, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
	if err != nil || !have || after.ID != release.ID || after.FencingToken != release.FencingToken {
		return fail()
	}
	_, afterPolicy, have, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil || !have || afterPolicy.ID != activation.ID || afterPolicy.FencingToken != activation.FencingToken {
		return fail()
	}
	if err := ctx.Err(); err != nil {
		return platformconfig.CompileRequest{}, err
	}
	if err := s.store.ValidateDNSProducerSelection(ctx, pin, model.PlatformPublicationPrecondition{ArtifactID: parent.ID, ContentHash: parent.ContentHash, ReleaseID: release.ID, FencingToken: release.FencingToken}); err != nil {
		return fail()
	}
	return request, nil
}

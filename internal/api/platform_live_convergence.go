package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

// A still-fresh receipt from a replaced Pod is historical evidence, not proof
// that the newly selected backend applied anything. Preserve the original fact
// and annotate its assessment using a fresh, read-only transport observation.
// Store publication retains its independent transactional checks; Kubernetes
// observation cannot provide a cross-system transaction or serving lease.
func (s *Server) evaluateLiveConsumerConvergence(ctx context.Context, set model.PlatformExpectedConsumerSet, consumers []model.PlatformConsumerInstance, binding *platformcontrol.ConsumerReleaseBinding) model.PlatformConsumerConvergenceStatus {
	status := platformcontrol.EvaluateConsumerConvergence(set, consumers, time.Now().UTC(), binding)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	backendReasonPrefix := "dns_public_backend_"
	if set.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle && strings.HasPrefix(set.ScopeKey, "authority-cell:") {
		if parent, err := s.store.GetPlatformArtifact(set.ReleaseSetID); err == nil && parent.Content["publication_role"] == platformconfig.PublicationRoleCellDNS {
			backendReasonPrefix = "dns_declared_backend_"
		}
	}
	backend := map[string]int{}
	for _, assessment := range status.Assessments {
		fact := assessment.Observed
		if assessment.State != model.InvariantEvidenceStatePass || fact == nil || fact.Component != model.PlatformConsumerComponentDNSServer || !strings.HasPrefix(fact.CredentialID, "kubernetes:") {
			continue
		}
		claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: fact.CredentialID, Component: fact.Component, NodeID: fact.NodeID, AuthorityID: assessment.Expected.AuthorityID, ScopeKey: fact.ScopeKey, ArtifactKinds: fact.SupportedKinds}
		if !platformcontrol.ExpectedConsumerIdentityMatches(assessment.Expected, claims) {
			backend[assessment.ConsumerID] = http.StatusForbidden
			continue
		}
		backend[assessment.ConsumerID] = s.validateDNSHeartbeatBackend(ctx, claims, platformcontrol.PlatformConsumerHeartbeatEnvelope{ApplyStatus: fact.ApplyStatus, ProbeStatus: fact.ProbeStatus, ReleaseSetID: fact.ReleaseSetID, ExpectedConsumerSetID: fact.ExpectedConsumerSetID, FencingToken: fact.FencingToken, GenerationSequence: fact.GenerationSequence}, set)
	}
	// Metadata reads take time. Do not return a pass whose source heartbeat
	// expired while we were observing Kubernetes. Never renew its timestamps.
	status = platformcontrol.EvaluateConsumerConvergence(set, consumers, time.Now().UTC(), binding)
	for i := range status.Assessments {
		assessment := &status.Assessments[i]
		code, checked := backend[assessment.ConsumerID]
		if !checked || code == http.StatusOK || assessment.State != model.InvariantEvidenceStatePass {
			continue
		}
		assessment.State = model.InvariantEvidenceStateFail
		reason := backendReasonPrefix + "mismatch"
		if code == http.StatusServiceUnavailable {
			assessment.State = model.InvariantEvidenceStateUnknown
			reason = backendReasonPrefix + "unavailable"
		}
		assessment.Reasons = append(assessment.Reasons, reason)
		if assessment.Required {
			status.RequiredPassing--
			if assessment.State == model.InvariantEvidenceStateFail || status.State == model.InvariantEvidenceStatePass {
				status.State = assessment.State
			}
			status.Pass = false
		}
	}
	return status
}

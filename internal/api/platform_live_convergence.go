package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"fugue/internal/model"
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
	backend := map[string]int{}
	for _, assessment := range status.Assessments {
		fact := assessment.Observed
		if assessment.State != model.InvariantEvidenceStatePass || fact == nil || fact.Component != model.PlatformConsumerComponentDNSServer || !strings.HasPrefix(fact.CredentialID, "kubernetes:") {
			continue
		}
		claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: fact.CredentialID, Component: fact.Component, NodeID: fact.NodeID, ScopeKey: fact.ScopeKey, ArtifactKinds: fact.SupportedKinds}
		backend[assessment.ConsumerID] = s.validateDNSHeartbeatBackend(ctx, claims, platformcontrol.PlatformConsumerHeartbeatEnvelope{ApplyStatus: fact.ApplyStatus, ProbeStatus: fact.ProbeStatus})
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
		reason := "dns_public_backend_mismatch"
		if code == http.StatusServiceUnavailable {
			assessment.State = model.InvariantEvidenceStateUnknown
			reason = "dns_public_backend_unavailable"
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

package api

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// A rollout's watch windows must not suspend the bounded quality refresh queue.
// Rebuild the current projection so observations retain their exact configuration
// context. Discard the projection: only the ordinary capture's runtime evidence
// cache is updated; no compiler input, artifact, release or LKG is published here.
// The next publication still captures and validates its own current inputs.
func (s *Server) refreshPendingProducerQuality(ctx context.Context, policy platformproducer.Policy, capture func(context.Context, model.Principal) (platformIntentProjectionResponse, error)) error {
	if policy.DNSPolicyArtifactID == "" || policy.Serving == nil || policy.Serving.SinglePublication {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a, err := s.store.GetPlatformArtifact(policy.DNSPolicyArtifactID)
	if err != nil {
		return err
	}
	if a.ID != policy.DNSPolicyArtifactID || a.ContentHash != policy.DNSPolicyDigest || a.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(a) != nil || !platformsafety.EvaluateArtifactIntegrity(a, s.bundleKeyring()).Pass {
		return fmt.Errorf("pending quality DNS policy reference is not trusted")
	}
	var input platformproducer.ProjectionPolicyInput
	if err := decodeCompilerArtifactContent(a, &input); err != nil {
		return err
	}
	if input.DNSQueryPolicy == nil || input.DNSQueryPolicy.DynamicQuality == nil {
		return nil
	}
	// Retain the configured query/concurrency limits and the existing 30-second
	// quality budget. Bound all surrounding reads so rollout progress cannot be
	// held up indefinitely by an unavailable observation dependency.
	refreshCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if capture == nil {
		capture = func(ctx context.Context, principal model.Principal) (platformIntentProjectionResponse, error) {
			return s.capturePlatformIntentForProducer(ctx, principal, policy)
		}
	}
	projection, err := capture(refreshCtx, platformProducerPrincipal())
	if err != nil {
		return fmt.Errorf("pending producer quality refresh: %w", err)
	}
	if s.log != nil {
		coverage, _ := projection.RuntimeSnapshot.Facts["dynamic_quality_coverage"].(map[string]any)
		s.log.Printf("platform producer pending quality refreshed scope=%s queries=%v scheduled=%v refreshed=%v budget_exhausted=%v", policy.TargetScope, coverage["query_count"], coverage["scheduled_queries"], coverage["refreshed_queries"], coverage["capture_budget_exhausted"])
	}
	return refreshCtx.Err()
}

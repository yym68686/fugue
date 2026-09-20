package api

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

type producedTrafficState struct {
	lkg                        *model.PlatformLKGSnapshot
	full, gray                 model.PlatformArtifactRelease
	fullArtifact, grayArtifact model.PlatformArtifact
	hasFull, hasGray           bool
}

func (s *Server) producedTrafficState(scope string) (producedTrafficState, error) {
	var state producedTrafficState
	lkg, err := s.store.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, scope)
	if err != nil || lkg == nil {
		return state, err
	}
	verified, err := s.store.GetPlatformArtifactRelease(lkg.VerifiedByReleaseID)
	if err != nil {
		return state, err
	}
	// Initial gray bootstrap remains an explicit operator action. Automatic
	// configuration updates start only after an actual full verified baseline.
	if verified.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || verified.VerificationState != model.PlatformArtifactVerificationStateVerified || verified.ArtifactID != lkg.ArtifactID {
		return state, nil
	}
	state.lkg = lkg
	state.fullArtifact, state.full, state.hasFull, err = s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, scope, model.PlatformArtifactReleaseChannelFull)
	if err != nil {
		return state, err
	}
	state.grayArtifact, state.gray, state.hasGray, err = s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, scope, model.PlatformArtifactReleaseChannelGray)
	return state, err
}

func isProducedTraffic(r model.PlatformArtifactRelease) bool {
	return r.ReleasedByType == model.ActorTypeBootstrap && r.ReleasedByID == platformproducer.Actor
}

// Reuse the publication ledger as the phase cursor. A pending serving candidate
// is completed or recovered before capturing a newer desired configuration.
func (s *Server) reconcilePendingProducedTraffic(ctx context.Context, policy platformproducer.Policy, authority model.PlatformArtifactRelease) (bool, error) {
	state, err := s.producedTrafficState(policy.TargetScope)
	if err != nil || state.lkg == nil || !state.hasFull {
		return false, err
	}
	principal := platformProducerPrincipal()
	current, parent := state.full, state.fullArtifact
	if state.hasGray && state.gray.ReleasedAt.After(current.ReleasedAt) {
		current, parent = state.gray, state.grayArtifact
	}
	if current.ArtifactID == state.lkg.ArtifactID || current.VerificationState == model.PlatformArtifactVerificationStateVerified || current.VerificationState == model.PlatformArtifactVerificationStateFailed {
		// Complete a durable recovery publication after restart. Preparation of
		// an unverified candidate must never block its timeout recovery below.
		if isProducedTraffic(state.full) && state.full.ArtifactID == state.lkg.ArtifactID {
			if _, err := s.preparePlatformReleaseSetConsumers(ctx, principal, state.fullArtifact, state.full); err != nil {
				return true, err
			}
		}
		return false, nil
	}
	if !isProducedTraffic(current) {
		return true, fmt.Errorf("automatic publication waits for the current operator release")
	}
	settings := policy.Serving
	if parent.Metadata[platformproducer.PolicyReleaseMetadata] != authority.ID || time.Since(current.ReleasedAt) >= time.Duration(settings.RolloutTimeoutSeconds)*time.Second {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		artifact, release, _, _, err := s.store.RollbackProducedTraffic(parent.ID, current.ID, authority.ID, state.full.ID, state.lkg.ArtifactID, model.PlatformArtifactRollbackRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull, ToGeneration: state.lkg.Generation, Reason: "producer restores verified baseline after timeout or policy revision"}, principal)
		if err != nil {
			return true, err
		}
		if _, err = s.preparePlatformReleaseSetConsumers(ctx, principal, artifact, release); err != nil {
			return true, err
		}
		s.appendAudit(principal, "platform_config.serving_rolled_back", "platform_release_set", parent.ID, "", map[string]string{"policy_release_id": authority.ID, "failed_release_id": current.ID, "recovery_release_id": release.ID, "lkg_artifact_id": artifact.ID})
		return true, nil
	}
	if _, err := s.preparePlatformReleaseSetConsumers(ctx, principal, parent, current); err != nil {
		return true, err
	}
	minimum := settings.GrayMinSeconds
	if current.ReleaseChannel == model.PlatformArtifactReleaseChannelFull {
		minimum = settings.FullMinSeconds
	}
	if time.Since(current.ReleasedAt) < time.Duration(minimum)*time.Second {
		return true, nil
	}
	if result := s.validateReleaseSetConvergence(parent); !result.Pass {
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if current.ReleaseChannel == model.PlatformArtifactReleaseChannelGray {
		artifact, release, _, _, err := s.store.ReleaseProducedTrafficArtifact(parent.ID, authority.ID, model.PlatformArtifactReleaseChannelFull, state.full.ID, state.full.ID, state.lkg.ArtifactID, "", principal)
		if err != nil {
			return true, err
		}
		if _, err = s.preparePlatformReleaseSetConsumers(ctx, principal, artifact, release); err != nil {
			return true, err
		}
		s.appendAudit(principal, "platform_config.full_produced", "platform_release_set", parent.ID, "", map[string]string{"policy_release_id": authority.ID, "release_id": release.ID})
		return true, nil
	}
	_, _, _, _, err = s.store.VerifyProducedTrafficLKG(current.ID, authority.ID, state.lkg.ArtifactID, model.PlatformArtifactVerifyLKGRequest{FencingToken: current.FencingToken, Reason: "producer full publication verified by fresh actual consumer evidence", Evidence: model.PlatformArtifactVerificationEvidence{ConsumerConvergence: true, LocalProbe: true, PlatformEvidence: true, WatchWindow: true, BaselineMonotonic: true, DatabaseRollbackCompatible: true, EvidenceRefs: []string{parent.ID, current.ID}}}, principal)
	if err == nil {
		s.appendAudit(principal, "platform_config.serving_verified", "platform_release_set", parent.ID, "", map[string]string{"policy_release_id": authority.ID, "release_id": current.ID})
	}
	return true, err
}

func (s *Server) producedSourcePreviouslyFailed(scope, policyReleaseID, sourceDigest string) (bool, error) {
	_, gray, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, scope, model.PlatformArtifactReleaseChannelGray)
	if err != nil || !found {
		return false, err
	}
	return gray.VerificationState == model.PlatformArtifactVerificationStateFailed && gray.VerificationEvidence["producer_policy_release_id"] == policyReleaseID && gray.VerificationEvidence["failed_source_digest"] == sourceDigest, nil
}

func (s *Server) stageProducedTraffic(ctx context.Context, policy platformproducer.Policy, authority model.PlatformArtifactRelease, parent model.PlatformArtifact) error {
	state, err := s.producedTrafficState(policy.TargetScope)
	if err != nil || state.lkg == nil || !state.hasFull || state.full.ArtifactID == parent.ID {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	artifact, release, _, _, err := s.store.ReleaseProducedTrafficArtifact(parent.ID, authority.ID, model.PlatformArtifactReleaseChannelGray, state.gray.ID, state.full.ID, state.lkg.ArtifactID, policy.Serving.CanaryRuleRef, platformProducerPrincipal())
	if err != nil {
		return err
	}
	if _, err = s.preparePlatformReleaseSetConsumers(ctx, platformProducerPrincipal(), artifact, release); err != nil {
		return err
	}
	s.appendAudit(platformProducerPrincipal(), "platform_config.gray_produced", "platform_release_set", parent.ID, "", map[string]string{"policy_release_id": authority.ID, "release_id": release.ID})
	return nil
}

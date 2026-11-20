package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/store"
)

type producedTrafficState struct {
	lkg                        *model.PlatformLKGSnapshot
	full, gray                 model.PlatformArtifactRelease
	fullArtifact, grayArtifact model.PlatformArtifact
	hasFull, hasGray           bool
}

const trafficPublicationTimeoutReason = "traffic publication did not converge before rollout timeout"

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
			// An operator successor can repair an unreachable historical baseline.
			// Do not require that broken rollback to converge first, or mark its
			// new fence verified using historical facts. The store independently
			// guards the successor's signed shadow/gray/full and fresh ACKs.
			if state.full.VerificationState == model.PlatformArtifactVerificationStateServingUnverified &&
				state.full.RollbackTargetGeneration == state.lkg.Generation &&
				state.full.PinnedRollbackGeneration == state.lkg.Generation &&
				state.fullArtifact.Metadata[platformproducer.PolicyReleaseMetadata] != "" &&
				(state.fullArtifact.Metadata[platformproducer.PolicyReleaseMetadata] != authority.ID ||
					legacyProducedCompiler(state.fullArtifact)) &&
				!s.validateReleaseSetConvergence(ctx, state.fullArtifact).Pass {
				return false, nil
			}
			if _, err := s.preparePlatformReleaseSetConsumers(ctx, principal, state.fullArtifact, state.full); err != nil {
				return true, err
			}
			if state.full.VerificationState == model.PlatformArtifactVerificationStateServingUnverified {
				if time.Since(state.full.ReleasedAt) < time.Duration(policy.Serving.FullMinSeconds)*time.Second ||
					!s.validateReleaseSetConvergence(ctx, state.fullArtifact).Pass {
					return true, nil
				}
				if err := ctx.Err(); err != nil {
					return true, err
				}
				_, _, _, _, err := s.store.VerifyProducedTrafficRecoveryLKG(state.full.ID, authority.ID, state.lkg.ArtifactID,
					model.PlatformArtifactVerifyLKGRequest{FencingToken: state.full.FencingToken, Reason: "producer recovery verified by fresh actual consumer evidence", Evidence: model.PlatformArtifactVerificationEvidence{ConsumerConvergence: true, LocalProbe: true, PlatformEvidence: true, WatchWindow: true, BaselineMonotonic: true, DatabaseRollbackCompatible: true, EvidenceRefs: []string{state.fullArtifact.ID, state.full.ID}}}, principal)
				if err == nil {
					s.appendAudit(principal, "platform_config.recovery_verified", "platform_release_set", state.fullArtifact.ID, "", map[string]string{"policy_release_id": authority.ID, "release_id": state.full.ID})
				}
				return true, err
			}
		}
		return false, nil
	}
	if !isProducedTraffic(current) {
		return true, fmt.Errorf("automatic publication waits for the current operator release")
	}
	settings := policy.Serving
	recoverBaseline := func(reason string) (bool, error) {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		artifact, release, _, _, err := s.store.RollbackProducedTraffic(parent.ID, current.ID, authority.ID, state.full.ID, state.lkg.ArtifactID, model.PlatformArtifactRollbackRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull, ToGeneration: state.lkg.Generation, Reason: reason}, principal)
		if err != nil {
			return true, err
		}
		if reason == trafficPublicationTimeoutReason {
			s.trafficPublicationTimeouts.Add(1)
		}
		s.appendAudit(principal, "platform_config.serving_rolled_back", "platform_release_set", parent.ID, "", map[string]string{"reason": reason, "channel": current.ReleaseChannel, "policy_release_id": authority.ID, "failed_release_id": current.ID, "recovery_release_id": release.ID, "lkg_artifact_id": artifact.ID})
		if s.log != nil {
			s.log.Printf("traffic publication rolled back channel=%s release=%s recovery=%s reason=%s", current.ReleaseChannel, current.ID, release.ID, reason)
		}
		if _, err = s.preparePlatformReleaseSetConsumers(ctx, principal, artifact, release); err != nil {
			return true, err
		}
		return true, nil
	}
	if parent.Metadata[platformproducer.PolicyReleaseMetadata] != authority.ID {
		return recoverBaseline("producer policy revision changed")
	}
	if time.Since(current.ReleasedAt) >= time.Duration(settings.RolloutTimeoutSeconds)*time.Second {
		return recoverBaseline(trafficPublicationTimeoutReason)
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
	if result := s.validateReleaseSetConvergence(ctx, parent); !result.Pass {
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if current.ReleaseChannel == model.PlatformArtifactReleaseChannelGray {
		if err := s.recheckTrafficWithdrawal(ctx, parent, state.fullArtifact); err != nil {
			if errors.Is(err, errObsoleteWithdrawal) {
				return recoverBaseline(err.Error())
			}
			return true, err
		}
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

// The continuous producer's existing signed intent authorizes fresh captures.
// An unreachable legacy rollback must not block that work just because its
// producer policy did not change. This never verifies the old fence, rewrites
// its input or waives the successor's ordinary shadow/gray/full/LKG checks.
func legacyProducedCompiler(a model.PlatformArtifact) bool {
	v := a.Metadata["compiler_version"]
	return v == platformconfig.CompilerVersion || v == platformconfig.ConfigurationInputCompilerVersion
}

func (s *Server) producedSourcePreviouslyFailed(scope, policyReleaseID, sourceDigest string) (bool, error) {
	_, gray, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, scope, model.PlatformArtifactReleaseChannelGray)
	if err != nil || !found {
		return false, err
	}
	return gray.VerificationState == model.PlatformArtifactVerificationStateFailed && gray.VerificationEvidence["producer_policy_release_id"] == policyReleaseID && gray.VerificationEvidence["failed_source_digest"] == sourceDigest, nil
}

// Retry without waiving current-source, compatibility, freshness or CAS gates.
// Failure must not freeze corrected input or retry a durably rejected source.
func (s *Server) resumeProducedShadow(ctx context.Context, policy platformproducer.Policy, authority model.PlatformArtifactRelease, parent model.PlatformArtifact) (bool, error) {
	state, err := s.producedTrafficState(policy.TargetScope)
	if err != nil || state.lkg == nil || !state.hasFull || state.full.ArtifactID == parent.ID || parent.Status != model.PlatformArtifactStatusValidated {
		return false, err
	}
	failed, err := s.producedSourcePreviouslyFailed(policy.TargetScope, authority.ID, parent.Metadata[platformproducer.SourceDigestMetadata])
	if err != nil || failed {
		return false, err
	}
	_, shadow, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, policy.TargetScope, model.PlatformArtifactReleaseChannelShadow)
	if err != nil || !found || shadow.ArtifactID != parent.ID || !isProducedTraffic(shadow) || parent.Metadata[platformproducer.PolicyReleaseMetadata] != authority.ID {
		return false, err
	}
	if _, err = s.preparePlatformReleaseSetConsumers(ctx, platformProducerPrincipal(), parent, shadow); err == nil {
		err = s.stageProducedTraffic(ctx, policy, authority, parent)
		if keepDNSShadowForPendingCapability(policy, shadow, store.PlatformReleaseFailureStage(err), time.Now()) && ctx.Err() == nil {
			// The next reconcile checks the same signed candidate. Do not
			// repeatedly move the fence while the consumers are still probing.
			if s.log != nil {
				s.log.Printf("DNS producer waiting for fresh shadow capability artifact=%s release=%s", parent.ID, shadow.ID)
			}
			return true, nil
		}
	}
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if s.log != nil {
			s.log.Printf("producer shadow not ready; capture may supersede it artifact=%s: %v", parent.ID, err)
		}
		return false, nil
	}
	return true, nil
}

func keepDNSShadowForPendingCapability(policy platformproducer.Policy, shadow model.PlatformArtifactRelease, failureStage string, now time.Time) bool {
	return policy.PublicationRole == platformconfig.PublicationRoleCellDNS && policy.Serving != nil &&
		failureStage == store.PlatformReleaseStageTrafficCapabilityPending &&
		!shadow.ReleasedAt.IsZero() && !shadow.ReleasedAt.After(now) &&
		now.Before(shadow.ReleasedAt.Add(time.Duration(policy.Serving.RolloutTimeoutSeconds)*time.Second))
}

func (s *Server) stageProducedTraffic(ctx context.Context, policy platformproducer.Policy, authority model.PlatformArtifactRelease, parent model.PlatformArtifact) error {
	state, err := s.producedTrafficState(policy.TargetScope)
	if err != nil || state.lkg == nil || !state.hasFull || state.full.ArtifactID == parent.ID {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err := s.recheckTrafficWithdrawal(ctx, parent, state.fullArtifact); err != nil {
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

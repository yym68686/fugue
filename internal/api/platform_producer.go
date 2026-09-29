package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
)

func platformProducerPrincipal() model.Principal {
	return model.Principal{ActorType: model.ActorTypeBootstrap, ActorID: platformproducer.Actor, Scopes: map[string]struct{}{"platform.admin": {}}}
}

// One existing store advisory lock elects the producer. Policy and target CAS
// checks still run in the publication transaction: leadership is not authority.
func (s *Server) StartBackgroundPlatformConfiguration(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	// Each authority has its own scheduling and lock. A cell's unavailable
	// dependencies cannot stall legacy publication or another cell's recovery.
	var workers sync.WaitGroup
	defer workers.Wait()
	started := map[string]context.CancelFunc{}
	defer func() {
		for _, cancel := range started {
			cancel()
		}
	}()
	start := func(scope string) {
		workerCtx, cancel := context.WithCancel(ctx)
		started[scope] = cancel
		workers.Add(1)
		go func() { defer workers.Done(); s.runPlatformConfigurationScope(workerCtx, scope) }()
	}
	start(platformproducer.Scope)
	for ctx.Err() == nil {
		scopes, err := s.store.ListPlatformProducerScopes()
		if err != nil {
			if s.log != nil {
				s.log.Printf("platform producer discovery unavailable; retaining existing workers: %v", err)
			}
		} else {
			wanted := map[string]bool{platformproducer.Scope: true}
			for _, scope := range scopes {
				wanted[scope] = true
			}
			for scope, cancel := range started {
				if !wanted[scope] {
					cancel()
					delete(started, scope)
				}
			}
			for _, scope := range scopes {
				if _, exists := started[scope]; exists {
					continue
				}
				start(scope)
			}
		}
		if !waitPlatformProducer(ctx, 30*time.Second) {
			return
		}
	}
}

func (s *Server) runPlatformConfigurationScope(ctx context.Context, scope string) {
	lock := platformproducer.Actor
	if scope != platformproducer.Scope {
		lock += "|" + scope
	}
	for ctx.Err() == nil {
		_, err := s.store.WithAdvisoryLock(ctx, lock, func() error {
			if s.log != nil {
				s.log.Printf("platform configuration producer leadership acquired scope=%s", scope)
			}
			var nextRun time.Time
			lastAuthority := ""
			for ctx.Err() == nil {
				_, authority, _, readErr := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, scope, "shadow")
				if readErr != nil {
					if s.log != nil {
						s.log.Printf("platform configuration producer policy unavailable scope=%s: %v", scope, readErr)
					}
				} else if authority.ID != lastAuthority || !time.Now().Before(nextRun) {
					interval, err := s.reconcilePlatformConfigurationScope(ctx, scope, nil)
					if err != nil && s.log != nil {
						s.log.Printf("platform configuration producer failed scope=%s; retaining release: %v", scope, err)
					}
					lastAuthority, nextRun = authority.ID, time.Now().Add(interval)
				}
				// Policy changes are noticed within this poll interval even when
				// the previous policy requested infrequent compilation.
				if !waitPlatformProducer(ctx, 30*time.Second) {
					break
				}
			}
			return nil
		})
		if err != nil && ctx.Err() == nil && s.log != nil {
			s.log.Printf("platform configuration producer leadership unavailable scope=%s: %v", scope, err)
		}
		if !waitPlatformProducer(ctx, 30*time.Second) {
			return
		}
	}
}

func waitPlatformProducer(ctx context.Context, interval time.Duration) bool {
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Server) reconcilePlatformConfiguration(ctx context.Context) (time.Duration, error) {
	return s.reconcilePlatformConfigurationWithCapture(ctx, nil)
}

func (s *Server) reconcilePlatformConfigurationWithCapture(ctx context.Context, capture func(context.Context, model.Principal) (platformIntentProjectionResponse, error)) (time.Duration, error) {
	return s.reconcilePlatformConfigurationScope(ctx, platformproducer.Scope, capture)
}

func (s *Server) reconcilePlatformConfigurationScope(ctx context.Context, scope string, capture func(context.Context, model.Principal) (platformIntentProjectionResponse, error)) (time.Duration, error) {
	interval := 30 * time.Second
	if err := ctx.Err(); err != nil {
		return interval, err
	}
	policyArtifact, authority, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, scope, "shadow")
	if err != nil || !found {
		return interval, err
	}
	policy, err := platformproducer.Decode(policyArtifact)
	if err != nil || policyArtifact.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(policyArtifact) != nil {
		return interval, fmt.Errorf("signed producer policy unavailable")
	}
	interval = time.Duration(policy.IntervalSeconds) * time.Second
	if policy.Mode == "paused" {
		return interval, nil
	}
	if policy.Mode == "serving" {
		if handled, err := s.reconcilePendingProducedTraffic(ctx, policy, authority); handled || err != nil {
			return interval, err
		}
	}
	principal := platformProducerPrincipal()
	current, previous, haveCurrent, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, policy.TargetScope, "shadow")
	if err != nil {
		return interval, err
	}
	owned := haveCurrent && previous.ReleasedByType == model.ActorTypeBootstrap && previous.ReleasedByID == platformproducer.Actor && current.Metadata[platformproducer.PolicyReleaseMetadata] == authority.ID
	if owned && policy.Mode != "serving" {
		// Complete a prior publication interrupted before expectation preparation.
		if _, err := s.preparePlatformReleaseSetConsumers(ctx, principal, current, previous); err != nil {
			return interval, err
		}
	}
	runCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var projection platformIntentProjectionResponse
	if capture != nil {
		projection, err = capture(runCtx, principal)
	} else {
		projection, err = s.capturePlatformIntentForProducer(runCtx, principal, policy)
	}
	if err != nil {
		return interval, err
	}
	sourceDigest, err := platformconfig.Digest(struct {
		Intent    platformconfig.PlatformIntent
		Policy    platformconfig.PolicySnapshot
		Authority string
	}{projection.Intent, projection.Policy, authority.ID})
	if err != nil {
		return interval, err
	}
	if policy.Mode == "serving" {
		failed, err := s.producedSourcePreviouslyFailed(policy.TargetScope, authority.ID, sourceDigest)
		if err != nil || failed {
			return interval, err
		}
	}
	if owned && current.Metadata[platformproducer.SourceDigestMetadata] == sourceDigest && time.Since(previous.ReleasedAt) < time.Duration(policy.RefreshSeconds)*time.Second {
		if policy.Mode == "serving" {
			// A rejected older candidate must not block capturing a corrected
			// source. Prepare only the candidate we are about to reuse, after
			// checking the durable failed-source marker.
			if _, err := s.preparePlatformReleaseSetConsumers(ctx, principal, current, previous); err != nil {
				return interval, err
			}
			return interval, s.stageProducedTraffic(ctx, policy, authority, current)
		}
		if s.log != nil {
			s.log.Printf("platform configuration producer unchanged policy_release=%s release=%s source_digest=%s", authority.ID, previous.ID, sourceDigest)
		}
		return interval, nil
	}
	if projection.Intent.Scope != policy.TargetScope || projection.Policy.Scope != policy.TargetScope {
		return interval, fmt.Errorf("producer capture scope differs")
	}
	if projection.RuntimeSnapshot.Facts == nil {
		projection.RuntimeSnapshot.Facts = map[string]any{}
	}
	projection.RuntimeSnapshot.Facts["configuration_producer"] = map[string]any{"policy_release_id": authority.ID, "source_digest": sourceDigest}
	if policy.InputSource == "business-static-intent" {
		binding := projection.RuntimeSnapshot.Facts["configuration_producer"].(map[string]any)
		binding["static_intent_artifact_id"], binding["static_intent_digest"] = policy.StaticIntentArtifactID, policy.StaticIntentDigest
		if policy.DNSPolicyArtifactID != "" {
			binding["dns_policy_artifact_id"], binding["dns_policy_digest"] = policy.DNSPolicyArtifactID, policy.DNSPolicyDigest
		}
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: projection.Intent, Policy: projection.Policy, RuntimeSnapshot: projection.RuntimeSnapshot})
	if err != nil {
		return interval, err
	}
	inputs, err := s.ensurePlatformProducerInputs(runCtx, compiled, principal)
	if err != nil {
		return interval, err
	}
	result, err := s.materializePlatformCompilation(runCtx, compiled, principal, &inputs, platformCompilationSource{PolicyReleaseID: authority.ID, SourceDigest: sourceDigest, StaticIntentID: policy.StaticIntentArtifactID, StaticIntentDigest: policy.StaticIntentDigest, DNSPolicyID: policy.DNSPolicyArtifactID, DNSPolicyDigest: policy.DNSPolicyDigest})
	if err != nil {
		return interval, err
	}
	if err = runCtx.Err(); err != nil {
		return interval, err
	}
	_, release, _, _, err := s.store.ReleaseProducedPlatformArtifact(result.ReleaseArtifact.ID, authority.ID, previous.ID, principal)
	if err != nil {
		return interval, err
	}
	if _, err = s.preparePlatformReleaseSetConsumers(runCtx, principal, result.ReleaseArtifact, release); err != nil {
		return interval, err
	}
	s.appendAudit(principal, "platform_config.shadow_produced", "platform_release_set", result.ReleaseArtifact.ID, "", map[string]string{"policy_release_id": authority.ID, "source_digest": sourceDigest, "business_snapshot_revision": projection.BusinessSnapshotRevision, "release_id": release.ID})
	if s.log != nil {
		s.log.Printf("platform configuration producer published policy_release=%s release=%s artifact=%s source_digest=%s business_revision=%s", authority.ID, release.ID, result.ReleaseArtifact.ID, sourceDigest, projection.BusinessSnapshotRevision)
	}
	if policy.Mode == "serving" {
		return interval, s.stageProducedTraffic(ctx, policy, authority, result.ReleaseArtifact)
	}
	return interval, nil
}

func (s *Server) loadStaticPlatformIntent(id, digest string) (platformproducer.StaticIntentInput, error) {
	a, err := s.store.GetPlatformArtifact(id)
	if err != nil {
		return platformproducer.StaticIntentInput{}, err
	}
	if a.ID != id || a.Status != model.PlatformArtifactStatusValidated || digest != "" && a.ContentHash != digest || s.store.VerifyPlatformArtifactIntegrity(a) != nil || !platformsafety.EvaluateArtifactIntegrity(a, s.bundleKeyring()).Pass {
		return platformproducer.StaticIntentInput{}, fmt.Errorf("static intent reference is not trusted")
	}
	return platformproducer.DecodeStaticIntent(a)
}

func (s *Server) ensurePlatformProducerInputs(ctx context.Context, compiled platformconfig.CompileResult, principal model.Principal) (platformConfigStoredInputs, error) {
	inputs := []model.PlatformArtifact{compiled.IntentArtifact, compiled.PolicyArtifact}
	for i, a := range inputs {
		if err := ctx.Err(); err != nil {
			return platformConfigStoredInputs{}, err
		}
		a.CreatedByType, a.CreatedByID = principal.ActorType, principal.ActorID
		_, scope := store.NormalizePlatformArtifactScope(a.Scope)
		if existing, err := s.store.GetPlatformArtifactByIdentity(a.ArtifactKind, scope, a.Generation); err == nil {
			if existing.ArtifactKind != a.ArtifactKind || existing.ScopeKey != a.Scope.Key {
				return platformConfigStoredInputs{}, store.ErrConflict
			}
			// Preserve the first creator. Ensure still compares every immutable
			// content/metadata field and verifies the stored artifact's signature.
			a.CreatedByType, a.CreatedByID = existing.CreatedByType, existing.CreatedByID
		} else if !errors.Is(err, store.ErrNotFound) {
			return platformConfigStoredInputs{}, err
		}
		stored, _, err := s.store.EnsurePlatformArtifact(a)
		if err != nil {
			return platformConfigStoredInputs{}, err
		}
		inputs[i], err = s.store.ValidatePlatformArtifact(stored.ID, []model.PlatformArtifactValidationResult{{Name: "platform_config.producer_input", Pass: true, Severity: model.RobustnessSeverityBlockPublish, Message: "deterministic compiler input validated"}})
		if err != nil {
			return platformConfigStoredInputs{}, err
		}
	}
	return platformConfigStoredInputs{Intent: inputs[0], Policy: inputs[1]}, nil
}

func (s *Server) preservePlatformArtifactCreator(a *model.PlatformArtifact) error {
	_, scope := store.NormalizePlatformArtifactScope(a.Scope)
	existing, err := s.store.GetPlatformArtifactByIdentity(a.ArtifactKind, scope, a.Generation)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if existing.ArtifactKind != a.ArtifactKind || existing.ScopeKey != scope {
		return store.ErrConflict
	}
	a.CreatedByType, a.CreatedByID = existing.CreatedByType, existing.CreatedByID
	return nil
}

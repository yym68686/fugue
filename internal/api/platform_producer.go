package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
)

// Includes the Store's independently bounded compiler-input write plus
// validation, immutable artifact readback and materialization.
const platformProducerPersistenceTimeout = 3 * time.Minute

func platformProducerPrincipal() model.Principal {
	return model.Principal{ActorType: model.ActorTypeBootstrap, ActorID: platformproducer.Actor, Scopes: map[string]struct{}{"platform.admin": {}}}
}

// The durable bridge is deliberately not installed by NewServer. A future
// approved enrollment protocol must prove all replicas use it before any
// control can claim a complete drain. It performs no schema installation.
func (s *Server) admitPlatformProducerCapture(ctx context.Context, scope string, policy model.PlatformPublicationPrecondition) (func(context.Context) error, bool, error) {
	if s.producerCaptureParticipant == nil {
		return nil, false, fmt.Errorf("producer capture participant unavailable")
	}
	return s.producerCaptureParticipant.admit(ctx, scope, policy)
}

// One existing store advisory lock elects the producer. Policy and target CAS
// checks still run in the publication transaction: leadership is not authority.
func (s *Server) StartBackgroundPlatformConfiguration(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	s.markPlatformProducerCaptureStarted()
	defer s.platformProducerRuntime().manager()()
	// Registered participants close only after scope workers have returned.
	// Loss of the leader-lock connection is not evidence of that return.
	defer s.closePlatformProducerCaptureParticipant(ctx)
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
		returned := s.platformProducerRuntime().worker(workerCtx, scope)
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer returned()
			s.runPlatformConfigurationScope(workerCtx, scope)
		}()
	}
	start(platformproducer.Scope)
	workers.Add(1)
	go func() { defer workers.Done(); s.runOfflineEdgeRecovery(ctx) }()
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
			// Pending whole-release observation must keep its own cadence.
			// Host captures are coalesced, canceled and joined with this leader.
			kick, stop := s.pendingHostWorker(ctx, scope)
			defer stop()
			lastAuthority := ""
			failures := 0
			for ctx.Err() == nil {
				_, authority, _, readErr := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, scope, "shadow")
				if readErr != nil {
					if s.log != nil {
						s.log.Printf("platform configuration producer policy unavailable scope=%s: %v", scope, readErr)
					}
				} else if authority.ID != lastAuthority || !time.Now().Before(nextRun) {
					if authority.ID != lastAuthority {
						failures = 0
					}
					interval, err := s.reconcilePlatformConfigurationScope(ctx, scope, nil, kick)
					if err != nil {
						failures = min(failures+1, 4)
					} else {
						failures = 0
					}
					if err != nil && s.log != nil {
						s.log.Printf("platform configuration producer failed scope=%s; retaining release: %v", scope, err)
					}
					lastAuthority, nextRun = authority.ID, time.Now().Add(platformProducerNextInterval(interval, scope, failures))
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

func (s *Server) reconcilePlatformConfigurationScope(ctx context.Context, scope string, capture func(context.Context, model.Principal) (platformIntentProjectionResponse, error), pending ...func(platformproducer.Policy, model.PlatformArtifactRelease)) (interval time.Duration, resultErr error) {
	s.markPlatformProducerCaptureStarted()
	interval = 30 * time.Second
	started, phaseStarted, phase := time.Now(), time.Now(), "load_policy"
	phaseDurations := map[string]int64{}
	decision := "policy_or_pending"
	advance := func(next string) {
		phaseDurations[phase] += time.Since(phaseStarted).Milliseconds()
		phase, phaseStarted = next, time.Now()
	}
	defer func() {
		phaseDurations[phase] += time.Since(phaseStarted).Milliseconds()
		if s.log != nil && (resultErr != nil || time.Since(started) > 30*time.Second) {
			s.log.Printf("platform configuration producer phase timing scope=%s phase=%s elapsed_ms=%d durations_ms=%v decision=%s error=%v", scope, phase, time.Since(started).Milliseconds(), phaseDurations, decision, resultErr)
		}
	}()
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
		advance("reconcile_pending")
		handled, err := s.reconcilePendingProducedTraffic(ctx, policy, authority)
		if err != nil {
			return interval, err
		}
		if handled {
			if len(pending) == 1 && pending[0] != nil && canCapturePendingHosts(scope, policy) {
				pending[0](policy, authority)
			}
			decision = "pending_observation"
			advance("refresh_pending_quality")
			return interval, s.refreshPendingProducerQuality(ctx, policy)
		}
		if policy.Serving.SinglePublication {
			done, err := s.store.HasVerifiedProducerPublication(policy.TargetScope, authority.ID)
			if err != nil || done {
				return interval, err
			}
		}
	}
	principal := platformProducerPrincipal()
	advance("read_previous")
	current, previous, haveCurrent, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, policy.TargetScope, "shadow")
	if err != nil {
		return interval, err
	}
	owned := haveCurrent && previous.ReleasedByType == model.ActorTypeBootstrap && previous.ReleasedByID == platformproducer.Actor && current.Metadata[platformproducer.PolicyReleaseMetadata] == authority.ID
	if owned && policy.Mode == "serving" && time.Since(previous.ReleasedAt) < time.Duration(policy.Serving.RolloutTimeoutSeconds)*time.Second {
		// Retry the exact shadow before replacing the assignment its newly
		// arrived executable-reader ACKs authenticate.
		handled, err := s.resumeProducedShadow(ctx, policy, authority, current)
		if handled || err != nil {
			if handled && err == nil && len(pending) == 1 && pending[0] != nil && canCapturePendingHosts(scope, policy) {
				pending[0](policy, authority)
			}
			decision = "resume_shadow"
			return interval, err
		}
	}
	if owned && policy.Mode != "serving" {
		// Complete a prior publication interrupted before expectation preparation.
		if _, err := s.preparePlatformReleaseSetConsumers(ctx, principal, current, previous); err != nil {
			return interval, err
		}
	}
	// Keep pending safety recovery, timeout, no-new-input quality observation
	// and interrupted shadow preparation above this new-input-only boundary.
	// A ticket covers the entire capture/compile/persist/publication attempt.
	// Track this boundary even when durable admission is not installed. A
	// canceled worker remains observable until its entire function returns.
	advance("read_input_deferral")
	deferred, err := s.store.ProducerInputDeferred(ctx, scope, model.PlatformPublicationPrecondition{ArtifactID: policyArtifact.ID, ContentHash: policyArtifact.ContentHash, ReleaseID: authority.ID, FencingToken: authority.FencingToken})
	if err != nil {
		return interval, errors.Join(err, s.refreshPendingProducerQuality(ctx, policy))
	}
	if deferred {
		decision = "new_input_deferred"
		return interval, s.refreshPendingProducerQuality(ctx, policy)
	}
	if scope == platformproducer.Scope {
		_, done, acquired := s.hostedCaptureGate.enter(ctx, false)
		if !acquired {
			decision = "returning_from_host_only_capture"
			return 30 * time.Second, s.refreshPendingProducerQuality(ctx, policy)
		}
		defer done()
	}
	defer s.platformProducerRuntime().input(scope)()
	if s.producerCaptureAdmission != nil {
		advance("admit_new_input")
		finish, deferred, err := s.producerCaptureAdmission(ctx, scope, model.PlatformPublicationPrecondition{ArtifactID: policyArtifact.ID, ContentHash: policyArtifact.ContentHash, ReleaseID: authority.ID, FencingToken: authority.FencingToken})
		if err != nil {
			return interval, err
		}
		if deferred {
			decision = "new_input_deferred"
			return interval, s.refreshPendingProducerQuality(ctx, policy)
		}
		if finish == nil {
			return interval, fmt.Errorf("producer capture admission missing completion handle")
		}
		defer func() {
			exitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, finish(exitCtx))
		}()
	}
	// A business capture can itself require the old full to have converged.
	// Do the independent, membership-only attempt before that dependency.
	advance("discover_offline_consumer_contraction")
	contraction, provenance, err := s.prepareOfflineContraction(ctx, policy, authority)
	if err != nil {
		return interval, err
	}
	if contraction != nil {
		decision = "offline_consumer_contraction"
		return interval, s.persistProducedCompilation(ctx, policy, authority, previous, principal, *contraction, provenance, "", advance)
	}
	// Runtime capture includes several independently bounded network probes.
	// Neither a new phase nor a new deadline renews evidence timestamps or
	// relaxes policy, integrity, convergence or LKG checks.
	captureCtx, cancelCapture := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelCapture()
	var projection platformIntentProjectionResponse
	var dnsRequest platformconfig.CompileRequest
	advance("capture_inputs")
	if policy.InputSource == platformproducer.DNSProducerInputSource {
		dnsRequest, err = s.captureDNSProducer(captureCtx, policy)
		projection.Intent, projection.Policy, projection.RuntimeSnapshot = dnsRequest.Intent, dnsRequest.Policy, dnsRequest.RuntimeSnapshot
	} else if capture != nil {
		projection, err = capture(captureCtx, principal)
	} else {
		projection, err = s.capturePlatformIntentForProducer(captureCtx, principal, policy)
	}
	if err != nil {
		return interval, err
	}
	if err := captureCtx.Err(); err != nil {
		return interval, err
	}
	cancelCapture()
	s.reconcileHostedWithdrawals(ctx, policy, authority, projection)
	s.rememberProducerQualityPlan(policy, projection)
	advance("source_digest")
	var sourceDigest string
	if policy.InputSource == platformproducer.DNSProducerInputSource {
		sourceDigest, err = platformconfig.Digest(struct {
			Authority string
			Request   platformconfig.CompileRequest
		}{authority.ID, dnsRequest})
	} else {
		sourceDigest, err = platformProducerSourceDigest(projection, authority.ID)
	}
	if err != nil {
		return interval, err
	}
	// Failed-source suppression is scoped to a compiler contract. A legacy
	// failed invocation must not suppress a fresh compact-only compilation;
	// its original observations and signature are never reused or rewritten.
	compiler := platformconfig.CompilerVersion
	if projection.Policy.RuntimeInputMode == platformconfig.RuntimeInputConfigurationV1 {
		compiler = platformconfig.ConfigurationInputCompilerVersion
	}
	if s.compilerInputOmitOriginal {
		compiler = platformconfig.CompactOnlyInputCompilerVersion
	}
	sourceDigest, err = platformconfig.Digest(struct{ Compiler, Source string }{compiler, sourceDigest})
	if err != nil {
		return interval, err
	}
	hosted, err := s.discoverHostedCapture(policy, projection)
	if err != nil {
		return interval, err
	}
	handoff := hosted.producer != "" && hosted.producer != authority.ID
	var restore *platformconfig.HostedDeclarationRestorePlan
	var restoreBase model.PlatformArtifactRelease
	if !handoff {
		restore, restoreBase, err = s.discoverHostedDeclarationRestore(ctx, policy)
		if err != nil {
			return interval, err
		}
	}
	ordinaryOffers := hosted.ordinaryPathOffers()
	sourceDigest, err = hosted.sourceDigest(sourceDigest)
	if handoff && err == nil {
		sourceDigest, err = platformconfig.Digest([]any{platformconfig.HostedFleetHandoffMetadata, sourceDigest, hosted.selected.ID})
		ordinaryOffers = false
	}
	if err != nil {
		return interval, err
	}
	if restore != nil {
		sourceDigest, err = platformconfig.Digest([]any{platformconfig.HostedDeclarationRestoreMetadata, sourceDigest, restoreBase.ID, restore})
		if err != nil {
			return interval, err
		}
		ordinaryOffers = false // don't keep resubmitting a failed restore
	}
	var failed bool
	if policy.Mode == "serving" {
		failed, err = s.producedSourcePreviouslyFailed(policy.TargetScope, authority.ID, sourceDigest)
		if err != nil || (failed && !ordinaryOffers) {
			return interval, err
		}
	}
	unchanged := owned && current.Metadata[platformproducer.SourceDigestMetadata] == sourceDigest && time.Since(previous.ReleasedAt) < time.Duration(policy.RefreshSeconds)*time.Second
	if unchanged && !ordinaryOffers {
		decision = "unchanged_source"
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
	decision = "initial_publication"
	if owned {
		decision = "source_changed"
		if current.Metadata[platformproducer.SourceDigestMetadata] == sourceDigest {
			decision = "refresh_due"
		}
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
	advance("compile")
	req := platformconfig.CompileRequest{OmitOriginalInput: s.compilerInputOmitOriginal, Intent: projection.Intent, Policy: projection.Policy, RuntimeSnapshot: projection.RuntimeSnapshot, PreviousTrafficPublication: dnsRequest.PreviousTrafficPublication}
	var compiled platformconfig.CompileResult
	provenance = platformCompilationSource{PolicyReleaseID: authority.ID, SourceDigest: sourceDigest, StaticIntentID: policy.StaticIntentArtifactID, StaticIntentDigest: policy.StaticIntentDigest, DNSPolicyID: policy.DNSPolicyArtifactID, DNSPolicyDigest: policy.DNSPolicyDigest}
	if handoff {
		advance("compile_hosted_fleet_handoff")
		source, e := s.readHostedAddressSourceFor(ctx, policy, authority, projection.hostedAddressHolds, s.observeRetainedTrafficSource, true)
		if e != nil {
			return interval, fmt.Errorf("hosted fleet handoff base unavailable: %w", e)
		}
		if source.Publication.ReleaseID != hosted.selected.ID || source.Publication.FencingToken != hosted.selected.FencingToken || source.Publication.ReleaseSetID != hosted.selected.ArtifactID {
			return interval, fmt.Errorf("hosted fleet handoff base changed before compilation")
		}
		compiled, err = platformconfig.CompileHostedFleetHandoff(req, source)
		provenance.hostedSource = source.Publication
	} else if restore != nil {
		advance("compile_hosted_declaration_restore")
		compiled, provenance.hostedSource, err = s.compileHostedDeclarationRestore(ctx, policy, authority, restoreBase, projection.hostedAddressHolds, req, *restore)
	} else if hosted.phase == "" {
		compiled, err = platformconfig.Compile(req)
	} else {
		source, e := s.readHostedAddressSource(ctx, policy, authority, projection.hostedAddressHolds)
		globalGate := e
		if errors.Is(e, errFullConsumerNotConverged) {
			source, e = s.readHostedAddressSourceWith(ctx, policy, authority, projection.hostedAddressHolds, s.observeHostedDraftBase)
		}
		if e != nil {
			return interval, e
		}
		if source.Publication.ReleaseID != hosted.selected.ID || source.Publication.FencingToken != hosted.selected.FencingToken || source.Publication.ReleaseSetID != hosted.selected.ArtifactID {
			return interval, fmt.Errorf("hosted address source changed before compilation")
		}
		var scoped *platformconfig.HostedTransitionCapture
		compiled, scoped, err = platformconfig.CompileHostedPathWithCapture(req, source)
		if err == nil && scoped != nil {
			advance("record_host_path_proposals")
			// Record the compiler-owned offer before constructing a scoped
			// successor. An offer is never itself a serving publication.
			s.recordPathOffers(ctx, authority, source, compiled, scoped)
		}
		if failed {
			// A failed ordinary shadow is not a failed hostname program.
			// Offer each current host above, but never re-stage this shadow.
			decision = "failed_global_source"
			return interval, err
		}
		managedCustody := false
		for _, r := range source.Inherited {
			managedCustody = managedCustody || r.Record.RecordKind == "hosted"
		}
		isolate := scoped != nil && (len(scoped.StableHosts) > 0 || (len(scoped.ExternalHosts) > 0 && len(scoped.Hosts) == 0 && !managedCustody))
		if globalGate != nil {
			if !errors.Is(globalGate, errFullConsumerNotConverged) {
				return interval, globalGate
			}
			advance("compile_offline_consumer_contraction")
			compiled, err = s.compileHostedOfflineContraction(ctx, req, source)
			if err != nil {
				return interval, errors.Join(globalGate, err)
			}
			provenance.offlinePredecessor = model.PlatformPublicationPrecondition{
				ArtifactID: source.Publication.ReleaseSetID, ContentHash: source.Publication.ReleaseSetDigest,
				ReleaseID: source.Publication.ReleaseID, FencingToken: source.Publication.FencingToken,
			}
		} else if err == nil && isolate {
			baseline, e := platformconfig.IndependentManagedBaselineRequest(source, req, scoped)
			if e != nil {
				return interval, e
			}
			var ordinaryInput string
			if ordinaryOffers {
				ordinaryInput, e = isolatedBaselineDigest(baseline, authority.ID, compiler, source.EffectiveHolds)
				if e != nil {
					return interval, e
				}
			}
			if sameHostedBaseline(hosted, ordinaryInput, current, owned, previous.ReleasedAt, source.Authority.ExpiresAt, time.Now(), interval, time.Duration(policy.RefreshSeconds)*time.Second) {
				decision = "host_offers_on_current_baseline"
				return interval, nil
			}
			if unchanged && ordinaryInput != "" && current.Metadata[isolatedBaselineInput] == ordinaryInput && current.ID != hosted.selected.ArtifactID {
				// Same exact parent-bound shadow, not a replacement for a
				// different parent: finish the outstanding ordinary rollout.
				if _, err := s.preparePlatformReleaseSetConsumers(ctx, principal, current, previous); err != nil {
					return interval, err
				}
				return interval, s.stageProducedTraffic(ctx, policy, authority, current)
			}
			compiled, err = platformconfig.CompileIndependentManagedBaseline(source, baseline)
			provenance.isolatedInput = ordinaryInput
		} else if err == nil {
			advance("compile_scoped_host_successor")
			successor, claim, selected, e := s.compileOfferedHostedSuccessor(ctx, req, source, scoped)
			if e != nil {
				if !errors.Is(e, platformconfig.ErrHostedSuccessorNeedsBaseline) || !canRefreshHostedBaseline(compiled, source, scoped) {
					return interval, e
				}
				advance("compile_hosted_baseline_refresh")
				compiled, err = platformconfig.CompileHostedBaselineRefresh(compiled, source, scoped)
				if err != nil {
					return interval, errors.Join(e, err)
				}
				if s.log != nil {
					s.log.Printf("hosted path successor deferred for baseline refresh base_release=%s error=%v", source.Publication.ReleaseID, e)
				}
			}
			if selected {
				compiled, provenance.hostedClaim = successor, claim
			}
		}
		if err == nil && len(compiled.InputSnapshot.HostedAddressReservations) > 0 {
			provenance.hostedSource = source.Publication
		}
	}
	if err == nil && policy.InputSource == platformproducer.DNSProducerInputSource {
		err = platformproducer.ValidateDNSProducerOutput(policy, compiled.DNSArtifact)
	}
	if err != nil {
		return interval, err
	}
	return interval, s.persistProducedCompilation(ctx, policy, authority, previous, principal, compiled, provenance, projection.BusinessSnapshotRevision, advance)
}

func (s *Server) persistProducedCompilation(ctx context.Context, policy platformproducer.Policy, authority, previous model.PlatformArtifactRelease, principal model.Principal, compiled platformconfig.CompileResult, provenance platformCompilationSource, businessRevision string, advance func(string)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// The verified compiler-input write has its own two-minute ceiling. Start
	// the outer deadline only now, so digesting and compilation cannot consume
	// the write's allowance or the time required to verify child artifacts.
	persistCtx, cancelPersist := context.WithTimeout(ctx, platformProducerPersistenceTimeout)
	defer cancelPersist()
	advance("persist_inputs")
	inputs, err := s.ensurePlatformProducerInputs(persistCtx, compiled, principal)
	if err != nil {
		return err
	}
	advance("persist_artifacts")
	result, err := s.materializePlatformCompilation(persistCtx, compiled, principal, &inputs, provenance)
	if err != nil {
		return err
	}
	if err = persistCtx.Err(); err != nil {
		return err
	}
	cancelPersist()
	advance("publish_shadow")
	s.warmDNSProducerPublication(ctx, policy, result.ReleaseArtifact)
	if err := ctx.Err(); err != nil {
		return err
	}
	_, release, _, _, err := s.store.ReleaseProducedPlatformArtifact(result.ReleaseArtifact.ID, authority.ID, previous.ID, principal)
	if err != nil {
		return err
	}
	advance("prepare_consumers")
	prepareCtx, cancelPrepare := context.WithTimeout(ctx, 2*time.Minute)
	defer cancelPrepare()
	if _, err = s.preparePlatformReleaseSetConsumers(prepareCtx, principal, result.ReleaseArtifact, release); err != nil {
		return err
	}
	cancelPrepare()
	s.appendAudit(principal, "platform_config.shadow_produced", "platform_release_set", result.ReleaseArtifact.ID, "", map[string]string{"policy_release_id": authority.ID, "source_digest": provenance.SourceDigest, "business_snapshot_revision": businessRevision, "release_id": release.ID})
	if s.log != nil {
		s.log.Printf("platform configuration producer published policy_release=%s release=%s artifact=%s source_digest=%s business_revision=%s", authority.ID, release.ID, result.ReleaseArtifact.ID, provenance.SourceDigest, businessRevision)
	}
	if policy.Mode == "serving" {
		advance("stage_traffic")
		return s.stageProducedTraffic(ctx, policy, authority, result.ReleaseArtifact)
	}
	return nil
}

// Compare the route behavior resolved from fixed runtime facts as well as
// configuration. A newly ready origin must replace a startup error route on
// the next check, not wait for the periodic artifact renewal. The resolvers
// validate freshness before hashing; timestamps alone do not cause churn.
// This fingerprint schedules compilation, never changes intent or grants
// permission to bypass signed policy, consumer convergence, or the LKG gates.
func platformProducerSourceDigest(projection platformIntentProjectionResponse, authority string) (string, error) {
	policy := platformconfig.NormalizePolicySnapshot(projection.Policy)
	routes, err := platformconfig.ResolveRouteOrigins(projection.Intent.Routes, projection.RuntimeSnapshot, policy)
	if err != nil {
		return "", err
	}
	routes, err = platformconfig.ApplyRoutePolicyConstraints(routes, policy, projection.RuntimeSnapshot.CapturedAt)
	if err != nil {
		return "", err
	}
	routes, err = platformconfig.ApplyTrafficPolicyConstraints(routes, policy, projection.RuntimeSnapshot)
	if err != nil {
		return "", err
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Hostname != routes[j].Hostname {
			return routes[i].Hostname < routes[j].Hostname
		}
		return routes[i].PathPrefix < routes[j].PathPrefix
	})
	// Endpoint eligibility can change when recovery evidence passes even when
	// the current winner has not changed. Fingerprint identities, not heartbeat
	// timestamps, so recovery is compiled on the next producer cycle.
	type endpointIdentity struct {
		EdgeID, EdgeGroupID string
		A, AAAA             []string
	}
	endpoints := make([]endpointIdentity, 0, len(projection.RuntimeSnapshot.DNSEdgeEndpoints))
	for _, endpoint := range projection.RuntimeSnapshot.DNSEdgeEndpoints {
		addresses, addresses6 := append([]string(nil), endpoint.A...), append([]string(nil), endpoint.AAAA...)
		sort.Strings(addresses)
		sort.Strings(addresses6)
		endpoints = append(endpoints, endpointIdentity{endpoint.EdgeID, endpoint.EdgeGroupID, addresses, addresses6})
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].EdgeID < endpoints[j].EdgeID })
	source := struct {
		Intent    platformconfig.PlatformIntent
		Policy    platformconfig.PolicySnapshot
		Authority string
		Routes    []platformconfig.CompiledRoute
		Endpoints []endpointIdentity
	}{projection.Intent, projection.Policy, authority, routes, endpoints}
	type assignment struct {
		QualityState   string `json:"QualityState,omitempty"`
		NodeID         string
		Hostname       string
		Type           string
		Version        string
		PrimaryEdgeID  string
		OrderedEdgeIDs []string
		Scope          string
		PrimarySince   *time.Time
	}
	assignments := []assignment{}
	for _, fact := range projection.RuntimeSnapshot.DNSSelections {
		selection := fact.PhysicalSelection
		if selection == nil {
			continue
		}
		if err := model.ValidateDNSPhysicalSelection(selection); err != nil {
			return "", err
		}
		assignments = append(assignments, assignment{NodeID: fact.NodeID, Hostname: fact.Hostname, Type: fact.Type,
			QualityState: selection.QualityState,
			Version:      selection.Version, PrimaryEdgeID: selection.PrimaryEdgeID, OrderedEdgeIDs: selection.OrderedEdgeIDs,
			Scope: selection.Scope, PrimarySince: selection.PrimarySince})
	}
	if len(assignments) == 0 {
		return platformconfig.Digest(source)
	}
	sort.Slice(assignments, func(left, right int) bool {
		first, second := assignments[left], assignments[right]
		return first.NodeID+"\x00"+first.Hostname+"\x00"+first.Type < second.NodeID+"\x00"+second.Hostname+"\x00"+second.Type
	})
	return platformconfig.Digest(struct {
		Source              any
		PhysicalAssignments []assignment
	}{source, assignments})
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

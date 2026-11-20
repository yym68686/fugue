package dnsserver

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
)

var errDNSShadowAlreadyAdvanced = errors.New("exact DNS shadow release set already advanced to serving")

type dnsShadowRouteSource struct {
	assignment model.PlatformConsumerAssignment
	release    model.PlatformArtifactRelease
	snapshot   model.PlatformDNSRouteSourceSnapshot
}

// Back-to-back shadow sweeps need distinct physical observations, not full
// downloads of the same source closure. Keep one authenticated response for
// conditional transport revalidation, including while the candidate is
// recovering. A fresh API observation, signature verification, a physical
// probe sweep and both source-selection rechecks are still required for an ACK.
func (s *Service) observeShadowDNSRouteSources(ctx context.Context, client platformconsumer.Client, id platformconsumer.Identity, candidate dnsPlatformCandidate, payload dnsServingPayload, probe dnsReadinessProbeFunc, evaluate dnsSourceEvaluation) error {
	var prior []model.PlatformDNSRouteSourceSnapshot
	if cached := s.shadowDNSRouteSource; cached != nil &&
		reflect.DeepEqual(cached.assignment, candidate.Assignment) && reflect.DeepEqual(cached.release, candidate.Release) {
		prior = []model.PlatformDNSRouteSourceSnapshot{cached.snapshot}
	} else {
		s.shadowDNSRouteSource = nil
	}
	// Record a hint only after the source was verified and rechecked against
	// the live ledger. A later query or recheck failure must not persist a
	// cursor, apply a candidate or acknowledge that release.
	read := dnsSelectedRouteSourceReader(client, id, candidate, prior...)
	_, _, err := s.observeDNSRouteSourcesWithReader(ctx, candidate, payload, nil, probe, read, func(observed dnsServingPayload, facts []dnsReadinessFact) error {
		snapshot := observed.routeSources.Snapshot
		if serving := s.shadowValidationParent.currentDNSServingState(); serving != nil {
			// Share exactly equal immutable content with the loaded closure.
			reuseDNSRouteSourceContent(&snapshot, serving.record.RouteSources)
		}
		s.shadowDNSRouteSource = &dnsShadowRouteSource{candidate.Assignment, candidate.Release, snapshot}
		return evaluate(observed, facts)
	})
	return err
}

// A shadow lane can still reference the exact ReleaseSet that has already
// advanced to gray or full. The heartbeat API only accepts the newest active
// publication of that set. Avoid another full source sweep for a receipt that
// cannot count, but never skip a different artifact or infer selection from
// the local checkpoint: a later rollback can make the shadow relevant again.
func (s *Service) dnsShadowAlreadyAdvanced(ctx context.Context, client platformconsumer.Client, id platformconsumer.Identity, shadow dnsPlatformCandidate) bool {
	if s.shadowValidationParent == nil {
		return false
	}
	loaded := s.shadowValidationParent.platformServing.Load()
	if loaded == nil || !loaded.record.Positive {
		return false
	}
	serving := loaded.record.Candidate
	a, b := shadow.Assignment, serving.Assignment
	if a.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
		(b.ReleaseChannel != model.PlatformArtifactReleaseChannelGray && b.ReleaseChannel != model.PlatformArtifactReleaseChannelFull) ||
		a.ReleaseSetID == "" || a.ReleaseSetID != b.ReleaseSetID || a.ArtifactID != b.ArtifactID || a.ContentHash != b.ContentHash ||
		a.ScopeKey != b.ScopeKey || a.ArtifactKind != b.ArtifactKind || shadow.Release.ReleasedAt.IsZero() || !serving.Release.ReleasedAt.After(shadow.Release.ReleasedAt) {
		return false
	}
	return client.CheckServingAssignment(ctx, id, b) == nil
}

func (s *Service) newIsolatedDNSShadowValidator() *Service {
	authority := platformcontrol.ConsumerAuthorityID(s.Config.EdgeGroupID)
	if s.PlatformTokenFile == "" || strings.TrimSpace(s.Config.CachePath) == "" || authority == "" || s.Config.PlatformScopeKey != "authority-cell:"+authority {
		return nil
	}
	cfg := s.Config
	cfg.CachePath += ".shadow-validation"
	// This object never starts listeners, ambient configuration, zone services or
	// serving synchronization. Only the existing signed shadow validator runs.
	return &Service{Config: cfg, HTTPClient: s.HTTPClient, Logger: s.Logger, PlatformTokenFile: s.PlatformTokenFile, shadowValidationParent: s, sharedContentBudget: s.contentBudget()}
}

func (s *Service) runIsolatedDNSShadow(ctx context.Context, worker *Service) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		s.syncIsolatedDNSShadowOnce(ctx, worker)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) syncIsolatedDNSShadowOnce(ctx context.Context, worker *Service) {
	err := worker.SyncPlatformShadowOnce(ctx)
	if errors.Is(err, errDNSContentBusy) {
		// Resource contention is not failed readiness or successful validation.
		// The bounded worker interval retries without queuing ahead of renewal.
		return
	}
	if ctx.Err() != nil {
		return
	}
	if errors.Is(err, errDNSShadowAlreadyAdvanced) {
		s.mu.Lock()
		s.platformCandidate = PlatformCandidateStatus{State: "awaiting_release"}
		s.mu.Unlock()
		return
	}
	s.recordDNSSnapshotFailure(err)
	worker.mu.Lock()
	candidate := worker.platformCandidate
	worker.mu.Unlock()
	if err != nil {
		candidate.State = "failed"
		candidate.LastError = err.Error()
		if errors.Is(err, platformconsumer.ErrAssignmentChanged) || errors.Is(err, platformconsumer.ErrNoShadowAssignment) {
			candidate.State = "awaiting_release"
		}
	}
	s.mu.Lock()
	s.platformCandidate = candidate
	s.mu.Unlock()
	if err != nil {
		s.Logger.Printf("DNS isolated shadow validation: %v", err)
	}
}

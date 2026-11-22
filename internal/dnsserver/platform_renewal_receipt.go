package dnsserver

import (
	"context"
	"reflect"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
)

// An independent sweep may finish while the configuration loop is still
// verifying unchanged content. Its transport receipt can only refresh the
// SAME already verified full release, never admit a new release, persist a
// checkpoint, acknowledge gray or borrow a retained/fallback authority.
func (s *Service) reportCompletedDNSRenewal(ctx context.Context, observation *dnsServingRenewal, id platformconsumer.Identity, wire func(*dnsServingState) error) error {
	if observation == nil || observation.base == nil || observation.state == nil || observation.publication == nil ||
		observation.publication.state != observation.base || wire == nil {
		return nil
	}
	s.mu.Lock()
	last, signature := s.platformServingReported, s.platformReceiptSignature
	s.mu.Unlock()
	// A receipt from the configuration loop takes priority. Never report an
	// independent success before that writer has reported in this process.
	if last.IsZero() || time.Since(last) < platformDNSConsumerPollInterval {
		return nil
	}
	base, state := observation.base, observation.state
	r, a := base.record.Candidate.Release, base.record.Candidate.Assignment
	if !sameDNSRenewalPublication(base, state) || !s.platformServingBound.Load() ||
		r.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || r.Status != model.PlatformArtifactReleaseStatusActive || r.VerificationState != model.PlatformArtifactVerificationStateVerified ||
		a.ReleaseChannel != r.ReleaseChannel || a.ArtifactReleaseID != r.ID || r.ID == "" || r.VerifiedAt == nil ||
		a.ExpectedGeneration == "" || r.VerifiedLKGGeneration == "" || r.VerifiedLKGGeneration != r.Generation ||
		r.ArtifactID != a.ReleaseSetID || r.ScopeKey != a.ScopeKey || r.FencingToken != a.FencingToken ||
		!reflect.DeepEqual(base.record.Candidate.Release, state.record.Candidate.Release) ||
		base.fallback != "" || state.fallback != "" || base.transition != nil || state.transition != nil ||
		base.record.Signature == "" || signature != base.record.Signature || state.record.RouteSources == nil || state.record.RouteSources.RetainedDNSLKG != nil ||
		state.payload.Policy.DNSReadiness == nil || !state.checkedAt.After(last) {
		return nil
	}
	checkpoint := s.platformCheckpoint.Load()
	if checkpoint == nil || checkpoint.record.Signature != base.record.Signature {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, platformDNSConsumerPollInterval)
	defer cancel()
	ready := func() bool {
		now := time.Now().UTC()
		if ctx.Err() != nil || s.platformCheckpoint.Load() != checkpoint || s.platformServing.publication.Load() != observation.publication ||
			s.platformRenewal.Load() != observation || s.currentDNSServingState() != state ||
			state.checkedAt.After(now) || now.Sub(state.checkedAt) >= time.Duration(state.payload.Policy.DNSReadiness.ProbeIntervalSeconds)*time.Second {
			return false
		}
		return dnsServingReady(state, now) && checkDNSServingSnapshot(state, now, false) == nil
	}
	if !ready() {
		return nil
	}
	client := s.platformConsumerClient()
	if err := wire(state); err != nil {
		return err
	}
	if err := s.checkDNSRouteSources(ctx, client, id, state.record.Candidate, state.record.RouteSources); err != nil {
		return err
	}
	if !s.platformReceiptMu.TryLock() {
		return nil // the main receipt is already under way
	}
	defer s.platformReceiptMu.Unlock()
	s.mu.Lock()
	unchanged := s.platformServingReported.Equal(last)
	s.mu.Unlock()
	if !unchanged || !ready() {
		return nil
	}
	h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ActualGeneration: a.ExpectedGeneration, LKGGeneration: a.ExpectedGeneration, ApplyStatus: "applied", ProbeStatus: "passed"}
	sendCtx, finish := startDNSSyncTrace(ctx, s.Logger, "completed_renewal_receipt")
	err := s.reportDNSAssignmentStateLocked(sendCtx, client, id, a, base, h, ready)
	finish(err)
	return err
}

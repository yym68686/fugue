package api

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/platformconfig"
)

type dnsObservationState struct {
	mu                      sync.Mutex
	runs, failures, skipped int64
	lastSuccess             time.Time
	mode                    string
}

func (s *Server) StartBackgroundDNSObservations(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	ticker := time.NewTicker(edgeDNSArtifactControllerInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		// Reuse the old writer lock during rolling upgrades; old and new processes
		// cannot update the ranking observation cursor concurrently.
		acquired, err := s.store.WithAdvisoryLock(ctx, edgeDNSArtifactControllerLockName, func() error { return s.reconcileDNSObservations(ctx, time.Now().UTC()) })
		s.dnsObservation.mu.Lock()
		if !acquired {
			s.dnsObservation.skipped++
		} else {
			s.dnsObservation.runs++
			if err != nil {
				s.dnsObservation.failures++
			} else {
				s.dnsObservation.lastSuccess = time.Now().UTC()
			}
		}
		s.dnsObservation.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			s.log.Printf("DNS observations retained previous facts: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) verifiedDNSObservationPolicy() (model.PlatformArtifact, platformconfig.DNSQueryPolicy, error) {
	a, found, err := s.verifiedPlatformArtifactForScope(model.PlatformArtifactKindPolicySnapshot, "global")
	if err != nil || !found {
		return model.PlatformArtifact{}, platformconfig.DNSQueryPolicy{}, errors.New("verified DNS observation policy unavailable")
	}
	var p platformconfig.PolicySnapshot
	if decodeCompilerArtifactContent(a, &p) != nil || p.Generation != a.Generation || p.Scope != a.ScopeKey || platformconfig.ValidatePolicySnapshot(p) != nil || p.DNSQueryPolicy == nil || platformconfig.ValidateDNSQueryPolicy(p.DNSQueryPolicy) != nil {
		return model.PlatformArtifact{}, platformconfig.DNSQueryPolicy{}, errors.New("verified DNS observation policy invalid")
	}
	return a, *p.DNSQueryPolicy, nil
}

func (s *Server) reconcileDNSObservations(ctx context.Context, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := s.reconcileHostedDNSFlattenRecords(ctx, now); err != nil {
		return err
	}
	artifact, policy, err := s.verifiedDNSObservationPolicy()
	if err != nil {
		return err
	}
	s.dnsObservation.mu.Lock()
	s.dnsObservation.mode = policy.RankingMode
	s.dnsObservation.mu.Unlock()
	if policy.RankingMode == "disabled" {
		return nil
	}
	builder, _, err := s.loadEdgeDNSLatencyProfileBuilder(ctx, now, false)
	if err != nil {
		return err
	}
	previous, err := s.store.ListEdgeDNSRoutingDecisions("")
	if err != nil {
		return err
	}
	_, updates := builder.finishWithCooldown(previous, now, time.Duration(policy.SwitchCooldownSeconds)*time.Second, true)
	if err := ctx.Err(); err != nil {
		return err
	}
	latest, _, err := s.verifiedDNSObservationPolicy()
	if err != nil || latest.ID != artifact.ID || latest.ContentHash != artifact.ContentHash {
		return errors.New("DNS observation policy changed during capture")
	}
	if err := s.store.UpsertEdgeDNSRoutingDecisions(updates); err != nil {
		return err
	}
	s.log.Printf("DNS observations refreshed; policy_digest=%s ranking=%s cooldown_seconds=%d decisions=%d", artifact.ContentHash, policy.RankingMode, policy.SwitchCooldownSeconds, len(updates))
	return nil
}

func (s *Server) writeDNSObservationMetrics(w io.Writer) {
	s.dnsObservation.mu.Lock()
	runs, failed, skipped, last, mode := s.dnsObservation.runs, s.dnsObservation.failures, s.dnsObservation.skipped, s.dnsObservation.lastSuccess, s.dnsObservation.mode
	s.dnsObservation.mu.Unlock()
	observability.WriteCounterMetric(w, "fugue_dns_observation_runs_total", "DNS observation controller runs.", nil, float64(runs))
	observability.WriteCounterMetric(w, "fugue_dns_observation_errors_total", "DNS observation controller failures preserving previous facts.", nil, float64(failed))
	observability.WriteCounterMetric(w, "fugue_dns_observation_skipped_total", "DNS observation controller lock skips.", nil, float64(skipped))
	observability.WriteGaugeMetric(w, "fugue_dns_observation_policy_mode", "Last verified DNS observation ranking mode.", map[string]string{"mode": mode}, 1)
	if !last.IsZero() {
		observability.WriteGaugeMetric(w, "fugue_dns_observation_last_success_timestamp_seconds", "Last successful observation refresh.", nil, float64(last.Unix()))
	}
}

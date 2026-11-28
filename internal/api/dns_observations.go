package api

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"fugue/internal/cellpublication"
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
		// This compatibility monitor is read-only. Retain its per-fleet sampling
		// cadence until the signed-policy checks move to their own executor.
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
	_, policy, err := s.verifiedDNSObservationPolicy()
	if err != nil {
		return err
	}
	s.dnsObservation.mu.Lock()
	s.dnsObservation.mode = "retired"
	s.dnsObservation.mu.Unlock()
	if policy.RankingMode != "disabled" && policy.OrderedProjection == nil {
		return errors.New("legacy DNS ranking retired; serving configuration requires ordered projection")
	}
	return nil
}

func (s *Server) writeDNSObservationMetrics(w io.Writer) {
	const verificationCacheMetric = "fugue_dns_route_source_verification_cache_total"
	observability.WriteMetricHeader(w, verificationCacheMetric, "DNS route source immutable verification material lookups, not trust decisions.", "counter")
	hits, misses, bypasses := cellpublication.DNSRouteSourceVerificationReuse()
	observability.WriteMetricSample(w, verificationCacheMetric, map[string]string{"result": "hit"}, float64(hits))
	observability.WriteMetricSample(w, verificationCacheMetric, map[string]string{"result": "miss"}, float64(misses))
	observability.WriteMetricSample(w, verificationCacheMetric, map[string]string{"result": "bypass"}, float64(bypasses))

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

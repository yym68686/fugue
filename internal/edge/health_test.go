package edge

import (
	"errors"
	"io"
	"log"
	"testing"
	"time"

	"fugue/internal/config"
)

func TestCaddyRecoveryPreservesRouteHealthDecision(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	bundle := testBundle("route-health-caddy-recovery")
	bundle.ValidUntil = now.Add(-2 * time.Hour)
	service := NewService(config.EdgeConfig{CaddyEnabled: true, MaxStale: time.Hour}, log.New(io.Discard, "", 0))
	service.recordSyncSuccess(bundle, "etag", now, false)
	service.recordCaddyApply(bundle.Version, len(bundle.Routes), "signature", errors.New("caddy unavailable"))
	service.recordCaddyApply(bundle.Version, len(bundle.Routes), "signature", nil)

	status := service.Status()
	if status.Healthy || status.Status != "unhealthy" || !status.MaxStaleExceeded || status.CaddyLastError != "" {
		t.Fatalf("Caddy recovery masked an expired route bundle: %+v", status)
	}

	service.recordNoCandidate(now)
	service.recordCaddyApply(bundle.Version, len(bundle.Routes), "signature", errors.New("caddy unavailable"))
	service.recordCaddyApply(bundle.Version, len(bundle.Routes), "signature", nil)
	status = service.Status()
	if !status.Healthy || status.Status != "degraded" || !status.MaxStaleExceeded ||
		status.DegradedReason != "inactive candidate is empty; retaining standby LKG beyond max_stale" {
		t.Fatalf("Caddy recovery lost the bounded standby decision: %+v", status)
	}
}

func TestEvaluateEdgeRouteHealthModes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		input edgeRouteHealthInput
		want  edgeRouteHealthDecision
	}{
		{name: "missing", input: edgeRouteHealthInput{Now: now}, want: edgeRouteHealthDecision{Status: "unhealthy"}},
		{name: "current", input: edgeRouteHealthInput{BundlePresent: true, ValidUntil: now.Add(time.Hour), Now: now, MaxStale: time.Hour}, want: edgeRouteHealthDecision{Status: "ok", Ready: true}},
		{name: "sync failed", input: edgeRouteHealthInput{BundlePresent: true, ValidUntil: now.Add(time.Hour), Now: now, MaxStale: time.Hour, Mode: edgeRouteHealthSyncFailed}, want: edgeRouteHealthDecision{Status: "stale", Ready: true, Stale: true, DegradedReason: "route bundle sync failed; serving cache"}},
		{name: "expired", input: edgeRouteHealthInput{BundlePresent: true, ValidUntil: now.Add(-time.Minute), Now: now, MaxStale: time.Hour}, want: edgeRouteHealthDecision{Status: "degraded", Ready: true, Stale: true, DegradedReason: "route bundle valid_until expired"}},
		{name: "max stale", input: edgeRouteHealthInput{BundlePresent: true, ValidUntil: now.Add(-2 * time.Hour), Now: now, MaxStale: time.Hour}, want: edgeRouteHealthDecision{Status: "unhealthy", Stale: true, MaxStaleExceeded: true, DegradedReason: "route bundle valid_until exceeded max_stale"}},
		{name: "standby", input: edgeRouteHealthInput{BundlePresent: true, ValidUntil: now.Add(-2 * time.Hour), Now: now, MaxStale: time.Hour, Mode: edgeRouteHealthStandbyWithoutCandidate}, want: edgeRouteHealthDecision{Status: "degraded", Ready: true, Stale: true, MaxStaleExceeded: true, DegradedReason: "inactive candidate is empty; retaining standby LKG beyond max_stale"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := evaluateEdgeRouteHealth(test.input); got != test.want {
				t.Fatalf("health decision=%+v, want %+v", got, test.want)
			}
		})
	}
}

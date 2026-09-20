package edge

import (
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestInventoryPreservesVerifiedServingLKGAfterCandidateRejection(t *testing.T) {
	now := time.Now().UTC()
	loaded, expires := now.Add(-time.Minute), now.Add(time.Minute)
	base := Status{Healthy: true, StaleCache: true, RouteBundleSource: edgeControlRouteSourceV1,
		BundleVersion: "old.p7.r1", CaddyEnabled: true, CaddyAppliedVersion: "old.p7.r1",
		ServingGeneration: "old", LKGGeneration: "old", LastSuccessAt: &loaded, BundleValidUntil: &expires,
		LastError: "new candidate rejected by route preservation gate"}
	cfg := config.EdgeConfig{CaddyEnabled: true}
	state, healthy, serving, bootstrap := inventoryProducerHealth(base, cfg)
	if state != model.EdgeHealthHealthy || !healthy || !serving || bootstrap {
		t.Fatalf("valid serving LKG cannot keep recovery alive: %s healthy=%t serving=%t bootstrap=%t", state, healthy, serving, bootstrap)
	}
	if !base.StaleCache || base.LastError == "" || base.BundleVersion != "old.p7.r1" {
		t.Fatal("reporting health changed candidate diagnostics or serving identity")
	}
	for name, change := range map[string]func(*Status, *config.EdgeConfig){
		"expired":         func(s *Status, _ *config.EdgeConfig) { s.BundleValidUntil = &loaded },
		"missing lease":   func(s *Status, _ *config.EdgeConfig) { s.BundleValidUntil = nil },
		"unapplied":       func(s *Status, _ *config.EdgeConfig) { s.CaddyAppliedVersion = "other" },
		"candidate":       func(s *Status, _ *config.EdgeConfig) { s.CandidateBundleLoaded = true },
		"no LKG":          func(s *Status, _ *config.EdgeConfig) { s.LKGGeneration = "" },
		"different LKG":   func(s *Status, _ *config.EdgeConfig) { s.LKGGeneration = "other" },
		"unhealthy":       func(s *Status, _ *config.EdgeConfig) { s.Healthy = false },
		"stale limit":     func(s *Status, _ *config.EdgeConfig) { s.MaxStaleExceeded = true },
		"caddy error":     func(s *Status, _ *config.EdgeConfig) { s.CaddyLastError = "apply failed" },
		"signature error": func(s *Status, _ *config.EdgeConfig) { s.FailureClass = model.EdgeInstanceFailureSignatureInvalid },
		"legacy source":   func(s *Status, _ *config.EdgeConfig) { s.RouteBundleSource = "" },
		"missing success": func(s *Status, _ *config.EdgeConfig) { s.LastSuccessAt = nil },
		"future success":  func(s *Status, _ *config.EdgeConfig) { s.LastSuccessAt = &expires },
		"disabled Caddy":  func(_ *Status, c *config.EdgeConfig) { c.CaddyEnabled = false },
		"draining":        func(_ *Status, c *config.EdgeConfig) { c.Draining = true },
	} {
		t.Run(name, func(t *testing.T) {
			s, c := base, cfg
			change(&s, &c)
			_, _, serving, bootstrap := inventoryProducerHealth(s, c)
			if serving || bootstrap {
				t.Fatal("unverified or non-serving cache authorized recovery")
			}
		})
	}
}

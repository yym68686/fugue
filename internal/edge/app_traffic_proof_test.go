package edge

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
)

func TestAppTrafficProofRequiresExactAppliedServingBundle(t *testing.T) {
	for name, change := range map[string]func(*Service, *model.EdgeRouteBundle){
		"applied":           func(*Service, *model.EdgeRouteBundle) {},
		"caddy disabled":    func(s *Service, _ *model.EdgeRouteBundle) { s.Config.CaddyEnabled = false },
		"old caddy version": func(s *Service, _ *model.EdgeRouteBundle) { s.snapshot.CaddyAppliedVersion = "old" },
		"caddy error":       func(s *Service, _ *model.EdgeRouteBundle) { s.snapshot.CaddyLastError = "apply failed" },
		"unhealthy":         func(s *Service, _ *model.EdgeRouteBundle) { s.snapshot.Healthy = false },
		"stale":             func(s *Service, _ *model.EdgeRouteBundle) { s.snapshot.StaleCache = true },
		"expired":           func(_ *Service, b *model.EdgeRouteBundle) { b.ValidUntil = time.Now().Add(-time.Second) },
		"candidate":         func(s *Service, _ *model.EdgeRouteBundle) { s.routePublication.Candidate = true },
		"inactive upstream": func(_ *Service, b *model.EdgeRouteBundle) {
			b.Routes[0].Upstreams[1].Status = model.EdgeRouteStatusUnavailable
		},
		"missing release":      func(_ *Service, b *model.EdgeRouteBundle) { b.Routes[0].Upstreams[1].ReleaseID = "" },
		"incomplete partition": func(_ *Service, b *model.EdgeRouteBundle) { b.Routes[0].Upstreams[1].Weight = 10 },
	} {
		t.Run(name, func(t *testing.T) {
			now := time.Now().UTC()
			service := NewService(config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a", CaddyEnabled: true}, nil)
			bundle := model.EdgeRouteBundle{Version: "applied", ValidUntil: now.Add(time.Minute), Routes: []model.EdgeRouteBinding{{Hostname: "app.example.test", PathPrefix: "/api", AppID: "app", TenantID: "tenant", EdgeGroupID: "group-a", Status: model.EdgeRouteStatusActive, RoutePolicy: model.EdgeRoutePolicyEnabled, UpstreamURL: "http://old:8080", Upstreams: []model.EdgeRouteUpstream{
				{Role: "stable", ReleaseID: "release-old", Weight: 80, UpstreamURL: "http://old:8080", Status: "active"},
				{Role: "candidate", ReleaseID: "release-new", Weight: 20, UpstreamURL: "http://new:8080", Status: "active"},
			}}}}
			service.recordSyncSuccess(bundle, bundle.Version, now, false)
			service.mu.Lock()
			service.snapshot.Healthy, service.snapshot.CaddyAppliedVersion = true, bundle.Version
			change(service, &bundle)
			service.bundle = &bundle
			service.routeIndex.Store(buildEdgeRouteIndex(bundle, service.Config.EdgeGroupID, service.routePublication))
			service.mu.Unlock()
			req := httptest.NewRequest(http.MethodHead, "https://app.example.test/api/items", nil)
			nonce := "0123456789abcdef0123456789abcdef"
			req.Header.Set(routeproof.RequestHeader, "1")
			req.Header.Set(routeproof.NonceHeader, nonce)
			rec := httptest.NewRecorder()
			service.ProxyHandler().ServeHTTP(rec, req)
			if name != "applied" {
				if got := rec.Header().Get(routeproof.AppTrafficHeader); got != "" {
					t.Fatalf("unapplied traffic obtained proof: %s", got)
				}
				return
			}
			want, err := routeproof.AppTrafficDigest(bundle.Routes[0])
			if err != nil {
				t.Fatal(err)
			}
			proof, err := routeprobe.ParseResponse(rec.Result(), nonce, now)
			if err != nil || proof.AppTrafficDigest != want || proof.Version != bundle.Version || !proof.ValidUntil.Equal(bundle.ValidUntil) || rec.Body.Len() != 0 {
				t.Fatalf("proof=%+v error=%v", proof, err)
			}
		})
	}
}

func TestAppTrafficProofRejectsReplacedIndex(t *testing.T) {
	s := NewService(config.EdgeConfig{CaddyEnabled: true, EdgeID: "edge", EdgeGroupID: "group"}, nil)
	b := model.EdgeRouteBundle{Version: "same-generation", ValidUntil: time.Now().Add(time.Minute)}
	s.recordSyncSuccess(b, b.Version, time.Now(), false)
	old := s.currentRouteIndex()
	s.recordSyncSuccess(b, b.Version, time.Now(), false)
	s.mu.Lock()
	s.snapshot.Healthy, s.snapshot.CaddyAppliedVersion = true, b.Version
	s.mu.Unlock()
	if s.appTrafficProofApplied(old) {
		t.Fatal("replaced index obtained proof")
	}
	if !s.appTrafficProofApplied(s.currentRouteIndex()) {
		t.Fatal("current applied index rejected")
	}
}

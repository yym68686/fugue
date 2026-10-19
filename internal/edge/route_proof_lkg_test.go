package edge

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
)

func TestRetainedRouteProofObservesAppliedSignedLKGWithoutRenewal(t *testing.T) {
	for _, scenario := range []string{"offline", "candidate", "expired offline window", "tampered", "revoked", "unsigned", "unapplied", "proxy failure", "excluded"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			s := NewService(config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a", CaddyEnabled: true, MaxStale: time.Hour,
				BundleSigningKey: "synthetic-lkg-signing-key", BundleSigningKeyID: "key-a"}, nil)
			b := model.EdgeRouteBundle{Version: "already-applied", EdgeGroupID: "group-a", GeneratedAt: now.Add(-2 * time.Minute), Routes: []model.EdgeRouteBinding{
				{Hostname: "app.example.test", PathPrefix: "/", EdgeGroupID: "group-a", Status: "active", RoutePolicy: "edge_enabled", UpstreamURL: "http://origin.invalid"},
			}}
			if scenario == "excluded" {
				b.Routes[0].ExcludedEdgeIDs = []string{"edge-a"}
			}
			b = bundleauth.SignEdgeRouteBundle(b, s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, time.Minute)
			switch scenario {
			case "expired offline window":
				s.Config.MaxStale = 30 * time.Second
			case "tampered":
				b.Routes[0].UpstreamURL = "http://other.invalid"
			case "revoked":
				s.Config.BundleRevokedKeyIDs = []string{"key-a"}
			case "unsigned":
				b.Signature, b.Signatures = "", nil
			}
			s.recordSyncSuccessWithPublication(b, "", now, true, routePublicationMetadata{Candidate: scenario == "candidate"})
			if scenario != "unapplied" {
				s.recordCaddyApply(b.Version, len(b.Routes), "applied", nil)
			}
			if scenario == "proxy failure" {
				s.recordCaddyApply(b.Version, len(b.Routes), "applied", errors.New("executor unavailable"))
			}
			before, _ := s.Bundle()
			request := func(retained bool) *httptest.ResponseRecorder {
				r := httptest.NewRequest(http.MethodHead, "https://app.example.test/", nil)
				r.Header.Set(routeproof.RequestHeader, "1")
				r.Header.Set(routeproof.NonceHeader, "0123456789abcdef0123456789abcdef")
				if retained {
					r.Header.Set(routeproof.ModeHeader, routeproof.RetainedLKGMode)
				}
				w := httptest.NewRecorder()
				s.ProxyHandler().ServeHTTP(w, r)
				return w
			}
			if ordinary := request(false); ordinary.Code != 503 {
				t.Fatal("expired lease authorized an ordinary rollout probe", ordinary)
			}
			for range 2 {
				w := request(true)
				if scenario != "offline" {
					if w.Code != 503 || w.Header().Get(routeproof.DigestHeader) != "" {
						t.Fatal("invalid LKG was observed", w)
					}
					continue
				}
				if w.Code != 204 || w.Header().Get(routeproof.ModeHeader) != routeproof.RetainedLKGMode ||
					w.Header().Get(routeproof.BundleExpiryHeader) != b.ValidUntil.Format(time.RFC3339Nano) ||
					w.Header().Get(routeproof.ExpiryHeader) != b.ValidUntil.Add(time.Hour).Format(time.RFC3339Nano) ||
					w.Header().Get(routeproof.AppTrafficHeader) != "" {
					t.Fatal("retained proof lost original lease or renewed its deadline", w)
				}
				if _, err := routeprobe.ParseResponse(w.Result(), "0123456789abcdef0123456789abcdef", now); err == nil {
					t.Fatal("retained observation admitted as a new release proof")
				}
			}
			after, _ := s.Bundle()
			if !reflect.DeepEqual(before, after) {
				t.Fatal("observation changed immutable serving configuration")
			}
		})
	}
}

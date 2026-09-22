package edge

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
)

func TestExcludedRouteProofCannotAuthorizeServingOrContactOrigin(t *testing.T) {
	for _, scenario := range []string{"edge", "group", "omitted", "wrong_edge", "wrong_group", "foreign", "candidate", "expired", "inactive_request"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			origin := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
			defer origin.Close()
			s := NewService(config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a"}, nil)
			route := model.EdgeRouteBinding{Hostname: "app.example.test", PathPrefix: "/", Status: model.EdgeRouteStatusActive, RoutePolicy: model.EdgeRoutePolicyEnabled, EdgeGroupID: "group-a", UpstreamURL: origin.URL, ExcludedEdgeIDs: []string{"edge-a"}}
			bundle := model.EdgeRouteBundle{Version: "bound", ValidUntil: time.Now().Add(time.Minute)}
			state := routeproof.StateExcluded
			switch scenario {
			case "group":
				route.ExcludedEdgeIDs, route.ExcludedEdgeGroupIDs = nil, []string{"group-a"}
			case "omitted":
				state = ""
			case "wrong_edge":
				route.ExcludedEdgeIDs = []string{"other-edge"}
			case "wrong_group":
				route.ExcludedEdgeIDs, route.ExcludedEdgeGroupIDs = nil, []string{"other-group"}
			case "foreign":
				route.EdgeGroupID = "other-group"
			case "expired":
				bundle.ValidUntil = time.Now().Add(-time.Minute)
			case "inactive_request":
				state = model.EdgeRouteStatusDisabled
			}
			bundle.Routes = []model.EdgeRouteBinding{route}
			s.recordSyncSuccessWithPublication(bundle, "", time.Now(), false, routePublicationMetadata{Candidate: scenario == "candidate"})
			r := httptest.NewRequest(http.MethodHead, "https://app.example.test/", nil)
			r.Header.Set(routeproof.RequestHeader, "1")
			r.Header.Set(routeproof.NonceHeader, "0123456789abcdef0123456789abcdef")
			if state != "" {
				r.Header.Set(routeproof.StateHeader, state)
			}
			w := httptest.NewRecorder()
			s.ProxyHandler().ServeHTTP(w, r)
			if scenario == "edge" || scenario == "group" {
				proof, err := routeprobe.ParseResponse(w.Result(), r.Header.Get(routeproof.NonceHeader), time.Now())
				digest, _ := routeproof.Digest(route)
				if err != nil || proof.State != routeproof.StateExcluded || proof.Digest != digest || proof.AppTrafficDigest != "" {
					t.Fatal("excluded proof differs", proof, err)
				}
			} else if w.Code != http.StatusServiceUnavailable || w.Header().Get(routeproof.DigestHeader) != "" {
				t.Fatal("invalid exclusion obtained evidence", w)
			}
			if calls.Load() != 0 {
				t.Fatal("exclusion proof reached origin")
			}
		})
	}
}

func TestRouteProofUsesLoadedPathAndNeverContactsOrigin(t *testing.T) {
	var calls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusOK) }))
	defer origin.Close()
	route := model.EdgeRouteBinding{Hostname: "app.example.test", PathPrefix: "/", AppID: "app-a", TenantID: "tenant-a", EdgeGroupID: "group-a", Status: "active", RoutePolicy: model.EdgeRoutePolicyEnabled, UpstreamURL: origin.URL}
	pathRoute := route
	pathRoute.PathPrefix, pathRoute.UpstreamURL = "/api", "http://private-user:credential@private-origin:8080"
	bundle := model.EdgeRouteBundle{Version: "loaded-a", ValidUntil: time.Now().Add(time.Hour), Routes: []model.EdgeRouteBinding{route, pathRoute}}
	service := NewService(config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a"}, nil)
	service.recordSyncSuccess(bundle, "loaded-a", time.Now(), false)
	nonce := "0123456789abcdef0123456789abcdef"
	for _, path := range []string{"/", "/api", "/api/items"} {
		req := httptest.NewRequest(http.MethodHead, "https://app.example.test"+path, nil)
		req.Header.Set(routeproof.RequestHeader, "1")
		req.Header.Set(routeproof.NonceHeader, nonce)
		rec := httptest.NewRecorder()
		service.ProxyHandler().ServeHTTP(rec, req)
		expected := route
		if path != "/" {
			expected = pathRoute
		}
		digest, _ := routeproof.Digest(expected)
		for key, want := range map[string]string{routeproof.DigestHeader: digest, routeproof.NonceHeader: nonce, routeproof.VersionHeader: "loaded-a", routeproof.ExpiryHeader: bundle.ValidUntil.UTC().Format(time.RFC3339Nano), routeproof.EdgeHeader: "edge-a", routeproof.GroupHeader: "group-a", "Cache-Control": "no-store"} {
			if got := rec.Header().Get(key); got != want {
				t.Fatalf("%s=%s want %s", key, got, want)
			}
		}
		if rec.Code != 204 || rec.Body.Len() != 0 || strings.Contains(rec.Header().Get(routeproof.DigestHeader), "credential") {
			t.Fatal(rec)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("proof reached origin")
	}
	rec := httptest.NewRecorder()
	service.ProxyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "https://app.example.test/", nil))
	if calls.Load() != 1 || rec.Code != 200 || rec.Header().Get(routeproof.DigestHeader) != "" {
		t.Fatal("ordinary proxy changed", rec.Code, calls.Load())
	}
}

func TestRouteProofRejectsUnusableRoutesAndInvalidRequests(t *testing.T) {
	for name, change := range map[string]func(*model.EdgeRouteBundle, *http.Request){
		"expired":        func(b *model.EdgeRouteBundle, r *http.Request) { b.ValidUntil = time.Now().Add(-time.Second) },
		"missing expiry": func(b *model.EdgeRouteBundle, r *http.Request) { b.ValidUntil = time.Time{} },
		"foreign group":  func(b *model.EdgeRouteBundle, r *http.Request) { b.Routes[0].EdgeGroupID = "group-other" },
		"excluded edge":  func(b *model.EdgeRouteBundle, r *http.Request) { b.Routes[0].ExcludedEdgeIDs = []string{"edge-a"} },
		"excluded group": func(b *model.EdgeRouteBundle, r *http.Request) {
			b.Routes[0].ExcludedEdgeGroupIDs = []string{"group-a"}
		},
		"inactive": func(b *model.EdgeRouteBundle, r *http.Request) { b.Routes[0].Status = "disabled" },
		"policy disabled": func(b *model.EdgeRouteBundle, r *http.Request) {
			b.Routes[0].RoutePolicy = "disabled"
		},
		"missing host": func(b *model.EdgeRouteBundle, r *http.Request) {
			r.Host = "other.example.test"
			r.Header.Set("X-Fugue-Edge-Route-Host", "app.example.test")
		},
		"empty bundle":  func(b *model.EdgeRouteBundle, r *http.Request) { b.Routes = nil },
		"wrong method":  func(b *model.EdgeRouteBundle, r *http.Request) { r.Method = http.MethodPost },
		"missing nonce": func(b *model.EdgeRouteBundle, r *http.Request) { r.Header.Del(routeproof.NonceHeader) },
		"invalid nonce": func(b *model.EdgeRouteBundle, r *http.Request) {
			r.Header.Set(routeproof.NonceHeader, "arbitrary-input")
		},
		"ambiguous nonce": func(b *model.EdgeRouteBundle, r *http.Request) {
			r.Header.Add(routeproof.NonceHeader, "0123456789abcdef0123456789abcdef")
		},
		"unknown version":   func(b *model.EdgeRouteBundle, r *http.Request) { r.Header.Set(routeproof.RequestHeader, "2") },
		"ambiguous version": func(b *model.EdgeRouteBundle, r *http.Request) { r.Header.Add(routeproof.RequestHeader, "1") },
	} {
		t.Run(name, func(t *testing.T) {
			service := NewService(config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a"}, nil)
			bundle := model.EdgeRouteBundle{Version: "loaded", ValidUntil: time.Now().Add(time.Hour), Routes: []model.EdgeRouteBinding{{Hostname: "app.example.test", EdgeGroupID: "group-a", Status: "active", RoutePolicy: model.EdgeRoutePolicyEnabled}}}
			req := httptest.NewRequest(http.MethodHead, "https://app.example.test/", nil)
			req.Header.Set(routeproof.RequestHeader, "1")
			req.Header.Set(routeproof.NonceHeader, "0123456789abcdef0123456789abcdef")
			change(&bundle, req)
			service.recordSyncSuccess(bundle, "loaded", time.Now(), false)
			rec := httptest.NewRecorder()
			service.ProxyHandler().ServeHTTP(rec, req)
			if rec.Code < 400 || rec.Header().Get(routeproof.DigestHeader) != "" || rec.Header().Get(routeproof.NonceHeader) != "" || rec.Header().Get(routeproof.ExpiryHeader) != "" {
				t.Fatal("invalid request obtained evidence", rec)
			}
		})
	}
}

func TestRouteProofRejectsCandidateAndKeepsBundleExpiry(t *testing.T) {
	service := NewService(config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a"}, nil)
	expires := time.Now().Add(time.Minute).UTC()
	bundle := model.EdgeRouteBundle{Version: "candidate", ValidUntil: expires, Routes: []model.EdgeRouteBinding{{Hostname: "app.example.test", EdgeGroupID: "group-a", Status: "active", RoutePolicy: model.EdgeRoutePolicyEnabled}}}
	service.recordSyncSuccessWithPublication(bundle, "candidate", time.Now(), false, routePublicationMetadata{Candidate: true})
	probe := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodHead, "https://app.example.test/", nil)
		req.Header.Set(routeproof.RequestHeader, "1")
		req.Header.Set(routeproof.NonceHeader, "0123456789abcdef0123456789abcdef")
		rec := httptest.NewRecorder()
		service.ProxyHandler().ServeHTTP(rec, req)
		return rec
	}
	if rec := probe(); rec.Code != 503 || rec.Header().Get(routeproof.DigestHeader) != "" {
		t.Fatal("candidate proved serving", rec)
	}
	bundle.Version = "published"
	service.recordSyncSuccess(bundle, "published", time.Now(), false)
	for range 2 {
		if rec := probe(); rec.Code != 204 || rec.Header().Get(routeproof.VersionHeader) != "published" || rec.Header().Get(routeproof.ExpiryHeader) != expires.Format(time.RFC3339Nano) {
			t.Fatal("proof renewed/mixed publication", rec)
		}
	}
	loaded, _ := service.Bundle()
	if loaded.Version != bundle.Version || !loaded.ValidUntil.Equal(expires) {
		t.Fatal("probe changed serving bundle")
	}
}

func TestInactiveRouteProofRequiresExactLocalNonOriginState(t *testing.T) {
	for _, state := range []string{"disabled", "unavailable"} {
		for name, mutate := range map[string]func(*model.EdgeRouteBundle, *http.Request){
			"valid":           func(*model.EdgeRouteBundle, *http.Request) {},
			"wrong state":     func(b *model.EdgeRouteBundle, r *http.Request) { b.Routes[0].Status = "active" },
			"origin":          func(b *model.EdgeRouteBundle, r *http.Request) { b.Routes[0].UpstreamURL = "http://origin:8080" },
			"weighted origin": func(b *model.EdgeRouteBundle, r *http.Request) { b.Routes[0].Upstreams = []model.EdgeRouteUpstream{{}} },
			"foreign":         func(b *model.EdgeRouteBundle, r *http.Request) { b.Routes[0].EdgeGroupID = "other" },
			"excluded":        func(b *model.EdgeRouteBundle, r *http.Request) { b.Routes[0].ExcludedEdgeIDs = []string{"edge-a"} },
			"route policy":    func(b *model.EdgeRouteBundle, r *http.Request) { b.Routes[0].RoutePolicy = "route_a_only" },
			"expired":         func(b *model.EdgeRouteBundle, r *http.Request) { b.ValidUntil = time.Now().Add(-time.Second) },
			"unknown mode":    func(b *model.EdgeRouteBundle, r *http.Request) { r.Header.Set(routeproof.StateHeader, "any") },
			"duplicate mode":  func(b *model.EdgeRouteBundle, r *http.Request) { r.Header.Add(routeproof.StateHeader, state) },
			"empty mode":      func(b *model.EdgeRouteBundle, r *http.Request) { r.Header.Set(routeproof.StateHeader, "") },
		} {
			t.Run(state+"/"+name, func(t *testing.T) {
				s := NewService(config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a"}, nil)
				b := model.EdgeRouteBundle{Version: "published", ValidUntil: time.Now().Add(time.Minute), Routes: []model.EdgeRouteBinding{{Hostname: "app.example.test", EdgeGroupID: "group-a", Status: state, RoutePolicy: "edge_enabled"}}}
				r := httptest.NewRequest(http.MethodHead, "https://app.example.test/", nil)
				r.Header.Set(routeproof.RequestHeader, "1")
				r.Header.Set(routeproof.NonceHeader, "0123456789abcdef0123456789abcdef")
				r.Header.Set(routeproof.StateHeader, state)
				mutate(&b, r)
				s.recordSyncSuccess(b, "published", time.Now(), false)
				w := httptest.NewRecorder()
				s.ProxyHandler().ServeHTTP(w, r)
				if name == "valid" {
					want, _ := routeproof.Digest(b.Routes[0])
					if w.Code != 204 || w.Header().Get(routeproof.StateHeader) != state || w.Header().Get(routeproof.DigestHeader) != want || w.Header().Get(routeproof.ExpiryHeader) != b.ValidUntil.UTC().Format(time.RFC3339Nano) {
						t.Fatal(w)
					}
				} else if w.Code < 400 || w.Header().Get(routeproof.DigestHeader) != "" || w.Header().Get(routeproof.StateHeader) != "" {
					t.Fatal("invalid inactive proof accepted", w)
				}
			})
		}
	}
}

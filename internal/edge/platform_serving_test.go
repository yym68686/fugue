package edge

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	"fugue/internal/routeartifact"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
	"fugue/internal/trafficbinding"
)

func TestTrafficServingReportsOnlyDurablyAppliedAndProbedRelease(t *testing.T) {
	for _, mode := range []string{"valid", "route-proof-mismatch", "tls-failed", "cache-missing", "cache-different", "caddy-not-applied", "assignment-changed", "bundle-changed", "shadow", "newer-full", "unverified-parent", "unverified-tls", "cursor-corrupt", "cursor-unwritable", "recover-after-failure", "restart-after-failure", "report-rejected", "activation-lost", "inactive"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().UTC()
			group := "edge-group-test"
			host := "app.example.test"
			compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: host, UpstreamURL: "http://origin:8080", Enabled: true, TLSPolicy: model.EdgeRouteTLSPolicyPlatform}}, TLS: []platformconfig.TLSIntent{{Hostname: host, Policy: model.EdgeRouteTLSPolicyPlatform}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: "global", TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{group}}}, TLSReadiness: &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 10, ProbeTimeoutSeconds: 1, FactFreshnessSeconds: 60, MaxConcurrency: 2, MaxProbes: 10}}})
			if err != nil {
				t.Fatal(err)
			}
			keys := bundleauth.NewKeyring("synthetic-serving-key", "key", "", "", nil)
			sign := func(a model.PlatformArtifact, id string) model.PlatformArtifact {
				a.ID, a.ScopeKey, a.Status, a.GenerationSequence = id, "global", model.PlatformArtifactStatusValidated, 1
				a.ContentHash, _ = platformconfig.Digest(a.Content)
				a, err = platformsafety.SignPlatformArtifact(a, keys)
				if err != nil {
					t.Fatal(err)
				}
				return a
			}
			route := sign(compiled.RouteArtifact, "route")
			tlsArtifact := sign(compiled.TLSArtifact, "tls")
			parent := sign(platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{"route", "dns", "tls"}, now), "parent")
			release := model.PlatformArtifactRelease{ID: "release", ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, ScopeKey: "global", Generation: parent.Generation, ReleaseChannel: "gray", CanaryRuleRef: "cohort=first", FencingToken: 2, Status: model.PlatformArtifactReleaseStatusActive}
			assignment := func(a model.PlatformArtifact) model.PlatformConsumerAssignment {
				return model.PlatformConsumerAssignment{ExpectedConsumerSetID: "set-" + a.ID, ReleaseSetID: parent.ID, ArtifactReleaseID: release.ID, ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: "global", Revision: 1, ExpectedGeneration: a.Generation, ContentHash: a.ContentHash, GenerationSequence: 1, FencingToken: 2, ReleaseChannel: release.ReleaseChannel}
			}
			ra, ta := assignment(route), assignment(tlsArtifact)
			projection, err := routeartifact.ProjectRelease(parent, route, ra, release, keys)
			if err != nil {
				t.Fatal(err)
			}
			bundle, err := routeartifact.MaterializeSnapshotForGroup(projection, group)
			if err != nil {
				t.Fatal(err)
			}
			bundle.Version = "group-publication"
			bundle.GeneratedAt = now
			bundle = bundleauth.SignEdgeRouteBundleWithKeyring(bundle, keys, time.Hour)
			var reports []platformcontrol.PlatformConsumerHeartbeatEnvelope
			changed := false
			rejectReport := false
			routeCalls := 0
			tlsCalls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/platform-state/consumers/identity":
					json.NewEncoder(w).Encode(map[string]any{"token": "token", "component": "edge-worker", "node_id": "edge", "scope_key": "global", "artifact_kinds": []string{route.ArtifactKind, tlsArtifact.ArtifactKind}, "expires_at": time.Now().Add(time.Minute)})
				case "/v1/platform-state/consumers/assignment":
					if r.URL.Query().Get("serving_only") != "true" {
						t.Error("serving observation used an unselected release lane")
					}
					items := []model.PlatformConsumerAssignment{ra, ta}
					if changed {
						items[0].FencingToken++
						if mode == "newer-full" {
							items[0].ReleaseChannel, items[1].ReleaseChannel = "full", "full"
						}
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: items})
				case "/v1/platform-state/consumers/artifacts/route":
					json.NewEncoder(w).Encode(map[string]any{"artifact": route, "assignment": ra, "release": release})
				case "/v1/platform-state/consumers/artifacts/tls":
					json.NewEncoder(w).Encode(map[string]any{"artifact": tlsArtifact, "assignment": ta, "release": release})
				case "/v1/platform-state/consumers/artifacts/parent":
					json.NewEncoder(w).Encode(map[string]any{"artifact": parent, "assignment": ra, "release": release})
				case "/v1/platform-state/consumers/trusted-heartbeat":
					var h platformcontrol.PlatformConsumerHeartbeatEnvelope
					if err := json.NewDecoder(r.Body).Decode(&h); err != nil {
						t.Fatal(err)
					}
					if !slices.Contains(h.CompatibilityCapabilities, platformcontrol.TrafficReleaseCapabilityV1) {
						t.Error("executor capability missing")
					}
					reports = append(reports, h)
					if rejectReport && h.ArtifactKind == route.ArtifactKind {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerHeartbeatResponse{Consumer: model.PlatformConsumerInstance{IdentityVerified: true, ConsumerID: h.ConsumerID, Sequence: h.Sequence, EvidenceHash: h.EvidenceHash, ExpectedConsumerSetID: h.ExpectedConsumerSetID}})
				default:
					t.Error("unexpected request", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			dir := t.TempDir()
			cache := filepath.Join(dir, "routes.json")
			token := filepath.Join(dir, "token")
			os.WriteFile(token, []byte("pod-token"), 0600)
			s := NewService(config.EdgeConfig{APIURL: server.URL, EdgeID: "edge", EdgeGroupID: group, CachePath: cache, CaddyEnabled: true, CaddyListenAddr: "127.0.0.1:8443", CaddyTLSMode: "internal", BundleSigningKey: "synthetic-serving-key", BundleSigningKeyID: "key"}, nil)
			s.PlatformTokenFile = token
			if mode == "activation-lost" || mode == "inactive" {
				activeSlot := model.EdgeSlotA
				if mode == "inactive" {
					activeSlot = model.EdgeSlotB
				}
				s.Config.EdgeSlot = model.EdgeSlotA
				s.RouteBundleSource = RouteBundleSourceConfig{URL: server.URL + edgeControlBundlePath, CandidateURL: server.URL + edgeControlCandidateBundlePath, TokenFile: token, VerifierKeyringFile: filepath.Join(dir, "verifier"), ActivationStateFile: writeInventoryActivationFixture(t, now, group, activeSlot, strings.Repeat("1", 40))}
			}
			s.recordSyncSuccess(bundle, "", now, false)
			s.recordCaddyApply(bundle.Version, len(bundle.Routes), "config", nil)
			raw, _ := json.Marshal(cacheFile{Version: cacheFileVersion, Bundle: bundle})
			os.WriteFile(cache, raw, 0600)
			switch mode {
			case "cache-missing":
				os.Remove(cache)
			case "cache-different":
				os.WriteFile(cache, []byte("{}"), 0600)
			case "caddy-not-applied":
				s.recordCaddyApply("old", 1, "config", nil)
			case "unverified-parent":
				parent.ContentHash = "sha256:tampered"
			case "unverified-tls":
				tlsArtifact.ContentHash = "sha256:tampered"
			case "cursor-corrupt":
				os.WriteFile(cache+".platform-serving-cursor.json", []byte("invalid"), 0600)
			case "cursor-unwritable":
				os.Mkdir(cache+".platform-serving-cursor.json", 0700)
			case "shadow":
				s.bundle.TrafficRelease.ReleaseChannel = "shadow"
			}
			probe := func(_ context.Context, r model.EdgeRouteBinding, _ time.Duration) (routeprobe.Proof, error) {
				routeCalls++
				d, _ := routeproof.Digest(r)
				p := routeprobe.Proof{Digest: d, Version: bundle.Version, EdgeID: "edge", GroupID: group, CheckedAt: time.Now().UTC(), ValidUntil: time.Now().Add(time.Minute)}
				if mode == "activation-lost" {
					os.Remove(s.RouteBundleSource.ActivationStateFile)
				}
				if mode == "route-proof-mismatch" {
					p.Version = "old"
				}
				if mode == "assignment-changed" || mode == "newer-full" {
					changed = true
				}
				return p, nil
			}
			tlsProbe := func(context.Context, string, string, bool, time.Duration) (*platformTLSCertificate, error) {
				tlsCalls++
				if mode == "tls-failed" {
					return nil, errors.New("handshake failed")
				}
				if mode == "bundle-changed" {
					s.bundle.Version = "changed"
				}
				leaf := &x509.Certificate{Raw: []byte("certificate"), DNSNames: []string{host}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
				return &platformTLSCertificate{Leaf: leaf, ValidUntil: leaf.NotAfter}, nil
			}
			err = s.syncPlatformServingOnce(context.Background(), probe, tlsProbe)
			if mode == "inactive" {
				if err != nil || len(reports) != 0 || routeCalls != 0 || tlsCalls != 0 {
					t.Fatal("inactive worker reported serving", err, reports)
				}
				return
			}
			if mode == "recover-after-failure" || mode == "restart-after-failure" || mode == "report-rejected" {
				if err != nil || len(reports) != 2 {
					t.Fatal("initial positive observation failed", err, reports)
				}
				positive, err := os.ReadFile(cache + ".platform-serving.json")
				if err != nil {
					t.Fatal(err)
				}
				// A persisted cursor ahead of the wall clock must remain monotonic.
				future := time.Now().Add(time.Hour).UnixNano()
				raw, _ := json.Marshal(future)
				if err := os.WriteFile(cache+".platform-serving-cursor.json", raw, 0600); err != nil {
					t.Fatal(err)
				}
				s.recordCaddyApply("old", 1, "config", nil)
				rejectReport = mode == "report-rejected"
				if err := s.syncPlatformServingOnce(context.Background(), probe, tlsProbe); err == nil || len(reports) != 4 || s.platformServingEvidence != nil {
					t.Fatal("failure retained positive convergence", err, reports)
				}
				for _, h := range reports[2:] {
					assertTrafficServingFailure(t, h)
					if h.Sequence <= future {
						t.Fatal("failure cursor moved backwards")
					}
				}
				unchanged, err := os.ReadFile(cache + ".platform-serving.json")
				if err != nil || string(unchanged) != string(positive) {
					t.Fatal("failure replaced positive receipt", err)
				}
				current, _ := s.Bundle()
				if !reflect.DeepEqual(current, bundle) {
					t.Fatal("failure changed serving bundle")
				}
				if mode == "restart-after-failure" {
					s = NewService(s.Config, nil)
					s.PlatformTokenFile = token
					s.recordSyncSuccess(bundle, "", now, false)
				}
				rejectReport = false
				s.recordCaddyApply(bundle.Version, len(bundle.Routes), "config", nil)
				if err := s.syncPlatformServingOnce(context.Background(), probe, tlsProbe); err != nil || len(reports) != 6 || routeCalls != 2 || tlsCalls != 2 {
					t.Fatal("recovery failed to reprobe", err, reports, routeCalls, tlsCalls)
				}
				for _, h := range reports[4:] {
					if h.Sequence <= reports[2].Sequence || h.ApplyStatus != "applied" || h.ProbeStatus != "passed" {
						t.Fatal("recovery cursor or evidence invalid", h)
					}
				}
				return
			}
			if mode != "valid" {
				wantReports := 0
				switch mode {
				case "route-proof-mismatch", "tls-failed", "cache-missing", "cache-different", "caddy-not-applied":
					wantReports = 2
				}
				if err == nil || len(reports) != wantReports {
					t.Fatal("incorrect failed serving report", mode, err, reports)
				}
				for _, h := range reports {
					assertTrafficServingFailure(t, h)
				}
				if _, err := os.Stat(cache + ".platform-serving.json"); !os.IsNotExist(err) {
					t.Fatal("failure persisted positive receipt", err)
				}
				return
			}
			if err != nil || len(reports) != 2 || routeCalls != 1 || tlsCalls != 1 {
				t.Fatal("valid serving failed", err, reports, routeCalls, tlsCalls)
			}
			for _, h := range reports {
				if h.ApplyStatus != "applied" || h.ProbeStatus != "passed" || h.ActualGeneration != h.DesiredGeneration || h.LKGGeneration != bundle.Generation || h.ServingLKG {
					t.Fatal("incorrect serving or LKG claim", h)
				}
			}
			var receipt platformServingReceipt
			data, e := os.ReadFile(cache + ".platform-serving.json")
			if e != nil || json.Unmarshal(data, &receipt) != nil || receipt.Sequence != reports[0].Sequence || receipt.Route.ReleaseSet.ID != parent.ID {
				t.Fatal("missing durable evidence")
			}
			before := receipt.Sequence
			if err = s.syncPlatformServingOnce(context.Background(), probe, tlsProbe); err != nil {
				t.Fatal(err)
			}
			if routeCalls != 1 || tlsCalls != 1 || reports[2].Sequence <= before {
				t.Fatal("signed probe interval ignored")
			}
			s.platformServingEvidence = nil // restart loses the volatile proof cache
			if err = s.syncPlatformServingOnce(context.Background(), probe, tlsProbe); err != nil {
				t.Fatal(err)
			}
			if routeCalls != 2 || tlsCalls != 2 {
				t.Fatal("restart reused persisted evidence")
			}
		})
	}
}

func assertTrafficServingFailure(t *testing.T, h platformcontrol.PlatformConsumerHeartbeatEnvelope) {
	t.Helper()
	if h.ApplyStatus != "failed" || h.ProbeStatus != "failed" || h.ActualGeneration != "" || h.LKGGeneration != "" || h.ServingLKG || h.LastError == "" || h.ExpectedConsumerSetID == "" || h.Sequence <= 0 || h.Nonce == "" {
		t.Fatal("failure claimed unverified serving evidence", h)
	}
	if err := platformcontrol.ValidatePlatformConsumerHeartbeat(h, nil, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
		t.Fatal("negative fact violates trusted heartbeat protocol", err)
	}
	digest, err := platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
	if err != nil || digest != h.EvidenceHash {
		t.Fatal("failure evidence mismatch", err)
	}
}

func TestTrafficServingRouteProbeUsesTLSAndNonce(t *testing.T) {
	now := time.Now()
	cert, key := testCaddyTLSKeyPairAt(t, "app.example.test", now.Add(-time.Hour), now.Add(time.Hour))
	pair, err := tls.X509KeyPair([]byte(cert), []byte(key))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(cert))
	route := model.EdgeRouteBinding{Hostname: "app.example.test", PathPrefix: "/", EdgeGroupID: "edge-group-test", RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive, UpstreamURL: "http://never-contacted"}
	s := NewService(config.EdgeConfig{EdgeID: "edge", EdgeGroupID: "edge-group-test"}, nil)
	s.recordSyncSuccess(model.EdgeRouteBundle{Version: "serving", ValidUntil: now.Add(time.Minute), Routes: []model.EdgeRouteBinding{route}}, "", now, false)
	server := httptest.NewUnstartedServer(s.ProxyHandler())
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	server.StartTLS()
	defer server.Close()
	u, _ := url.Parse(server.URL)
	s.Config.CaddyListenAddr = u.Host
	proof, err := s.probePlatformServingRouteWithRoots(context.Background(), route, time.Second, roots)
	if err != nil || proof.Version != "serving" || proof.EdgeID != "edge" {
		t.Fatal("real TLS route proof failed", proof, err)
	}
	// Bind the same real HTTPS path to a traffic release. The parser must
	// preserve immutable provenance and refuse malformed/shadow bindings.
	d := "sha256:" + strings.Repeat("a", 64)
	binding := &model.TrafficReleaseBinding{Schema: trafficbinding.Schema, ReleaseSetID: "parent", ReleaseSetDigest: d, ReleaseSetGeneration: "parent-gen", RouteArtifactID: "route", RouteArtifactDigest: d, RouteArtifactGeneration: "route-gen", RouteArtifactSequence: 1, ReleaseID: "release", ReleaseChannel: "full", FencingToken: 1, ScopeKey: "global", IntentDigest: d, PolicyDigest: d, InputSnapshotDigest: d, CompilerVersion: "compiler", ProjectionDigest: d}
	s.recordSyncSuccess(model.EdgeRouteBundle{Version: "bound", ValidUntil: now.Add(time.Minute), TrafficRelease: binding, Routes: []model.EdgeRouteBinding{route}}, "", now, false)
	proof, err = s.probePlatformServingRouteWithRoots(context.Background(), route, time.Second, roots)
	if err != nil || !reflect.DeepEqual(proof.TrafficRelease, binding) {
		t.Fatal("HTTPS traffic proof lost release binding", proof, err)
	}
	binding = trafficbinding.Clone(binding)
	binding.ReleaseChannel = "shadow"
	s.recordSyncSuccess(model.EdgeRouteBundle{Version: "invalid", ValidUntil: now.Add(time.Minute), TrafficRelease: binding, Routes: []model.EdgeRouteBinding{route}}, "", now, false)
	if _, err = s.probePlatformServingRouteWithRoots(context.Background(), route, time.Second, roots); err == nil {
		t.Fatal("shadow traffic proof accepted")
	}
	if _, err = s.probePlatformServingRoute(context.Background(), route, time.Second); err == nil {
		t.Fatal("untrusted TLS accepted")
	}
	route.Hostname = "other.example.test"
	if _, err = s.probePlatformServingRouteWithRoots(context.Background(), route, time.Second, roots); err == nil {
		t.Fatal("wrong hostname accepted")
	}
}

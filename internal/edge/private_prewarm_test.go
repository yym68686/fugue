package edge

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/edgegroupfront"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestPrivatePrewarmTLSAndEndFences(t *testing.T) {
	for _, mode := range []string{"good", "startup refused", "identity denied", "untrusted TLS", "publication changed", "activation changed"} {
		t.Run(mode, func(t *testing.T) {
			group, node, source := "edge-group-fixture", "node-test", strings.Repeat("d", 40)
			image := "registry.example.test/worker@sha256:" + strings.Repeat("e", 64)
			path := writeInventoryActivationFixtureWithBundle(t, time.Now().UTC(), group, "b", strings.Repeat("a", 40), "current.p5.r2")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			pin := fmt.Sprintf("%s@sha256:%x", node, sha256.Sum256(raw))
			dir := t.TempDir()
			token, keyfile := filepath.Join(dir, "token"), filepath.Join(dir, "keys")
			if err := os.WriteFile(token, []byte(strings.Repeat("r", 40)), 0600); err != nil {
				t.Fatal(err)
			}
			keyID, key := "verify-fixture", []byte(strings.Repeat("k", 32))
			writeEdgeVerifierKeyring(t, keyfile, group, keyID, key)
			tlsServer := httptest.NewTLSServer(http.NotFoundHandler())
			defer tlsServer.Close()
			host := tlsServer.Certificate().DNSNames[0]
			b := signedEdgeControlTestBundle(group, "current", 5, 2, keyID, key)
			b.Routes[0].Hostname = host
			b = bundleauth.SignEdgeRouteBundleWithKeyring(b, bundleauth.NewKeyring(string(key), keyID, "", "", nil), 30*time.Minute)
			wire, err := json.Marshal(b)
			if err != nil {
				t.Fatal(err)
			}
			etag := strconv.Quote(fmt.Sprintf("sha256:%x", sha256.Sum256(wire)))
			authority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != edgeControlBundlePath {
					t.Error("not a read of current")
				}
				w.Header().Set("ETag", etag)
				w.Header().Set(edgeControlGroupHeader, group)
				w.Header().Set(edgeControlGenerationHeader, "current")
				w.Header().Set(edgeControlPublicationHeader, "5")
				w.Header().Set(edgeControlRecoveryEpochHeader, "2")
				if r.Header.Get("If-None-Match") == etag {
					if mode == "publication changed" {
						w.Header().Set(edgeControlPublicationHeader, "6")
					}
					if mode == "activation changed" {
						_, err := edgegroupfront.ApplyActivationCAS(path, edgegroupfront.ActivationCASRequest{GroupID: group, ExpectedGeneration: 1, ExpectedSlot: "b", TargetSlot: "a", BundleGeneration: b.Version, WorkerSourceCommit: source, WorkerImageDigest: strings.Split(image, "@")[1], Operation: edgegroupfront.ActivationOperationPromote, Reason: "local fixture activation"}, time.Now().UTC())
						if err != nil {
							t.Error(err)
						}
					}
					w.WriteHeader(304)
					return
				}
				_, _ = w.Write(wire)
			}))
			defer authority.Close()
			admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.URL.Path != "/load" {
					t.Error("unexpected local Caddy request")
				}
			}))
			defer admin.Close()
			s := NewServiceWithRouteBundleSource(config.EdgeConfig{EdgeID: node, EdgeGroupID: group, EdgeInstanceUID: "pod-test", EdgeSlot: "a", EdgeReleaseEpoch: source, CaddyEnabled: true, CaddyAdminURL: admin.URL, CaddyListenAddr: tlsServer.Listener.Addr().String(), CaddyProxyListenAddr: "127.0.0.1:38081", CaddyTLSMode: caddyTLSModeSharedOnly, CaddyDataDir: dir, CaddySharedTLSEnabled: true}, RouteBundleSourceConfig{URL: authority.URL + edgeControlBundlePath, CandidateURL: authority.URL + edgeControlCandidateBundlePath, TokenFile: token, VerifierKeyringFile: keyfile, ReporterImageRef: &image, ActivationStateFile: path, PrewarmPredecessor: &pin}, nil)
			s.caddyImportedTLSRoots = x509.NewCertPool()
			calls := 0
			tr := s.RouteBundleHTTPClient.Transport
			s.RouteBundleHTTPClient.Transport = inventoryRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if calls == 1 && mode == "startup refused" {
					return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
				}
				if mode == "identity denied" {
					return inventoryJSONResponse(http.StatusForbidden, map[string]string{"error": "not authorized"}), nil
				}
				return tr.RoundTrip(r)
			})
			if mode != "untrusted TLS" {
				s.caddyImportedTLSRoots.AddCert(tlsServer.Certificate())
			}
			out, err := s.CheckPrivatePrewarm(t.Context(), []string{host})
			if mode == "good" || mode == "startup refused" {
				if err != nil || out == nil || out.TLS[host] != fmt.Sprintf("%x", sha256.Sum256(tlsServer.Certificate().Raw)) {
					t.Fatal(out, err)
				}
				return
			}
			if mode == "identity denied" && calls != 1 {
				t.Fatal("retried a rejected identity", calls)
			}
			if err == nil || out != nil {
				t.Fatal("unsafe private result", out, err)
			}
		})
	}
}

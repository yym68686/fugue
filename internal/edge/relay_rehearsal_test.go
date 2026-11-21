package edge

// The hosted CI rehearsal runs this binary in three network namespaces. It
// injects a synthetic route, but never substitutes a transport, clock, health
// sample, permit or forwarding decision.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/edge/relayroute"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
)

const rehearsalOriginName = "origin.rehearsal.svc.cluster.local"
const rehearsalOriginListen = "10.231.2.2:8080"
const rehearsalOriginDial = rehearsalOriginName + ":8080"
const rehearsalHost = "service.rehearsal.example"
const rehearsalPath = "/v1/stream"

var rehearsalIngress = relayroute.Endpoint{Edge: "edge-a", Slot: "a", Address: "10.231.1.1:8443", TLSName: "slot-a.edge-a.example"}
var rehearsalEgress = relayroute.Endpoint{Edge: "edge-b", Slot: "a", Address: "10.231.1.2:8443", TLSName: "slot-a.edge-b.example"}

func TestOriginRelayPrivateNetworkRehearsal(t *testing.T) {
	role := os.Getenv("FUGUE_RELAY_REHEARSAL_ROLE")
	if role == "" {
		t.Skip("requires scripts/rehearse_edge_origin_relay.sh on isolated Linux")
	}
	dir := os.Getenv("FUGUE_RELAY_REHEARSAL_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("absolute isolated fixture directory required")
	}
	switch role {
	case "material":
		rehearsalMaterial(t, dir)
	case "origin":
		rehearsalOrigin(t, dir)
	case "egress":
		e := rehearsalWorker(t, dir, rehearsalEgress)
		ctx, stop := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() { defer close(done); e.run(ctx) }()
		defer func() { stop(); <-done }()
		rehearsalWait(t, 100*time.Second, "fixture shutdown", func() bool {
			_, err := os.Stat(filepath.Join(dir, "stop"))
			return err == nil
		})
	case "ingress":
		rehearsalIngressExercise(t, dir, rehearsalWorker(t, dir, rehearsalIngress))
	default:
		t.Fatal("unknown rehearsal role")
	}
}

func rehearsalMaterial(t *testing.T, dir string) {
	now := time.Now()
	root, _, issue := relayFixtureCertificates(t, now)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rehearsalWrite(t, dir, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: root.Raw}))
	rehearsalWrite(t, dir, "verify", []byte(hex.EncodeToString(pub)))
	rehearsalWrite(t, dir, "sign", priv)
	for i, endpoint := range []relayroute.Endpoint{rehearsalIngress, rehearsalEgress} {
		subdir := filepath.Join(dir, endpoint.Edge)
		if err := os.Mkdir(subdir, 0700); err != nil {
			t.Fatal(err)
		}
		cert := issue(endpoint.TLSName, int64(i)+2)
		der, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
		if err != nil {
			t.Fatal(err)
		}
		rehearsalWrite(t, subdir, "leaf.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Leaf.Raw}))
		rehearsalWrite(t, subdir, "key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	}
	rehearsalWrite(t, dir, "executions", nil)
	rehearsalPolicy(t, dir, "off", 1)
}

func rehearsalWrite(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	if err := lkgcache.AtomicWriteFile(filepath.Join(dir, name), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func rehearsalRoute() model.EdgeRouteBinding {
	r := model.EdgeRouteBinding{Hostname: rehearsalHost, PathPrefix: "/", RouteKind: model.EdgeRouteKindCustomDomain,
		TenantID: "tenant-test", AppID: "app-test", EdgeGroupID: "rehearsal", RoutePolicy: model.EdgeRoutePolicyEnabled,
		Status: model.EdgeRouteStatusActive, UpstreamURL: "http://" + rehearsalOriginDial, Streaming: true,
		UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService}
	r.Upstreams = []model.EdgeRouteUpstream{{Role: "stable", Weight: 100, ReleaseID: "release-test", UpstreamURL: r.UpstreamURL,
		UpstreamKind: r.UpstreamKind, UpstreamScope: r.UpstreamScope}}
	return r
}

func rehearsalPolicy(t *testing.T, dir, mode string, generation uint64) []byte {
	t.Helper()
	now, route := time.Now(), rehearsalRoute()
	digest := relayRouteDigest(route)
	p := relayroute.Policy{Schema: relayroute.Schema, Generation: generation, Mode: mode, MaxConcurrent: 1,
		NotBefore: now.Add(-time.Second), Expires: now.Add(3 * time.Minute),
		Endpoints: []relayroute.Endpoint{rehearsalIngress, rehearsalEgress}}
	if mode != "off" {
		p.Bindings = []relayroute.Binding{{ID: "single-hop", Host: rehearsalHost, Path: rehearsalPath,
			Source: rehearsalIngress.Edge, Peer: rehearsalEgress.Edge, Tenant: route.TenantID, App: route.AppID,
			Origin: route.UpstreamURL, Release: route.Upstreams[0].ReleaseID, SourceDigest: digest, PeerDigest: digest, MaxBodyBytes: 1024}}
	}
	if mode == "active" {
		p.RelayPercent = 25
	}
	key, err := os.ReadFile(filepath.Join(dir, "sign"))
	if err != nil || len(key) != ed25519.PrivateKeySize {
		t.Fatal("invalid fixture signing key")
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	k := ed25519.PrivateKey(key)
	signed, err := json.Marshal(relayroute.SignedPolicy{Payload: raw, Signature: ed25519.Sign(k, raw)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relayroute.Verify(signed, k.Public().(ed25519.PublicKey), now); err != nil {
		t.Fatal(err)
	}
	rehearsalWrite(t, dir, "policy", signed)
	return signed
}

func rehearsalWorker(t *testing.T, dir string, endpoint relayroute.Endpoint) *edgeOriginRelay {
	t.Helper()
	subdir := filepath.Join(dir, endpoint.Edge)
	for k, v := range map[string]string{
		"LISTEN_ADDR": endpoint.Address, "POLICY_FILE": filepath.Join(dir, "policy"),
		"STATE_FILE": filepath.Join(subdir, "fence"), "POLICY_VERIFY_KEY_FILE": filepath.Join(dir, "verify"),
		"TLS_CERT_FILE": filepath.Join(subdir, "leaf.pem"), "TLS_KEY_FILE": filepath.Join(subdir, "key.pem"),
		"CA_FILE": filepath.Join(dir, "ca.pem"),
	} {
		t.Setenv("FUGUE_EDGE_ORIGIN_RELAY_"+k, v)
	}
	route := rehearsalRoute()
	s := NewService(config.EdgeConfig{CaddyEnabled: true, EdgeID: endpoint.Edge, EdgeSlot: endpoint.Slot, EdgeGroupID: route.EdgeGroupID,
		RequestBodyBufferPath: t.TempDir(), RequestBodyBufferMaxBytes: 2048, RequestBodyBufferTotalMaxBytes: 4096}, log.New(io.Discard, "", 0))
	bundle := model.EdgeRouteBundle{Version: "rehearsal", ValidUntil: time.Now().Add(4 * time.Minute), Routes: []model.EdgeRouteBinding{route}}
	s.bundle, s.snapshot = &bundle, Status{Healthy: true, CaddyAppliedVersion: bundle.Version}
	s.routeIndex.Store(buildEdgeRouteIndex(bundle, route.EdgeGroupID, routePublicationMetadata{}))
	server, listener, err := s.initOriginRelay()
	if err != nil || server == nil {
		t.Fatalf("start real private listener: %v", err)
	}
	rehearsalServe(t, server, func() error { return server.ServeTLS(listener, "", "") })
	t.Cleanup(s.closeEdgeProxyTransports)
	return s.originRelay
}

func rehearsalServe(t *testing.T, server *http.Server, serve func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- serve() }()
	t.Cleanup(func() {
		_ = server.Close()
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("fixture listener: %v", err)
		}
	})
}

func rehearsalOrigin(t *testing.T, dir string) {
	listener, err := net.Listen("tcp", rehearsalOriginListen)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{ReadHeaderTimeout: 2 * time.Second}
	srv.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(r.Body, 1024))
		if err != nil || string(raw) != "rehearsal" || r.Method != http.MethodPost || r.URL.Path != rehearsalPath ||
			!strings.HasPrefix(r.RemoteAddr, "10.231.2.1:") || r.Header.Get(relayPermitHeader) != "" {
			t.Error("origin received a request not sent once by the exact egress interface")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f, err := os.OpenFile(filepath.Join(dir, "executions"), os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, writeErr := f.WriteString("1\n")
		closeErr := f.Close()
		if err := errors.Join(writeErr, closeErr); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.RawQuery == "drop=1" {
			panic(http.ErrAbortHandler) // the origin executed but no result was reported
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: single-hop\n\n")
		w.(http.Flusher).Flush()
	})
	rehearsalServe(t, srv, func() error { return srv.Serve(listener) })
	rehearsalWait(t, 100*time.Second, "fixture shutdown", func() bool {
		_, err := os.Stat(filepath.Join(dir, "stop"))
		return err == nil
	})
}

func rehearsalWait(t *testing.T, timeout time.Duration, what string, ready func() bool) {
	t.Helper()
	until := time.Now().Add(timeout)
	for {
		if ready() {
			return
		}
		if !time.Now().Before(until) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(350 * time.Millisecond)
	}
}

func rehearsalIngressExercise(t *testing.T, dir string, e *edgeOriginRelay) {
	e.refresh(time.Now())
	waitEgressFence := func(generation uint64) {
		t.Helper()
		rehearsalWait(t, 14*time.Second, fmt.Sprintf("egress fence %d", generation), func() bool {
			raw, _ := os.ReadFile(filepath.Join(dir, rehearsalEgress.Edge, "fence"))
			var fence relayPolicyFenceRecord
			return json.Unmarshal(raw, &fence) == nil && fence.Generation == generation
		})
	}
	waitEgressFence(1)
	// DNS resolves the same named origin on both sides. There is no route
	// from ingress to that network and no default route in any namespace.
	if ips, err := net.LookupIP(rehearsalOriginName + "."); err != nil || len(ips) != 1 || ips[0].String() != "10.231.2.2" {
		t.Fatalf("unexpected isolated DNS result: %v, %v", ips, err)
	}
	if c, err := net.DialTimeout("tcp", rehearsalOriginDial, time.Second); err == nil {
		_ = c.Close()
		t.Fatal("ingress has a forbidden direct route")
	}
	count := func(want int) {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, "executions"))
		if err != nil || bytes.Count(raw, []byte("\n")) != want {
			t.Fatalf("origin executed %d times, want %d (read error %v)", bytes.Count(raw, []byte("\n")), want, err)
		}
	}
	selectPath := func(query string) (*httptest.ResponseRecorder, edgeProxyObservation, bool) {
		t.Helper()
		r := httptest.NewRequest("POST", "https://"+rehearsalHost+rehearsalPath+query, strings.NewReader("rehearsal"))
		r.Header.Set("Idempotency-Key", "fixture-only")
		route := rehearsalRoute()
		o := edgeProxyObservation{Host: rehearsalHost, Route: route, NetworkRoute: &route, ReleaseID: route.Upstreams[0].ReleaseID,
			BundleVersion: "rehearsal", RequestBodyBuffered: true, RequestBodyEOF: true, EdgeRequestID: relayRandomID(), TraceID: relayRandomID()}
		w := httptest.NewRecorder()
		forwarded := e.tryForward(newEdgeProxyObservationResponseWriter(w, time.Now(), &o), r, rehearsalHost, &o)
		return w, o, forwarded
	}
	ctx, cancel := context.WithTimeout(t.Context(), 65*time.Second)
	defer cancel()
	mode := func(name string, generation uint64) []byte {
		t.Helper()
		raw := rehearsalPolicy(t, dir, name, generation)
		e.refresh(time.Now())
		waitEgressFence(generation)
		if name == "off" {
			return raw
		}
		p := e.current(time.Now())
		if p == nil || p.Mode != name {
			t.Fatal("ingress did not accept the signed lease")
		}
		b := p.Bindings[0]
		rehearsalWait(t, 16*time.Second, "three measured path legs", func() bool {
			e.probeOrigins(ctx, p)
			e.pollPeer(ctx, p, b, rehearsalEgress)
			now := time.Now()
			local := e.originHealth(relayProbeKey{"rehearsal", b.SourceDigest}, now)
			e.mu.Lock()
			peer := e.remote[b.ID]
			e.mu.Unlock()
			return local.State == relayroute.Degraded && peer.link.Health(now).State == relayroute.Healthy && peer.origin.State == relayroute.Healthy
		})
		return raw
	}
	mode("shadow", 2)
	if _, o, sent := selectPath(""); sent || o.RelayPeer != rehearsalEgress.Edge || o.RelayDispatched {
		t.Fatalf("shadow did not select without dispatch: %s", o.RelayReason)
	}
	count(0)
	active := mode("active", 3)
	forwards := func(query string, code int) {
		t.Helper()
		// Exercise the actual 25% sampling gate, never set the runtime to 100%.
		for range 80 {
			w, o, sent := selectPath(query)
			if !sent {
				if o.RelayReason != "outside_canary" {
					t.Fatalf("unexpected dispatch decision: %s", o.RelayReason)
				}
				continue
			}
			if !o.RelayDispatched || w.Code != code || (code == http.StatusOK && (!w.Flushed || w.Body.String() != "data: single-hop\n\n")) {
				t.Fatalf("unexpected end-to-end response: code %d, dispatched %v, flush %v", w.Code, o.RelayDispatched, w.Flushed)
			}
			return
		}
		t.Fatal("25% sample did not select any of 80 independent synthetic trials")
	}
	forwards("", http.StatusOK)
	count(1)
	forwards("?drop=1", http.StatusBadGateway)
	count(2)

	p := e.current(time.Now())
	mode("off", 4)
	rehearsalWrite(t, dir, "policy", active) // fault injection in the lab only
	e.refresh(time.Now())
	if _, _, sent := selectPath(""); sent || e.current(time.Now()) != nil {
		t.Fatal("ingress restored a revoked lease")
	}
	if _, err := e.control(ctx, rehearsalEgress, "state", relayRequest{
		Binding: p.Bindings[0].ID, PolicyHash: hex.EncodeToString(p.Hash[:]),
	}); err == nil {
		t.Fatal("egress accepted a revoked lease")
	}
	count(2)
	t.Log("isolated live probes: ingress→origin degraded, ingress→egress and egress→origin healthy; shadow 0 executions, active 1, lost result 1, withdrawn 0")
}

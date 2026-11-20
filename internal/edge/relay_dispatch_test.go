package edge

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/edge/relayroute"
	"fugue/internal/model"
)

func relayFixtureCertificates(t *testing.T, now time.Time) (*x509.Certificate, *x509.CertPool, func(string, int64) tls.Certificate) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test relay CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(root)
	return root, pool, func(name string, serial int64) tls.Certificate {
		t.Helper()
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{name}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		leafDER, err := x509.CreateCertificate(rand.Reader, tmpl, root, pub, priv)
		if err != nil {
			t.Fatal(err)
		}
		leaf, err := x509.ParseCertificate(leafDER)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: key, Leaf: leaf}
	}
}

func TestOriginRelayFirstDispatch(t *testing.T) {
	now := time.Now()
	_, roots, cert := relayFixtureCertificates(t, now)
	aID, bID := "edge-a", "edge-b"
	host, path := "tenant.example.test", "/v1/stream"
	originURL := "http://service.ns.svc.cluster.local:8000"
	group := "west"
	route := model.EdgeRouteBinding{
		Hostname: host, PathPrefix: "/v1", RouteKind: model.EdgeRouteKindCustomDomain, AppID: "app-test", TenantID: "tenant-test", EdgeGroupID: group,
		RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive, UpstreamURL: originURL,
		UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService,
		Upstreams: []model.EdgeRouteUpstream{{ReleaseID: "release-test", Role: "stable", Weight: 100, UpstreamURL: originURL,
			UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService}},
	}
	bundle := model.EdgeRouteBundle{Version: "live", ValidUntil: now.Add(time.Hour), Routes: []model.EdgeRouteBinding{route}}
	makeEdge := func(id, tlsName string, serial int64) *edgeOriginRelay {
		s := NewService(config.EdgeConfig{CaddyEnabled: true, EdgeID: id, EdgeSlot: "slot-a", EdgeGroupID: group,
			RequestBodyBufferPath: t.TempDir(), RequestBodyBufferMaxBytes: 2048, RequestBodyBufferTotalMaxBytes: 4096}, log.New(io.Discard, "", 0))
		s.bundle, s.snapshot = &bundle, Status{Healthy: true, CaddyAppliedVersion: "live"}
		s.routeIndex.Store(buildEdgeRouteIndex(bundle, group, routePublicationMetadata{}))
		return &edgeOriginRelay{s: s, certificate: cert(tlsName, serial), roots: roots, process: relayRandomID(),
			probes: map[relayProbeKey]relayroute.Window{}, remote: map[string]relayRemote{}, clients: map[relayroute.Endpoint]*http.Client{},
			permits: map[string]relayPermit{}, dispatches: map[relayDispatchKey]time.Time{}}
	}
	aTLS, bTLS := "worker-a.example.test", "worker-b.example.test"
	a, b := makeEdge(aID, aTLS, 2), makeEdge(bID, bTLS, 3)
	var count atomic.Int64
	var drop atomic.Bool
	var hold atomic.Bool
	canceled := make(chan struct{}, 1)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := count.Add(1)
		raw, err := io.ReadAll(r.Body)
		if err != nil || string(raw) != "payload" || r.URL.Path != path || r.URL.RawQuery != "z=1&a=x%20y" ||
			r.Header.Get("Authorization") != "Bearer test" || r.Header.Get("X-Forwarded-For") != "203.0.113.11" ||
			r.Header.Get(relayPermitHeader) != "" || r.Header.Get("X-Fugue-Release-Id") != "release-test" {
			t.Errorf("dispatch %d changed request: %v path=%q query=%q", n, err, r.URL.Path, r.URL.RawQuery)
		}
		if drop.Load() {
			panic(http.ErrAbortHandler)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: observed\n\n")
		w.(http.Flusher).Flush()
		if hold.Load() {
			select {
			case <-r.Context().Done():
				canceled <- struct{}{}
			case <-time.After(5 * time.Second):
				t.Error("origin never received cancellation")
			}
			return
		}
		io.WriteString(w, "data: done\n\n")
	}))
	defer source.Close()
	transport := newDefaultEdgeProxyTransport().(*http.Transport)
	transport.DialContext = func(ctx context.Context, _, address string) (net.Conn, error) {
		if address != "service.ns.svc.cluster.local:8000" {
			t.Errorf("unexpected origin address %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", source.Listener.Addr().String())
	}
	b.s.proxyBase = transport
	defer b.s.closeEdgeProxyTransports()
	peer := httptest.NewUnstartedServer(b)
	peer.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{b.certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	peer.StartTLS()
	defer peer.Close()
	digest := relayRouteDigest(route)
	ap := relayroute.Endpoint{Edge: aID, TLSName: aTLS, Slot: "slot-a", Address: "10.0.0.1:8001"}
	bp := relayroute.Endpoint{Edge: bID, TLSName: bTLS, Slot: "slot-a", Address: peer.Listener.Addr().String()}
	binding := relayroute.Binding{ID: "test-binding", Source: aID, Peer: bID, Host: host, Path: path, Tenant: route.TenantID, App: route.AppID,
		Release: "release-test", Origin: originURL, SourceDigest: digest, PeerDigest: digest, MaxBodyBytes: 2048}
	p := &relayroute.Policy{Mode: "active", RelayPercent: 100, MaxConcurrent: 2, NotBefore: now.Add(-time.Minute), Expires: now.Add(time.Minute),
		Endpoints: []relayroute.Endpoint{ap, bp}, Bindings: []relayroute.Binding{binding}, Hash: [32]byte{1}}
	a.policy.Store(p)
	b.policy.Store(p)
	for _, r := range []*edgeOriginRelay{a, b} {
		failed := r == a
		for i := 1; i <= 2; i++ {
			rtt := 50.0
			sample := model.EdgeNetworkSample{BundleVersion: "live", RouteDigest: digest, ObservedAt: now.Add(-time.Second),
				ServiceConnectFailed: &failed}
			if !failed {
				sample.ServiceRTTMS = &rtt
			}
			r.noteProbe(sample, uint64(i), now)
		}
	}
	a.pollPeer(t.Context(), p, binding, bp)
	a.pollPeer(t.Context(), p, binding, bp)
	defer a.client(bp).CloseIdleConnections()
	call := func() (bool, *httptest.ResponseRecorder, edgeProxyObservation) {
		r := httptest.NewRequest("POST", "https://"+host+path+"?z=1&a=x%20y", strings.NewReader("payload"))
		r.RemoteAddr = "203.0.113.11:3000"
		r.Header.Set("Authorization", "Bearer test")
		r.Header.Set("Idempotency-Key", "kept-but-not-replayed")
		r.Header.Set(relayPermitHeader, "untrusted")
		o := edgeProxyObservation{Host: host, Route: route, NetworkRoute: &route, ReleaseID: "release-test", BundleVersion: "live",
			RequestBodyBuffered: true, RequestBodyEOF: true, EdgeRequestID: relayRandomID(), TraceID: relayRandomID()}
		w := httptest.NewRecorder()
		chosen := a.tryForward(newEdgeProxyObservationResponseWriter(w, time.Now(), &o), r, host, &o)
		return chosen, w, o
	}
	shadow := *p
	shadow.Mode = "shadow"
	a.policy.Store(&shadow)
	if chose, _, o := call(); chose || o.RelayPeer != bID || count.Load() != 0 {
		t.Fatalf("shadow changed traffic: %+v", o)
	}
	a.policy.Store(p)

	// A stale B directory with the same human binding ID must not accept a
	// preflight (including if the route currently resolves to the same app).
	stale := *p
	stale.Hash = [32]byte{2}
	b.policy.Store(&stale)
	if chose, _, o := call(); chose || count.Load() != 0 || o.RelayReason != "prepare_unavailable" {
		t.Fatalf("stale directory authorized: %+v", o)
	}
	b.policy.Store(p)

	wrong := makeEdge("edge-wrong", "wrong.example.test", 4)
	defer wrong.client(bp).CloseIdleConnections()
	if _, err := wrong.control(t.Context(), bp, "state", relayRequest{
		Binding: binding.ID, PolicyHash: hex.EncodeToString(p.Hash[:]),
	}); err == nil {
		t.Fatal("trusted CA certificate without exact authorized SAN was accepted")
	}

	if chose, w, o := call(); !chose || !o.RelayDispatched || w.Code != http.StatusOK || count.Load() != 1 ||
		w.Body.String() != "data: observed\n\ndata: done\n\n" || !w.Flushed {
		t.Fatalf("relay failed: chosen=%v status=%d origin calls=%d reason=%s", chose, w.Code, count.Load(), o.RelayReason)
	}

	drop.Store(true)
	if chose, w, o := call(); !chose || !o.RelayDispatched || w.Code != http.StatusBadGateway || count.Load() != 2 {
		t.Fatalf("unknown result fell through: chose=%v status=%d origin calls=%d reason=%s", chose, w.Code, count.Load(), o.RelayReason)
	}
	drop.Store(false)
	cmd := relayRequest{Binding: binding.ID, PolicyHash: hex.EncodeToString(p.Hash[:]), Dispatch: relayRandomID(),
		QueryHash: relayQueryHash("z=1&a=x%20y"), Bytes: 7}
	prepared, err := a.control(t.Context(), bp, "prepare", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.control(t.Context(), bp, "prepare", cmd); err == nil {
		t.Fatal("issued a duplicate dispatch permit")
	}
	request := func(permit string) int {
		r, _ := http.NewRequestWithContext(t.Context(), "POST", "https://"+bp.Address+relayPrefix+"forward?z=1&a=x%20y", strings.NewReader("payload"))
		r.Header.Set("Authorization", "Bearer test")
		r.Header.Set("X-Forwarded-For", "203.0.113.11")
		r.Header.Set(relayDispatchHeader, cmd.Dispatch)
		r.Header.Set(relayPermitHeader, permit)
		r.GetBody = nil
		resp, err := a.client(bp).Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	if code := request(prepared.Permit); code != http.StatusOK || count.Load() != 3 {
		t.Fatalf("valid permit: %d, count %d", code, count.Load())
	}
	if code := request(prepared.Permit); code != http.StatusForbidden || count.Load() != 3 {
		t.Fatalf("replayed permit: %d, count %d", code, count.Load())
	}
	cmd.Dispatch = relayRandomID()
	prepared, err = a.control(t.Context(), bp, "prepare", cmd)
	if err != nil {
		t.Fatal(err)
	}
	off := *p
	off.Mode = "off"
	b.policy.Store(&off)
	if code := request(prepared.Permit); code != http.StatusForbidden || count.Load() != 3 {
		t.Fatalf("revoked permit started origin: %d, count %d", code, count.Load())
	}
	b.policy.Store(p)

	// A genuine public request must reach the relay only after the existing
	// upload buffer reaches EOF; verify flush and cancellation on real sockets.
	a.s.originRelay = a
	hold.Store(true)
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.RemoteAddr = "203.0.113.11:3000"
		a.s.handleProxy(w, r)
	}))
	defer public.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, http.MethodPost, public.URL+path+"?z=1&a=x%20y", strings.NewReader("payload"))
	r.ContentLength = -1
	r.TransferEncoding = []string{"chunked"}
	r.Host = host
	r.Header.Set("Accept", "text/event-stream")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer test")
	r.Header.Set(relayPermitHeader, "attacker-controlled")
	resp, err := public.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || resp.StatusCode != http.StatusOK || line != "data: observed\n" || count.Load() != 4 {
		t.Fatalf("public streaming failed: status %d, line %q, executions %d, error %v", resp.StatusCode, line, count.Load(), err)
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("client cancellation did not reach origin")
	}
	public.Close()
	if used := a.s.edgeRequestBodyBufferManager().usedBytes(); used != 0 {
		t.Fatalf("source failed to release its request buffer: %d", used)
	}
}

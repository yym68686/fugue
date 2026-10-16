package agentedge

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func edgeTLSServer(t *testing.T, hostname string, handler http.Handler) (*httptest.Server, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: hostname}, DNSNames: []string{hostname},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	s := httptest.NewUnstartedServer(handler)
	s.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	s.StartTLS()
	t.Cleanup(s.Close)
	return s, roots
}

func selectedClient(t *testing.T, server *httptest.Server, roots *x509.CertPool, lifetime time.Duration) (*http.Client, *atomic.Int32) {
	t.Helper()
	g, private, keys, fixed := grantFixture(t)
	now := time.Now().UTC()
	delta := now.Sub(fixed)
	g.IssuedAt = g.IssuedAt.Add(delta)
	g.ValidUntil = now.Add(lifetime)
	g.PolicyReference.PublishedAt = g.PolicyReference.PublishedAt.Add(delta)
	for i := range g.Candidates {
		g.Candidates[i].Publication.PublishedAt = g.Candidates[i].Publication.PublishedAt.Add(delta)
		g.Candidates[i].EvidenceObservedAt = g.Candidates[i].EvidenceObservedAt.Add(delta)
		g.Candidates[i].EvidenceValidUntil = g.Candidates[i].EvidenceValidUntil.Add(delta)
	}
	anchor := keys["key-one"]
	anchor.NotBefore = anchor.NotBefore.Add(delta)
	anchor.NotAfter = anchor.NotAfter.Add(delta)
	keys["key-one"] = anchor
	raw := encodeGrant(t, g, private)
	s := &Selector{}
	if err := s.Install(raw, keys, g.Audience, g.Origin, now); err != nil {
		t.Fatal(err)
	}
	v, err := Verify(raw, keys, g.Audience, g.Origin, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Observe(measurementRound(g, v.Digest(), now, time.Millisecond, 2*time.Millisecond), keys, now); err != nil {
		t.Fatal(err)
	}
	client, err := NewHTTPClient(s, func() map[string]TrustKey { return keys }, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	target := strings.TrimPrefix(server.URL, "https://")
	dials := &atomic.Int32{}
	rt := client.Transport.(*transport)
	rt.roots = roots
	rt.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		if address != "8.8.8.8:443" {
			t.Errorf("request escaped selected endpoint: %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, target)
	}
	return client, dials
}

func TestSelectedTransportPreservesHostnameTLSAndCredentials(t *testing.T) {
	var calls atomic.Int32
	s, roots := edgeTLSServer(t, "api.example.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Host != "api.example.test" || r.TLS.ServerName != "api.example.test" || r.URL.RequestURI() != "/v1/agent/heartbeat?probe=false" || r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("selected transport changed logical API identity or authentication")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"status":"ready"}` {
			t.Error("request body changed")
		}
		w.Write([]byte("ok"))
	}))
	client, dials := selectedClient(t, s, roots, 30*time.Second)
	req, err := http.NewRequest(http.MethodPost, "https://api.example.test/v1/agent/heartbeat?probe=false", bytes.NewBufferString(`{"status":"ready"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer fixture-token")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if calls.Load() != 1 || dials.Load() != 1 || req.URL.Host != "api.example.test" || req.GetBody == nil {
		t.Fatal("request was replayed or caller request was mutated")
	}
}

func TestSelectedTransportNeverReplaysMutationOrFollowsRedirect(t *testing.T) {
	for _, scenario := range []string{"response lost", "redirect"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			s, roots := edgeTLSServer(t, "api.example.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				io.Copy(io.Discard, r.Body)
				if scenario == "redirect" {
					w.Header().Set("Location", "https://api.example.test/redirected")
					w.WriteHeader(http.StatusTemporaryRedirect)
					return
				}
				connection, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				connection.Close()
			}))
			client, dials := selectedClient(t, s, roots, 30*time.Second)
			req, _ := http.NewRequest(http.MethodPost, "https://api.example.test/v1/agent/operations/operation-test/complete", bytes.NewBufferString(`{"ok":true}`))
			req.Header.Set("Idempotency-Key", "caller-provided")
			response, err := client.Do(req)
			if response != nil {
				response.Body.Close()
			}
			if scenario == "response lost" && err == nil {
				t.Fatal("lost mutation response was hidden")
			}
			if scenario == "redirect" && (err != nil || response.StatusCode != 307) {
				t.Fatal("redirect was not returned without replay", err)
			}
			if calls.Load() != 1 || dials.Load() != 1 {
				t.Fatal("mutating request was replayed", calls.Load(), dials.Load())
			}
		})
	}
}

func TestSelectedTransportRejectsOtherOriginAndTLSIdentity(t *testing.T) {
	var calls atomic.Int32
	s, roots := edgeTLSServer(t, "wrong.example.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	client, dials := selectedClient(t, s, roots, 30*time.Second)
	for _, target := range []string{"http://api.example.test/v1/agent/operations", "https://foreign.example.test/v1/agent/operations", "https://api.example.test:8443/v1/agent/operations"} {
		response, err := client.Get(target)
		if response != nil {
			response.Body.Close()
		}
		if err == nil {
			t.Fatal("foreign origin accepted")
		}
	}
	if dials.Load() != 0 {
		t.Fatal("unauthorized origin was dialed")
	}
	response, err := client.Get("https://api.example.test/v1/agent/operations")
	if response != nil {
		response.Body.Close()
	}
	if err == nil || calls.Load() != 0 || dials.Load() != 1 {
		t.Fatal("TLS hostname verification was bypassed", err, calls.Load(), dials.Load())
	}
}

func TestSelectedTransportResponseCannotOutliveGrant(t *testing.T) {
	s, roots := edgeTLSServer(t, "api.example.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	client, _ := selectedClient(t, s, roots, time.Second)
	started := time.Now()
	response, err := client.Get("https://api.example.test/v1/agent/operations")
	if err != nil {
		t.Fatal("grant expired before response headers", err)
	}
	_, err = io.ReadAll(response.Body)
	response.Body.Close()
	if err == nil || time.Since(started) > 1700*time.Millisecond {
		t.Fatal("response ignored original grant expiry", err, time.Since(started))
	}
}

type lateControlResponse struct{ advance func() }

func (b lateControlResponse) Read(p []byte) (int, error) {
	b.advance()
	return copy(p, "late response"), io.EOF
}

func (lateControlResponse) Close() error { return nil }

func TestLeaseBodyRejectsSuccessfulEOFAfterAbsoluteExpiry(t *testing.T) {
	now := time.Now()
	until := now.Add(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &leaseBody{ReadCloser: lateControlResponse{advance: func() { now = until }}, ctx: ctx, cancel: cancel, validUntil: until, now: func() time.Time { return now }}
	data, err := io.ReadAll(body)
	if !errors.Is(err, context.DeadlineExceeded) || len(data) != 0 || ctx.Err() != nil {
		t.Fatal("late successful EOF escaped the absolute lease before timer cancellation", string(data), err)
	}
	if err := body.Close(); err != nil || ctx.Err() != context.Canceled {
		t.Fatal("response cleanup lost context cancellation", err)
	}
}

func TestSelectedTransportJoinsInFlightGrantBeforeSending(t *testing.T) {
	scenarios := []string{"replacement", "authority-unavailable", "tampered-grant", "unmeasured-grant", "revoked-key", "expired-grant", "canceled-request"}
	for _, scenario := range append([]string{}, scenarios...) {
		scenarios = append(scenarios, "negative-proof/"+scenario)
	}
	for _, scenario := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			negativeProof := strings.HasPrefix(scenario, "negative-proof/")
			scenario = strings.TrimPrefix(scenario, "negative-proof/")
			m, g, private, _, now := controlFixture(t)
			if err := m.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			g = activateControl(t, m, g, private, now)
			*now = now.Add(time.Duration(g.Policy.ProbeIntervalSeconds) * time.Second)
			v, err := m.selector.permission(m.currentKeys(), *now)
			if err != nil {
				t.Fatal(err)
			}
			round := measurementRound(g, v.Digest(), *now, time.Millisecond, time.Millisecond)
			for i := range round.Measurements {
				round.Measurements[i].Success = false
				round.Measurements[i].Disqualified = true
			}
			if !negativeProof {
				if _, err = m.selector.Observe(round, m.currentKeys(), *now); err == nil {
					t.Fatal("negative evidence did not remove eligibility")
				}
			}
			*now = now.Add(time.Second)
			next := cloneGrant(g)
			next.IssuedAt = *now
			for i := range next.Candidates {
				next.Candidates[i].Publication.ReleaseID = "next-release"
				next.Candidates[i].Publication.FencingToken++
				next.Candidates[i].Publication.PublishedAt = *now
			}
			raw := encodeGrant(t, next, private)
			if scenario == "tampered-grant" {
				raw = []byte(`{"untrusted":true}`)
			}
			if scenario == "unmeasured-grant" {
				m.probe = func(_ context.Context, _ Grant, c Candidate) Measurement {
					return Measurement{EdgeID: c.EdgeID, Address: c.Address}
				}
			}
			if negativeProof {
				probe := m.probe
				m.probe = func(ctx context.Context, grant Grant, c Candidate) Measurement {
					if !grant.IssuedAt.Equal(next.IssuedAt) {
						return Measurement{EdgeID: c.EdgeID, Address: c.Address, Disqualified: true}
					}
					return probe(ctx, grant, c)
				}
			}
			var clock atomic.Value
			clock.Store(*now)
			m.now = func() time.Time { return clock.Load().(time.Time) }
			entered, release := make(chan struct{}), make(chan struct{})
			var fetches, calls, dials atomic.Int32
			m.fetch = func(context.Context, string) ([]byte, error) {
				fetches.Add(1)
				if negativeProof {
					clock.Store(clock.Load().(time.Time).Add(time.Millisecond))
				}
				close(entered)
				<-release
				if scenario == "authority-unavailable" {
					return nil, errors.New("authority unavailable")
				}
				return raw, nil
			}
			m.refreshAt = time.Time{}
			if negativeProof {
				m.refreshAt = now.Add(time.Minute)
			}
			finished := make(chan error, 1)
			go func() { finished <- m.Step(context.Background()) }()
			<-entered
			if m.Status().Primary != "" || m.pendingRefresh() == nil {
				close(release)
				<-finished
				t.Fatal("replacement must revoke old eligibility and expose one pending refresh")
			}
			server, roots := edgeTLSServer(t, "api.example.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, _ := io.ReadAll(r.Body)
				if r.Method != http.MethodPost || string(body) != `{"status":"ready"}` || r.Header.Get("Authorization") != "Bearer synthetic-runtime-token" {
					t.Error("queued request changed")
				}
				w.WriteHeader(http.StatusOK)
			}))
			rt := m.selected.Transport.(*transport)
			rt.roots = roots
			rt.now = m.now
			rt.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
				dials.Add(1)
				return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(server.URL, "https://"))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			body := &observedRequestBody{reader: strings.NewReader(`{"status":"ready"}`)}
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, g.Origin+"/v1/agent/heartbeat", body)
			req.Header.Set("Authorization", "Bearer synthetic-runtime-token")
			responded := make(chan error, 1)
			go func() {
				response, e := m.selected.Do(req)
				if response != nil {
					response.Body.Close()
				}
				responded <- e
			}()
			select {
			case e := <-responded:
				close(release)
				<-finished
				t.Fatal("request did not join pending refresh", e)
			case <-time.After(30 * time.Millisecond):
			}
			if dials.Load() != 0 || calls.Load() != 0 || body.reads.Load() != 0 {
				close(release)
				<-finished
				t.Fatal("request was sent before fresh permission")
			}
			if scenario == "canceled-request" {
				cancel()
				select {
				case e := <-responded:
					if !errors.Is(e, context.Canceled) {
						t.Error("cancellation lost", e)
					}
				case <-time.After(time.Second):
					t.Error("waiting ignored cancellation")
				}
				close(release)
				<-finished
			} else {
				if scenario == "revoked-key" {
					m.mu.Lock()
					m.keys = map[string]TrustKey{}
					m.mu.Unlock()
				}
				if scenario == "expired-grant" {
					clock.Store(next.ValidUntil.Add(time.Second))
				}
				close(release)
				stepErr := <-finished
				e := <-responded
				if scenario == "replacement" {
					if stepErr != nil || e != nil || calls.Load() != 1 || dials.Load() != 1 {
						t.Fatal("fresh signed and measured replacement did not send exactly once", stepErr, e, calls.Load(), dials.Load())
					}
				} else if e == nil || stepErr == nil || calls.Load() != 0 || dials.Load() != 0 {
					t.Fatal("failed replacement authorized request", stepErr, e, calls.Load(), dials.Load())
				}
			}
			if fetches.Load() != 1 || !body.closed.Load() {
				t.Fatal("request created extra acquisitions or leaked body", fetches.Load(), body.closed.Load())
			}
		})
	}
}

type observedRequestBody struct {
	reader io.Reader
	reads  atomic.Int32
	closed atomic.Bool
}

func (b *observedRequestBody) Read(p []byte) (int, error) { b.reads.Add(1); return b.reader.Read(p) }
func (b *observedRequestBody) Close() error               { b.closed.Store(true); return nil }

func TestSelectedTransportDoesNotWaitWithoutRefreshOrDuringAcquisition(t *testing.T) {
	for _, acquisition := range []bool{false, true} {
		selector := &Selector{}
		client, err := NewHTTPClient(selector, func() map[string]TrustKey { return nil }, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		rt := client.Transport.(*transport)
		if acquisition {
			rt.refreshDone = func() <-chan struct{} { return make(chan struct{}) }
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if acquisition {
			ctx = context.WithValue(ctx, grantAcquisitionContextKey{}, true)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example.test/v1/agent/edge-candidates", nil)
		_, err = client.Do(req)
		if err == nil || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			t.Fatal("unavailable authority waited without a usable refresh or deadlocked its own acquisition", acquisition, err)
		}
	}
}

func TestSelectedTransportKeepsPositiveFastPathDuringRefresh(t *testing.T) {
	var calls atomic.Int32
	server, roots := edgeTLSServer(t, "api.example.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusOK) }))
	client, dials := selectedClient(t, server, roots, 30*time.Second)
	client.Transport.(*transport).refreshDone = func() <-chan struct{} {
		t.Error("healthy selected path consulted pending refresh")
		return make(chan struct{})
	}
	response, err := client.Get("https://api.example.test/v1/agent/operations")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if calls.Load() != 1 || dials.Load() != 1 {
		t.Fatal("healthy request did not send once", calls.Load(), dials.Load())
	}
}

func TestSelectedTransportRefreshWaitRespectsExistingClientTimeout(t *testing.T) {
	var dials atomic.Int32
	client, err := NewHTTPClient(&Selector{}, func() map[string]TrustKey { return nil }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.Timeout = 50 * time.Millisecond
	rt := client.Transport.(*transport)
	rt.refreshDone = func() <-chan struct{} { return make(chan struct{}) }
	rt.dial = func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("unexpected dial")
	}
	body := &observedRequestBody{reader: strings.NewReader("request")}
	req, _ := http.NewRequest(http.MethodPost, "https://api.example.test/v1/agent/heartbeat", body)
	start := time.Now()
	_, err = client.Do(req)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second || dials.Load() != 0 || body.reads.Load() != 0 || !body.closed.Load() {
		t.Fatal("pending refresh escaped timeout or sent request", err, dials.Load(), body.reads.Load(), body.closed.Load())
	}
}

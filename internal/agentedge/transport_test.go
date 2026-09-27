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
	g.Publication.PublishedAt = g.Publication.PublishedAt.Add(delta)
	for i := range g.Candidates {
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

package edgecontrol

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/inputarchive"
)

func pendingClientRequestFixture() inputarchive.PendingRootRequest {
	return inputarchive.PendingRootRequest{Schema: inputarchive.PendingRootRequestSchema, SourceSchema: "fugue.edge-control.pending-root-source.v1", Source: []byte(`{"original":"neutral"}`), Registration: inputarchive.PendingRootRegistration{Schema: inputarchive.PendingRootSchema, Owner: "edge-control/cell-alpha", Generation: "neutral-generation", SourceDigest: "sha256:" + strings.Repeat("a", 64), References: []inputarchive.HotReference{{Kind: "release", ID: "neutral-release"}}}}
}

func pendingClientJWT(audience, nonce string) string {
	return "eyJhbGciOiJFUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(`{"aud":["`+audience+`"],"nonce":"`+nonce+`"}`)) + ".c2lnbmF0dXJl"
}

func pendingRuntimeClientFixture(t *testing.T, handler http.Handler) (*PendingRootClient, string) {
	t.Helper()
	const host = "fugue-api-tls.platform-test.svc"
	ca, certificate := routeIntentTestPKI(t, time.Now(), host)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13, NextProtos: []string{"http/1.1"}}
	server.StartTLS()
	t.Cleanup(server.Close)
	tokenFile := writeRouteIntentIssuerFixture(t, pendingClientJWT(inputarchive.PendingRootAudience, "first"))
	client, err := NewPendingRootClient(PendingRootClientConfig{Owner: "edge-control/cell-alpha", Namespace: "platform-test", Endpoint: "https://" + host + ":8443" + inputarchive.PendingRootHTTPPath, PodTokenFile: tokenFile, CAFile: ca})
	if err != nil {
		t.Fatal(err)
	}
	transport := client.client.Transport.(*http.Transport)
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	t.Cleanup(transport.CloseIdleConnections)
	return client, tokenFile
}

func TestPendingRootClientUsesPrivateTLSAndRotatedDedicatedToken(t *testing.T) {
	r := pendingClientRequestFixture()
	_, hash, err := r.Registration.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	client, tokenFile := pendingRuntimeClientFixture(t, http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		n := calls.Add(1)
		nonce := "first"
		if n == 2 {
			nonce = "second"
		}
		if q.Method != "POST" || q.URL.EscapedPath() != inputarchive.PendingRootHTTPPath || q.URL.RawQuery != "" || q.Host != "fugue-api-tls.platform-test.svc:8443" || q.TLS == nil || q.TLS.Version != tls.VersionTLS13 || q.TLS.ServerName != "fugue-api-tls.platform-test.svc" || q.ProtoMajor != 1 || q.Header.Get("Authorization") != "Bearer "+pendingClientJWT(inputarchive.PendingRootAudience, nonce) {
			t.Error("private dedicated transport drift")
		}
		body, e := io.ReadAll(q.Body)
		decoded, e2 := inputarchive.DecodePendingRootHTTP(body)
		if e != nil || e2 != nil || !bytes.Equal(decoded.Source, r.Source) || decoded.Registration.Owner != r.Registration.Owner {
			t.Error("source envelope changed", e, e2)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		json.NewEncoder(w).Encode(inputarchive.PendingRootReceipt{RegistrationSHA256: hash, RegistryRevision: 3, SourceEpoch: 0})
	}))
	transport := client.client.Transport.(*http.Transport)
	if transport.Proxy != nil || transport.ForceAttemptHTTP2 || transport.TLSClientConfig.InsecureSkipVerify || transport.TLSClientConfig.RootCAs == nil || len(transport.TLSNextProto) != 0 {
		t.Fatal("unsafe transport")
	}
	if receipt, err := client.RetainPendingCompilerInputExternalRoot(t.Context(), r); err != nil || !receipt.Matches(r.Registration) {
		t.Fatal(receipt, err)
	}
	if err := os.WriteFile(tokenFile, []byte(pendingClientJWT(inputarchive.PendingRootAudience, "second")), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RetainPendingCompilerInputExternalRoot(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal(calls.Load())
	}
	if err := os.WriteFile(tokenFile, []byte(pendingClientJWT("reader-audience", "reader")), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RetainPendingCompilerInputExternalRoot(t.Context(), r); err == nil || calls.Load() != 2 {
		t.Fatal("reader token submitted")
	}
	if err := os.Chmod(tokenFile, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RetainPendingCompilerInputExternalRoot(t.Context(), r); err == nil || calls.Load() != 2 {
		t.Fatal("public token file accepted")
	}
}

func TestPendingRootClientAmbiguousResponsesAreNotRetriedOrAccepted(t *testing.T) {
	r := pendingClientRequestFixture()
	_, hash, _ := r.Registration.Canonical()
	for _, mode := range []string{"denied", "redirect", "lost-ack", "wrong-receipt", "duplicate-receipt", "cacheable", "oversize", "slow-body"} {
		t.Run(mode, func(t *testing.T) {
			var calls, redirects atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirects.Add(1) }))
			defer target.Close()
			client, _ := pendingRuntimeClientFixture(t, http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
				calls.Add(1)
				io.Copy(io.Discard, q.Body)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Cache-Control", "no-store")
				switch mode {
				case "denied":
					http.Error(w, "private-token-value", 403)
				case "redirect":
					http.Redirect(w, q, target.URL, 307)
				case "lost-ack":
					c, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					c.Close()
				case "wrong-receipt":
					json.NewEncoder(w).Encode(inputarchive.PendingRootReceipt{RegistrationSHA256: strings.Repeat("b", 64), RegistryRevision: 3})
				case "duplicate-receipt":
					w.Write([]byte(`{"registration_sha256":"` + hash + `","registry_revision":2,"registry_revision":3,"source_epoch":0}`))
				case "cacheable":
					w.Header().Del("Cache-Control")
					json.NewEncoder(w).Encode(inputarchive.PendingRootReceipt{RegistrationSHA256: hash, RegistryRevision: 3})
				case "oversize":
					w.Write(bytes.Repeat([]byte{' '}, (16<<10)+1))
				case "slow-body":
					w.Write([]byte(`{"registration_sha256":"`))
					w.(http.Flusher).Flush()
					<-q.Context().Done()
				}
			}))
			ctx := t.Context()
			cancel := func() {}
			if mode == "slow-body" {
				ctx, cancel = context.WithTimeout(ctx, time.Second)
			}
			defer cancel()
			start := time.Now()
			receipt, err := client.RetainPendingCompilerInputExternalRoot(ctx, r)
			if err == nil || receipt.RegistryRevision != 0 || calls.Load() != 1 || redirects.Load() != 0 || strings.Contains(err.Error(), "private-token-value") {
				t.Fatal("unsafe response", receipt, err, calls.Load(), redirects.Load())
			}
			if mode == "slow-body" && time.Since(start) > 3*time.Second {
				t.Fatal("body read reset request deadline")
			}
		})
	}
}

func TestPendingRootClientRequiresExactExplicitConfiguration(t *testing.T) {
	base := PendingRootClientConfig{Owner: "edge-control/cell-alpha", Namespace: "platform-test", Endpoint: "https://fugue-api-tls.platform-test.svc:8443" + inputarchive.PendingRootHTTPPath, PodTokenFile: "/private/token", CAFile: "/private/ca"}
	for _, change := range []func(*PendingRootClientConfig){
		func(c *PendingRootClientConfig) { c.Endpoint = strings.Replace(c.Endpoint, "https:", "http:", 1) },
		func(c *PendingRootClientConfig) { c.Endpoint += "?" },
		func(c *PendingRootClientConfig) { c.Endpoint += "?token=reader" },
		func(c *PendingRootClientConfig) { c.Endpoint += "#fragment" },
		func(c *PendingRootClientConfig) { c.Endpoint = strings.Replace(c.Endpoint, ":8443", ":443", 1) },
		func(c *PendingRootClientConfig) { c.Namespace = "different" },
		func(c *PendingRootClientConfig) { c.Owner = "tenant/neutral" },
		func(c *PendingRootClientConfig) { c.PodTokenFile = "token" },
		func(c *PendingRootClientConfig) { c.CAFile = c.PodTokenFile },
	} {
		c := base
		change(&c)
		if _, err := NewPendingRootClient(c); err == nil {
			t.Fatal("unsafe config accepted")
		}
	}
}

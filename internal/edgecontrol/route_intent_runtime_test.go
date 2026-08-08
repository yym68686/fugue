package edgecontrol

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

const routeIntentTestServerName = "route-intent.test"

func reconcileRouteIntents(ctx context.Context, source RouteIntentSource, compiler GroupShadowCompiler, groups []string) (GroupShadowBatch, error) {
	snapshot, err := source.FetchRouteIntents(ctx)
	if err != nil {
		return GroupShadowBatch{}, err
	}
	return compiler.Reconcile(ctx, snapshot, groups)
}

func TestRouteIntentClientAuthenticatesAndBindsVersion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 13, 0, 0, 0, time.UTC)
	keyring, token := routeIntentIdentityFixture(t, now, 5*time.Minute, model.PlatformConsumerComponentEdgeControl)
	tokenFile := writeRouteIntentIssuerFixture(t, token)
	snapshot := routeIntentFixture()
	var requests atomic.Int32
	server, caFile, serverName, dialAddress := newRouteIntentTLSServer(t, now, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.Method != http.MethodGet || request.URL.Path != RouteIntentPathV1 || request.URL.RawQuery != "" || request.ProtoMajor != 1 || request.TLS == nil || request.TLS.Version != tls.VersionTLS13 || request.TLS.ServerName != routeIntentTestServerName {
			t.Errorf("unexpected request target %s %s", request.Method, request.URL.String())
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		claims, err := platformcontrol.ParsePlatformComponentIdentity(keyring, strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "), now)
		if err != nil || claims.Component != model.PlatformConsumerComponentEdgeControl || claims.ScopeKey != "global" || len(claims.ArtifactKinds) != 1 || claims.ArtifactKinds[0] != model.PlatformArtifactKindEdgeRouteIntent {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", strconv.Quote(snapshot.Generation))
		w.Header().Set(RouteIntentGenerationHeader, snapshot.Generation)
		_ = json.NewEncoder(w).Encode(snapshot)
	}))
	defer server.Close()

	client, err := NewRouteIntentClient(RouteIntentClientConfig{
		Endpoint: server.URL + RouteIntentPathV1, IssuerFile: tokenFile, IdentityNodeID: "edge-control-test", CAFile: caFile, ServerName: serverName, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	bindRouteIntentTestDialer(t, client, dialAddress)
	transport, ok := client.client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || transport.ForceAttemptHTTP2 || transport.TLSNextProto == nil || len(transport.TLSNextProto) != 0 ||
		transport.TLSClientConfig == nil || transport.TLSClientConfig.RootCAs == nil || transport.TLSClientConfig.ServerName != serverName ||
		transport.TLSClientConfig.MinVersion != tls.VersionTLS13 || transport.TLSClientConfig.InsecureSkipVerify ||
		len(transport.TLSClientConfig.NextProtos) != 1 || transport.TLSClientConfig.NextProtos[0] != "http/1.1" {
		t.Fatalf("unsafe RouteIntent transport: %+v", transport)
	}
	got, err := client.FetchRouteIntents(context.Background())
	if err != nil {
		t.Fatalf("FetchRouteIntents() error = %v", err)
	}
	if got.Generation != snapshot.Generation || got.SchemaVersion != model.EdgeRouteIntentSchemaVersionV1 || requests.Load() != 1 {
		t.Fatalf("fetched snapshot = %+v requests=%d", got, requests.Load())
	}
}

func TestRouteIntentClientCredentialFailuresAreClosedBeforeLedger(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 14, 0, 0, 0, time.UTC)
	_, valid := routeIntentIdentityFixture(t, now, time.Minute, model.PlatformConsumerComponentEdgeControl)

	for _, test := range []struct {
		name   string
		issuer string
	}{
		{name: "malformed", issuer: "not-json"},
		{name: "wrong schema", issuer: strings.Replace(valid, routeIntentIssuerSchemaV1, "edge-route-intent-issuer/v2", 1)},
		{name: "short key", issuer: `{"schema":"edge-route-intent-issuer/v1","generation":1,"key_id":"route-key","key":"short"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			server, caFile, serverName, dialAddress := newRouteIntentTLSServer(t, now, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				http.Error(w, "unexpected", http.StatusInternalServerError)
			}))
			defer server.Close()
			client, err := NewRouteIntentClient(RouteIntentClientConfig{
				Endpoint:   server.URL + RouteIntentPathV1,
				IssuerFile: writeRouteIntentIssuerFixture(t, test.issuer), IdentityNodeID: "edge-control-test", CAFile: caFile, ServerName: serverName, Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}
			bindRouteIntentTestDialer(t, client, dialAddress)
			store, err := OpenPersistentGroupStore(privateStateDir(t))
			if err != nil {
				t.Fatal(err)
			}
			groupID := "edge-group-country-us"
			inventory := groupInventoryFixture(groupID, model.EdgeSlotB, "epoch-us-b", "inventory-us-1", false)
			if err := store.StoreGroupInventoryCAS(context.Background(), groupID, 0, inventory); err != nil {
				t.Fatal(err)
			}
			if _, err := reconcileRouteIntents(context.Background(), client, GroupShadowCompiler{Inventory: store, Ledger: store}, []string{groupID}); !errors.Is(err, ErrRouteIntentCredential) && !errors.Is(err, ErrRouteIntentUnauthorized) {
				t.Fatalf("RunOnce() error = %v", err)
			}
			if requests.Load() != 0 {
				t.Fatalf("locally rejected credential made %d requests", requests.Load())
			}
			if history, err := store.History(context.Background(), groupID); err != nil || len(history) != 0 {
				t.Fatalf("credential failure wrote ledger: %+v, %v", history, err)
			}
		})
	}
}

func TestRouteIntentClientVersionMismatchesFailClosed(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 15, 0, 0, 0, time.UTC)
	_, token := routeIntentIdentityFixture(t, now, time.Minute, model.PlatformConsumerComponentEdgeControl)
	for _, test := range []struct {
		name   string
		mutate func(http.ResponseWriter, *model.EdgeRouteIntentSnapshot)
	}{
		{name: "missing generation header", mutate: func(_ http.ResponseWriter, _ *model.EdgeRouteIntentSnapshot) {}},
		{name: "wrong generation header", mutate: func(w http.ResponseWriter, _ *model.EdgeRouteIntentSnapshot) {
			w.Header().Set(RouteIntentGenerationHeader, "wrong")
		}},
		{name: "wrong etag", mutate: func(w http.ResponseWriter, snapshot *model.EdgeRouteIntentSnapshot) {
			w.Header().Set(RouteIntentGenerationHeader, snapshot.Generation)
			w.Header().Set("ETag", strconv.Quote("wrong"))
		}},
		{name: "wrong schema", mutate: func(w http.ResponseWriter, snapshot *model.EdgeRouteIntentSnapshot) {
			w.Header().Set(RouteIntentGenerationHeader, snapshot.Generation)
			w.Header().Set("ETag", strconv.Quote(snapshot.Generation))
			snapshot.SchemaVersion = "edge-route-intent/v2"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := routeIntentFixture()
			server, caFile, serverName, dialAddress := newRouteIntentTLSServer(t, now, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				test.mutate(w, &snapshot)
				_ = json.NewEncoder(w).Encode(snapshot)
			}))
			defer server.Close()
			client, err := NewRouteIntentClient(RouteIntentClientConfig{
				Endpoint:   server.URL + RouteIntentPathV1,
				IssuerFile: writeRouteIntentIssuerFixture(t, token), IdentityNodeID: "edge-control-test", CAFile: caFile, ServerName: serverName, Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}
			bindRouteIntentTestDialer(t, client, dialAddress)
			if _, err := client.FetchRouteIntents(context.Background()); !errors.Is(err, ErrRouteIntentVersionBinding) {
				t.Fatalf("FetchRouteIntents() error = %v, want %v", err, ErrRouteIntentVersionBinding)
			}
		})
	}
}

func TestRouteIntentClientRejectsUnsafeEndpointAndIssuerFile(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 15, 15, 0, 0, time.UTC)
	caFile, _ := routeIntentTestPKI(t, now, "api.example.test")
	if _, err := NewRouteIntentClient(RouteIntentClientConfig{Endpoint: "http://api.example.test" + RouteIntentPathV1, IssuerFile: "/tmp/token", IdentityNodeID: "edge-control-test", CAFile: caFile, ServerName: "api.example.test"}); err == nil {
		t.Fatal("plain HTTP RouteIntent endpoint unexpectedly accepted")
	}
	if _, err := NewRouteIntentClient(RouteIntentClientConfig{Endpoint: "https://api.example.test/v1/edge/routes", IssuerFile: "/tmp/token", IdentityNodeID: "edge-control-test", CAFile: caFile, ServerName: "api.example.test"}); err == nil {
		t.Fatal("wrong RouteIntent path unexpectedly accepted")
	}
	path := writeRouteIntentIssuerFixture(t, "fugue_pc_v1.invalid.invalid.invalid")
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	client, err := NewRouteIntentClient(RouteIntentClientConfig{Endpoint: "https://api.example.test" + RouteIntentPathV1, IssuerFile: path, IdentityNodeID: "edge-control-test", CAFile: caFile, ServerName: "api.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.FetchRouteIntents(context.Background()); !errors.Is(err, ErrRouteIntentCredential) {
		t.Fatalf("world-readable issuer file error = %v", err)
	}
	groupWritable := writeRouteIntentIssuerFixture(t, "fugue_pc_v1.invalid.invalid.invalid")
	if err := os.Chmod(groupWritable, 0o660); err != nil {
		t.Fatal(err)
	}
	client, err = NewRouteIntentClient(RouteIntentClientConfig{Endpoint: "https://api.example.test" + RouteIntentPathV1, IssuerFile: groupWritable, IdentityNodeID: "edge-control-test", CAFile: caFile, ServerName: "api.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.readBoundCredential(now); !errors.Is(err, ErrRouteIntentCredential) {
		t.Fatalf("group-writable issuer file error = %v", err)
	}
}

func TestRouteIntentClientRequiresExplicitCAAndServerName(t *testing.T) {
	t.Parallel()

	config := RouteIntentClientConfig{
		Endpoint:   "https://fugue-api-tls.fugue-system.svc:8443" + RouteIntentPathV1,
		IssuerFile: "/var/run/secrets/fugue-edge-control/route-intent/token", IdentityNodeID: "edge-control-test",
		CAFile:     "/var/run/secrets/fugue-edge-control/route-intent-ca/ca.crt",
		ServerName: "fugue-api-tls.fugue-system.svc",
	}
	if err := ValidateRouteIntentClientConfig(config); err != nil {
		t.Fatalf("explicit RouteIntent TLS contract rejected: %v", err)
	}
	if _, err := NewRouteIntentClient(config); err == nil {
		t.Fatal("missing projected RouteIntent CA unexpectedly allowed process startup")
	}
	for name, mutate := range map[string]func(*RouteIntentClientConfig){
		"missing CA":             func(value *RouteIntentClientConfig) { value.CAFile = "" },
		"relative CA":            func(value *RouteIntentClientConfig) { value.CAFile = "ca.crt" },
		"missing server name":    func(value *RouteIntentClientConfig) { value.ServerName = "" },
		"wildcard server name":   func(value *RouteIntentClientConfig) { value.ServerName = "*.fugue-system.svc" },
		"mismatched server name": func(value *RouteIntentClientConfig) { value.ServerName = "other.fugue-system.svc" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := config
			mutate(&candidate)
			if err := ValidateRouteIntentClientConfig(candidate); err == nil {
				t.Fatal("unsafe RouteIntent TLS contract unexpectedly accepted")
			}
		})
	}
}

func TestRouteIntentClientRejectsMalformedCAUntrustedChainAndSANMismatch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 15, 20, 0, 0, time.UTC)
	_, token := routeIntentIdentityFixture(t, now, time.Minute, model.PlatformConsumerComponentEdgeControl)
	server, caFile, serverName, dialAddress := newRouteIntentTLSServer(t, now, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not reached", http.StatusInternalServerError)
	}))
	defer server.Close()
	base := RouteIntentClientConfig{Endpoint: server.URL + RouteIntentPathV1, IssuerFile: writeRouteIntentIssuerFixture(t, token), IdentityNodeID: "edge-control-test", CAFile: caFile, ServerName: serverName, Now: func() time.Time { return now }}

	malformed := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(malformed, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	candidate := base
	candidate.CAFile = malformed
	if _, err := NewRouteIntentClient(candidate); err == nil {
		t.Fatal("malformed explicit CA unexpectedly accepted")
	}

	worldReadable, _ := routeIntentTestPKI(t, now, serverName)
	if err := os.Chmod(worldReadable, 0o644); err != nil {
		t.Fatal(err)
	}
	candidate = base
	candidate.CAFile = worldReadable
	if _, err := NewRouteIntentClient(candidate); err == nil {
		t.Fatal("world-readable explicit CA unexpectedly accepted")
	}

	fsGroupReadable, _ := routeIntentTestPKI(t, now, serverName)
	if err := os.Chmod(fsGroupReadable, 0o640); err != nil {
		t.Fatal(err)
	}
	candidate = base
	candidate.CAFile = fsGroupReadable
	if _, err := NewRouteIntentClient(candidate); err != nil {
		t.Fatalf("kubelet fsGroup-private explicit CA rejected: %v", err)
	}

	unrelatedCA, _ := routeIntentTestPKI(t, now, "unrelated.test")
	candidate = base
	candidate.CAFile = unrelatedCA
	client, err := NewRouteIntentClient(candidate)
	if err != nil {
		t.Fatal(err)
	}
	bindRouteIntentTestDialer(t, client, dialAddress)
	if _, err := client.FetchRouteIntents(context.Background()); !errors.Is(err, ErrRouteIntentFetch) {
		t.Fatalf("untrusted RouteIntent chain error=%v", err)
	}

	candidate = base
	candidate.ServerName = "wrong.route-intent.test"
	candidate.Endpoint = "https://wrong.route-intent.test:" + serverPort(t, server.URL) + RouteIntentPathV1
	client, err = NewRouteIntentClient(candidate)
	if err != nil {
		t.Fatal(err)
	}
	bindRouteIntentTestDialer(t, client, dialAddress)
	groupID := "edge-group-country-us"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, model.EdgeSlotB, "epoch-us-b", "inventory-us-1", false)
	if err := store.StoreGroupInventoryCAS(context.Background(), groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcileRouteIntents(context.Background(), client, GroupShadowCompiler{Inventory: store, Ledger: store}, []string{groupID}); !errors.Is(err, ErrRouteIntentFetch) {
		t.Fatalf("RouteIntent SAN mismatch error=%v", err)
	}
	if history, err := store.History(context.Background(), groupID); err != nil || len(history) != 0 {
		t.Fatalf("RouteIntent SAN mismatch changed ledger: %+v, %v", history, err)
	}
}

func TestRouteIntentClientAcceptsPrivateProjectedSecretRotation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 15, 30, 0, 0, time.UTC)
	firstKeyring, first := routeIntentIdentityFixture(t, now, time.Minute, model.PlatformConsumerComponentEdgeControl)
	secondKeyring, second := routeIntentIdentityFixture(t, now.Add(time.Second), time.Minute, model.PlatformConsumerComponentEdgeControl)
	caFile, _ := routeIntentTestPKI(t, now, "api.example.test")
	root := t.TempDir()
	writeVersion := func(name, token string) {
		directory := filepath.Join(root, name)
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "issuer.json"), []byte(token+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeVersion("..2026_08_04_15_30_00", first)
	writeVersion("..2026_08_04_15_30_01", second)
	if err := os.Symlink("..2026_08_04_15_30_00", filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..data/issuer.json", filepath.Join(root, "issuer.json")); err != nil {
		t.Fatal(err)
	}

	client, err := NewRouteIntentClient(RouteIntentClientConfig{
		Endpoint:       "https://api.example.test" + RouteIntentPathV1,
		IssuerFile:     filepath.Join(root, "issuer.json"),
		IdentityNodeID: "edge-control-test",
		CAFile:         caFile,
		ServerName:     "api.example.test",
		Now:            func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := client.readBoundCredential(now); err != nil {
		t.Fatalf("first projected credential: %v", err)
	} else if _, err := platformcontrol.ParsePlatformComponentIdentity(firstKeyring, got, now); err != nil {
		t.Fatalf("first minted credential: %v", err)
	}
	if err := os.Remove(filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.readBoundCredential(now.Add(time.Second)); !errors.Is(err, ErrRouteIntentCredential) {
		t.Fatalf("missing rotation target did not fail closed: %v", err)
	}
	if err := os.Symlink("..2026_08_04_15_30_01", filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	if got, err := client.readBoundCredential(now.Add(time.Second)); err != nil {
		t.Fatalf("rotated projected credential: %v", err)
	} else if _, err := platformcontrol.ParsePlatformComponentIdentity(secondKeyring, got, now.Add(time.Second)); err != nil {
		t.Fatalf("rotated minted credential: %v", err)
	}
}

func TestRouteIntentClientRejectsProjectedSecretEscape(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 15, 45, 0, 0, time.UTC)
	_, token := routeIntentIdentityFixture(t, now, time.Minute, model.PlatformConsumerComponentEdgeControl)
	caFile, _ := routeIntentTestPKI(t, now, "api.example.test")
	root := t.TempDir()
	escapeRoot := t.TempDir()
	outside := filepath.Join(escapeRoot, "token")
	if err := os.WriteFile(outside, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	projected := filepath.Join(root, "token")
	if err := os.Symlink(outside, projected); err != nil {
		t.Fatal(err)
	}
	client, err := NewRouteIntentClient(RouteIntentClientConfig{Endpoint: "https://api.example.test" + RouteIntentPathV1, IssuerFile: projected, IdentityNodeID: "edge-control-test", CAFile: caFile, ServerName: "api.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.readBoundCredential(now); !errors.Is(err, ErrRouteIntentCredential) {
		t.Fatalf("escaped projected credential error = %v", err)
	}
}

func newRouteIntentTLSServer(t *testing.T, now time.Time, handler http.Handler) (*httptest.Server, string, string, string) {
	t.Helper()
	caFile, certificate := routeIntentTestPKI(t, now, routeIntentTestServerName)
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}}
	server.StartTLS()
	dialAddress := server.Listener.Addr().String()
	server.URL = "https://" + net.JoinHostPort(routeIntentTestServerName, serverPort(t, server.URL))
	return server, caFile, routeIntentTestServerName, dialAddress
}

func serverPort(t *testing.T, rawURL string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Port() == "" {
		t.Fatalf("test TLS server URL is invalid: %q", rawURL)
	}
	return parsed.Port()
}

func bindRouteIntentTestDialer(t *testing.T, client *RouteIntentClient, address string) {
	t.Helper()
	transport, ok := client.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("RouteIntent transport type = %T", client.client.Transport)
	}
	dialer := &net.Dialer{Timeout: defaultRouteIntentDialTime}
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, address)
	}
}

func routeIntentTestPKI(t *testing.T, _ time.Time, serverName string) (string, tls.Certificate) {
	t.Helper()
	certificateNow := time.Now().UTC()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "RouteIntent Test CA"},
		NotBefore: certificateNow.Add(-24 * time.Hour), NotAfter: certificateNow.Add(365 * 24 * time.Hour),
		BasicConstraintsValid: true, IsCA: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: serverName}, DNSNames: []string{serverName},
		NotBefore: certificateNow.Add(-24 * time.Hour), NotAfter: certificateNow.Add(24 * time.Hour),
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKeyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: leafKeyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return caFile, certificate
}

func routeIntentIdentityFixture(t *testing.T, now time.Time, ttl time.Duration, component string) (platformcontrol.PlatformComponentIdentityKeyring, string) {
	t.Helper()
	_ = ttl
	_ = component
	keyID := fmt.Sprintf("edge-control-test-key-%d", now.Unix())
	rawKey := fmt.Sprintf("edge-control-route-intent-test-secret-%d", now.UnixNano())
	keyring := platformcontrol.DerivePlatformComponentIdentityKeyring(rawKey, keyID, "", "", nil)
	issuer, err := json.Marshal(map[string]any{"schema": routeIntentIssuerSchemaV1, "generation": uint64(1), "key_id": keyID, "key": rawKey})
	if err != nil {
		t.Fatal(err)
	}
	return keyring, string(issuer)
}

func writeRouteIntentIssuerFixture(t *testing.T, issuer string) string {
	t.Helper()
	path := t.TempDir() + "/issuer.json"
	if err := os.WriteFile(path, []byte(issuer+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

package entryfailover

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testTLSServer(t *testing.T, edgeID string) (*httptest.Server, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "example.test"},
		DNSNames: []string{"example.test", "api.example.test"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(8 * 24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Fugue-Static-Edge", edgeID)
		if r.Host == "example.test" && r.URL.Path == "/" {
			w.WriteHeader(307)
			return
		}
		if r.Host == "api.example.test" && r.URL.Path == "/v1/health" {
			w.WriteHeader(200)
			return
		}
		w.WriteHeader(404)
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return srv, pool
}

func TestDirectProbePreservesOriginalSNIHostAndEdgeIdentity(t *testing.T) {
	srv, roots := testTLSServer(t, "west-edge")
	defer srv.Close()
	dial := func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
	}
	probe := Prober{RootCAs: roots, Dial: dial}
	p := testPolicy()
	got := probe.Probe(context.Background(), p, p.Targets[0])
	if !got.Healthy || len(got.Checks) != 2 {
		t.Fatalf("healthy route probe=%+v", got)
	}
	for _, c := range got.Checks {
		if c.Duration <= 0 || c.Address != p.Targets[0].Address {
			t.Fatalf("probe duration or pinned address lost: %+v", c)
		}
	}
	srv.Close()
	bad, _ := testTLSServer(t, "wrong-edge")
	defer bad.Close()
	probe.Dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, bad.Listener.Addr().String())
	}
	got = probe.Probe(context.Background(), p, p.Targets[0])
	if got.Healthy {
		t.Fatalf("wrong certificate or edge identity accepted: %+v", got)
	}
}

package edge

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/edge/relayroute"
)

func TestRelayPrivateListenerCanStartBeforePolicy(t *testing.T) {
	now := time.Now()
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root, _, issue := relayFixtureCertificates(t, now)
	endpoint := relayroute.Endpoint{Edge: "edge-a", Slot: "a", Address: "10.42.1.2:8443", TLSName: "a.example.test"}
	cert := issue(endpoint.TLSName, 2)
	der, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, data []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	policyFile := filepath.Join(dir, "policy.json")
	for suffix, value := range map[string]string{
		"LISTEN_ADDR":            endpoint.Address,
		"POLICY_FILE":            policyFile,
		"STATE_FILE":             filepath.Join(dir, "fence.json"),
		"POLICY_VERIFY_KEY_FILE": write("verify", []byte(hex.EncodeToString(pub))),
		"CA_FILE":                write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: root.Raw})),
		"TLS_CERT_FILE":          write("leaf.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Leaf.Raw})),
		"TLS_KEY_FILE":           write("key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
	} {
		t.Setenv("FUGUE_EDGE_ORIGIN_RELAY_"+suffix, value)
	}
	s := &Service{Config: config.EdgeConfig{EdgeID: endpoint.Edge, EdgeSlot: endpoint.Slot}}
	srv, listener, err := s.initOriginRelayWithListen(func(network, address string) (net.Listener, error) {
		if network != "tcp" || address != endpoint.Address {
			t.Fatalf("listener attempted to bind %s %s", network, address)
		}
		return net.Listen("tcp", "127.0.0.1:0") // local fixture for the unavailable Pod IP
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	e := s.originRelay
	if e.local != endpoint || srv.Handler != e {
		t.Fatal("private listener did not pin the declared local identity")
	}
	e.refresh(now)
	if e.current(now) != nil {
		t.Fatal("listener came up with relay authority before a signed policy")
	}
	publish := func(generation uint64, mode string, ep relayroute.Endpoint) {
		t.Helper()
		payload, err := json.Marshal(relayroute.Policy{Schema: relayroute.Schema, Generation: generation, Mode: mode, MaxConcurrent: 1,
			NotBefore: now.Add(-time.Minute), Expires: now.Add(time.Minute), Endpoints: []relayroute.Endpoint{ep}})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(relayroute.SignedPolicy{Payload: payload, Signature: ed25519.Sign(key, payload)})
		if err != nil {
			t.Fatal(err)
		}
		write("policy.json", raw)
	}
	publish(1, "off", endpoint)
	e.refresh(now)
	other := endpoint
	other.Address = "10.42.1.3:8443"
	publish(2, "shadow", other)
	e.refresh(now)
	if e.current(now) != nil {
		t.Fatal("listener accepted a lease for another Pod address")
	}
	publish(2, "shadow", endpoint)
	e.refresh(now)
	if p := e.current(now); p == nil || p.Mode != "shadow" {
		t.Fatal("listener did not accept the matching policy without restart")
	}

	t.Setenv("FUGUE_EDGE_ORIGIN_RELAY_LISTEN_ADDR", "0.0.0.0:8443")
	if _, _, err := s.initOriginRelayWithListen(func(string, string) (net.Listener, error) {
		t.Fatal("wildcard made it to the listener")
		return nil, nil
	}); err == nil {
		t.Fatal("wildcard address was accepted")
	}
}

func TestRelayCertificateRejectsAliasedDNSIdentity(t *testing.T) {
	const want = "worker-a.example.test"
	for _, dns := range [][]string{{"*.example.test"}, {want, "worker-b.example.test"}} {
		if relayCertificateName(&x509.Certificate{DNSNames: dns}, want) {
			t.Fatalf("accepted non-exact certificate DNS identity %v", dns)
		}
	}
}

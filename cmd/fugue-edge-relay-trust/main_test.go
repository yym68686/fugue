package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func pilot() declaration {
	return declaration{Schema: schema, Peers: []peerIdentity{
		{Edge: "edge-a", Slot: "a", TLSName: "slot-a.edge-a.example.test"},
		{Edge: "edge-a", Slot: "b", TLSName: "slot-b.edge-a.example.test"},
		{Edge: "edge-b", Slot: "a", TLSName: "slot-a.edge-b.example.test"},
	}}
}

func TestTrustPrivatePackagesAreSeparatedAndExact(t *testing.T) {
	d, out, now := pilot(), filepath.Join(t.TempDir(), "trust"), time.Now().UTC().Truncate(time.Second)
	if err := createTrust(d, out, now, 3*time.Hour); err != nil {
		t.Fatal(err)
	}
	file := func(dir, name string) []byte {
		t.Helper()
		if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0700 {
			t.Fatalf("not a private directory: %v", err)
		}
		path := filepath.Join(dir, name)
		if info, err := os.Lstat(path); err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			t.Fatalf("not a private file: %v", err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	var r trustReceipt
	if raw := file(out, "trust-receipt.json"); json.Unmarshal(raw, &r) != nil || len(r.Leaves) != len(d.Peers) || r.LeafNotAfter.Sub(r.LeafNotBefore) != 3*time.Hour {
		t.Fatal("incomplete public receipt")
	}
	issuer := filepath.Join(out, "issuer")
	readKey := func(name string) ed25519.PrivateKey {
		t.Helper()
		block, _ := pem.Decode(file(issuer, name))
		if block == nil || block.Type != "PRIVATE KEY" {
			t.Fatal("missing expected private key")
		}
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return parsed.(ed25519.PrivateKey)
	}
	caKey, signer := readKey("ca-key.pem"), readKey("policy-signer.pem")
	if bytes.Equal(caKey, signer) || hex.EncodeToString(signer.Public().(ed25519.PublicKey)) != r.PolicyVerify {
		t.Fatal("policy signer is not independent from the TLS CA")
	}
	rootPEM := file(issuer, "ca.pem")
	block, _ := pem.Decode(rootPEM)
	if block == nil || certDigest(block.Bytes) != r.Root {
		t.Fatal("root fingerprint mismatch")
	}
	root, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !root.IsCA || !root.MaxPathLenZero || !root.PublicKey.(ed25519.PublicKey).Equal(caKey.Public()) {
		t.Fatal("expected independent constrained CA")
	}
	pool := x509.NewCertPool()
	pool.AddCert(root)
	keys := [][]byte{caKey, signer}
	for i, entry := range r.Leaves {
		if entry.peerIdentity != d.Peers[i] {
			t.Fatal("public receipt does not bind the exact slot and identity")
		}
		dir := filepath.Join(out, entry.Directory)
		leaf, err := tls.X509KeyPair(file(dir, "leaf.pem"), file(dir, "key.pem"))
		if err != nil || certDigest(leaf.Certificate[0]) != entry.Leaf || !slices.Equal(leaf.Leaf.DNSNames, []string{entry.TLSName}) || leaf.Leaf.IsCA {
			t.Fatalf("incorrect leaf identity: %v", err)
		}
		for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
			if _, err := leaf.Leaf.Verify(x509.VerifyOptions{DNSName: entry.TLSName, Roots: pool, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
				t.Fatal(err)
			}
		}
		for _, prior := range keys {
			if bytes.Equal(prior, leaf.PrivateKey.(ed25519.PrivateKey)) {
				t.Fatal("private key reused across roles or slots")
			}
		}
		keys = append(keys, leaf.PrivateKey.(ed25519.PrivateKey))
		if string(file(dir, "policy-verify-key")) != r.PolicyVerify+"\n" || !bytes.Equal(file(dir, "ca.pem"), rootPEM) {
			t.Fatal("mismatched public trust projection")
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 4 {
			t.Fatal("slot package contains unexpected or issuer material")
		}
	}
	if err := createTrust(d, out, now, time.Hour); err == nil {
		t.Fatal("an existing generation's private material was overwritten")
	}
}

func TestTrustRejectsAmbiguousRecipients(t *testing.T) {
	for name, change := range map[string]func(*declaration){
		"same slot":   func(d *declaration) { d.Peers[0].Slot = "b" },
		"same SAN":    func(d *declaration) { d.Peers[1].TLSName = d.Peers[0].TLSName },
		"wildcard":    func(d *declaration) { d.Peers[1].TLSName = "*.edge-a.example.test" },
		"single node": func(d *declaration) { d.Peers = d.Peers[:2] },
		"path escape": func(d *declaration) { d.Peers[2].Edge = "../foreign" },
	} {
		t.Run(name, func(t *testing.T) {
			d := pilot()
			change(&d)
			path := filepath.Join(t.TempDir(), "trust")
			if err := createTrust(d, path, time.Now(), 2*time.Hour); err == nil {
				t.Fatal("accepted an ambiguous trust recipient")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("wrote output before validation")
			}
		})
	}
}

package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"time"

	"fugue/internal/edge/relayroute"
)

const maxCheckBytes = 96 << 10
const stagingMargin = 2 * time.Hour

// Only individual leaf keys cross the release boundary. The root and policy
// issuer are deliberately not accepted by this interface.
type leafPackage struct {
	peerIdentity
	Leaf string `json:"leaf_sha256"`
	Cert string `json:"cert_pem"`
	Key  string `json:"key_pem"`
}

type checkInput struct {
	CA           string        `json:"ca_pem"`
	Root         string        `json:"ca_sha256"`
	PolicyVerify string        `json:"policy_verify_key"`
	Packages     []leafPackage `json:"packages"`
}

func checkPackages(in io.Reader, now time.Time) error {
	bad := errors.New("trust package does not match the declared identities, fingerprints or validity window")
	raw, err := io.ReadAll(io.LimitReader(in, maxCheckBytes+1))
	defer clear(raw)
	var row checkInput
	if err != nil || len(raw) > maxCheckBytes || relayroute.Decode(bytes.NewReader(raw), &row) != nil {
		return bad
	}
	d := declaration{Schema: schema}
	for _, leaf := range row.Packages {
		d.Peers = append(d.Peers, leaf.peerIdentity)
	}
	if d.validate() != nil {
		return bad
	}
	verify, err := hex.DecodeString(row.PolicyVerify)
	root, ok := exactCert(row.CA, row.Root)
	if err != nil || len(verify) != ed25519.PublicKeySize || hex.EncodeToString(verify) != row.PolicyVerify || !ok ||
		!root.IsCA || !root.BasicConstraintsValid || !root.MaxPathLenZero || root.KeyUsage != x509.KeyUsageCertSign || root.CheckSignatureFrom(root) != nil {
		return bad
	}
	key, ok := root.PublicKey.(ed25519.PublicKey)
	if !ok || bytes.Equal(key, verify) || now.Before(root.NotBefore) || !now.Add(stagingMargin).Before(root.NotAfter) {
		return bad
	}
	seen := map[string]bool{string(key): true, string(verify): true}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	for _, pkg := range row.Packages {
		leaf, ok := exactCert(pkg.Cert, pkg.Leaf)
		if !ok || leaf.IsCA || !leaf.BasicConstraintsValid || len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != pkg.TLSName ||
			len(leaf.IPAddresses)+len(leaf.EmailAddresses)+len(leaf.URIs) != 0 || leaf.KeyUsage != x509.KeyUsageDigitalSignature ||
			len(leaf.ExtKeyUsage) != 2 || len(leaf.UnknownExtKeyUsage) != 0 || leaf.NotAfter.Sub(leaf.NotBefore) > 7*24*time.Hour ||
			now.Before(leaf.NotBefore) || !now.Add(stagingMargin).Before(leaf.NotAfter) || leaf.NotAfter.After(root.NotAfter) {
			return bad
		}
		pub, ok := leaf.PublicKey.(ed25519.PublicKey)
		if !ok || seen[string(pub)] {
			return bad
		}
		seen[string(pub)] = true
		for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
			if _, err := leaf.Verify(x509.VerifyOptions{DNSName: pkg.TLSName, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{usage}, Roots: roots}); err != nil {
				return bad
			}
		}
		keyPEM := []byte(pkg.Key)
		block, rest := pem.Decode(keyPEM)
		if block == nil || block.Type != "PRIVATE KEY" || len(block.Headers) != 0 || len(bytes.TrimSpace(rest)) != 0 {
			clear(keyPEM)
			return bad
		}
		pair, pairErr := tls.X509KeyPair([]byte(pkg.Cert), keyPEM)
		clear(block.Bytes)
		clear(keyPEM)
		if pairErr != nil {
			return bad
		}
		if p, ok := pair.PrivateKey.(ed25519.PrivateKey); !ok {
			return bad
		} else {
			clear(p)
		}
	}
	return nil
}

func exactCert(raw, digest string) (*x509.Certificate, bool) {
	if len(raw) > 8192 {
		return nil, false
	}
	trimmed := bytes.TrimSpace([]byte(raw))
	if !bytes.HasPrefix(trimmed, []byte("-----BEGIN CERTIFICATE-----")) {
		return nil, false
	}
	block, rest := pem.Decode(trimmed)
	if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 || len(bytes.TrimSpace(rest)) != 0 || certDigest(block.Bytes) != digest {
		return nil, false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	return cert, err == nil
}

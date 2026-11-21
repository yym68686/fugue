// fugue-edge-relay-trust creates one offline trust epoch for a relay pilot.
// It never contacts Kubernetes or publishes a lease.
package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"fugue/internal/edge/relayroute"
)

const schema = "fugue.edge.origin-relay-trust/v1"
const maxDeclarationBytes = 16 << 10

var identityPart = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,126}[a-z0-9])?$`)
var dnsName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$`)

type peerIdentity struct {
	Edge    string `json:"edge"`
	Slot    string `json:"slot"`
	TLSName string `json:"tls_name"`
}

type declaration struct {
	Schema string         `json:"schema"`
	Peers  []peerIdentity `json:"peers"`
}

type leafReceipt struct {
	peerIdentity
	Directory string `json:"directory"`
	Leaf      string `json:"leaf_sha256"`
}

type trustReceipt struct {
	Schema        string        `json:"schema"`
	Root          string        `json:"ca_sha256"`
	PolicyVerify  string        `json:"policy_verify_key"`
	LeafNotBefore time.Time     `json:"leaf_not_before"`
	LeafNotAfter  time.Time     `json:"leaf_not_after"`
	Leaves        []leafReceipt `json:"leaves"`
}

func main() {
	in := flag.String("identities", "", "JSON declaration of exact Edge/slot TLS identities")
	out := flag.String("output", "", "new absolute directory on a trusted offline filesystem")
	ttl := flag.Duration("leaf-ttl", 72*time.Hour, "leaf validity; at most 168h")
	flag.Parse()
	if *in == "" || *out == "" || flag.NArg() != 0 {
		fail(errors.New("-identities and -output are required"))
	}
	d, err := readDeclaration(*in)
	if err == nil {
		err = createTrust(d, *out, time.Now().UTC(), *ttl)
	}
	if err != nil {
		fail(err)
	}
	fmt.Println("Generated an offline trust epoch; no policy or credentials were published.")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "fugue-edge-relay-trust:", err)
	os.Exit(1)
}

func readDeclaration(path string) (declaration, error) {
	var d declaration
	invalid := errors.New("expected one bounded, regular identities file")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxDeclarationBytes {
		return d, invalid
	}
	f, err := os.Open(path)
	if err != nil {
		return d, invalid
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return d, invalid
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxDeclarationBytes+1))
	if err != nil || len(raw) > maxDeclarationBytes || relayroute.Decode(bytes.NewReader(raw), &d) != nil {
		return d, invalid
	}
	return d, d.validate()
}

func (d declaration) validate() error {
	if d.Schema != schema || len(d.Peers) < 2 || len(d.Peers) > 16 {
		return errors.New("expected 2–16 exact Edge/slot identities")
	}
	locations, names, edges := map[[2]string]bool{}, map[string]bool{}, map[string]bool{}
	for _, p := range d.Peers {
		location := [2]string{p.Edge, p.Slot}
		if !identityPart.MatchString(p.Edge) || !identityPart.MatchString(p.Slot) || len(p.TLSName) > 253 ||
			!dnsName.MatchString(p.TLSName) || locations[location] || names[p.TLSName] {
			return errors.New("each Edge/slot needs its own exact DNS SAN")
		}
		locations[location], names[p.TLSName], edges[p.Edge] = true, true, true
	}
	if len(edges) < 2 {
		return errors.New("at least two distinct Edge identities are required")
	}
	return nil
}

func createTrust(d declaration, output string, now time.Time, ttl time.Duration) error {
	if err := d.validate(); err != nil {
		return err
	}
	if ttl < time.Hour || ttl > 7*24*time.Hour || !filepath.IsAbs(output) || filepath.Clean(output) != output || output == filepath.Dir(output) {
		return errors.New("require a new absolute directory and a leaf TTL of 1h–168h")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		return errors.New("could not exclusively create private output directory")
	}
	if err := writeTrust(d, output, now.Truncate(time.Second), ttl); err != nil {
		// A failed package is intentionally not retryable at the same path.
		// Do not print private input or overwrite output while recovering.
		return errors.New("trust generation failed; protect the incomplete directory and use a new path")
	}
	return nil
}

func writeTrust(d declaration, output string, now time.Time, ttl time.Duration) error {
	issuer := filepath.Join(output, "issuer")
	if err := os.Mkdir(issuer, 0700); err != nil {
		return err
	}
	caPub, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	defer clear(caKey)
	caSerial, err := serial()
	if err != nil {
		return err
	}
	nbf := now.Add(-time.Minute)
	exp := nbf.Add(ttl)
	ca := &x509.Certificate{SerialNumber: caSerial, Subject: pkix.Name{CommonName: "Fugue private Edge relay CA"},
		NotBefore: nbf, NotAfter: exp.Add(24 * time.Hour), KeyUsage: x509.KeyUsageCertSign,
		BasicConstraintsValid: true, IsCA: true, MaxPathLenZero: true}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caPub, caKey)
	if err != nil {
		return err
	}
	root, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	verifyKey, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	defer clear(signingKey)
	if err := writeKey(issuer, "policy-signer.pem", signingKey); err != nil {
		return err
	}
	if err := writeKey(issuer, "ca-key.pem", caKey); err != nil {
		return err
	}
	rootPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	if err := writeFile(issuer, "ca.pem", rootPEM); err != nil {
		return err
	}
	receipt := trustReceipt{Schema: schema, Root: certDigest(caDER), PolicyVerify: hex.EncodeToString(verifyKey), LeafNotBefore: nbf, LeafNotAfter: exp}
	for i, peer := range d.Peers {
		name := fmt.Sprintf("peer-%02d", i+1)
		leaf, err := writeLeaf(filepath.Join(output, name), peer.TLSName, root, caKey, rootPEM, receipt, nbf, exp)
		if err != nil {
			return err
		}
		receipt.Leaves = append(receipt.Leaves, leafReceipt{peerIdentity: peer, Directory: name, Leaf: leaf})
	}
	raw, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(output, "trust-receipt.json", append(raw, '\n'))
}

func writeLeaf(dir, tlsName string, root *x509.Certificate, caKey ed25519.PrivateKey, rootPEM []byte, receipt trustReceipt, nbf, exp time.Time) (string, error) {
	if err := os.Mkdir(dir, 0700); err != nil {
		return "", err
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", err
	}
	defer clear(key)
	number, err := serial()
	if err != nil {
		return "", err
	}
	leaf := &x509.Certificate{SerialNumber: number, DNSNames: []string{tlsName}, NotBefore: nbf, NotAfter: exp,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, root, pub, caKey)
	if err != nil {
		return "", err
	}
	if err := writeKey(dir, "key.pem", key); err != nil {
		return "", err
	}
	for _, file := range []struct {
		name string
		raw  []byte
	}{{"leaf.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, {"ca.pem", rootPEM}, {"policy-verify-key", []byte(receipt.PolicyVerify + "\n")}} {
		if err := writeFile(dir, file.name, file.raw); err != nil {
			return "", err
		}
	}
	return certDigest(der), nil
}

func serial() (*big.Int, error) {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return nil, err
	}
	return n.Add(n, big.NewInt(1)), nil
}

func certDigest(der []byte) string {
	digest := sha256.Sum256(der)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func writeKey(dir, file string, key ed25519.PrivateKey) error {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	defer clear(der)
	raw := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	defer clear(raw)
	return writeFile(dir, file, raw)
}

func writeFile(dir, name string, raw []byte) error {
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(raw)
	return errors.Join(err, f.Sync(), f.Close())
}

// fugue-edge-relay-policy creates a signed, short-lived relay lease locally.
// It does not connect to the network or write production configuration.
package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"fugue/internal/edge/relayroute"
	"fugue/internal/model"
	"fugue/internal/routeproof"
)

func main() {
	payload := flag.String("policy", "", "policy payload JSON including independently observed route proof digests")
	keyFile := flag.String("signing-key", "", "dedicated Ed25519 PKCS#8 PEM file (owner-only permissions)")
	source := flag.String("source-route", "", "exact source EdgeRouteBinding JSON (required for shadow and active)")
	peer := flag.String("peer-route", "", "exact peer EdgeRouteBinding JSON (required for shadow and active)")
	allowActive := flag.Bool("allow-active", false, "allow signing an explicitly reviewed active policy")
	flag.Parse()
	if *payload == "" || *keyFile == "" || flag.NArg() != 0 {
		fail(errors.New("-policy and -signing-key are required"))
	}
	raw, err := readFile(*payload, relayroute.MaxPolicyBytes, false)
	if err != nil {
		fail(errors.New("unable to read policy payload"))
	}
	var p relayroute.Policy
	if relayroute.Decode(bytes.NewReader(raw), &p) != nil {
		fail(errors.New("invalid relay policy payload"))
	}
	var a, b model.EdgeRouteBinding
	if p.Mode != "off" {
		if err := readRoute(*source, &a); err != nil {
			fail(fmt.Errorf("source: %w", err))
		}
		if err := readRoute(*peer, &b); err != nil {
			fail(fmt.Errorf("peer: %w", err))
		}
	}
	keyRaw, err := readFile(*keyFile, 4096, true)
	if err != nil {
		fail(errors.New("unable to read owner-only signing key"))
	}
	key, err := parseSigningKey(keyRaw)
	clear(keyRaw)
	if err != nil {
		fail(err)
	}
	out, err := issue(raw, a, b, key, *allowActive, time.Now().UTC())
	clear(key)
	if err != nil {
		fail(err)
	}
	if _, err := os.Stdout.Write(append(out, '\n')); err != nil {
		fail(errors.New("unable to write signed relay policy"))
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "fugue-edge-relay-policy:", err)
	os.Exit(1)
}

func readFile(path string, limit int64, private bool) ([]byte, error) {
	// Check the path before opening to avoid waiting on a named pipe, then
	// recheck the actual descriptor before reading any bytes.
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || (private && info.Mode().Perm()&0077 != 0) {
		return nil, errors.New("invalid file type or permissions")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() || (private && opened.Mode().Perm()&0077 != 0) {
		return nil, errors.New("invalid file type or permissions")
	}
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, errors.New("unreadable or oversized file")
	}
	return raw, nil
}

func parseSigningKey(raw []byte) (ed25519.PrivateKey, error) {
	p, rest := pem.Decode(raw)
	if p != nil && p.Type == "PRIVATE KEY" && len(bytes.TrimSpace(rest)) == 0 {
		defer clear(p.Bytes)
		parsed, err := x509.ParsePKCS8PrivateKey(p.Bytes)
		if key, ok := parsed.(ed25519.PrivateKey); err == nil && ok && len(key) == ed25519.PrivateKeySize {
			return key, nil
		}
	}
	return nil, errors.New("expected one dedicated Ed25519 PKCS#8 signing key")
}

func readRoute(path string, route *model.EdgeRouteBinding) error {
	raw, err := readFile(path, 128<<10, false)
	if err != nil || relayroute.Decode(bytes.NewReader(raw), route) != nil {
		return errors.New("expected exact EdgeRouteBinding JSON")
	}
	return nil
}

func issue(raw []byte, source, peer model.EdgeRouteBinding, key ed25519.PrivateKey, allowActive bool, now time.Time) ([]byte, error) {
	var p relayroute.Policy
	if len(raw) > relayroute.MaxPolicyBytes || len(key) != ed25519.PrivateKeySize || relayroute.Decode(bytes.NewReader(raw), &p) != nil {
		return nil, errors.New("invalid relay policy or signing key")
	}
	if p.Mode == "active" && !allowActive {
		return nil, errors.New("active policy requires -allow-active")
	}
	if p.Mode != "off" {
		if len(p.Bindings) != 1 {
			return nil, errors.New("pilot issuance requires exactly one binding")
		}
		b := p.Bindings[0]
		if !matchRoute(source, b, b.Source, b.SourceDigest) || !matchRoute(peer, b, b.Peer, b.PeerDigest) {
			return nil, errors.New("route witness differs from the declared stable origin and proof")
		}
	}
	envelope, err := json.Marshal(relayroute.SignedPolicy{Payload: raw, Signature: ed25519.Sign(key, raw)})
	if err != nil {
		return nil, err
	}
	if _, err := relayroute.Verify(envelope, key.Public().(ed25519.PublicKey), now); err != nil {
		return nil, err
	}
	return envelope, nil
}

func matchRoute(r model.EdgeRouteBinding, b relayroute.Binding, edgeID, proof string) bool {
	if strings.TrimSuffix(strings.ToLower(strings.TrimSpace(r.Hostname)), ".") != b.Host ||
		r.Status != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(r.RoutePolicy) ||
		slices.Contains(r.ExcludedEdgeIDs, edgeID) || r.TenantID != b.Tenant || r.AppID != b.App ||
		r.UpstreamKind != model.EdgeRouteUpstreamKindKubernetesService || r.UpstreamScope != model.EdgeRouteUpstreamScopeLocalService ||
		r.UpstreamURL != b.Origin || len(r.Upstreams) != 1 {
		return false
	}
	prefix := model.NormalizeAppRoutePathPrefix(r.PathPrefix)
	if prefix != "/" && b.Path != prefix && !strings.HasPrefix(b.Path, prefix+"/") {
		return false
	}
	up := r.Upstreams[0]
	if up.Role != "stable" || up.Weight != 100 || up.ReleaseID != b.Release || up.UpstreamURL != r.UpstreamURL ||
		up.UpstreamKind != r.UpstreamKind || up.UpstreamScope != r.UpstreamScope ||
		(up.Status != "" && up.Status != model.EdgeRouteStatusActive) {
		return false
	}
	digest, err := routeproof.Digest(r)
	return err == nil && digest == proof
}

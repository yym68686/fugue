package relayroute

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const Schema = "fugue.edge.origin-relay/v1"
const MaxPolicyBytes = 128 << 10

type Endpoint struct {
	Edge    string `json:"edge"`
	Slot    string `json:"slot"`
	Address string `json:"address"`
	TLSName string `json:"tls_name"`
}

// Binding pins each side's complete route proof, not just a service alias.
type Binding struct {
	ID           string `json:"id"`
	Source       string `json:"source"`
	Peer         string `json:"peer"`
	Host         string `json:"host"`
	Path         string `json:"path"`
	Tenant       string `json:"tenant"`
	App          string `json:"app"`
	Release      string `json:"release"`
	Origin       string `json:"origin"`
	SourceDigest string `json:"source_digest"`
	PeerDigest   string `json:"peer_digest"`
	MaxBodyBytes int64  `json:"max_body_bytes"`
}

type Policy struct {
	Schema        string     `json:"schema"`
	Generation    uint64     `json:"generation"`
	NotBefore     time.Time  `json:"not_before"`
	Expires       time.Time  `json:"expires"`
	Mode          string     `json:"mode"`
	RelayPercent  int        `json:"relay_percent"`
	MaxConcurrent int        `json:"max_concurrent"`
	Endpoints     []Endpoint `json:"endpoints"`
	Bindings      []Binding  `json:"bindings"`
	Hash          [32]byte   `json:"-"`
}

type SignedPolicy struct {
	Payload   []byte `json:"payload"`
	Signature []byte `json:"signature"`
}

var nodeName = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,126}[a-z0-9])?$`)
var dnsName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$`)
var requestPath = regexp.MustCompile(`^/[a-zA-Z0-9_/-]{1,255}$`)

// ValidEndpoint is also used by the listener bootstrap, which has to bind
// its exact Pod address before a publisher can issue a matching policy.
func ValidEndpoint(e Endpoint) bool {
	_, ok := endpointAddress(e)
	return ok
}

func endpointAddress(e Endpoint) (netip.AddrPort, bool) {
	addr, err := netip.ParseAddrPort(e.Address)
	return addr, err == nil && addr.Addr().IsPrivate() && !addr.Addr().Is4In6() &&
		addr.Addr().Zone() == "" && addr.Port() >= 1024 && nodeName.MatchString(e.Edge) &&
		nodeName.MatchString(e.Slot) && len(e.TLSName) <= 253 && dnsName.MatchString(e.TLSName)
}

func Decode(r io.Reader, out any) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("relay: trailing JSON")
	}
	return nil
}

func Verify(raw []byte, key ed25519.PublicKey, now time.Time) (*Policy, error) {
	if len(key) != ed25519.PublicKeySize || len(raw) > MaxPolicyBytes {
		return nil, errors.New("relay: invalid policy envelope")
	}
	var envelope SignedPolicy
	if Decode(bytes.NewReader(raw), &envelope) != nil || !ed25519.Verify(key, envelope.Payload, envelope.Signature) {
		return nil, errors.New("relay: invalid policy signature")
	}
	var p Policy
	if Decode(bytes.NewReader(envelope.Payload), &p) != nil || p.validate(now) != nil {
		return nil, errors.New("relay: invalid policy")
	}
	p.Hash = sha256.Sum256(envelope.Payload)
	return &p, nil
}

func (p *Policy) Endpoint(edge string) (Endpoint, bool) {
	for _, e := range p.Endpoints {
		if e.Edge == edge {
			return e, true
		}
	}
	return Endpoint{}, false
}

func (p *Policy) Binding(id string) (Binding, bool) {
	for _, b := range p.Bindings {
		if b.ID == id {
			return b, true
		}
	}
	return Binding{}, false
}

func (p *Policy) Current(now time.Time) bool {
	return p != nil && !now.Before(p.NotBefore) && now.Before(p.Expires)
}

func (p *Policy) validate(now time.Time) error {
	bad := errors.New("invalid relay policy")
	if p.Schema != Schema || p.Generation == 0 || p.NotBefore.IsZero() || !p.Current(now) ||
		p.Expires.Sub(p.NotBefore) > 15*time.Minute || (p.Mode != "off" && p.Mode != "shadow" && p.Mode != "active") ||
		(p.Mode != "active" && p.RelayPercent != 0) || (p.Mode == "active" && (p.RelayPercent < 1 || p.RelayPercent > 25)) ||
		p.MaxConcurrent < 1 || p.MaxConcurrent > 32 || len(p.Endpoints) > 16 || len(p.Bindings) > 64 {
		return bad
	}
	nodes, names, addresses := map[string]bool{}, map[string]bool{}, map[netip.AddrPort]bool{}
	for _, e := range p.Endpoints {
		addr, ok := endpointAddress(e)
		if !ok || nodes[e.Edge] || names[e.TLSName] || addresses[addr] {
			return bad
		}
		nodes[e.Edge], names[e.TLSName], addresses[addr] = true, true, true
	}
	ids, pairs, candidates := map[string]bool{}, map[[4]string]bool{}, map[[3]string]int{}
	probeTargets := map[string]map[string]bool{}
	sourceBindings := map[string]int{}
	for _, b := range p.Bindings {
		k := [3]string{b.Source, b.Host, b.Path}
		pair := [4]string{b.Source, b.Host, b.Path, b.Peer}
		if !nodeName.MatchString(b.ID) || ids[b.ID] || !nodes[b.Source] || !nodes[b.Peer] || b.Source == b.Peer ||
			!dnsName.MatchString(b.Host) || len(b.Host) > 253 || !requestPath.MatchString(b.Path) || strings.Contains(b.Path, "//") ||
			!nodeName.MatchString(b.Tenant) || !nodeName.MatchString(b.App) || !nodeName.MatchString(b.Release) ||
			!validOrigin(b.Origin) || !digest(b.SourceDigest) || !digest(b.PeerDigest) ||
			b.MaxBodyBytes < 1 || b.MaxBodyBytes > 16<<20 || pairs[pair] || candidates[k] >= 2 {
			return bad
		}
		ids[b.ID], pairs[pair] = true, true
		candidates[k]++
		sourceBindings[b.Source]++
		if sourceBindings[b.Source] > 16 {
			return bad
		}
		for _, target := range [][2]string{{b.Source, b.SourceDigest}, {b.Peer, b.PeerDigest}} {
			if probeTargets[target[0]] == nil {
				probeTargets[target[0]] = map[string]bool{}
			}
			probeTargets[target[0]][target[1]] = true
			if len(probeTargets[target[0]]) > 8 {
				return bad
			}
		}
	}
	return nil
}

func validOrigin(s string) bool {
	if len(s) > 512 {
		return false
	}
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.User == nil &&
		dnsName.MatchString(u.Hostname()) && strings.HasSuffix(u.Hostname(), ".svc.cluster.local") &&
		u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && (u.Path == "" || u.Path == "/")
}

func digest(s string) bool {
	if len(s) != 71 || !strings.HasPrefix(s, "sha256:") || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s[7:])
	return err == nil
}

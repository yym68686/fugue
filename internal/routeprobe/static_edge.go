package routeprobe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"net/http"
	"net/netip"
	"net/url"
	"time"
)

const StaticEdgeProofKind = "static-edge-dns-v1"
const StaticEdgeHeader = "X-Fugue-Static-Edge"
const StaticRouteHeader = "X-Fugue-Static-Route"
const StaticNonceHeader = "X-Fugue-Static-Nonce"

// ProbeStatic verifies public TLS at the exact IP, bypassing DNS and proxies.
// The independent config echoes a random nonce on the actual app route. Its
// certificate and our bounded observation lease are the only expiry authority;
// it never impersonates a platform route release or prolongs one.
func ProbeStatic(ctx context.Context, requirement platformconfig.DNSReadinessProbe, timeout, freshness time.Duration) (Proof, error) {
	ip, err := netip.ParseAddr(requirement.Address)
	if err != nil || !platformconfig.PublicDNSFlattenIP(ip) || requirement.StaticEdge == nil || requirement.State != "" || requirement.EdgeGroupID != platformconfig.StaticEdgeDNSGroup(requirement.EdgeID) || timeout < time.Second || timeout > 10*time.Second || freshness < time.Second || freshness > 600*time.Second {
		return Proof{}, errors.New("invalid static edge probe requirement")
	}
	return probeStaticWithTransport(ctx, requirement, timeout, freshness, probeTransports.get(requirement.Hostname, requirement.Address))
}

func probeStaticWithTransport(ctx context.Context, p platformconfig.DNSReadinessProbe, timeout, freshness time.Duration, transport http.RoundTripper) (Proof, error) {
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return Proof{}, err
	}
	nonce := hex.EncodeToString(nonceBytes)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	target := url.URL{Scheme: "https", Host: p.Hostname, Path: p.Path}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, target.String(), nil)
	if err != nil {
		return Proof{}, err
	}
	req.Header.Set(StaticNonceHeader, nonce)
	req.Header.Set("Cache-Control", "no-cache")
	client := http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		return Proof{}, classifyTransportError(err)
	}
	defer response.Body.Close()
	now := time.Now().UTC()
	if response.StatusCode != p.StaticEdge.StatusCode || response.TLS == nil || len(response.TLS.VerifiedChains) == 0 {
		return Proof{}, errors.New("static edge route or TLS not ready")
	}
	for name, want := range map[string]string{StaticEdgeHeader: p.EdgeID, StaticRouteHeader: p.RouteDigest, StaticNonceHeader: nonce, "Cache-Control": "no-store"} {
		values := response.Header.Values(name)
		if len(values) != 1 || values[0] != want {
			return Proof{}, errors.New("static edge route identity or nonce mismatch")
		}
	}
	expires := now.Add(freshness)
	for _, certificate := range response.TLS.VerifiedChains[0] {
		if now.Before(certificate.NotBefore) || !certificate.NotAfter.After(now) {
			return Proof{}, errors.New("static edge certificate expired or not yet valid")
		}
		if certificate.NotAfter.Before(expires) {
			expires = certificate.NotAfter
		}
	}
	return Proof{Kind: StaticEdgeProofKind, Version: model.StaticEdgeDNSVersion, Digest: p.RouteDigest, EdgeID: p.EdgeID, GroupID: p.EdgeGroupID, CheckedAt: now, ValidUntil: expires}, nil
}

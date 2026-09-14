package api

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/routeproof"
)

type placementRouteProof struct {
	Digest, Version, EdgeID, GroupID string
	ValidUntil                       time.Time
}

type placementRouteProbe func(context.Context, string, string, string) (placementRouteProof, error)

func probePlacementRoute(ctx context.Context, host, path, address string) (placementRouteProof, error) {
	ip, err := netip.ParseAddr(address)
	if err != nil || !platformconfig.PublicDNSFlattenIP(ip) || host == "" || strings.ContainsAny(host, "/:@?#") || !strings.HasPrefix(path, "/") {
		return placementRouteProof{}, errors.New("invalid placement probe target")
	}
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return placementRouteProof{}, errors.New("placement probe nonce unavailable")
	}
	nonce := hex.EncodeToString(nonceBytes)
	ctx, cancel := context.WithTimeout(ctx, trafficOverrideProbeTimeout)
	defer cancel()
	dialer := &net.Dialer{Timeout: trafficOverrideProbeTimeout}
	transport := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}, MaxResponseHeaderBytes: 16 << 10, ForceAttemptHTTP2: true,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), "443"))
		}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: trafficOverrideProbeTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	target := url.URL{Scheme: "https", Host: host, Path: path}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, target.String(), nil)
	if err != nil {
		return placementRouteProof{}, errors.New("placement probe request invalid")
	}
	req.Header.Set(routeproof.RequestHeader, "1")
	req.Header.Set(routeproof.NonceHeader, nonce)
	response, err := client.Do(req)
	if err != nil {
		return placementRouteProof{}, errors.New("placement TLS probe unavailable")
	}
	defer response.Body.Close()
	proof, err := parsePlacementRouteProof(response, nonce, time.Now().UTC())
	if err != nil {
		return placementRouteProof{}, err
	}
	// A route lease cannot outlive the certificate chain that authenticated it.
	if response.TLS == nil || len(response.TLS.VerifiedChains) == 0 {
		return placementRouteProof{}, errors.New("placement TLS identity not verified")
	}
	for _, certificate := range response.TLS.VerifiedChains[0] {
		if certificate.NotAfter.Before(proof.ValidUntil) {
			proof.ValidUntil = certificate.NotAfter
		}
	}
	return proof, nil
}

func parsePlacementRouteProof(response *http.Response, nonce string, now time.Time) (placementRouteProof, error) {
	fail := errors.New("placement route proof invalid")
	if response.StatusCode != http.StatusNoContent || !routeproof.ValidNonce(nonce) {
		return placementRouteProof{}, fail
	}
	for _, header := range []string{routeproof.DigestHeader, routeproof.NonceHeader, routeproof.VersionHeader, routeproof.ExpiryHeader, routeproof.EdgeHeader, routeproof.GroupHeader} {
		values := response.Header.Values(header)
		if len(values) != 1 || strings.TrimSpace(values[0]) == "" || values[0] != strings.TrimSpace(values[0]) {
			return placementRouteProof{}, fail
		}
	}
	if response.Header.Get(routeproof.NonceHeader) != nonce || response.Header.Get("Cache-Control") != "no-store" {
		return placementRouteProof{}, fail
	}
	digest := response.Header.Get(routeproof.DigestHeader)
	decoded, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
	if err != nil || len(decoded) != 32 || len(digest) != 71 || !strings.HasPrefix(digest, "sha256:") || strings.ToLower(digest) != digest {
		return placementRouteProof{}, fail
	}
	expires, err := time.Parse(time.RFC3339Nano, response.Header.Get(routeproof.ExpiryHeader))
	if err != nil || !expires.After(now) {
		return placementRouteProof{}, fail
	}
	return placementRouteProof{Digest: digest, Version: response.Header.Get(routeproof.VersionHeader), EdgeID: response.Header.Get(routeproof.EdgeHeader), GroupID: response.Header.Get(routeproof.GroupHeader), ValidUntil: expires}, nil
}

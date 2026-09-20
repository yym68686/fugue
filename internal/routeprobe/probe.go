package routeprobe

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeproof"
	"fugue/internal/trafficbinding"
)

// Proof is a newly collected HTTPS observation. ValidUntil is bounded by
// the original serving bundle and the certificate chain, never renewed here.
type Proof struct {
	AppTrafficDigest string                       `json:"app_traffic_digest,omitempty"`
	TrafficRelease   *model.TrafficReleaseBinding `json:"traffic_release,omitempty"`
	Digest           string                       `json:"digest"`
	Version          string                       `json:"version"`
	EdgeID           string                       `json:"edge_id"`
	GroupID          string                       `json:"group_id"`
	State            string                       `json:"state,omitempty"`
	ValidUntil       time.Time                    `json:"valid_until"`
	CheckedAt        time.Time                    `json:"checked_at"`
}

func Probe(ctx context.Context, host, path, address, expectedState string, timeout time.Duration) (Proof, error) {
	if timeout < time.Second || timeout > 10*time.Second {
		return Proof{}, errors.New("invalid route probe timeout")
	}
	if expectedState != "" && expectedState != "disabled" && expectedState != "unavailable" {
		return Proof{}, errors.New("invalid placement probe state")
	}
	ip, err := netip.ParseAddr(address)
	if err != nil || !platformconfig.PublicDNSFlattenIP(ip) || host == "" || strings.ContainsAny(host, "/:@?#") || !strings.HasPrefix(path, "/") {
		return Proof{}, errors.New("invalid placement probe target")
	}
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return Proof{}, errors.New("placement probe nonce unavailable")
	}
	nonce := hex.EncodeToString(nonceBytes)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}, MaxResponseHeaderBytes: 16 << 10, ForceAttemptHTTP2: true,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), "443"))
		}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	target := url.URL{Scheme: "https", Host: host, Path: path}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, target.String(), nil)
	if err != nil {
		return Proof{}, errors.New("placement probe request invalid")
	}
	req.Header.Set(routeproof.RequestHeader, "1")
	req.Header.Set(routeproof.NonceHeader, nonce)
	if expectedState != "" {
		req.Header.Set(routeproof.StateHeader, expectedState)
	}
	response, err := client.Do(req)
	if err != nil {
		return Proof{}, errors.New("placement TLS probe unavailable")
	}
	defer response.Body.Close()
	checkedAt := time.Now().UTC()
	proof, err := ParseResponse(response, nonce, checkedAt)
	if err != nil {
		return Proof{}, err
	}
	if proof.State != expectedState {
		return Proof{}, errors.New("placement route state proof mismatch")
	}
	// A route lease cannot outlive the certificate chain that authenticated it.
	if response.TLS == nil || len(response.TLS.VerifiedChains) == 0 {
		return Proof{}, errors.New("placement TLS identity not verified")
	}
	for _, certificate := range response.TLS.VerifiedChains[0] {
		if certificate.NotAfter.Before(proof.ValidUntil) {
			proof.ValidUntil = certificate.NotAfter
		}
	}
	proof.CheckedAt = checkedAt
	return proof, nil
}

func ParseResponse(response *http.Response, nonce string, now time.Time) (Proof, error) {
	fail := errors.New("placement route proof invalid")
	if response.StatusCode != http.StatusNoContent || !routeproof.ValidNonce(nonce) {
		return Proof{}, fail
	}
	for _, header := range []string{routeproof.DigestHeader, routeproof.NonceHeader, routeproof.VersionHeader, routeproof.ExpiryHeader, routeproof.EdgeHeader, routeproof.GroupHeader} {
		values := response.Header.Values(header)
		if len(values) != 1 || strings.TrimSpace(values[0]) == "" || values[0] != strings.TrimSpace(values[0]) {
			return Proof{}, fail
		}
	}
	if response.Header.Get(routeproof.NonceHeader) != nonce || response.Header.Get("Cache-Control") != "no-store" {
		return Proof{}, fail
	}
	state := response.Header.Get(routeproof.StateHeader)
	if len(response.Header.Values(routeproof.StateHeader)) > 1 || (len(response.Header.Values(routeproof.StateHeader)) == 1 && state != "disabled" && state != "unavailable") {
		return Proof{}, fail
	}
	digest := response.Header.Get(routeproof.DigestHeader)
	decoded, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
	if err != nil || len(decoded) != 32 || len(digest) != 71 || !strings.HasPrefix(digest, "sha256:") || strings.ToLower(digest) != digest {
		return Proof{}, fail
	}
	expires, err := time.Parse(time.RFC3339Nano, response.Header.Get(routeproof.ExpiryHeader))
	if err != nil || !expires.After(now) {
		return Proof{}, fail
	}
	var binding *model.TrafficReleaseBinding
	appTraffic := response.Header.Get(routeproof.AppTrafficHeader)
	if values, ok := response.Header[http.CanonicalHeaderKey(routeproof.AppTrafficHeader)]; ok {
		decoded, err := hex.DecodeString(strings.TrimPrefix(appTraffic, "sha256:"))
		if len(values) != 1 || err != nil || len(decoded) != 32 || len(appTraffic) != 71 || !strings.HasPrefix(appTraffic, "sha256:") || strings.ToLower(appTraffic) != appTraffic || state != "" {
			return Proof{}, fail
		}
	}
	if values, ok := response.Header[http.CanonicalHeaderKey(routeproof.TrafficHeader)]; ok {
		if len(values) != 1 || len(values[0]) > 8192 {
			return Proof{}, fail
		}
		raw, err := base64.RawURLEncoding.DecodeString(values[0])
		if err != nil {
			return Proof{}, fail
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if dec.Decode(&binding) != nil || binding == nil || dec.Decode(&struct{}{}) != io.EOF || trafficbinding.ValidateGroup(binding, response.Header.Get(routeproof.GroupHeader), true) != nil {
			return Proof{}, fail
		}
	}
	return Proof{AppTrafficDigest: appTraffic, TrafficRelease: binding, Digest: digest, Version: response.Header.Get(routeproof.VersionHeader), EdgeID: response.Header.Get(routeproof.EdgeHeader), GroupID: response.Header.Get(routeproof.GroupHeader), ValidUntil: expires, State: state}, nil
}

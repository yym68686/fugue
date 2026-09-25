package entryfailover

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type ProbeCheck struct {
	Hostname            string        `json:"hostname"`
	Path                string        `json:"path"`
	Address             string        `json:"address"`
	Status              int           `json:"status"`
	Duration            time.Duration `json:"duration_ns"`
	EdgeID              string        `json:"edge_id,omitempty"`
	CertificateNotAfter time.Time     `json:"certificate_not_after,omitempty"`
	Error               string        `json:"error,omitempty"`
}

type ProbeResult struct {
	TargetID string       `json:"target_id"`
	At       time.Time    `json:"at"`
	Healthy  bool         `json:"healthy"`
	Checks   []ProbeCheck `json:"checks"`
	Error    string       `json:"error,omitempty"`
}

type Prober struct {
	Resolver *net.Resolver
	RootCAs  *x509.CertPool // nil uses the system trust store
	Dial     func(context.Context, string, string) (net.Conn, error)
}

func (p Prober) addresses(ctx context.Context, policy Policy, target Target) ([]string, error) {
	if target.Kind == "static-ip" {
		return []string{target.Address}, nil
	}
	resolver := p.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	answer, err := resolver.LookupIPAddr(ctx, target.Address)
	if err != nil {
		return nil, fmt.Errorf("resolve canonical Fugue target: %w", err)
	}
	if len(answer) == 0 || len(answer) > 8 {
		return nil, errors.New("canonical target requires 1-8 IP answers")
	}
	static := map[string]bool{}
	for _, t := range policy.Targets {
		if t.Kind == "static-ip" {
			static[t.Address] = true
		}
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(answer))
	for _, a := range answer {
		ip := a.IP
		if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() || static[ip.String()] {
			return nil, errors.New("canonical target resolved to an unsafe or static-edge address")
		}
		if !seen[ip.String()] {
			out = append(out, ip.String())
			seen[ip.String()] = true
		}
	}
	sort.Strings(out)
	return out, nil
}

func (p Prober) Probe(ctx context.Context, policy Policy, target Target) ProbeResult {
	result := ProbeResult{TargetID: target.ID, At: time.Now().UTC(), Healthy: false}
	if err := policy.Validate(); err != nil {
		result.Error = err.Error()
		return result
	}
	known := false
	for _, t := range policy.Targets {
		known = known || t == target
	}
	if !known {
		result.Error = "target is outside policy"
		return result
	}
	addresses, err := p.addresses(ctx, policy, target)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if len(addresses)*len(policy.Checks) > 64 {
		result.Error = "probe exceeds 64 check-address pairs"
		return result
	}
	result.Healthy = true
	for _, address := range addresses {
		for _, check := range policy.Checks {
			item := p.check(ctx, policy.ProbeTimeout(), time.Duration(policy.MinTLSValidityHours)*time.Hour, address, check, target)
			result.Checks = append(result.Checks, item)
			if item.Error != "" {
				result.Healthy = false
			}
		}
	}
	return result
}

func (p Prober) check(ctx context.Context, timeout, minValidity time.Duration, ip string, check Check, target Target) (result ProbeCheck) {
	result = ProbeCheck{Hostname: check.Hostname, Path: check.Path, Address: ip}
	started := time.Now()
	defer func() { result.Duration = time.Since(started) }()
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dial := p.Dial
	if dial == nil {
		dial = (&net.Dialer{Timeout: timeout}).DialContext
	}
	transport := &http.Transport{
		Proxy: nil, ForceAttemptHTTP2: false, DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dial(ctx, "tcp", net.JoinHostPort(ip, "443"))
		},
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12, ServerName: check.Hostname, RootCAs: p.RootCAs},
		TLSHandshakeTimeout: timeout, ResponseHeaderTimeout: timeout, MaxResponseHeaderBytes: 16 << 10,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	u := &url.URL{Scheme: "https", Host: check.Hostname, Path: check.Path}
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	resp, err := client.Do(req)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer resp.Body.Close()
	result.Status = resp.StatusCode
	if resp.TLS == nil || len(resp.TLS.PeerCertificates) == 0 {
		result.Error = "TLS certificate evidence missing"
		return result
	}
	result.CertificateNotAfter = resp.TLS.PeerCertificates[0].NotAfter.UTC()
	if time.Until(result.CertificateNotAfter) < minValidity {
		result.Error = "TLS certificate expires before required standby window"
	}
	result.EdgeID = strings.TrimSpace(resp.Header.Get("X-Fugue-Static-Edge"))
	if result.Status != check.Status {
		result.Error = fmt.Sprintf("HTTP %d, expected %d", result.Status, check.Status)
	}
	if target.Kind == "static-ip" && result.EdgeID != target.EdgeID {
		result.Error = "static edge identity mismatch"
	}
	return result
}

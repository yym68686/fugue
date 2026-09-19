package edge

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/proxyproto"
	"fugue/internal/routeartifact"
)

type TLSReadinessStatus struct {
	ReceiptDigest string    `json:"receipt_digest"`
	PolicyDigest  string    `json:"policy_digest"`
	BundleVersion string    `json:"bundle_version"`
	Probes        int       `json:"probes"`
	ReadyProbes   int       `json:"ready_probes"`
	FailedProbes  int       `json:"failed_probes"`
	CheckedAt     time.Time `json:"checked_at"`
	FreshUntil    time.Time `json:"fresh_until"`
	Serving       bool      `json:"serving"`
}

type platformTLSFact struct {
	Hostname          string    `json:"hostname"`
	Ready             bool      `json:"ready"`
	Reason            string    `json:"reason,omitempty"`
	CertificateDigest string    `json:"certificate_digest,omitempty"`
	NotBefore         time.Time `json:"not_before"`
	NotAfter          time.Time `json:"not_after"`
	TrustValidUntil   time.Time `json:"trust_valid_until"`
	CheckedAt         time.Time `json:"checked_at"`
	ValidUntil        time.Time `json:"valid_until"`
}

type platformTLSReadinessReceipt struct {
	Schema           string                           `json:"schema"`
	Assignment       model.PlatformConsumerAssignment `json:"assignment"`
	RouteArtifactID  string                           `json:"route_artifact_id"`
	RouteDigest      string                           `json:"route_digest"`
	NodeID           string                           `json:"node_id"`
	EdgeGroupID      string                           `json:"edge_group_id"`
	PolicyDigest     string                           `json:"policy_digest"`
	BundleVersion    string                           `json:"bundle_version"`
	BundleValidUntil time.Time                        `json:"bundle_valid_until"`
	CheckedAt        time.Time                        `json:"checked_at"`
	Facts            []platformTLSFact                `json:"facts"`
	ReceiptDigest    string                           `json:"receipt_digest,omitempty"`
}

type platformTLSCertificate struct {
	Leaf       *x509.Certificate
	ValidUntil time.Time
}

type platformTLSProbe func(context.Context, string, string, bool, time.Duration) (*platformTLSCertificate, error)

func localTLSProbeAddress(raw string) (string, error) {
	host, port, err := net.SplitHostPort(caddyProxyDialAddress(raw))
	if err != nil {
		return "", errors.New("TLS readiness listener is invalid")
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsUnspecified() {
		if ip.To4() != nil {
			host = "127.0.0.1"
		} else {
			host = "::1"
		}
		ip = net.ParseIP(host)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 || ip == nil || !ip.IsLoopback() {
		return "", errors.New("TLS readiness requires a local listener")
	}
	return net.JoinHostPort(host, port), nil
}

func probePlatformTLS(ctx context.Context, address, hostname string, proxy bool, timeout time.Duration) (*platformTLSCertificate, error) {
	return probePlatformTLSWithRoots(ctx, address, hostname, proxy, timeout, nil)
}

// Production always uses system trust roots and hostname verification. The
// optional pool exists only to exercise the real transport with a test CA.
func probePlatformTLSWithRoots(ctx context.Context, address, hostname string, proxy bool, timeout time.Duration, roots *x509.CertPool) (*platformTLSCertificate, error) {
	address, err := localTLSProbeAddress(address)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	defer raw.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err = raw.SetDeadline(deadline); err != nil {
			return nil, err
		}
	}
	if proxy {
		if _, err = io.WriteString(raw, proxyproto.HeaderV1(raw.LocalAddr(), raw.RemoteAddr())); err != nil {
			return nil, err
		}
	}
	conn := tls.Client(raw, &tls.Config{MinVersion: tls.VersionTLS12, ServerName: hostname, RootCAs: roots})
	if err = conn.HandshakeContext(ctx); err != nil {
		return nil, err
	}
	state := conn.ConnectionState()
	if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
		return nil, errors.New("TLS readiness has no verified certificate")
	}
	// Any verified chain can establish trust. Within a chain every
	// certificate must remain valid for the observation's entire lease.
	var trustedUntil time.Time
	for _, chain := range state.VerifiedChains {
		until := state.PeerCertificates[0].NotAfter
		for _, cert := range chain {
			if cert.NotAfter.Before(until) {
				until = cert.NotAfter
			}
		}
		if until.After(trustedUntil) {
			trustedUntil = until
		}
	}
	return &platformTLSCertificate{Leaf: state.PeerCertificates[0], ValidUntil: trustedUntil.UTC()}, nil
}

// Platform certificates may have no tenant owner themselves (e.g. a wildcard).
// The same signed route artifact supplies the exact path/owner authorization.
func (s *Service) tlsReadinessHostAuthorized(ref platformconfig.TLSIntent, expected []model.EdgeRouteBinding, bundle model.EdgeRouteBundle) bool {
	matched := 0
	for _, want := range expected {
		if want.Hostname != ref.Hostname || want.TLSPolicy != ref.Policy {
			continue
		}
		if ref.Policy == model.EdgeRouteTLSPolicyCustomDomain && (ref.AppID != want.AppID || ref.TenantID != want.TenantID) {
			return false
		}
		found := false
		for _, r := range bundle.Routes {
			if r.Hostname != want.Hostname || model.NormalizeAppRoutePathPrefix(r.PathPrefix) != model.NormalizeAppRoutePathPrefix(want.PathPrefix) {
				continue
			}
			if r.AppID != want.AppID || r.TenantID != want.TenantID || r.TLSPolicy != want.TLSPolicy {
				return false
			}
			if !s.routeWarmupAllowedForThisEdge(r) || !model.EdgeRoutePolicyAllowsTraffic(r.RoutePolicy) {
				continue
			}
			found = true
		}
		if !found {
			return false
		}
		matched++
	}
	if ref.Policy == model.EdgeRouteTLSPolicyCustomDomain {
		count := 0
		for _, a := range bundle.TLSAllowlist {
			if a.Hostname != ref.Hostname {
				continue
			}
			if a.AppID != ref.AppID || a.TenantID != ref.TenantID || a.Status != model.AppDomainStatusVerified {
				return false
			}
			count++
		}
		if count != 1 {
			return false
		}
	}
	return matched > 0
}

func (s *Service) observePlatformTLSReadiness(ctx context.Context, candidate edgePlatformCandidate, route model.PlatformArtifact, payload platformTLSCandidatePayload) (*platformTLSReadinessReceipt, error) {
	return s.observePlatformTLSReadinessWithProbe(ctx, candidate, route, payload, probePlatformTLS)
}

func (s *Service) observePlatformTLSReadinessWithProbe(ctx context.Context, candidate edgePlatformCandidate, route model.PlatformArtifact, payload platformTLSCandidatePayload, probe platformTLSProbe) (*platformTLSReadinessReceipt, error) {
	policy := payload.Policy.TLSReadiness
	if policy == nil {
		return nil, nil
	}
	if err := platformconfig.ValidateReadinessProbePolicy(policy); err != nil {
		return nil, err
	}
	if len(payload.Certificates) > policy.MaxProbes {
		return nil, errors.New("TLS readiness exceeds probe limit")
	}
	if !s.Config.CaddyEnabled || s.normalizedCaddyTLSMode() == caddyTLSModeOff {
		return nil, errors.New("TLS readiness requires TLS-enabled Caddy")
	}
	address, err := localTLSProbeAddress(s.Config.CaddyListenAddr)
	if err != nil {
		return nil, err
	}
	bundle, ok := s.Bundle()
	if !ok || bundle.Version == "" || !bundle.ValidUntil.After(time.Now()) {
		return nil, errors.New("TLS readiness requires an unexpired serving bundle")
	}
	status := s.Status()
	if status.CaddyAppliedVersion != bundle.Version {
		return nil, errors.New("TLS readiness Caddy bundle is not current")
	}
	policyDigest, err := platformconfig.Digest(policy)
	if err != nil {
		return nil, err
	}
	receipt := &platformTLSReadinessReceipt{Schema: "fugue.edge.tls-readiness/v1", Assignment: candidate.Assignment, RouteArtifactID: route.ID, RouteDigest: route.ContentHash, NodeID: s.Config.EdgeID, EdgeGroupID: s.Config.EdgeGroupID, PolicyDigest: policyDigest, BundleVersion: bundle.Version, BundleValidUntil: bundle.ValidUntil}
	s.mu.Lock()
	previous := s.platformTLSReadiness
	s.mu.Unlock()
	now := time.Now().UTC()
	if previous != nil && reflect.DeepEqual(previous.Assignment, receipt.Assignment) && previous.RouteArtifactID == receipt.RouteArtifactID && previous.RouteDigest == receipt.RouteDigest && previous.NodeID == receipt.NodeID && previous.EdgeGroupID == receipt.EdgeGroupID && previous.PolicyDigest == policyDigest && previous.BundleVersion == bundle.Version && previous.BundleValidUntil.Equal(bundle.ValidUntil) && !previous.CheckedAt.After(now) && now.Sub(previous.CheckedAt) < time.Duration(policy.ProbeIntervalSeconds)*time.Second {
		return previous, nil
	}
	candidateBundle, err := routeartifact.MaterializeForGroup(route, s.Config.EdgeGroupID)
	if err != nil {
		return nil, err
	}
	localHosts := make(map[string]bool, len(candidateBundle.Routes))
	for _, r := range candidateBundle.Routes {
		localHosts[r.Hostname] = true
	}
	refs := make([]platformconfig.TLSIntent, 0, len(payload.Certificates))
	for _, ref := range payload.Certificates {
		if localHosts[ref.Hostname] {
			refs = append(refs, ref)
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Hostname < refs[j].Hostname })
	facts := make([]platformTLSFact, len(refs))
	jobs := make(chan int)
	var workers sync.WaitGroup
	for n := 0; n < min(policy.MaxConcurrency, len(refs)); n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				ref := refs[i]
				fact := platformTLSFact{Hostname: ref.Hostname, CheckedAt: time.Now().UTC()}
				if !s.tlsReadinessHostAuthorized(ref, candidateBundle.Routes, bundle) {
					fact.Reason = "hostname_not_authorized_by_serving_bundle"
					facts[i] = fact
					continue
				}
				observation, probeErr := probe(ctx, address, ref.Hostname, s.Config.CaddyProxyProtocolEnabled, time.Duration(policy.ProbeTimeoutSeconds)*time.Second)
				fact.CheckedAt = time.Now().UTC()
				if probeErr != nil || observation == nil || observation.Leaf == nil {
					fact.Reason = "tls_handshake_failed"
					facts[i] = fact
					continue
				}
				cert := observation.Leaf
				if cert.VerifyHostname(ref.Hostname) != nil || fact.CheckedAt.Before(cert.NotBefore) || !fact.CheckedAt.Before(cert.NotAfter) || !fact.CheckedAt.Before(observation.ValidUntil) {
					fact.Reason = "certificate_invalid"
					facts[i] = fact
					continue
				}
				digest := sha256.Sum256(cert.Raw)
				fact.CertificateDigest = "sha256:" + hex.EncodeToString(digest[:])
				fact.NotBefore = cert.NotBefore.UTC()
				fact.NotAfter = cert.NotAfter.UTC()
				fact.TrustValidUntil = observation.ValidUntil.UTC()
				fact.ValidUntil = fact.CheckedAt.Add(time.Duration(policy.FactFreshnessSeconds) * time.Second)
				if observation.ValidUntil.Before(fact.ValidUntil) {
					fact.ValidUntil = observation.ValidUntil.UTC()
				}
				if bundle.ValidUntil.Before(fact.ValidUntil) {
					fact.ValidUntil = bundle.ValidUntil
				}
				fact.Ready = fact.ValidUntil.After(fact.CheckedAt)
				if !fact.Ready {
					fact.Reason = "serving_bundle_expired"
				}
				facts[i] = fact
			}
		}()
	}
	for i := range refs {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	current, ok := s.Bundle()
	if !ok || current.Version != bundle.Version || !current.ValidUntil.Equal(bundle.ValidUntil) || s.Status().CaddyAppliedVersion != bundle.Version {
		return nil, errors.New("TLS readiness serving bundle changed")
	}
	receipt.Facts, receipt.CheckedAt = facts, time.Now().UTC()
	receipt.ReceiptDigest, err = platformconfig.Digest(receipt)
	if err != nil {
		return nil, fmt.Errorf("digest TLS readiness: %w", err)
	}
	return receipt, nil
}

func summarizePlatformTLSReadiness(receipt *platformTLSReadinessReceipt, bundleVersion string, now time.Time) *TLSReadinessStatus {
	if receipt == nil {
		return nil
	}
	status := &TLSReadinessStatus{ReceiptDigest: receipt.ReceiptDigest, PolicyDigest: receipt.PolicyDigest, BundleVersion: receipt.BundleVersion, CheckedAt: receipt.CheckedAt, Probes: len(receipt.Facts)}
	for _, f := range receipt.Facts {
		if receipt.BundleVersion != bundleVersion || !receipt.BundleValidUntil.After(now) || !f.Ready || f.CheckedAt.After(now) || !f.ValidUntil.After(now) || f.ValidUntil.After(receipt.BundleValidUntil) || f.ValidUntil.After(f.NotAfter) || f.ValidUntil.After(f.TrustValidUntil) || f.CheckedAt.Before(f.NotBefore) || f.CertificateDigest == "" {
			continue
		}
		status.ReadyProbes++
		if status.FreshUntil.IsZero() || f.ValidUntil.Before(status.FreshUntil) {
			status.FreshUntil = f.ValidUntil
		}
	}
	status.FailedProbes = status.Probes - status.ReadyProbes
	return status
}

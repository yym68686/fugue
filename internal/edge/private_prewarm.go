package edge

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fugue/internal/model"
)

func (s *Service) readVerifiedPublication(resp *http.Response, sel routeSourceSelection, now time.Time) (b model.EdgeRouteBundle, p routePublicationMetadata, err error) {
	if err = json.NewDecoder(resp.Body).Decode(&b); err != nil {
		return b, p, fmt.Errorf("decode edge route bundle: %w", err)
	}
	if err = s.verifyBundle(b, now); err != nil {
		return b, p, fmt.Errorf("verify edge route bundle: %w", err)
	}
	p, err = routePublicationFromResponse(resp.Header, b, s.Config.EdgeGroupID)
	if err == nil && sel.candidate {
		p, err = bindCandidatePublication(resp.Header, p, s.Config.EdgeSlot)
	}
	if err == nil {
		err = validateRouteBundleActivationBinding(sel, p)
	}
	return
}

// PrivatePrewarmResult is process-local evidence, not a serving receipt.
// The Job cannot select a domain, publish inventory or install certificates
// on the serving Worker. Its own immutable certificate cache is disposable.
type PrivatePrewarmResult struct {
	Node        string            `json:"node"`
	Source      string            `json:"worker_source"`
	Image       string            `json:"worker_image_digest"`
	Predecessor string            `json:"predecessor"`
	Bundle      string            `json:"signed_publication"`
	TLS         map[string]string `json:"verified_loopback_tls_leaf_sha256"`
}

// CheckPrivatePrewarm intentionally does not call Run, LoadCache, apply any
// desired state or start the Caddy/tenant warmup and reporter loops.
func (s *Service) CheckPrivatePrewarm(ctx context.Context, hosts []string) (*PrivatePrewarmResult, error) {
	if len(hosts) == 0 || len(hosts) > 16 || !s.Config.CaddyEnabled || s.normalizedCaddyTLSMode() != caddyTLSModeSharedOnly ||
		!edgeProxyListenAddrIsLoopback(s.Config.CaddyListenAddr) || !edgeProxyListenAddrIsLoopback(s.Config.CaddyProxyListenAddr) ||
		s.Config.CaddyHTTPListenAddr != "" || s.Config.CaddyProxyProtocolEnabled || s.Config.EdgeToken != "" ||
		s.Config.EdgeDesiredStateURL != "" || s.InventoryProducer.enabled() || s.PlatformTokenFile != "" || s.Config.HostedExecutorAdmission {
		return nil, errors.New("private check requires isolated loopback TLS and no node write identity")
	}
	admin, err := s.normalizedCaddyAdminURL()
	if err != nil || !edgeProxyListenAddrIsLoopback(admin.Host) {
		return nil, errors.New("private check requires a loopback Caddy admin endpoint")
	}
	req, sel, err := s.newRoutesRequestWithSelection(ctx)
	if err != nil {
		return nil, err
	}
	if !sel.privatePrewarm || !sel.reporterInactive || sel.candidate || sel.preparation != nil || req.Header.Get("If-None-Match") != "" {
		return nil, errors.New("private check requires a fresh, unselected physical Worker")
	}
	resp, err := s.firstPrivatePublication(ctx, req, sel, 40*time.Second, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("private current read: HTTP %d", resp.StatusCode)
	}
	etag := resp.Header.Get("ETag")
	if digest, err := strconv.Unquote(etag); err != nil || !edgeRouteDigestPattern.MatchString(digest) || strconv.Quote(digest) != etag {
		return nil, errors.New("private current read lacks a strong publication digest")
	}
	b, p, err := s.readVerifiedPublication(resp, sel, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	wire, err := json.Marshal(b)
	if err != nil || etag != strconv.Quote(fmt.Sprintf("sha256:%x", sha256.Sum256(wire))) {
		return nil, errors.New("private publication digest does not match the signed bytes")
	}
	if err = s.validateRouteBundleSourceSelection(sel); err != nil {
		return nil, err
	}
	if _, err = s.applyCaddyConfigOnly(ctx, b); err != nil {
		return nil, err
	}
	out := &PrivatePrewarmResult{Node: s.Config.EdgeID, Source: s.Config.EdgeReleaseEpoch, Image: s.physicalImageDigest(), Predecessor: *s.RouteBundleSource.PrewarmPredecessor, Bundle: b.Version, TLS: map[string]string{}}
	index := buildEdgeRouteIndex(b, s.Config.EdgeGroupID, p)
	for _, h := range hosts {
		route, ok, fallback, _, _ := index.routeForRequest(h, "/")
		if !ok || fallback || route.Status != model.EdgeRouteStatusActive || h != route.Hostname || strings.ContainsAny(h, "*:/") || out.TLS[h] != "" {
			return nil, fmt.Errorf("private check has no unique active root for %q", h)
		}
		dial := tls.Dialer{Config: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: h, RootCAs: s.caddyImportedTLSRoots}}
		conn, err := dial.DialContext(ctx, "tcp", s.Config.CaddyListenAddr)
		if err != nil {
			return nil, fmt.Errorf("private TLS for %s: %w", h, err)
		}
		state := conn.(*tls.Conn).ConnectionState()
		_ = conn.Close()
		out.TLS[h] = fmt.Sprintf("%x", sha256.Sum256(state.PeerCertificates[0].Raw))
	}
	// Verify the group did not replace this publication during local apply
	// and handshakes. A 304 is usable only with the original signed facts.
	check := req.Clone(ctx)
	check.Header.Set("If-None-Match", etag)
	last, err := s.RouteBundleHTTPClient.Do(check)
	if err != nil {
		return nil, err
	}
	defer last.Body.Close()
	current, err := routePublicationFromResponse(last.Header, b, s.Config.EdgeGroupID)
	if last.StatusCode != http.StatusNotModified || last.Header.Get("ETag") != etag || err != nil || current != p {
		return nil, errors.New("private group publication changed during validation")
	}
	if err = s.verifyBundle(b, time.Now().UTC()); err != nil {
		return nil, err
	}
	if err = s.validateRouteBundleSourceSelection(sel); err != nil {
		return nil, err
	}
	return out, nil
}

// New Pods can resolve the authority Service before a TCP connection is
// accepted. Retry only connection establishment, before receiving any HTTP
// evidence. Always re-read the physical predecessor; never retry a rejected
// credential, signature, publication, redirect, TLS handshake or final CAS.
func (s *Service) firstPrivatePublication(ctx context.Context, req *http.Request, sel routeSourceSelection, window, delay time.Duration) (*http.Response, error) {
	deadline := time.Now().Add(window)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := s.validateRouteBundleSourceSelection(sel); err != nil {
			return nil, err
		}
		r, err := s.RouteBundleHTTPClient.Do(req.Clone(ctx))
		if err == nil {
			return r, nil
		}
		var op *net.OpError
		if !sel.privatePrewarm || !errors.As(err, &op) || op.Op != "dial" ||
			!(errors.Is(op, syscall.ECONNREFUSED) || errors.Is(op, syscall.EHOSTUNREACH) || errors.Is(op, syscall.ENETUNREACH) || op.Timeout()) ||
			time.Until(deadline) <= delay {
			return nil, err
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
		delay = min(2*delay, 5*time.Second)
	}
}

package edge

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeartifact"
)

type tlsProbeBufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c tlsProbeBufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func TestTLSReadinessTransportVerifiesCertificateAndProxyHeader(t *testing.T) {
	for _, scenario := range []string{"trusted", "proxy", "untrusted", "wrong hostname", "expired", "future"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now()
			before, after := now.Add(-time.Hour), now.Add(time.Hour)
			if scenario == "expired" {
				before, after = now.Add(-2*time.Hour), now.Add(-time.Hour)
			}
			if scenario == "future" {
				before, after = now.Add(time.Hour), now.Add(2*time.Hour)
			}
			cert, key := testCaddyTLSKeyPairAt(t, "tls.example.test", before, after)
			pair, err := tls.X509KeyPair([]byte(cert), []byte(key))
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			roots.AppendCertsFromPEM([]byte(cert))
			if scenario == "untrusted" {
				roots = x509.NewCertPool()
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan error, 1)
			go func() {
				raw, e := listener.Accept()
				if e != nil {
					done <- e
					return
				}
				defer raw.Close()
				raw.SetDeadline(time.Now().Add(2 * time.Second))
				var conn net.Conn = raw
				if scenario == "proxy" {
					reader := bufio.NewReader(raw)
					header, e := reader.ReadString('\n')
					if e != nil {
						done <- e
						return
					}
					if !strings.HasPrefix(header, "PROXY TCP4 ") {
						done <- errors.New("missing PROXY header")
						return
					}
					conn = tlsProbeBufferedConn{Conn: raw, reader: reader}
				}
				server := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12})
				done <- server.Handshake()
			}()
			hostname := "tls.example.test"
			if scenario == "wrong hostname" {
				hostname = "other.example.test"
			}
			leaf, err := probePlatformTLSWithRoots(context.Background(), listener.Addr().String(), hostname, scenario == "proxy", time.Second, roots)
			expected := scenario == "trusted" || scenario == "proxy"
			if (err == nil) != expected || (leaf != nil) != expected {
				t.Fatalf("certificate verification mismatch: %v", err)
			}
			serverErr := <-done
			if expected && serverErr != nil {
				t.Fatal(serverErr)
			}
		})
	}
	for _, address := range []string{"example.test:443", "192.0.2.1:443", "127.0.0.1:0", "127.0.0.1:65536"} {
		if _, err := localTLSProbeAddress(address); err == nil {
			t.Fatal("nonlocal or invalid target accepted", address)
		}
	}
	if address, err := localTLSProbeAddress("0.0.0.0:8443"); err != nil || address != "127.0.0.1:8443" {
		t.Fatal(address, err)
	}
}

func tlsReadinessFixture(t *testing.T) (*Service, edgePlatformCandidate, model.PlatformArtifact, platformTLSCandidatePayload, *x509.Certificate) {
	t.Helper()
	candidate, route := tlsShadowFixtures(t)
	s := NewService(config.EdgeConfig{EdgeID: "node-a", EdgeGroupID: "edge-group-test", BundleSigningKey: "synthetic-tls-key", BundleSigningKeyID: "signer", CaddyEnabled: true, CaddyTLSMode: caddyTLSModePublicOnDemand, CaddyListenAddr: "127.0.0.1:8443"}, log.New(io.Discard, "", 0))
	payload, err := s.verifyPlatformTLSCandidate(candidate, route)
	if err != nil {
		t.Fatal(err)
	}
	payload.Policy.TLSReadiness = &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 1, FactFreshnessSeconds: 120, MaxConcurrency: 2, MaxProbes: 100}
	bundle, err := routeartifact.MaterializeForGroup(route.Artifact, "edge-group-test")
	if err != nil {
		t.Fatal(err)
	}
	bundle.ValidUntil = time.Now().Add(time.Minute)
	s.recordSyncSuccess(bundle, "", time.Now(), false)
	s.recordCaddyApply(bundle.Version, len(bundle.Routes), "config", nil)
	cert, key := testCaddyTLSKeyPair(t, payload.Certificates[0].Hostname)
	pair, err := tls.X509KeyPair([]byte(cert), []byte(key))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return s, candidate, route.Artifact, payload, leaf
}

func TestTLSReadinessFreshnessBindingAndRestart(t *testing.T) {
	s, candidate, route, payload, leaf := tlsReadinessFixture(t)
	calls := 0
	probe := func(context.Context, string, string, bool, time.Duration) (*platformTLSCertificate, error) {
		calls++
		return &platformTLSCertificate{Leaf: leaf, ValidUntil: leaf.NotAfter}, nil
	}
	receipt, err := s.observePlatformTLSReadinessWithProbe(context.Background(), candidate, route, payload, probe)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(receipt.Facts) != 1 || !receipt.Facts[0].Ready || receipt.Facts[0].ValidUntil.After(receipt.BundleValidUntil) {
		t.Fatal("unbound TLS fact", receipt)
	}
	s.platformTLSReadiness = receipt
	s.platformTLSCandidate.State = "shadow_verified"
	original, _ := json.Marshal(receipt)
	reused, err := s.observePlatformTLSReadinessWithProbe(context.Background(), candidate, route, payload, probe)
	if err != nil || reused != receipt || calls != 1 {
		t.Fatal("fresh in-process observation was not reused", err)
	}
	current := s.Status().PlatformTLSCandidate
	if !current.TLSVerified || current.Serving || current.Readiness.Serving || current.Readiness.ReadyProbes != 1 {
		t.Fatal("fresh TLS evidence misreported", current)
	}
	expired := summarizePlatformTLSReadiness(receipt, receipt.BundleVersion, receipt.BundleValidUntil)
	if expired.ReadyProbes != 0 || expired.FailedProbes != 1 {
		t.Fatal("expired evidence stayed ready")
	}
	changed := summarizePlatformTLSReadiness(receipt, "other", time.Now())
	if changed.ReadyProbes != 0 {
		t.Fatal("other serving bundle reused facts")
	}
	after, _ := json.Marshal(receipt)
	if string(original) != string(after) {
		t.Fatal("summary renewed original evidence")
	}
	candidate.Assignment.FencingToken++
	if _, err = s.observePlatformTLSReadinessWithProbe(context.Background(), candidate, route, payload, probe); err != nil || calls != 2 {
		t.Fatal("new assignment reused old observation", err)
	}
	// A restarted process has no in-memory receipt, even if a saved candidate
	// file contains old observations. It must perform a real handshake again.
	s.platformTLSReadiness = nil
	if _, err = s.observePlatformTLSReadinessWithProbe(context.Background(), candidate, route, payload, probe); err != nil || calls != 3 {
		t.Fatal("restart skipped observation", err)
	}
}

func TestTLSReadinessFailsClosedWithoutChangingServing(t *testing.T) {
	for _, scenario := range []string{"untrusted TLS", "foreign tenant", "missing allowlist", "different bundle", "expired bundle", "cancelled", "invalid policy"} {
		t.Run(scenario, func(t *testing.T) {
			s, c, r, p, leaf := tlsReadinessFixture(t)
			bundle, _ := s.Bundle()
			initial, _ := json.Marshal(bundle)
			var calls atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "foreign tenant":
				bundle.Routes[0].TenantID = "other"
				s.recordSyncSuccess(bundle, "", time.Now(), false)
			case "missing allowlist":
				bundle.TLSAllowlist = nil
				s.recordSyncSuccess(bundle, "", time.Now(), false)
			case "expired bundle":
				bundle.ValidUntil = time.Now().Add(-time.Second)
				s.recordSyncSuccess(bundle, "", time.Now(), false)
			case "cancelled":
				cancel()
			case "invalid policy":
				p.Policy.TLSReadiness.MaxConcurrency = 0
			}
			probe := func(ctx context.Context, _, _ string, _ bool, _ time.Duration) (*platformTLSCertificate, error) {
				calls.Add(1)
				if scenario == "different bundle" {
					updated := bundle
					updated.Version = "different"
					s.recordSyncSuccess(updated, "", time.Now(), false)
				}
				if scenario == "untrusted TLS" {
					return nil, errors.New("untrusted certificate")
				}
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return &platformTLSCertificate{Leaf: leaf, ValidUntil: leaf.NotAfter}, nil
			}
			receipt, err := s.observePlatformTLSReadinessWithProbe(ctx, c, r, p, probe)
			if scenario == "untrusted TLS" || scenario == "foreign tenant" || scenario == "missing allowlist" {
				if err != nil || receipt == nil || len(receipt.Facts) != 1 || receipt.Facts[0].Ready || receipt.Facts[0].Reason == "" {
					t.Fatal("failure did not remain a negative fact", err, receipt)
				}
				if scenario != "untrusted TLS" && calls.Load() != 0 {
					t.Fatal("unauthorized hostname was probed")
				}
			} else if err == nil || receipt != nil {
				t.Fatal("invalid observation succeeded")
			}
			if scenario == "untrusted TLS" {
				after, _ := s.Bundle()
				encoded, _ := json.Marshal(after)
				if string(initial) != string(encoded) {
					t.Fatal("failed probe changed serving")
				}
			}
		})
	}
}

func TestTLSReadinessPlatformCertificateUsesSignedRouteOwner(t *testing.T) {
	s, _, r, p, _ := tlsReadinessFixture(t)
	b, _ := routeartifact.MaterializeForGroup(r, "edge-group-test")
	ref := p.Certificates[0]
	ref.Policy = model.EdgeRouteTLSPolicyPlatform
	ref.AppID = ""
	ref.TenantID = ""
	b.Routes[0].TLSPolicy = ref.Policy
	expected := append([]model.EdgeRouteBinding(nil), b.Routes...)
	if !s.tlsReadinessHostAuthorized(ref, expected, b) {
		t.Fatal("platform reference lost signed route owner")
	}
	b.Routes[0].TenantID = "other"
	if s.tlsReadinessHostAuthorized(ref, expected, b) {
		t.Fatal("platform reference authorized another tenant")
	}
}

func TestTLSReadinessProbeTimeoutIsBounded(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetReadDeadline(time.Now().Add(time.Second))
		io.Copy(io.Discard, conn)
	}()
	started := time.Now()
	_, err = probePlatformTLS(context.Background(), listener.Addr().String(), "tls.example.test", false, 50*time.Millisecond)
	if err == nil || time.Since(started) > time.Second {
		t.Fatal("stalled handshake was not bounded", err)
	}
	<-done
}

func TestTLSReadinessPartialFailureNeverClaimsAllVerified(t *testing.T) {
	s, c, r, p, leaf := tlsReadinessFixture(t)
	other := p.Certificates[0]
	other.Hostname = "second.example.test"
	p.Certificates = append(p.Certificates, other)
	rows := r.Content["routes"].([]any)
	encoded, _ := json.Marshal(rows[0])
	var second map[string]any
	json.Unmarshal(encoded, &second)
	second["hostname"] = other.Hostname
	r.Content["routes"] = append(rows, second)
	bundle, err := routeartifact.MaterializeForGroup(r, s.Config.EdgeGroupID)
	if err != nil {
		t.Fatal(err)
	}
	entry := bundle.TLSAllowlist[0]
	entry.Hostname = other.Hostname
	bundle.TLSAllowlist = append(bundle.TLSAllowlist, entry)
	bundle.ValidUntil = time.Now().Add(time.Minute)
	s.recordSyncSuccess(bundle, "", time.Now(), false)
	s.recordCaddyApply(bundle.Version, len(bundle.Routes), "config", nil)
	var calls atomic.Int32
	receipt, err := s.observePlatformTLSReadinessWithProbe(context.Background(), c, r, p, func(context.Context, string, string, bool, time.Duration) (*platformTLSCertificate, error) {
		calls.Add(1)
		return &platformTLSCertificate{Leaf: leaf, ValidUntil: leaf.NotAfter}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	s.platformTLSReadiness = receipt
	s.platformTLSCandidate.State = "shadow_verified"
	status := s.Status().PlatformTLSCandidate
	if calls.Load() != 2 || status.TLSVerified || status.Serving || status.Readiness.Probes != 2 || status.Readiness.ReadyProbes != 1 || status.Readiness.FailedProbes != 1 {
		t.Fatal("partial observations claimed success", status)
	}
}

func TestTLSReadinessSkipsHostnamesPinnedToAnotherGroup(t *testing.T) {
	s, c, r, p, leaf := tlsReadinessFixture(t)
	other := p.Certificates[0]
	other.Hostname = "other-group.example.test"
	p.Certificates = append(p.Certificates, other)
	rows := r.Content["routes"].([]any)
	encoded, _ := json.Marshal(rows[0])
	var remote map[string]any
	json.Unmarshal(encoded, &remote)
	remote["hostname"], remote["edge_group_mode"], remote["edge_group_id"] = other.Hostname, model.PlatformRouteEdgeGroupModePinned, "edge-group-other"
	r.Content["routes"] = append(rows, remote)
	calls := 0
	receipt, err := s.observePlatformTLSReadinessWithProbe(context.Background(), c, r, p, func(_ context.Context, _, host string, _ bool, _ time.Duration) (*platformTLSCertificate, error) {
		calls++
		if host == other.Hostname {
			t.Error("foreign group hostname was probed")
		}
		return &platformTLSCertificate{Leaf: leaf, ValidUntil: leaf.NotAfter}, nil
	})
	if err != nil || receipt == nil || len(receipt.Facts) != 1 || calls != 1 || !receipt.Facts[0].Ready {
		t.Fatal("group-scoped TLS requirement changed", err, receipt)
	}
}

func TestTLSReadinessCannotOutliveVerifiedChain(t *testing.T) {
	s, c, r, p, leaf := tlsReadinessFixture(t)
	trustUntil := time.Now().Add(20 * time.Second).UTC()
	receipt, err := s.observePlatformTLSReadinessWithProbe(context.Background(), c, r, p, func(context.Context, string, string, bool, time.Duration) (*platformTLSCertificate, error) {
		return &platformTLSCertificate{Leaf: leaf, ValidUntil: trustUntil}, nil
	})
	if err != nil || receipt == nil {
		t.Fatal(err)
	}
	fact := receipt.Facts[0]
	if !fact.Ready || !fact.ValidUntil.Equal(trustUntil) || !fact.NotAfter.After(trustUntil) {
		t.Fatal("chain expiry was not preserved", fact)
	}
	if summarizePlatformTLSReadiness(receipt, receipt.BundleVersion, trustUntil).ReadyProbes != 0 {
		t.Fatal("expired chain remained ready")
	}
}

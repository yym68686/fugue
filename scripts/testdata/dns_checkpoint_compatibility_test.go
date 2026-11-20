package dnsserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

// The verification workflow injects this test into each exact checkout. No
// production cache, endpoint, key, or API is used; only the fixture signing key
// and a loopback DNS socket are accessible to the test.
func TestHostedGlobalDNSCheckpointCrossVersion(t *testing.T) {
	path, mode := os.Getenv("FUGUE_CHECKPOINT_FIXTURE"), os.Getenv("FUGUE_CHECKPOINT_MODE")
	if path == "" || !filepath.IsAbs(path) {
		t.Fatal("absolute fixture path required")
	}
	cfg := config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "edge-group-a", PlatformScopeKey: "global", Zone: "example.test", CachePath: path,
		BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}
	s := NewService(cfg, nil)
	s.PlatformTokenFile = filepath.Join(filepath.Dir(path), "unused-fixture-token")
	persisted := path + ".platform-serving.json"
	if mode == "write" {
		parent, candidate := dnsServingFixture(t, true)
		record := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", Positive: true, NodeID: cfg.DNSNodeID, GroupID: cfg.EdgeGroupID,
			Parent: parent, Candidate: candidate, AppliedAt: time.Now().UTC()}
		if err := s.signDNSCheckpoint(&record); err != nil {
			t.Fatal(err)
		}
		raw := mustDNSJSON(t, record)
		f, err := os.OpenFile(persisted, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Write(raw); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	if mode != "read" {
		t.Fatal("unknown compatibility fixture mode")
	}
	raw, err := os.ReadFile(persisted)
	if err != nil {
		t.Fatal(err)
	}
	var expected dnsServingCheckpoint
	if err := json.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	if err := s.LoadCache(); err != nil {
		t.Fatal(err)
	}
	state := s.platformServing.Load()
	if state == nil || state.record.Candidate.Assignment.ScopeKey != "global" || state.record.Signature != expected.Signature || !state.record.AppliedAt.Equal(expected.AppliedAt) || dnsServingReady(state, time.Now()) {
		t.Fatal("restart did not keep exact global authority without transient readiness")
	}
	for _, transport := range []string{"udp", "tcp"} {
		if transport == "udp" {
			l, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			s.Config.UDPAddr = l.LocalAddr().String()
			startCheckpointWire(t, &dns.Server{PacketConn: l, Handler: s})
		} else {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			s.Config.TCPAddr = l.Addr().String()
			startCheckpointWire(t, &dns.Server{Listener: l, Handler: s})
		}
	}
	query := new(dns.Msg)
	query.SetQuestion("app.example.test.", dns.TypeA)
	checkWire := func(rcode, count int) {
		t.Helper()
		for network, address := range map[string]string{"udp": s.Config.UDPAddr, "tcp": s.Config.TCPAddr} {
			got, _, err := (&dns.Client{Net: network, Timeout: time.Second}).Exchange(query, address)
			if err != nil || !got.Authoritative || got.Rcode != rcode || len(got.Answer) != count {
				t.Fatalf("%s: expected rcode=%d answers=%d, got %v (%v)", network, rcode, count, got, err)
			}
			if count == 1 {
				if a, ok := got.Answer[0].(*dns.A); !ok || a.A.String() != "8.8.8.8" {
					t.Fatalf("%s: answer did not match the signed fixture", network)
				}
			}
		}
	}
	checkWire(dns.RcodeServerFailure, 0)
	refresh := func(wrongFence bool) *dnsServingState {
		a := state.record.Candidate
		got, err := s.refreshedDNSServingFacts(t.Context(), state, func(_ context.Context, host, path, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
			for _, p := range state.payload.Plan.Probes {
				if host != p.Hostname || path != p.Path || address != p.Address {
					continue
				}
				stamp := time.Now().UTC()
				fence := a.Release.FencingToken
				if wrongFence {
					fence++
				}
				return routeprobe.Proof{Digest: p.RouteDigest, Version: "serving", EdgeID: p.EdgeID, GroupID: p.EdgeGroupID, State: p.State,
					CheckedAt: stamp, ValidUntil: stamp.Add(time.Minute), TrafficRelease: &model.TrafficReleaseBinding{
						ReleaseSetID: state.record.Parent.ID, ReleaseSetDigest: state.record.Parent.ContentHash, RouteArtifactID: state.routeID,
						PolicyDigest: state.payload.Lineage.PolicyDigest, IntentDigest: state.payload.Lineage.IntentDigest,
						InputSnapshotDigest: state.payload.Lineage.InputSnapshotDigest, ReleaseID: a.Release.ID, ReleaseChannel: a.Release.ReleaseChannel,
						FencingToken: fence, ScopeKey: "global"}}, nil
			}
			return routeprobe.Proof{}, errors.New("unexpected synthetic probe target")
		}, "control_plane_unavailable", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	// Go-to-Go writer internals changed after the current production revisions;
	// asking the serving path to recover fresh evidence exercises each revision's
	// own atomic installation without substituting a mocked DNS server.
	if got := refresh(true); dnsServingReady(got, time.Now()) || got.answer(query, "", time.Now()).Rcode != dns.RcodeServerFailure {
		t.Fatal("reader accepted a route proof from a different fence")
	}
	good := refresh(false)
	if !dnsServingReady(good, time.Now()) || good.record.Signature != expected.Signature || !good.record.AppliedAt.Equal(expected.AppliedAt) ||
		good.answer(query, "", time.Now().Add(2*time.Minute)).Rcode != dns.RcodeServerFailure {
		t.Fatal("reader lost a fresh exact proof or extended signed authority")
	}
	if err := s.probeDNSServingListener(state); err != nil {
		t.Fatal(err)
	}
	// Use the unchanged recovery assignment path to publish the refreshed
	// state. Give the client a synthetic local HTTP failure, not a live URL.
	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer unavailable.Close()
	s.Config.APIURL = unavailable.URL
	if err := s.syncPlatformDNSServingOnce(t.Context(), func(_ context.Context, host, path, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
		for _, p := range state.payload.Plan.Probes {
			if p.Hostname == host && p.Path == path && p.Address == address {
				for _, f := range good.facts {
					if f.ProbeID == p.ID {
						return f.Proof, nil
					}
				}
			}
		}
		return routeprobe.Proof{}, errors.New("unexpected recovery probe")
	}, s.probeDNSServingListener); err == nil {
		t.Fatal("synthetic control-plane failure should remain visible")
	}
	checkWire(dns.RcodeSuccess, 1)
	if after, err := os.ReadFile(persisted); err != nil || !bytes.Equal(raw, after) {
		t.Fatal("rollback reader modified the other version's positive checkpoint", err)
	}
}

func startCheckpointWire(t *testing.T, server *dns.Server) {
	t.Helper()
	ready, stopped := make(chan struct{}), make(chan error, 1)
	server.NotifyStartedFunc = func() { close(ready) }
	go func() { stopped <- server.ActivateAndServe() }()
	select {
	case <-ready:
	case err := <-stopped:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("fixture DNS listener did not start")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.ShutdownContext(ctx)
	})
}

package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func TestUnselectedDNSCandidateKeepsVerifiedServingWithoutInventoryCredential(t *testing.T) {
	parent, candidate := dnsServingFixture(t, true)
	var reports, unexpected atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/platform-state/consumers/identity":
			if r.Header.Get("Authorization") != "Bearer candidate-pod-token" {
				t.Error("identity did not use the bound candidate credential")
			}
			json.NewEncoder(w).Encode(map[string]any{"token": "candidate-token", "expires_at": time.Now().Add(time.Minute), "component": "dns-server", "node_id": "dns-a", "scope_key": "global", "artifact_kinds": []string{candidate.Artifact.ArtifactKind}})
		case "/v1/platform-state/consumers/assignment":
			json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{candidate.Assignment}})
		case "/v1/platform-state/consumers/artifacts/dns":
			json.NewEncoder(w).Encode(candidate)
		case "/v1/platform-state/consumers/artifacts/parent":
			json.NewEncoder(w).Encode(map[string]any{"artifact": parent, "assignment": candidate.Assignment, "release": candidate.Release})
		case "/v1/platform-state/consumers/trusted-heartbeat":
			reports.Add(1)
			http.Error(w, "consumer is not the selected public backend", http.StatusConflict)
		default:
			unexpected.Add(1)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	token := filepath.Join(dir, "candidate-token")
	if err := os.WriteFile(token, []byte("candidate-pod-token"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := config.DNSConfig{APIURL: server.URL, DNSNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", CachePath: filepath.Join(dir, "candidate", "cache"), PublicIPv4: "192.0.2.8", ListenAddr: ":7834", UDPAddr: "127.0.0.1:5353", TCPAddr: "127.0.0.1:5353", BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}
	s := NewService(cfg, nil)
	s.PlatformTokenFile = token
	if err := s.validateConfig(); err != nil || s.heartbeatEnabled() {
		t.Fatal("candidate requires or enables inventory registration", err)
	}
	if err := s.LoadCache(); err == nil || dnsQuery(t, s, "app.example.test", dns.TypeA).Rcode != dns.RcodeServerFailure {
		t.Fatal("fresh candidate served without a verified artifact")
	}
	payload, routeID, err := s.verifyDNSServingRelease(parent, candidate)
	if err != nil {
		t.Fatal(err)
	}
	probe := func(_ context.Context, host, path, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
		for _, req := range payload.Plan.Probes {
			if req.Hostname == host && req.Path == path && req.Address == address {
				now := time.Now().UTC()
				return routeprobe.Proof{Digest: req.RouteDigest, Version: "serving", EdgeID: req.EdgeID, GroupID: req.EdgeGroupID, CheckedAt: now, ValidUntil: now.Add(time.Minute), TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: routeID, PolicyDigest: payload.Lineage.PolicyDigest, IntentDigest: payload.Lineage.IntentDigest, InputSnapshotDigest: payload.Lineage.InputSnapshotDigest, ReleaseID: candidate.Release.ID, ReleaseChannel: candidate.Release.ReleaseChannel, FencingToken: candidate.Release.FencingToken, ScopeKey: "global"}}, nil
			}
		}
		return routeprobe.Proof{}, errors.New("unexpected probe")
	}
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()
	s.Config.UDPAddr, s.Config.TCPAddr = udp.LocalAddr().String(), tcp.Addr().String()
	udpServer, tcpServer := &dns.Server{PacketConn: udp, Handler: s}, &dns.Server{Listener: tcp, Handler: s}
	go udpServer.ActivateAndServe()
	go tcpServer.ActivateAndServe()
	defer udpServer.Shutdown()
	defer tcpServer.Shutdown()
	ctx := context.Background()
	for attempt := 0; attempt < 2; attempt++ {
		if err := s.syncPlatformDNSServingOnce(ctx, probe, s.probeDNSServingListener); err == nil {
			t.Fatal("unselected candidate hid rejected trusted heartbeat")
		}
		if !s.Status().Healthy || !s.Status().PlatformServing.ReportedAt.IsZero() || reports.Load() != int32(attempt+1) {
			t.Fatal("rejected heartbeat destroyed serving or claimed an accepted receipt", s.Status())
		}
		if err := s.HeartbeatOnce(ctx); err != nil || unexpected.Load() != 0 {
			t.Fatal("candidate tried to overwrite legacy inventory", err)
		}
		for _, network := range []string{"udp", "tcp"} {
			address := s.Config.UDPAddr
			if network == "tcp" {
				address = s.Config.TCPAddr
			}
			question := new(dns.Msg)
			question.SetQuestion("app.example.test.", dns.TypeA)
			answer, _, err := (&dns.Client{Net: network, Timeout: time.Second}).Exchange(question, address)
			if err != nil || answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 {
				t.Fatal("candidate lost its independently verified answer", network, answer, err)
			}
		}
	}
	restarted := NewService(cfg, nil)
	restarted.PlatformTokenFile = token
	if err := restarted.LoadCache(); err != nil || restarted.platformServing.Load() == nil {
		t.Fatal("heartbeat rejection discarded the durable verified checkpoint", err)
	}
	if dnsServingReady(restarted.platformServing.Load(), time.Now()) {
		t.Fatal("restart reused transient candidate readiness")
	}
	missingToken := NewService(cfg, nil)
	missingToken.PlatformTokenFile = filepath.Join(dir, "missing-token")
	if err := missingToken.syncPlatformDNSServingOnce(ctx, probe, missingToken.probeDNSServingListener); err == nil || missingToken.platformServing.Load() != nil || unexpected.Load() != 0 {
		t.Fatal("missing Pod token fell back to legacy authentication or unverified serving", err)
	}
}

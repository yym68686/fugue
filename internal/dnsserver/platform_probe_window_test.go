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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func TestDNSCompletesAcrossProbeWindowsOnlyForExactAssignment(t *testing.T) {
	const first, second, group = "app.example.test", "other.example.test", "edge-group-a"
	for _, mode := range []string{"same-assignment", "changed-fence", "expired", "explicit-negative", "restart", "standby-rejected-successor"} {
		t.Run(mode, func(t *testing.T) {
			parent, candidate := dnsServingFixtureWithIntent(t, group, "global", func(r *platformconfig.CompileRequest) {
				r.Policy.DNSReadiness.FactFreshnessSeconds = 120
				route, record := r.Intent.Routes[0], r.Intent.DNS[0]
				route.Hostname, route.AppID = second, "other"
				record.Hostname, record.AppID, record.Values = second, route.AppID, []string{route.AppID}
				r.Intent.Routes = append(r.Intent.Routes, route)
				r.Intent.DNS = []platformconfig.DNSIntent{r.Intent.DNS[0], record}
				rule, selection := r.Policy.DNSAnswerRules[0], r.RuntimeSnapshot.DNSSelections[0]
				rule.Hostname, selection.Hostname = second, second
				r.Policy.DNSAnswerRules = append(r.Policy.DNSAnswerRules, rule)
				r.RuntimeSnapshot.DNSSelections = append(r.RuntimeSnapshot.DNSSelections, selection)
			}, true)
			next := candidate
			next.Assignment.FencingToken++
			next.Assignment.ArtifactReleaseID = "next"
			next.Release.FencingToken = next.Assignment.FencingToken
			next.Release.ID = next.Assignment.ArtifactReleaseID
			next.Release.ReleasedAt = candidate.Release.ReleasedAt.Add(time.Second)
			standby := mode == "standby-rejected-successor"
			var round, reports, firstAttempts atomic.Int32
			current := func() dnsPlatformCandidate {
				if (round.Load() > 0 && mode == "changed-fence") || (round.Load() >= 0 && standby) {
					return next
				}
				return candidate
			}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c := current()
				switch r.URL.Path {
				case "/v1/platform-state/consumers/identity":
					_ = json.NewEncoder(w).Encode(platformconsumer.Identity{Token: "token", ExpiresAt: time.Now().Add(time.Minute), Component: "dns-server", NodeID: "dns-a", ScopeKey: "global", ArtifactKinds: []string{candidate.Artifact.ArtifactKind}})
				case "/v1/platform-state/consumers/assignment":
					_ = json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{c.Assignment}})
				case "/v1/platform-state/consumers/artifacts/dns":
					_ = json.NewEncoder(w).Encode(c)
				case "/v1/platform-state/consumers/artifacts/parent":
					_ = json.NewEncoder(w).Encode(dnsPlatformCandidate{Artifact: parent, Assignment: c.Assignment, Release: c.Release})
				case "/v1/platform-state/consumers/trusted-heartbeat":
					reports.Add(1)
					var h platformcontrol.PlatformConsumerHeartbeatEnvelope
					if standby {
						want := "passed"
						if round.Load() == 0 {
							want = "failed"
						}
						if json.NewDecoder(r.Body).Decode(&h) != nil || h.ProbeStatus != want || h.FencingToken != c.Assignment.FencingToken {
							t.Error("unexpected standby receipt", h.ProbeStatus, h.FencingToken)
						}
						w.WriteHeader(http.StatusConflict)
						_ = json.NewEncoder(w).Encode(map[string]string{"code": "dns_backend_not_selected", "error": "not selected"})
						return
					}
					if json.NewDecoder(r.Body).Decode(&h) != nil || h.ProbeStatus != "passed" || h.FencingToken != c.Assignment.FencingToken {
						t.Error("unexpected serving receipt")
					}
					_ = json.NewEncoder(w).Encode(model.PlatformConsumerHeartbeatResponse{Consumer: model.PlatformConsumerInstance{IdentityVerified: true, ConsumerID: h.ConsumerID, Sequence: h.Sequence, EvidenceHash: h.EvidenceHash, ExpectedConsumerSetID: h.ExpectedConsumerSetID}})
				default:
					t.Error("unexpected endpoint", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer api.Close()
			cfg := config.DNSConfig{APIURL: api.URL, DNSNodeID: "dns-a", EdgeGroupID: group, Zone: "example.test",
				CachePath: filepath.Join(t.TempDir(), "cache"), BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}
			tokenFile := cfg.CachePath + ".token"
			if err := os.WriteFile(tokenFile, []byte("pod-token"), 0600); err != nil {
				t.Fatal(err)
			}
			s := NewService(cfg, nil)
			s.PlatformTokenFile = tokenFile
			p, routeID, err := s.verifyDNSServingRelease(parent, candidate)
			if err != nil || len(p.Plan.Probes) != 2 {
				t.Fatal("expected two independent route proofs", err)
			}
			probe := func(_ context.Context, host, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
				c := current()
				if host == first && round.Load() >= 0 {
					firstAttempts.Add(1)
					if round.Load() > 0 {
						// A genuine rejection must invalidate even a still-live
						// proof. This is not the scheduler running out of time.
						return routeprobe.Proof{}, errors.New("explicit route rejection")
					}
				}
				if host == second && round.Load() == 0 {
					return routeprobe.Proof{}, routeprobe.ErrUnavailable
				}
				for _, req := range p.Plan.Probes {
					if req.Hostname == host && req.Path == path && req.Address == address {
						now := time.Now().UTC()
						validFor := 120 * time.Second
						if standby && round.Load() == 0 && host == first {
							validFor = 80 * time.Second
						}
						return routeprobe.Proof{Version: "serving", Digest: req.RouteDigest, State: state, EdgeID: req.EdgeID, GroupID: req.EdgeGroupID, CheckedAt: now, ValidUntil: now.Add(validFor),
							TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: routeID, PolicyDigest: p.Lineage.PolicyDigest, IntentDigest: p.Lineage.IntentDigest, InputSnapshotDigest: p.Lineage.InputSnapshotDigest, ReleaseID: c.Release.ID, ReleaseChannel: c.Release.ReleaseChannel, FencingToken: c.Release.FencingToken, ScopeKey: "global"}}, nil
					}
				}
				return routeprobe.Proof{}, errors.New("unexpected route probe")
			}
			ctx := context.Background()
			var baseline model.PlatformConsumerAssignment
			var wantReports int32
			if standby {
				round.Store(-1)
				if err := s.syncPlatformDNSServingOnce(ctx, probe, func(*dnsServingState) error { return nil }); !errors.Is(err, platformconsumer.ErrDNSBackendNotSelected) || s.platformServing.Load() == nil {
					t.Fatal("warm standby did not preserve ordinary heartbeat rejection", err)
				}
				baseline = s.platformServing.Load().record.Candidate.Assignment
				round.Store(0)
				wantReports = 2 // failed successor observation, on top of the baseline
			}
			prohibitedWire := func(*dnsServingState) error {
				t.Error("incomplete candidate reached listener apply")
				return errors.New("incomplete candidate")
			}
			if err := s.syncPlatformDNSServingOnce(ctx, probe, prohibitedWire); err == nil || !strings.Contains(err.Error(), "readiness is incomplete") {
				t.Fatal("first incomplete observation passed", err)
			}
			loaded := s.platformServing.Load()
			if (standby && (loaded == nil || loaded.record.Candidate.Assignment.ArtifactReleaseID != baseline.ArtifactReleaseID)) ||
				(!standby && loaded != nil) || reports.Load() != wantReports || s.pendingDNSProbeWindow == nil || len(s.pendingDNSProbeWindow.facts) != 1 {
				t.Fatal("a private observation was lost, served or acknowledged")
			}
			if _, err := os.Stat(cfg.CachePath + ".platform-serving.json"); !standby && !errors.Is(err, os.ErrNotExist) {
				t.Fatal("incomplete observation created a checkpoint", err)
			}
			if standby {
				// A partial candidate's previous sweep also paid for the
				// retained LKG. This proof can cover the next scan and poll,
				// even though it cannot cover another slow rejection cycle.
				s.servingDNSProbes.lastStarted = time.Now().Add(-70 * time.Second)
			}
			original := s.pendingDNSProbeWindow.facts[0]
			round.Add(1)
			switch mode {
			case "expired", "explicit-negative":
				offset := 2 * time.Minute
				if mode == "explicit-negative" {
					offset = time.Minute // still fresh, but due to be reprobed
				}
				fact := &s.pendingDNSProbeWindow.facts[0]
				fact.Proof.CheckedAt = fact.Proof.CheckedAt.Add(-offset)
				fact.Proof.ValidUntil = fact.Proof.ValidUntil.Add(-offset)
			case "restart":
				s = NewService(cfg, nil)
				s.PlatformTokenFile = tokenFile
			}
			if mode != "same-assignment" && !standby {
				if err := s.syncPlatformDNSServingOnce(ctx, probe, prohibitedWire); err == nil || firstAttempts.Load() != 2 || s.platformServing.Load() != nil || reports.Load() != 0 {
					t.Fatal("non-current proof was deferred or permitted publication", err, firstAttempts.Load(), reports.Load())
				}
				return
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
			err = s.syncPlatformDNSServingOnce(ctx, probe, s.probeDNSServingListener)
			if (standby && !errors.Is(err, platformconsumer.ErrDNSBackendNotSelected)) || (!standby && err != nil) ||
				firstAttempts.Load() != 1 || reports.Load() != wantReports+1 || s.pendingDNSProbeWindow != nil {
				t.Fatal("the missing observation did not complete the exact release", err, firstAttempts.Load(), reports.Load())
			}
			st := s.platformServing.Load()
			if standby && (st.record.Candidate.Assignment.ArtifactReleaseID != next.Assignment.ArtifactReleaseID || !s.Status().PlatformServing.ReportedAt.IsZero()) {
				t.Fatal("standby did not load the new fence locally, or claimed a trusted receipt")
			}
			got := validDNSReadinessFacts(p.Plan, p.Policy.DNSReadiness, st.facts, time.Now())[original.ProbeID]
			if !got.Proof.CheckedAt.Equal(original.Proof.CheckedAt) || !got.Proof.ValidUntil.Equal(original.Proof.ValidUntil) ||
				got.ObservationID != original.ObservationID || len(got.ProbeHistory) != len(original.ProbeHistory) {
				t.Fatal("deferral minted a new observation, deadline or recovery sample", got)
			}
			for network, addr := range map[string]string{"udp": s.Config.UDPAddr, "tcp": s.Config.TCPAddr} {
				for _, host := range []string{first, second} {
					q := new(dns.Msg)
					q.SetQuestion(dns.Fqdn(host), dns.TypeA)
					r, _, err := (&dns.Client{Net: network, Timeout: time.Second}).Exchange(q, addr)
					if err != nil || !r.Authoritative || r.Rcode != dns.RcodeSuccess || len(r.Answer) != 1 {
						t.Fatal("published candidate missing real wire answer", network, host, err, r)
					}
				}
			}
		})
	}
}

func TestDNSProbeDeferralLeavesTimeForFullCheckpointRefresh(t *testing.T) {
	now := time.Now().UTC()
	st := &dnsServingState{record: dnsServingCheckpoint{AppliedAt: now, Positive: true}, payload: dnsServingPayload{Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600}}}
	if !dnsServingMayDeferProbes(st, now.Add(10*time.Minute)) || dnsServingMayDeferProbes(st, now.Add(30*time.Minute)) {
		t.Fatal("deferral must leave a renewal window before checkpoint expiry")
	}
	next := st.record
	next.AppliedAt = now.Add(10 * time.Minute)
	retained := []dnsReadinessFact{{Reason: "retained_valid_proof"}}
	if !preserveDNSCheckpointAge(next, st, retained).AppliedAt.Equal(now) {
		t.Fatal("deferral extended the original positive checkpoint")
	}
	st.record.Positive = false
	if fresh := preserveDNSCheckpointAge(next, st, retained); !fresh.Positive || !fresh.AppliedAt.Equal(next.AppliedAt) {
		t.Fatal("a previous negative observation prevented positive apply")
	}
}

func TestFreshUnrecoveredAvailabilityProofDoesNotDeferNextObservation(t *testing.T) {
	for _, scenario := range []string{"needs-success", "needs-span", "failed", "other-node", "other-family", "outside-pool"} {
		t.Run(scenario, func(t *testing.T) {
			st, now := decisionTestState(t)
			p := st.payload
			p.Policy.DNSReadiness.FactFreshnessSeconds = 120
			record := &p.Queries[0].Records[0]
			record.AnswerPolicy.Availability = &model.DNSAvailabilityPolicy{
				Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"},
				RecoverySuccesses: 2, RecoveryMinSeconds: 10,
			}
			scheduler := &dnsReadinessScheduler{}
			for i := range st.facts {
				fact := &st.facts[i]
				fact.Proof.CheckedAt = now.Add(-12 * time.Second)
				fact.Proof.ValidUntil = fact.Proof.CheckedAt.Add(120 * time.Second)
				if i != 0 || scenario == "needs-span" {
					older := *fact
					gap := 11 * time.Second
					if i == 0 {
						gap = time.Second
					}
					older.Proof.CheckedAt = older.Proof.CheckedAt.Add(-gap)
					scheduler.observeProbeHistory(&older, p.Policy.DNSReadiness, now)
				}
				scheduler.observeProbeHistory(fact, p.Policy.DNSReadiness, now)
			}
			deferrable := validDNSReadinessFacts(p.Plan, p.Policy.DNSReadiness, st.facts, now)
			switch scenario {
			case "other-node":
				p.Queries[0].NodeID = "different-dns"
			case "other-family":
				record.Type = "AAAA"
			case "outside-pool":
				record.AnswerPolicy.Availability.OrderedEdgeIDs = []string{"edge-b"}
			}
			dnsDeferRecoveredProbes(p, "dns-a", deferrable, now)
			if scenario == "other-node" || scenario == "other-family" || scenario == "outside-pool" {
				if len(deferrable) != len(st.facts) {
					t.Fatal("policy changed scheduling of unrelated or recovered proofs")
				}
				return
			}
			before, err := buildDNSServingState(st.record, p, st.routeID, "dns-a", "edge-group-a", st.facts, now)
			if err != nil || checkDNSServingSnapshot(before, now, false) == nil {
				t.Fatal("incomplete history satisfied signed recovery", err)
			}
			var attempted atomic.Int32
			collected := scheduler.collectWindow(context.Background(), p.Plan, p.Policy.DNSReadiness, func(_ context.Context, host, path, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
				attempted.Add(1)
				requirement := p.Plan.Probes[0]
				if host != requirement.Hostname || path != requirement.Path || address != requirement.Address {
					t.Error("a recovered proof should have been deferred")
					return routeprobe.Proof{}, errors.New("unexpected probe")
				}
				if scenario == "failed" {
					return routeprobe.Proof{}, routeprobe.ErrUnavailable
				}
				proof := st.facts[0].Proof
				proof.CheckedAt = time.Now().UTC()
				proof.ValidUntil = proof.CheckedAt.Add(120 * time.Second)
				return proof, nil
			}, nil, deferrable, false)
			if attempted.Load() != 1 {
				t.Fatalf("expected exactly the unresolved recovery probe, got %d requests", attempted.Load())
			}
			evaluatedAt := time.Now().UTC()
			facts := retainValidDNSReadinessFacts(p.Plan, p.Policy.DNSReadiness, st.facts, collected, evaluatedAt)
			after, err := buildDNSServingState(st.record, p, st.routeID, "dns-a", "edge-group-a", facts, evaluatedAt)
			if err != nil {
				t.Fatal(err)
			}
			if passed := checkDNSServingSnapshot(after, evaluatedAt, false) == nil; passed != (scenario != "failed") {
				t.Fatalf("snapshot result disagrees with the new recovery observation: passed=%t history=%v", passed, facts[0].ProbeHistory)
			}
		})
	}
}

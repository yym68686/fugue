package dnsserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
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

func TestDNSRejectedSuccessorReportsFailureWithoutReplacingLKG(t *testing.T) {
	for _, assignmentChanges := range []bool{false, true} {
		name := "current_assignment"
		if assignmentChanges {
			name = "superseded_assignment"
		}
		t.Run(name, func(t *testing.T) {
			previousParent, previous := dnsServingFixture(t, true)
			parent, candidate := dnsServingFixtureWithIntent(t, "edge-group-a", "global", func(r *platformconfig.CompileRequest) {
				r.Intent.Generation = "intent-successor"
			}, true)
			candidate.Assignment.ArtifactReleaseID, candidate.Release.ID = "successor", "successor"
			candidate.Assignment.ExpectedConsumerSetID = "expected-successor"
			candidate.Assignment.FencingToken++
			candidate.Release.FencingToken++
			candidate.Release.ReleasedAt = previous.Release.ReleasedAt.Add(time.Millisecond)
			var changed atomic.Bool
			var mu sync.Mutex
			var reports []platformcontrol.PlatformConsumerHeartbeatEnvelope
			var cursor *platformcontrol.PlatformConsumerHeartbeatCursor
			expected := model.PlatformExpectedConsumerSet{ID: candidate.Assignment.ExpectedConsumerSetID, ReleaseSetID: candidate.Assignment.ReleaseSetID, ArtifactKind: candidate.Artifact.ArtifactKind, ScopeKey: "global", ExpectedGeneration: candidate.Assignment.ExpectedGeneration,
				Consumers: []model.PlatformExpectedConsumer{{ConsumerID: "dns-server:dns-a", Component: "dns-server", NodeID: "dns-a", ArtifactKind: candidate.Artifact.ArtifactKind, ScopeKey: "global", ExpectedGeneration: candidate.Assignment.ExpectedGeneration}}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/platform-state/consumers/identity":
					json.NewEncoder(w).Encode(platformconsumer.Identity{Token: "identity", ExpiresAt: time.Now().Add(time.Minute), Component: "dns-server", NodeID: "dns-a", ScopeKey: "global", ArtifactKinds: []string{candidate.Artifact.ArtifactKind}})
				case "/v1/platform-state/consumers/assignment":
					a := candidate.Assignment
					if changed.Load() {
						a.FencingToken++
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{a}})
				case "/v1/platform-state/consumers/artifacts/dns":
					json.NewEncoder(w).Encode(candidate)
				case "/v1/platform-state/consumers/artifacts/parent":
					json.NewEncoder(w).Encode(map[string]any{"artifact": parent, "assignment": candidate.Assignment, "release": candidate.Release})
				case "/v1/platform-state/consumers/trusted-heartbeat":
					var h platformcontrol.PlatformConsumerHeartbeatEnvelope
					if err := json.NewDecoder(r.Body).Decode(&h); err != nil {
						t.Error(err)
					}
					mu.Lock()
					now := time.Now().UTC()
					claims := platformcontrol.PlatformComponentIdentityClaims{Version: "v1", CredentialID: "synthetic-workload", TokenID: "synthetic-token", Component: "dns-server", NodeID: "dns-a", ScopeKey: "global", ArtifactKinds: []string{candidate.Artifact.ArtifactKind}, IssuedAtUnix: now.Add(-time.Minute).Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix()}
					consumer, next, err := platformcontrol.VerifyTrustedPlatformConsumerHeartbeat(claims, expected, h, cursor, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{})
					if err != nil || !consumer.IdentityVerified || !consumer.LastHeartbeatAt.Equal(now) {
						mu.Unlock()
						t.Error("failure heartbeat did not refresh verified backend identity", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					cursor = &next
					reports = append(reports, h)
					mu.Unlock()
					json.NewEncoder(w).Encode(model.PlatformConsumerHeartbeatResponse{Consumer: consumer})
				default:
					t.Error("unexpected endpoint", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			cfg := config.DNSConfig{APIURL: server.URL, DNSNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", CachePath: filepath.Join(t.TempDir(), "cache"), BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}
			s := NewService(cfg, nil)
			s.PlatformTokenFile = cfg.CachePath + ".token"
			if err := os.WriteFile(s.PlatformTokenFile, []byte("pod-token"), 0600); err != nil {
				t.Fatal(err)
			}
			payload, routeID, err := s.verifyDNSServingRelease(previousParent, previous)
			if err != nil {
				t.Fatal(err)
			}
			oldProof := func(_ context.Context, host, path, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
				for _, requirement := range payload.Plan.Probes {
					if requirement.Hostname == host && requirement.Path == path && requirement.Address == address {
						now := time.Now().UTC()
						return routeprobe.Proof{Digest: requirement.RouteDigest, EdgeID: requirement.EdgeID, GroupID: requirement.EdgeGroupID, State: requirement.State, Version: "retained", CheckedAt: now, ValidUntil: now.Add(time.Minute), TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: previousParent.ID, ReleaseSetDigest: previousParent.ContentHash, RouteArtifactID: routeID, PolicyDigest: payload.Lineage.PolicyDigest, IntentDigest: payload.Lineage.IntentDigest, InputSnapshotDigest: payload.Lineage.InputSnapshotDigest, ReleaseID: previous.Release.ID, ReleaseChannel: previous.Release.ReleaseChannel, FencingToken: previous.Release.FencingToken, ScopeKey: "global"}}, nil
					}
				}
				return routeprobe.Proof{}, routeprobe.ErrUnavailable
			}
			now := time.Now().UTC()
			checkpoint := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: cfg.DNSNodeID, GroupID: cfg.EdgeGroupID, Parent: previousParent, Candidate: previous, AppliedAt: now.Add(-time.Minute), Positive: true}
			if err := s.signDNSCheckpoint(&checkpoint); err != nil {
				t.Fatal(err)
			}
			saved := mustDNSJSON(t, checkpoint)
			if err := os.WriteFile(cfg.CachePath+".platform-serving.json", saved, 0600); err != nil {
				t.Fatal(err)
			}
			facts := collectDNSReadinessFacts(context.Background(), payload.Plan, payload.Policy.DNSReadiness, oldProof)
			now = time.Now().UTC()
			old, err := buildDNSServingState(checkpoint, payload, routeID, cfg.DNSNodeID, cfg.EdgeGroupID, facts, now)
			if err != nil || !dnsServingReady(old, now) {
				t.Fatal("positive baseline missing", err)
			}
			s.platformServing.Store(old)
			s.platformServingBound.Store(true)
			for attempt := 0; attempt < 2; attempt++ {
				err = s.syncPlatformDNSServingOnce(context.Background(), func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
					changed.Store(assignmentChanges)
					return oldProof(ctx, host, path, address, state, timeout)
				}, func(*dnsServingState) error {
					t.Error("rejected successor reached activation")
					return nil
				})
				if err == nil || assignmentChanges && !errors.Is(err, platformconsumer.ErrAssignmentChanged) {
					t.Fatal("successor was not rejected for the expected reason", err)
				}
				if !reflect.DeepEqual(s.platformServing.Load().record, checkpoint) {
					t.Fatal("failed candidate changed positive serving identity")
				}
				raw, err := os.ReadFile(cfg.CachePath + ".platform-serving.json")
				if err != nil || !bytes.Equal(raw, saved) {
					t.Fatal("failed candidate changed durable LKG", err)
				}
				query := new(dns.Msg)
				query.SetQuestion("app.example.test.", dns.TypeA)
				if answer := s.platformServing.Load().answer(query, "", time.Now()); answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 {
					t.Fatal("failed candidate interrupted retained DNS answer")
				}
				if assignmentChanges {
					break
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if assignmentChanges {
				if len(reports) != 0 {
					t.Fatal("superseded assignment reported current evidence")
				}
				return
			}
			if len(reports) != 2 {
				t.Fatalf("rejected successor stopped identity heartbeats: got %d failure reports, want 2", len(reports))
			}
			for _, h := range reports {
				digest, err := platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
				if err != nil || digest != h.EvidenceHash || h.ExpectedConsumerSetID != candidate.Assignment.ExpectedConsumerSetID || h.FencingToken != candidate.Assignment.FencingToken || h.DesiredGeneration != candidate.Assignment.ExpectedGeneration || h.ActualGeneration != previous.Assignment.ExpectedGeneration || h.LKGGeneration != previous.Assignment.ExpectedGeneration || h.ApplyStatus != "failed" || h.ProbeStatus != "failed" || !h.ServingLKG || h.LastError == "" {
					t.Fatal("failure heartbeat misrepresented candidate or retained serving state", h, err)
				}
			}
			if reports[1].Sequence <= reports[0].Sequence || reports[1].Nonce == reports[0].Nonce {
				t.Fatal("failure reports reused heartbeat replay identity")
			}
		})
	}
}

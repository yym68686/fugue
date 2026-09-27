package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func TestDNSAssignmentRacePreservesServingAndRetriesBoundedly(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		continuous bool
		negative   bool
	}{
		{"converges", false, false},
		{"bounded_churn", true, false},
		{"negative_observation_converges", false, true},
		{"negative_observation_bounded_churn", true, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			continuous := scenario.continuous
			parent, baseline := dnsServingFixture(t, true)
			candidate := baseline
			var mu sync.Mutex
			var s *Service
			var old *dnsServingState
			attempts, reads, reports := 0, 0, 0
			query := new(dns.Msg)
			query.SetQuestion("app.example.test.", dns.TypeA)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.URL.Path {
				case "/v1/platform-state/consumers/identity":
					attempts++
					if attempts > 1 {
						actual := s.platformServing.Load()
						if actual != old || !reflect.DeepEqual(actual.facts, old.facts) {
							t.Error("assignment race replaced the retained proof snapshot")
						}
						if got := actual.answer(query, "", time.Now()); got.Rcode != dns.RcodeSuccess || len(got.Answer) != 1 {
							t.Error("assignment race interrupted the retained DNS answer")
						}
					}
					json.NewEncoder(w).Encode(platformconsumer.Identity{Token: "identity", ExpiresAt: time.Now().Add(time.Minute), Component: "dns-server", NodeID: "dns-a", ScopeKey: "global", ArtifactKinds: []string{baseline.Artifact.ArtifactKind}})
				case "/v1/platform-state/consumers/assignment":
					reads++
					if reads == 2 || continuous && reads%2 == 0 {
						candidate.Assignment.FencingToken++
						candidate.Release.FencingToken++
						candidate.Release.ID = "release-" + strconv.FormatInt(candidate.Release.FencingToken, 10)
						candidate.Assignment.ArtifactReleaseID = candidate.Release.ID
						candidate.Release.ReleasedAt = time.Now().UTC()
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{candidate.Assignment}})
				case "/v1/platform-state/consumers/artifacts/dns":
					json.NewEncoder(w).Encode(candidate)
				case "/v1/platform-state/consumers/artifacts/parent":
					json.NewEncoder(w).Encode(map[string]any{"artifact": parent, "assignment": candidate.Assignment, "release": candidate.Release})
				case "/v1/platform-state/consumers/trusted-heartbeat":
					reports++
					var h platformcontrol.PlatformConsumerHeartbeatEnvelope
					if json.NewDecoder(r.Body).Decode(&h) != nil || h.ProbeStatus != "passed" || h.FencingToken != candidate.Release.FencingToken {
						t.Error("retry acknowledged another publication")
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerHeartbeatResponse{Consumer: model.PlatformConsumerInstance{IdentityVerified: true, ConsumerID: h.ConsumerID, Sequence: h.Sequence, EvidenceHash: h.EvidenceHash, ExpectedConsumerSetID: h.ExpectedConsumerSetID}})
				default:
					t.Error("unexpected endpoint", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			cfg := config.DNSConfig{APIURL: server.URL, DNSNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", CachePath: filepath.Join(t.TempDir(), "cache"), BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}
			s = NewService(cfg, nil)
			s.PlatformTokenFile = cfg.CachePath + ".token"
			if err := os.WriteFile(s.PlatformTokenFile, []byte("pod-token"), 0600); err != nil {
				t.Fatal(err)
			}
			payload, routeID, err := s.verifyDNSServingRelease(parent, baseline)
			if err != nil {
				t.Fatal(err)
			}
			probe := func(_ context.Context, host, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
				mu.Lock()
				defer mu.Unlock()
				for _, requirement := range payload.Plan.Probes {
					if requirement.Hostname == host && requirement.Path == path && requirement.Address == address {
						now := time.Now().UTC()
						proof := routeprobe.Proof{Digest: requirement.RouteDigest, Version: "serving", EdgeID: requirement.EdgeID, GroupID: requirement.EdgeGroupID, State: state, CheckedAt: now, ValidUntil: now.Add(time.Minute), TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: routeID, PolicyDigest: payload.Lineage.PolicyDigest, IntentDigest: payload.Lineage.IntentDigest, InputSnapshotDigest: payload.Lineage.InputSnapshotDigest, ReleaseID: candidate.Release.ID, ReleaseChannel: candidate.Release.ReleaseChannel, FencingToken: candidate.Release.FencingToken, ScopeKey: "global"}}
						if scenario.negative && attempts > 0 && (continuous || attempts == 1) {
							proof.TrafficRelease.FencingToken++
						}
						return proof, nil
					}
				}
				return routeprobe.Proof{}, errors.New("unexpected probe")
			}
			now := time.Now().UTC()
			checkpoint := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: cfg.DNSNodeID, GroupID: cfg.EdgeGroupID, Parent: parent, Candidate: baseline, AppliedAt: now.Add(-time.Minute), Positive: true}
			if err := s.signDNSCheckpoint(&checkpoint); err != nil {
				t.Fatal(err)
			}
			saved := mustDNSJSON(t, checkpoint)
			if err := os.WriteFile(cfg.CachePath+".platform-serving.json", saved, 0600); err != nil {
				t.Fatal(err)
			}
			facts := collectDNSReadinessFacts(context.Background(), payload.Plan, payload.Policy.DNSReadiness, probe)
			old, err = buildDNSServingState(checkpoint, payload, routeID, cfg.DNSNodeID, cfg.EdgeGroupID, facts, now.Add(-time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			s.platformServing.Store(old)
			s.platformServingBound.Store(true)
			err = s.syncPlatformDNSServing(context.Background(), probe, func(*dnsServingState) error { return nil })
			if continuous {
				if !errors.Is(err, platformconsumer.ErrAssignmentChanged) || attempts != 3 || reports != 0 || s.platformServing.Load() != old {
					t.Fatal("churning publication was acknowledged or retried without bounds", attempts, reports, err)
				}
				after, readErr := os.ReadFile(cfg.CachePath + ".platform-serving.json")
				if readErr != nil || string(after) != string(saved) {
					t.Fatal("publication race overwrote positive LKG", readErr)
				}
				if got := old.answer(query, "", now.Add(2*time.Minute)); got.Rcode != dns.RcodeServerFailure {
					t.Fatal("assignment retry renewed proof validity")
				}
			} else if err != nil || attempts != 2 || reports != 1 || !reflect.DeepEqual(s.platformServing.Load().record.Candidate.Assignment, candidate.Assignment) {
				t.Fatal("assignment race did not converge with fresh authority", attempts, reports, err)
			}
		})
	}
}

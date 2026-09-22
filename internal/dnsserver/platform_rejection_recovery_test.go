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
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func TestRejectedDNSCandidateRefreshesRetainedReleaseWithoutRenewingAuthority(t *testing.T) {
	for _, scenario := range []string{"parent-download", "parent-signature", "replay", "candidate-readiness", "listener", "persistence", "foreign-old-proof", "expired-checkpoint", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			parent, baseline := dnsServingFixture(t, true)
			candidate := baseline
			candidate.Release.ID = "next"
			candidate.Release.FencingToken++
			candidate.Assignment.ArtifactReleaseID = "next"
			candidate.Assignment.FencingToken++
			if scenario == "replay" {
				candidate.Release.FencingToken = 1
				candidate.Assignment.FencingToken = 1
			}
			var reports atomic.Int32
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/platform-state/consumers/identity":
					json.NewEncoder(w).Encode(map[string]any{"token": "token", "expires_at": time.Now().Add(time.Minute), "component": "dns-server", "node_id": "dns-a", "scope_key": "global", "artifact_kinds": []string{candidate.Artifact.ArtifactKind}})
				case "/v1/platform-state/consumers/assignment":
					json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{candidate.Assignment}})
				case "/v1/platform-state/consumers/artifacts/dns":
					json.NewEncoder(w).Encode(candidate)
				case "/v1/platform-state/consumers/artifacts/parent":
					if scenario == "parent-download" {
						w.WriteHeader(503)
						return
					}
					p := parent
					if scenario == "parent-signature" || scenario == "foreign-old-proof" || scenario == "expired-checkpoint" {
						p.Provenance.Signature = "invalid"
					}
					json.NewEncoder(w).Encode(map[string]any{"artifact": p, "assignment": candidate.Assignment, "release": candidate.Release})
				case "/v1/platform-state/consumers/trusted-heartbeat":
					reports.Add(1)
					w.WriteHeader(500)
				default:
					t.Error("unexpected endpoint", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer remote.Close()
			dir := t.TempDir()
			cfg := config.DNSConfig{APIURL: remote.URL, DNSNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", CachePath: filepath.Join(dir, "cache"), BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}
			s := NewService(cfg, nil)
			s.PlatformTokenFile = filepath.Join(dir, "token")
			if err := os.WriteFile(s.PlatformTokenFile, []byte("pod-token"), 0600); err != nil {
				t.Fatal(err)
			}
			p, routeID, err := s.verifyDNSServingRelease(parent, baseline)
			if err != nil {
				t.Fatal(err)
			}
			applied := time.Now().Add(-time.Minute)
			if scenario == "expired-checkpoint" {
				applied = time.Now().Add(-2 * time.Hour)
			}
			checkpoint := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: cfg.DNSNodeID, GroupID: cfg.EdgeGroupID, Parent: parent, Candidate: baseline, AppliedAt: applied, Positive: true}
			if err = s.signDNSCheckpoint(&checkpoint); err != nil {
				t.Fatal(err)
			}
			saved := mustDNSJSON(t, checkpoint)
			cache := cfg.CachePath + ".platform-serving.json"
			if err = os.WriteFile(cache, saved, 0600); err != nil {
				t.Fatal(err)
			}
			old, err := buildDNSServingState(checkpoint, p, routeID, cfg.DNSNodeID, cfg.EdgeGroupID, nil, time.Now().Add(-time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			s.platformServing.Store(old)
			s.platformServingBound.Store(true)
			if scenario == "persistence" {
				s.Config.CachePath = filepath.Join(dir, "blocked", "cache")
				if err = os.WriteFile(filepath.Join(dir, "blocked"), []byte("not-directory"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var probes atomic.Int32
			probe := func(_ context.Context, host, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
				n := probes.Add(1)
				selected := baseline
				if (scenario == "listener" || scenario == "persistence") && n <= int32(len(p.Plan.Probes)) {
					selected = candidate
				}
				for _, req := range p.Plan.Probes {
					if req.Hostname == host && req.Path == path && req.Address == address {
						now := time.Now().UTC()
						proof := routeprobe.Proof{Digest: req.RouteDigest, Version: "serving", EdgeID: req.EdgeID, GroupID: req.EdgeGroupID, CheckedAt: now, ValidUntil: now.Add(time.Minute), TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: routeID, PolicyDigest: p.Lineage.PolicyDigest, IntentDigest: p.Lineage.IntentDigest, InputSnapshotDigest: p.Lineage.InputSnapshotDigest, ReleaseID: selected.Release.ID, ReleaseChannel: selected.Release.ReleaseChannel, FencingToken: selected.Release.FencingToken, ScopeKey: "global"}}
						if scenario == "foreign-old-proof" {
							proof.TrafficRelease.FencingToken++
						}
						return proof, nil
					}
				}
				return routeprobe.Proof{}, errors.New("unexpected probe")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "cancelled" {
				cancel()
			}
			err = s.syncPlatformDNSServingOnce(ctx, probe, func(*dnsServingState) error {
				if scenario == "listener" {
					return errors.New("candidate listener failed")
				}
				return nil
			})
			if err == nil {
				t.Fatal("candidate failure hidden")
			}
			actual := s.platformServing.Load()
			if !reflect.DeepEqual(actual.record, checkpoint) {
				t.Fatal("fallback changed artifact/release/applied time/signature")
			}
			after, e := os.ReadFile(cache)
			if e != nil || string(after) != string(saved) {
				t.Fatal("fallback changed persisted checkpoint", e)
			}
			if reports.Load() != 0 {
				t.Fatal("candidate emitted apply-success evidence")
			}
			if scenario == "cancelled" {
				if probes.Load() != 0 || actual != old {
					t.Fatal("cancelled sync refreshed")
				}
				return
			}
			if probes.Load() == 0 || !actual.checkedAt.After(old.checkedAt) || actual.fallback != "candidate_rejected" {
				t.Fatal("rejected candidate starved retained readiness", actual.fallback, probes.Load())
			}
			q := new(dns.Msg)
			q.SetQuestion("app.example.test.", dns.TypeA)
			answer := actual.answer(q, "", time.Now())
			switch scenario {
			case "foreign-old-proof":
				if len(answer.Answer) != 0 || dnsServingReady(actual, time.Now()) {
					t.Fatal("foreign release proof granted readiness")
				}
			case "expired-checkpoint":
				if answer.Rcode != dns.RcodeServerFailure || dnsServingReady(actual, time.Now()) {
					t.Fatal("refresh renewed stale artifact authority")
				}
			default:
				if answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 || !dnsServingReady(actual, time.Now()) {
					t.Fatal("valid old release stopped serving", answer)
				}
			}
		})
	}
}

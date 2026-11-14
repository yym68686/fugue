package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
)

func TestDNSLongObservationRenewsIdentityWithoutAcknowledgingFailedSuccessor(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-assignment", true: "changed-assignment"}[changed], func(t *testing.T) {
			id := platformconsumer.Identity{Token: "expired-after-long-observation", ExpiresAt: time.Now().Add(-time.Second), Component: "dns-server", NodeID: "node-a", ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindDNSAnswerBundle}}
			a := model.PlatformConsumerAssignment{ReleaseSetID: "successor", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: "global", ExpectedConsumerSetID: "expected", FencingToken: 8, ReleaseChannel: "gray", ExpectedGeneration: "new-generation"}
			reports := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/platform-state/consumers/identity" {
					next := id
					next.Token, next.ExpiresAt = "renewed", time.Now().Add(2*time.Minute)
					json.NewEncoder(w).Encode(next)
					return
				}
				if r.Header.Get("Authorization") != "Bearer renewed" {
					t.Error("long DNS observation used expired identity")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if r.URL.Path == "/v1/platform-state/consumers/assignment" {
					next := a
					if changed {
						next.FencingToken++
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{next}})
					return
				}
				if r.URL.Path != "/v1/platform-state/consumers/trusted-heartbeat" {
					t.Error("unexpected endpoint")
					w.WriteHeader(http.StatusNotFound)
					return
				}
				reports++
				var h platformcontrol.PlatformConsumerHeartbeatEnvelope
				if json.NewDecoder(r.Body).Decode(&h) != nil || h.ApplyStatus != "failed" || h.ProbeStatus != "failed" || !h.ServingLKG || h.ActualGeneration != "old-generation" || h.DesiredGeneration != "new-generation" {
					t.Error("renewal turned a rejected successor into a positive acknowledgement")
				}
				json.NewEncoder(w).Encode(model.PlatformConsumerHeartbeatResponse{Consumer: model.PlatformConsumerInstance{IdentityVerified: true, ConsumerID: h.ConsumerID, Sequence: h.Sequence, EvidenceHash: h.EvidenceHash, ExpectedConsumerSetID: h.ExpectedConsumerSetID}})
			}))
			defer server.Close()
			cache := filepath.Join(t.TempDir(), "dns")
			token := cache + ".token"
			if err := os.WriteFile(token, []byte("pod-token"), 0600); err != nil {
				t.Fatal(err)
			}
			s := NewService(config.DNSConfig{CachePath: cache}, nil)
			appliedAt := time.Now().Add(-time.Hour)
			loaded := &dnsServingState{record: dnsServingCheckpoint{Positive: true, AppliedAt: appliedAt, Candidate: dnsPlatformCandidate{Assignment: model.PlatformConsumerAssignment{ExpectedGeneration: "old-generation"}}}}
			s.platformServing.Store(loaded)
			err := s.reportDNSAssignmentState(context.Background(), platformconsumer.Client{BaseURL: server.URL, TokenFile: token}, id, a, loaded, platformcontrol.PlatformConsumerHeartbeatEnvelope{ActualGeneration: "old-generation", ApplyStatus: "failed", ProbeStatus: "failed", ServingLKG: true})
			if changed {
				if !errors.Is(err, platformconsumer.ErrAssignmentChanged) || reports != 0 {
					t.Fatal("renewed identity bypassed assignment CAS", err)
				}
			} else if err != nil || reports != 1 {
				t.Fatal("long DNS observation lost its authenticated failure heartbeat", err)
			}
			if s.platformServing.Load() != loaded || !loaded.record.AppliedAt.Equal(appliedAt) {
				t.Fatal("credential renewal changed publication identity or configuration age")
			}
		})
	}
}

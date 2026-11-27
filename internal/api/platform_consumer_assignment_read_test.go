package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

func TestConsumerReadsDiscardMixedParentAuthority(t *testing.T) {
	for _, endpoint := range []string{"assignment", "parent", "child"} {
		for _, scenario := range []string{"stabilizes", "keeps-changing", "fresh-read-unavailable"} {
			t.Run(endpoint+"/"+scenario, func(t *testing.T) {
				s, _, parent, _ := retainedTrafficAPIFixture(t)
				claims := platformcontrol.PlatformComponentIdentityClaims{Component: model.PlatformConsumerComponentEdgeWorker, NodeID: "edge-a", ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle}}
				initial, err := s.resolvePlatformConsumerAssignments(claims)
				if err != nil || len(initial) == 0 {
					t.Fatal("fixture has no assignment", err)
				}
				wanted := initial[0]
				artifactID := parent.ID
				if endpoint == "child" {
					artifactID = wanted.Artifact.ID
				}
				sessions := 0
				factory := func() func(string) (model.PlatformArtifact, error) {
					sessions++
					parentReads := 0
					load := s.store.NewPlatformArtifactReadSession()
					return func(id string) (model.PlatformArtifact, error) {
						artifact, err := load(id)
						if id == parent.ID && err == nil {
							parentReads++
							if parentReads == 2 {
								if scenario == "fresh-read-unavailable" {
									return model.PlatformArtifact{}, errors.New("synthetic fresh read outage")
								}
								if sessions == 1 || scenario == "keeps-changing" {
									artifact.UpdatedAt = artifact.UpdatedAt.Add(time.Second)
								}
							}
						}
						return artifact, err
					}
				}
				if endpoint == "assignment" {
					items, _, err := s.readCheckedConsumerAssignments(t.Context(), claims, false, time.Now(), factory)
					if (err == nil && len(items) > 0) != (scenario == "stabilizes") {
						t.Fatal("unexpected assignment result", len(items), err)
					}
				} else {
					item, status, message := s.readCheckedConsumerArtifact(t.Context(), claims, artifactID, wanted.Assignment.ExpectedConsumerSetID, factory)
					if scenario == "stabilizes" {
						if status != http.StatusOK || item.Artifact.ID != artifactID || !reflect.DeepEqual(item.Assignment, wanted.Assignment) || !reflect.DeepEqual(item.Release, wanted.Release) {
							t.Fatal("fresh result lost exact artifact or assignment", status, message)
						}
					} else if status != http.StatusServiceUnavailable || item.Artifact.ID != "" {
						t.Fatal("unstable authority disclosed artifact", status, message)
					}
				}
				wantSessions := 2
				if scenario == "fresh-read-unavailable" {
					wantSessions = 1
				}
				if sessions != wantSessions {
					t.Fatalf("made %d sessions, want %d", sessions, wantSessions)
				}
			})
		}
	}
}

func assignmentReadStageCounts(t *testing.T, s *Server) map[string]uint64 {
	t.Helper()
	value, err := s.OperationSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot struct {
		Stages []struct {
			Stage string `json:"stage"`
			Count uint64 `json:"count"`
		} `json:"stages"`
	}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	out := map[string]uint64{}
	for _, row := range snapshot.Stages {
		out[row.Stage] = row.Count
	}
	return out
}

func TestConsumerAssignmentReadRetriesCompleteVerificationTransition(t *testing.T) {
	for _, servingOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "all", true: "serving"}[servingOnly], func(t *testing.T) {
			f := setupAgentAuthorityFixture(t)
			s := f.s
			claims := platformcontrol.PlatformComponentIdentityClaims{Component: model.PlatformConsumerComponentEdgeWorker, NodeID: "edge-a", ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig}}
			_, gray, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "gray")
			if err != nil || !found || gray.VerificationState == "verified" {
				t.Fatal("fixture must start unverified", gray, err)
			}
			sessions := 0
			verified := false
			factory := func() func(string) (model.PlatformArtifact, error) {
				sessions++
				load := s.store.NewPlatformArtifactReadSession()
				parentReads := 0
				return func(id string) (model.PlatformArtifact, error) {
					a, err := load(id)
					if id == f.compiled.ReleaseArtifact.ID {
						parentReads++
					}
					// Mutate after discovery captured the gray release envelope
					// and before the final gray lookup. In serving-only mode
					// there is no preliminary read of the shared shadow parent.
					atBoundary := parentReads == 2
					if servingOnly {
						atBoundary = parentReads == 1 && id == f.compiled.RouteArtifact.ID
					}
					if err == nil && atBoundary && !verified {
						verified = true
						_, _, _, _, verifyErr := s.store.VerifyPlatformArtifactReleaseLKG(gray.ID, model.PlatformArtifactVerifyLKGRequest{FencingToken: gray.FencingToken, AllowInitialLKG: true, Reason: "synthetic concurrent verification", Evidence: model.PlatformArtifactVerificationEvidence{ConsumerConvergence: true, LocalProbe: true, PlatformEvidence: true, WatchWindow: true, BaselineMonotonic: true, DatabaseRollbackCompatible: true, EvidenceRefs: []string{"synthetic"}}}, platformProducerPrincipal())
						if verifyErr != nil {
							t.Fatal(verifyErr)
						}
					}
					return a, err
				}
			}
			items, stage, err := s.readCheckedConsumerAssignments(context.Background(), claims, servingOnly, time.Now(), factory)
			if err != nil || stage != "" || sessions != 2 || !verified || len(items) == 0 {
				t.Fatal("stable verified reread did not recover", len(items), stage, err, sessions, verified)
			}
			current, err := s.store.GetPlatformArtifactRelease(gray.ID)
			if err != nil || current.VerificationState != "verified" {
				t.Fatal(current, err)
			}
			grayItems := 0
			for _, item := range items {
				if item.Release.ID == gray.ID {
					grayItems++
					if !reflect.DeepEqual(item.Release, current) {
						t.Fatal("returned pre-verification envelope")
					}
				}
			}
			if grayItems != 2 {
				t.Fatal("lost authorized artifact kinds", grayItems)
			}
			counts := assignmentReadStageCounts(t, s)
			if counts["consumer-assignment-retry"] != 1 || counts["consumer-assignment-retry-ok"] != 1 || counts["consumer-assignment-retry-exhausted"] != 0 {
				t.Fatal(counts)
			}
			s.captures.mu.Lock()
			defer s.captures.mu.Unlock()
			if len(s.captures.consumerFailures) != 1 || s.captures.consumerFailures[0].InputRead == nil || s.captures.consumerFailures[0].InputRead.Stage != "assignment_changed" {
				t.Fatal("recovered original attempt evidence was lost", s.captures.consumerFailures)
			}
		})
	}
}

func TestConsumerAssignmentReadDoesNotRetryOtherFailures(t *testing.T) {
	for _, failure := range []string{"cold", "database", "conflict", "canceled"} {
		t.Run(failure, func(t *testing.T) {
			s, _, _, _ := retainedTrafficAPIFixture(t)
			claims := platformcontrol.PlatformComponentIdentityClaims{Component: model.PlatformConsumerComponentEdgeWorker, NodeID: "edge-a", ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "canceled" {
				cancel()
			}
			sessions := 0
			factory := func() func(string) (model.PlatformArtifact, error) {
				sessions++
				return func(string) (model.PlatformArtifact, error) {
					err := store.ErrNotFound
					switch failure {
					case "database":
						err = errors.New("synthetic database read failure")
					case "conflict":
						err = store.ErrConflict
					}
					return model.PlatformArtifact{}, err
				}
			}
			items, _, err := s.readCheckedConsumerAssignments(ctx, claims, false, time.Now(), factory)
			if err == nil || len(items) != 0 || sessions != 1 {
				t.Fatal("unrelated failure retried or disclosed assignments", len(items), err, sessions)
			}
			for name, count := range assignmentReadStageCounts(t, s) {
				if (name == "consumer-assignment-retry" || name == "consumer-assignment-retry-ok" || name == "consumer-assignment-retry-empty" || name == "consumer-assignment-retry-exhausted") && count != 0 {
					t.Fatal("unrelated failure counted as assignment change", name, count)
				}
			}
		})
	}
}

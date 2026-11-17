package api

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

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
					// At the final authority reread, after the first discovery has
					// captured the old envelope, perform the real LKG transition.
					if err == nil && parentReads == 2 && !verified {
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

func TestConsumerAssignmentReadRetryRemainsBoundedAndFailClosed(t *testing.T) {
	for _, change := range []string{"stable", "continued-change", "removed-membership", "revoked-key", "cold-input", "canceled"} {
		t.Run(change, func(t *testing.T) {
			s, _, parent, path := retainedTrafficAPIFixture(t)
			claims := platformcontrol.PlatformComponentIdentityClaims{Component: model.PlatformConsumerComponentEdgeWorker, NodeID: "edge-a", ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mutate := func() {
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var state model.State
				if err := json.Unmarshal(raw, &state); err != nil {
					t.Fatal(err)
				}
				for i := range state.PlatformArtifactReleases {
					state.PlatformArtifactReleases[i].UpdatedAt = state.PlatformArtifactReleases[i].UpdatedAt.Add(time.Second)
				}
				raw, err = json.Marshal(state)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			sessions := 0
			factory := func() func(string) (model.PlatformArtifact, error) {
				sessions++
				if sessions == 2 {
					switch change {
					case "removed-membership":
						sets, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ReleaseSetID: parent.ID})
						if err != nil {
							t.Fatal(err)
						}
						for _, set := range sets {
							set.ID = model.NewID("expect")
							set.Revision += 100
							for i := range set.Consumers {
								set.Consumers[i].NodeID = "replacement-node"
								set.Consumers[i].ConsumerID = set.Consumers[i].Component + ":replacement-node"
							}
							if _, err := s.store.CreatePlatformExpectedConsumerSet(set); err != nil {
								t.Fatal(err)
							}
						}
					case "revoked-key":
						s.bundleRevokedKeyIDs = append(s.bundleRevokedKeyIDs, parent.Provenance.KeyID)
						s.store.ConfigurePlatformArtifactSigning(s.bundleKeyring())
					case "canceled":
						cancel()
					}
				}
				load := s.store.NewPlatformArtifactReadSession()
				parentReads := 0
				return func(id string) (model.PlatformArtifact, error) {
					if sessions == 2 && change == "cold-input" {
						return model.PlatformArtifact{}, store.ErrNotFound
					}
					a, err := load(id)
					if id == parent.ID {
						parentReads++
					}
					if err == nil && parentReads == 2 && (sessions == 1 || change == "continued-change") {
						mutate()
					}
					return a, err
				}
			}
			items, _, err := s.readCheckedConsumerAssignments(ctx, claims, false, time.Now(), factory)
			if sessions != 2 {
				t.Fatal("must make exactly two complete attempts", sessions, err)
			}
			switch change {
			case "stable":
				if err != nil || len(items) == 0 {
					t.Fatal("stable reread rejected", items, err)
				}
			case "removed-membership":
				if err != nil || len(items) != 0 {
					t.Fatal("resurrected removed consumer", items)
				}
			default:
				if err == nil || len(items) != 0 {
					t.Fatal("failed reread disclosed old assignments", len(items), err)
				}
				if change == "continued-change" && !errors.Is(err, store.ErrConflict) {
					t.Fatal("continued change did not preserve conflict", err)
				}
			}
			counts := assignmentReadStageCounts(t, s)
			if counts["consumer-assignment-retry"] != 1 || counts["consumer-assignment-retry-exhausted"] != map[bool]uint64{true: 1}[change == "continued-change"] {
				t.Fatal("retry counters changed", counts)
			}
			if counts["consumer-assignment-retry-empty"] != map[bool]uint64{true: 1}[change == "removed-membership"] || counts["consumer-assignment-retry-ok"] != map[bool]uint64{true: 1}[change == "stable"] {
				t.Fatal("retry result counters changed", counts)
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

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
	"fugue/internal/testfixture/celldns"
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

func TestDNSOnlyAssignmentReadRetriesSelectedPublicationChange(t *testing.T) {
	for _, scenario := range []string{"gray-to-full", "full-to-gray", "continued-change", "missing-membership", "no-selection", "selection-error", "same-release"} {
		t.Run(scenario, func(t *testing.T) {
			req, _, _, inputs, _ := celldns.CompletePublication(t, false, false)
			before := celldns.Compile(t, req)
			after := before
			if scenario != "gray-to-full" {
				// Also exercise a change of signed parent, not only promotion
				// of the same parent with a different release ID and fence.
				req.Intent.Generation += "-next"
				after = celldns.Compile(t, req)
				after.DNSArtifact = celldns.Sign(t, after.DNSArtifact, "dns-child-next", 2)
				after.ReleaseArtifact = celldns.Sign(t, platformconfig.BuildReleaseSetArtifact(after.ReleaseSet, []string{after.DNSArtifact.ID}, req.CreatedAt), "dns-parent-next", 2)
			}
			path := t.TempDir() + "/state.json"
			st := store.New(path)
			if err := st.Init(); err != nil {
				t.Fatal(err)
			}
			st.ConfigurePlatformArtifactSigning(celldns.Keys())
			persistInput := func(c platformconfig.CompileResult) {
				t.Helper()
				if err := st.EnsurePlatformCompilerInput(c.InputSnapshot, c.Lineage.InputSnapshotDigest); err != nil {
					t.Fatal(err)
				}
			}
			persistInput(before)
			persistInput(after)
			for _, c := range inputs {
				persistInput(c)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var state model.State
			if err := json.Unmarshal(raw, &state); err != nil {
				t.Fatal(err)
			}
			write := func() {
				t.Helper()
				raw, err := json.Marshal(state)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			for i, source := range req.CellRoutePublications {
				policy := celldns.Sign(t, inputs[i].PolicyArtifact, "source-policy-"+source.Parent.ID, 1)
				state.PlatformArtifacts = append(state.PlatformArtifacts, source.Parent, source.Route, source.TLS, policy)
				state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, celldns.Publication(source))
			}
			add := func(c platformconfig.CompileResult, id, lane string, fence int64, offset time.Duration, withMember bool) model.PlatformArtifactRelease {
				t.Helper()
				p := c.ReleaseArtifact
				seen := false
				for _, a := range state.PlatformArtifacts {
					seen = seen || a.ID == p.ID
				}
				if !seen {
					state.PlatformArtifacts = append(state.PlatformArtifacts, p, c.DNSArtifact)
				}
				foundPolicy := false
				for _, a := range state.PlatformArtifacts {
					foundPolicy = foundPolicy || a.ArtifactKind == c.PolicyArtifact.ArtifactKind && a.ScopeKey == p.ScopeKey && a.Generation == c.PolicyArtifact.Generation
				}
				if !foundPolicy {
					state.PlatformArtifacts = append(state.PlatformArtifacts, celldns.Sign(t, c.PolicyArtifact, "policy-"+p.ID, fence))
				}
				r := model.PlatformArtifactRelease{ID: id, ArtifactID: p.ID, ArtifactKind: p.ArtifactKind, Scope: p.Scope, ScopeKey: p.ScopeKey,
					Generation: p.Generation, ReleaseChannel: lane, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: req.CreatedAt.Add(offset),
					LaneKey: platformsafety.ReleaseLaneKey(p.ArtifactKind, p.ScopeKey, lane), FencingToken: fence}
				if lane == "gray" {
					r.CanaryRuleRef = "cohort=complete"
				}
				state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
				if withMember {
					topology, _, err := platformcontrol.DeclaredTrafficConsumerTopology(p)
					if err != nil {
						t.Fatal(err)
					}
					set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: p.ID, ArtifactReleaseID: r.ID,
						ArtifactKind: c.DNSArtifact.ArtifactKind, Generation: c.DNSArtifact.Generation, Scope: p.Scope, ScopeKey: p.ScopeKey, Revision: 1,
						PreparedAt: r.ReleasedAt, Topology: topology})
					if err != nil {
						t.Fatal(err)
					}
					state.ExpectedConsumerSets = append(state.ExpectedConsumerSets, set)
				}
				return r
			}
			firstLane, nextLane := "full", "gray"
			if scenario == "gray-to-full" {
				firstLane, nextLane = "gray", "full"
			}
			first := add(before, "initial", firstLane, 4, 0, true)
			write()
			s := &Server{store: st}
			claims := platformcontrol.PlatformComponentIdentityClaims{Component: model.PlatformConsumerComponentDNSServer, NodeID: "dns-a", AuthorityID: "cell-dns",
				ScopeKey: first.ScopeKey, ArtifactKinds: []string{model.PlatformArtifactKindDNSAnswerBundle}}
			sessions, transitions := 0, 0
			var next model.PlatformArtifactRelease
			factory := func() func(string) (model.PlatformArtifact, error) {
				sessions++
				load := st.NewPlatformArtifactReadSession()
				watched := first.ArtifactID
				if sessions == 2 {
					watched = next.ArtifactID
				}
				reads := 0
				return func(id string) (model.PlatformArtifact, error) {
					a, err := load(id)
					if id == watched {
						reads++
					}
					if err != nil || reads != 2 || id != watched || (sessions > 1 && scenario != "continued-change") {
						return a, err
					}
					// The final resolver already read the old release row.
					// Change selection after discovery and real input admission.
					transitions++
					switch scenario {
					case "no-selection":
						state.PlatformArtifactReleases = state.PlatformArtifactReleases[:len(state.PlatformArtifactReleases)-1]
					case "selection-error":
						broken := add(after, "broken", nextLane, 5, time.Second, true)
						state.PlatformArtifactReleases[len(state.PlatformArtifactReleases)-1].ArtifactID = broken.ArtifactID + "-missing"
					case "same-release":
						state.PlatformArtifactReleases[len(state.PlatformArtifactReleases)-1].FencingToken++
					default:
						if sessions == 1 {
							next = add(after, "next", nextLane, 5, time.Second, scenario != "missing-membership")
						} else {
							add(before, "newer", firstLane, 6, 2*time.Second, true)
						}
					}
					write()
					return a, err
				}
			}
			items, stage, err := s.readCheckedConsumerAssignments(t.Context(), claims, true, time.Now(), factory)
			success := scenario == "gray-to-full" || scenario == "full-to-gray"
			empty := scenario == "missing-membership"
			exhausted := scenario == "continued-change"
			retry := success || empty || exhausted
			switch {
			case success:
				if err != nil || stage != "" || len(items) != 1 || !reflect.DeepEqual(items[0].Release, next) ||
					items[0].Assignment.ArtifactID != after.DNSArtifact.ID || items[0].Assignment.FencingToken != next.FencingToken {
					t.Fatal("did not return only the newly authorized publication", items, err, stage)
				}
			case empty:
				if err != nil || stage != "" || len(items) != 0 {
					t.Fatal("disclosed an old or unauthorized publication", items, err, stage)
				}
			default:
				if !errors.Is(err, store.ErrConflict) || stage != "input_recheck" || len(items) != 0 {
					t.Fatal("did not fail closed", items, err, stage)
				}
			}
			want := map[bool]int{true: 1}
			if sessions != 1+want[retry] || transitions != 1+want[exhausted] {
				t.Fatal("unexpected attempt or transition count", sessions, transitions)
			}
			counts := assignmentReadStageCounts(t, s)
			if counts["consumer-assignment-retry"] != uint64(want[retry]) ||
				counts["consumer-assignment-retry-ok"] != uint64(want[success]) ||
				counts["consumer-assignment-retry-empty"] != uint64(want[empty]) ||
				counts["consumer-assignment-retry-exhausted"] != uint64(want[exhausted]) {
				t.Fatal(counts)
			}
			failures := s.captures.consumerFailures
			wantStage := "serving_not_selected"
			if scenario == "selection-error" {
				wantStage = "serving_selection_read"
			}
			if len(failures) != 1+want[exhausted] || failures[0].InputRead == nil || failures[0].InputRead.Stage != wantStage {
				t.Fatal("original selection failure was not retained", failures)
			}
		})
	}
}

package platformconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestShadowTransportRejectsRedirectAndUnboundResponse(t *testing.T) {
	var credentialLeak bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { credentialLeak = true }))
	defer other.Close()
	for _, mode := range []string{"redirect", "wrong-node", "wrong-scope", "missing-capability", "ambiguous", "changed-assignment", "oversized", "trailing-json", "valid", "valid-cell", "foreign-cell", "missing-cell-id", "legacy-id-in-cell"} {
		t.Run(mode, func(t *testing.T) {
			assignment := model.PlatformConsumerAssignment{ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ArtifactID: "candidate", ScopeKey: "global", ReleaseChannel: "shadow", ExpectedConsumerSetID: "set"}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/platform-state/consumers/identity":
					if mode == "redirect" {
						http.Redirect(w, r, other.URL, 307)
						return
					}
					id := Identity{Token: "component-token", Component: "edge-worker", NodeID: "node", ScopeKey: "global", ArtifactKinds: []string{assignment.ArtifactKind}, ExpiresAt: time.Now().Add(time.Minute)}
					if mode == "wrong-node" {
						id.NodeID = "other"
					}
					if mode == "wrong-scope" {
						id.ScopeKey = "foreign"
					}
					if mode == "missing-capability" {
						id.ArtifactKinds = nil
					}
					if strings.Contains(mode, "cell") {
						id.AuthorityID, id.ConsumerID = "cell-a", "edge-worker:cell-a:node"
						if mode == "foreign-cell" {
							id.AuthorityID, id.ConsumerID = "cell-b", "edge-worker:cell-b:node"
						}
						if mode == "missing-cell-id" {
							id.ConsumerID = ""
						}
						if mode == "legacy-id-in-cell" {
							id.ConsumerID = "edge-worker:node"
						}
					}
					json.NewEncoder(w).Encode(id)
				case "/v1/platform-state/consumers/assignment":
					if r.Header.Get("Authorization") != "Bearer component-token" {
						t.Error("wrong credential")
					}
					a := []model.PlatformConsumerAssignment{assignment}
					if mode == "ambiguous" {
						a = append(a, assignment)
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: a})
				case "/v1/platform-state/consumers/artifacts/candidate":
					a := assignment
					if mode == "changed-assignment" {
						a.ExpectedConsumerSetID = "other"
					}
					json.NewEncoder(w).Encode(map[string]any{"assignment": a, "artifact": model.PlatformArtifact{ID: "candidate"}})
					if mode == "oversized" {
						w.Write([]byte(strings.Repeat(" ", 8<<20)))
					}
					if mode == "trailing-json" {
						w.Write([]byte(`{}`))
					}
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
				}
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(path, []byte("pod-token"), 0600); err != nil {
				t.Fatal(err)
			}
			client := Client{BaseURL: server.URL + "?authority_service=test", TokenFile: path}
			if strings.Contains(mode, "cell") {
				client.AuthorityID = "cell-a"
			}
			_, _, artifact, _, err := client.Sync(context.Background(), "edge-worker", "node", "global", assignment.ArtifactKind)
			if mode == "valid" || mode == "valid-cell" {
				if err != nil || artifact.ID != "candidate" {
					t.Fatalf("valid download failed: %v", err)
				}
			} else if err == nil {
				t.Fatal("unbound or invalid response accepted")
			}
			if credentialLeak {
				t.Fatal("redirect forwarded a credential")
			}
		})
	}
}

func TestSyncSelectsExplicitReleaseChannel(t *testing.T) {
	for _, channel := range []string{model.PlatformArtifactReleaseChannelShadow, model.PlatformArtifactReleaseChannelGray, model.PlatformArtifactReleaseChannelFull} {
		t.Run(channel, func(t *testing.T) {
			assignment := model.PlatformConsumerAssignment{ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ArtifactID: "candidate-" + channel, ScopeKey: "global", ReleaseChannel: channel, ExpectedConsumerSetID: "set-" + channel}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/platform-state/consumers/identity":
					_ = json.NewEncoder(w).Encode(Identity{Token: "component-token", Component: "edge-worker", NodeID: "node", ScopeKey: "global", ArtifactKinds: []string{assignment.ArtifactKind}, ExpiresAt: time.Now().Add(time.Minute)})
				case "/v1/platform-state/consumers/assignment":
					all := []model.PlatformConsumerAssignment{}
					for _, lane := range []string{"full", "gray", "shadow"} {
						item := assignment
						item.ReleaseChannel, item.ArtifactID, item.ExpectedConsumerSetID = lane, "candidate-"+lane, "set-"+lane
						all = append(all, item)
					}
					_ = json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: all})
				case "/v1/platform-state/consumers/artifacts/" + assignment.ArtifactID:
					_ = json.NewEncoder(w).Encode(map[string]any{"assignment": assignment, "artifact": model.PlatformArtifact{ID: assignment.ArtifactID}})
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
				}
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(path, []byte("pod-token"), 0600); err != nil {
				t.Fatal(err)
			}
			client := Client{BaseURL: server.URL, TokenFile: path}
			_, selected, _, _, err := client.SyncChannel(context.Background(), "edge-worker", "node", "global", assignment.ArtifactKind, channel)
			if err != nil || selected.ReleaseChannel != channel {
				t.Fatalf("channel assignment not selected: channel=%s selected=%s err=%v", channel, selected.ReleaseChannel, err)
			}
		})
	}
}

func TestSyncRejectsInvalidReleaseChannel(t *testing.T) {
	client := Client{}
	_, _, _, _, err := client.SyncChannel(context.Background(), "edge-worker", "node", "global", model.PlatformArtifactKindEdgeRouteBundle, "production")
	if err == nil || !strings.Contains(err.Error(), "release channel") {
		t.Fatalf("invalid release channel accepted: %v", err)
	}
}

func TestParentReadAllowsVerificationCompletionButRejectsAuthorityChanges(t *testing.T) {
	for _, scenario := range []string{"unchanged", "verified", "new-fence", "new-cohort", "failed", "rolled-back", "foreign-parent", "assignment-race", "verification-replay"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			a := model.PlatformConsumerAssignment{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ArtifactID: "dns", ReleaseSetID: "parent", ArtifactReleaseID: "release", ScopeKey: "global", ReleaseChannel: "full", ExpectedConsumerSetID: "set", FencingToken: 2}
			r := model.PlatformArtifactRelease{ID: "release", ArtifactID: "parent", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "global", Generation: "generation", ReleaseChannel: "full", Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 2, Version: 1, VerificationState: model.PlatformArtifactVerificationStateServingUnverified, ServingUnverifiedGeneration: "generation", ReleasedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute)}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/v1/platform-state/consumers/assignment" {
					next := a
					if scenario == "assignment-race" {
						next.FencingToken++
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{next}})
					return
				}
				after := r
				parent := model.PlatformArtifact{ID: "parent", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "global"}
				if scenario != "unchanged" {
					after.VerificationState, after.VerifiedLKGGeneration, after.ServingUnverifiedGeneration = model.PlatformArtifactVerificationStateVerified, r.Generation, ""
					after.VerifiedAt, after.UpdatedAt, after.Version = &now, now, 2
					after.VerificationEvidence = map[string]string{"consumer_convergence": "true"}
				}
				switch scenario {
				case "new-fence", "assignment-race":
					after.FencingToken++
				case "new-cohort":
					after.CanaryRuleRef = "cohort=other"
				case "failed":
					after.VerificationState = model.PlatformArtifactVerificationStateFailed
				case "rolled-back":
					after.Status = model.PlatformArtifactReleaseStatusRolledBack
				case "foreign-parent":
					parent.ID = "foreign"
				case "verification-replay":
					after.Version = 1
				}
				json.NewEncoder(w).Encode(map[string]any{"assignment": a, "artifact": parent, "release": after})
			}))
			defer server.Close()
			client := Client{BaseURL: server.URL}
			parent, err := client.ReleaseSet(context.Background(), Identity{Token: "identity", ScopeKey: a.ScopeKey, ArtifactKinds: []string{a.ArtifactKind}}, a, r)
			if scenario == "verified" || scenario == "unchanged" {
				if err != nil || parent.ID != "parent" {
					t.Fatal("same publication rejected", err)
				}
			} else if err == nil || errors.Is(err, ErrAssignmentChanged) != (scenario == "assignment-race") {
				t.Fatal("authority change incorrectly classified", err)
			}
		})
	}
}

func TestDownloadConflictRetriesOnlyAfterObservedAssignmentChange(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusConflict, http.StatusForbidden, http.StatusServiceUnavailable} {
		for _, changed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/changed=%t", code, changed), func(t *testing.T) {
				a := model.PlatformConsumerAssignment{ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ArtifactID: "child", ReleaseSetID: "parent", ScopeKey: "global", ReleaseChannel: "full", ExpectedConsumerSetID: "set", FencingToken: 1}
				reads := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/platform-state/consumers/identity":
						json.NewEncoder(w).Encode(Identity{Token: "identity", Component: "edge-worker", NodeID: "node", ScopeKey: "global", ArtifactKinds: []string{a.ArtifactKind}, ExpiresAt: time.Now().Add(time.Minute)})
					case "/v1/platform-state/consumers/assignment":
						reads++
						current := a
						if changed && reads > 1 {
							current.FencingToken++
						}
						json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{current}})
					default:
						w.WriteHeader(code)
					}
				}))
				defer server.Close()
				token := filepath.Join(t.TempDir(), "token")
				if err := os.WriteFile(token, []byte("pod-token"), 0600); err != nil {
					t.Fatal(err)
				}
				client := Client{BaseURL: server.URL, TokenFile: token}
				_, _, _, _, err := client.SyncServing(context.Background(), "edge-worker", "node", "global", a.ArtifactKind)
				wantRace := changed && (code == 404 || code == 409)
				if err == nil || errors.Is(err, ErrAssignmentChanged) != wantRace {
					t.Fatalf("child: race=%t err=%v", wantRace, err)
				}
				reads = 1
				_, err = client.ReleaseSet(context.Background(), Identity{Token: "identity", ScopeKey: "global", ArtifactKinds: []string{a.ArtifactKind}}, a, model.PlatformArtifactRelease{})
				if err == nil || errors.Is(err, ErrAssignmentChanged) != wantRace {
					t.Fatalf("parent: race=%t err=%v", wantRace, err)
				}
			})
		}
	}
}

func TestCheckAssignmentRejectsChangedMissingAndDuplicateAuthority(t *testing.T) {
	want := model.PlatformConsumerAssignment{ScopeKey: "global", ArtifactKind: "edge_route_bundle", ReleaseChannel: "shadow", ExpectedConsumerSetID: "expected", ArtifactID: "artifact", FencingToken: 5}
	for _, scenario := range []string{"same", "other lanes", "missing", "new fence", "new revision", "duplicate", "unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "Bearer identity-token" {
					t.Error("incorrect assignment request")
				}
				values := []model.PlatformConsumerAssignment{want}
				switch scenario {
				case "other lanes":
					item := want
					item.ReleaseChannel = "full"
					item.FencingToken = 1
					values = append(values, item)
				case "missing":
					values = nil
				case "new fence":
					values[0].FencingToken++
				case "new revision":
					values[0].ExpectedConsumerSetID = "other"
				case "duplicate":
					values = append(values, want)
				case "unavailable":
					w.WriteHeader(503)
					return
				}
				_ = json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: values})
			}))
			defer server.Close()
			client := Client{BaseURL: server.URL + "?authority_service=example"}
			err := client.CheckAssignment(context.Background(), Identity{Token: "identity-token"}, want)
			if (err == nil) != (scenario == "same" || scenario == "other lanes") {
				t.Fatalf("unexpected assignment result: %v", err)
			}
		})
	}
}

func TestHeartbeatConflictsKeepStandbyAndAssignmentRacesDistinct(t *testing.T) {
	for _, tc := range []struct {
		code   string
		status int
		want   error
	}{
		{"dns_backend_not_selected", 409, ErrDNSBackendNotSelected},
		{"platform_assignment_changed", 409, ErrAssignmentChanged},
		{"conflict", 409, nil}, {"dns_backend_not_selected", 403, nil},
	} {
		t.Run(fmt.Sprintf("%s/%d", tc.code, tc.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				json.NewEncoder(w).Encode(map[string]string{"code": tc.code, "error": "untrusted server detail secret"})
			}))
			defer server.Close()
			client := Client{BaseURL: server.URL}
			var reply any
			err := client.PostJSON(context.Background(), "/v1/platform-state/consumers/trusted-heartbeat", "identity", struct{}{}, &reply)
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) || tc.want == nil && (errors.Is(err, ErrDNSBackendNotSelected) || errors.Is(err, ErrAssignmentChanged)) || strings.Contains(err.Error(), "secret") {
				t.Fatal("incorrect conflict classification", err)
			}
			err = client.GetJSON(context.Background(), "/v1/platform-state/consumers/assignment", "identity", &reply)
			if errors.Is(err, ErrDNSBackendNotSelected) || errors.Is(err, ErrAssignmentChanged) {
				t.Fatal("another endpoint granted heartbeat semantics", err)
			}
		})
	}
}

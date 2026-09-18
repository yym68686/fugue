package platformconsumer

import (
	"context"
	"encoding/json"
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
	for _, mode := range []string{"redirect", "wrong-node", "wrong-scope", "missing-capability", "ambiguous", "changed-assignment", "oversized", "trailing-json", "valid"} {
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
			_, _, artifact, _, err := client.Sync(context.Background(), "edge-worker", "node", "global", assignment.ArtifactKind)
			if mode == "valid" {
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

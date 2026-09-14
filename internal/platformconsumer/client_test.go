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
	for _, mode := range []string{"redirect", "wrong-node", "missing-capability", "ambiguous", "changed-assignment", "oversized", "trailing-json", "valid"} {
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

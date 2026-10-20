package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestArtifactStateDoesNotTreatPublishedAsVerified(t *testing.T) {
	artifact := model.PlatformArtifact{ID: "artifact_test", ArtifactKind: "edge_route_bundle", ScopeKey: "global", Generation: "generation_test", ContentHash: "sha256:known"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/admin/artifacts/artifact_test":
			json.NewEncoder(w).Encode(map[string]any{"artifact": artifact})
		case "/v1/platform-state/artifacts/edge_route_bundle":
			json.NewEncoder(w).Encode(map[string]any{"artifact": artifact, "release": model.PlatformArtifactRelease{ID: "release_test", ArtifactID: artifact.ID, Status: "active", VerificationState: "serving_unverified"}})
		case "/v1/admin/artifacts/artifact_test/consumers":
			fmt.Fprint(w, `{"consumers":[]}`)
		case "/v1/admin/platform-state/convergence":
			fmt.Fprint(w, `{"convergence":[]}`)
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	client, _ := newClientWithOptions(srv.URL, "test", clientOptions{RequireToken: true})
	c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	view, err := c.loadArtifactState(client, artifact.ID, "full")
	if err != nil {
		t.Fatal(err)
	}
	if view.VerifiedLKG || view.ConsumersConverged {
		t.Fatalf("publication is not verification: %+v", view)
	}
	if len(view.MissingEvidence) == 0 {
		t.Fatal("missing expected consumers must be visible")
	}
}
func TestArtifactWaitDeadlineNeverPublishesOrVerifies(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" {
			t.Errorf("unexpected mutation %s", r.Method)
		}
		switch r.URL.Path {
		case "/v1/admin/artifacts/artifact_test":
			fmt.Fprint(w, `{"artifact":{"id":"artifact_test","artifact_kind":"edge_route_bundle","scope_key":"global","generation":"generation_test"}}`)
		case "/v1/admin/platform-state/convergence":
			fmt.Fprint(w, `{"convergence":[]}`)
		case "/v1/admin/artifacts/artifact_test/consumers":
			fmt.Fprint(w, `{"consumers":[]}`)
		default:
			fmt.Fprint(w, `{}`)
		}
	}))
	defer srv.Close()
	var out, stderr bytes.Buffer
	start := time.Now()
	err := runWithStreams([]string{"--base-url", srv.URL, "--token", "test", "--json", "admin", "artifact", "wait", "artifact_test", "--timeout", "30ms", "--interval", "1ms"}, &out, &stderr)
	if ExitCodeForError(err) != 6 || time.Since(start) > time.Second || calls == 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	var result artifactStateView
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.VerifiedLKG {
		t.Fatal("false verification")
	}
}

func TestCLIUsesAuthoritativeConvergenceAndRequiresAllReleaseMembers(t *testing.T) {
	artifact := model.PlatformArtifact{ID: "release-set", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "global", Generation: "generation", Content: map[string]any{"artifact_kinds": []any{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatal("state inspection mutated")
		}
		switch r.URL.Path {
		case "/v1/admin/artifacts/release-set":
			json.NewEncoder(w).Encode(map[string]any{"artifact": artifact})
		case "/v1/platform-state/artifacts/release_set":
			json.NewEncoder(w).Encode(map[string]any{"artifact": artifact, "release": model.PlatformArtifactRelease{ID: "current-release", ArtifactID: artifact.ID, Status: "active"}})
		case "/v1/admin/artifacts/release-set/consumers":
			fmt.Fprint(w, `{"consumers":[]}`)
		case "/v1/admin/platform-state/convergence":
			if r.URL.Query().Get("release_set_id") != artifact.ID || r.URL.Query().Get("artifact_release_id") != "current-release" || r.URL.Query().Get("artifact_kind") != "" {
				t.Fatal("CLI did not bind release query")
			}
			json.NewEncoder(w).Encode(map[string]any{"convergence": []model.PlatformConsumerConvergenceStatus{{ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, Pass: true}}})
		default:
			t.Errorf("unexpected local-evaluation read %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	client, _ := newClientWithOptions(srv.URL, "test", clientOptions{RequireToken: true})
	c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	view, err := c.loadArtifactState(client, artifact.ID, "shadow")
	if err != nil {
		t.Fatal(err)
	}
	if view.ConsumersConverged || len(view.Convergence) != 1 || len(view.MissingEvidence) == 0 {
		t.Fatal("partial authoritative assessments became full convergence", view)
	}
}

func TestCLIStateRetriesWhenServingArtifactChangesDuringRead(t *testing.T) {
	artifactA := model.PlatformArtifact{ID: "artifact-a", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "global", Generation: "generation-a", ContentHash: "hash-a", Content: map[string]any{"artifact_kinds": []any{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig}}}
	artifactB := model.PlatformArtifact{ID: "artifact-b", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "global", Generation: "generation-b", ContentHash: "hash-b", Content: map[string]any{"artifact_kinds": []any{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig}}}
	releaseB := model.PlatformArtifactRelease{ID: "release-b", ArtifactID: artifactB.ID, Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 2}
	currentReads := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("state inspection mutated: %s", r.Method)
		}
		switch r.URL.Path {
		case "/v1/admin/artifacts/artifact-a":
			json.NewEncoder(w).Encode(map[string]any{"artifact": artifactA})
		case "/v1/admin/artifacts/artifact-b":
			json.NewEncoder(w).Encode(map[string]any{"artifact": artifactB})
		case "/v1/platform-state/artifacts/release_set":
			currentReads++
			if currentReads == 1 {
				json.NewEncoder(w).Encode(map[string]any{"artifact": artifactB, "release": releaseB})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"artifact": artifactB, "release": releaseB})
		case "/v1/admin/artifacts/artifact-b/consumers":
			fmt.Fprint(w, `{"consumers":[]}`)
		case "/v1/admin/platform-state/convergence":
			if r.URL.Query().Get("release_set_id") != artifactB.ID || r.URL.Query().Get("artifact_release_id") != releaseB.ID || r.URL.Query().Get("artifact_kind") != "" {
				t.Fatalf("convergence was not bound to retried release: %s", r.URL.RawQuery)
			}
			json.NewEncoder(w).Encode(map[string]any{"convergence": []model.PlatformConsumerConvergenceStatus{{ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ExpectedGeneration: "route-b", Pass: true}, {ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ExpectedGeneration: "dns-b", Pass: true}, {ArtifactKind: model.PlatformArtifactKindCaddyRouteConfig, ExpectedGeneration: "tls-b", Pass: true}}})
		default:
			t.Errorf("unexpected state read %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	client, _ := newClientWithOptions(srv.URL, "test", clientOptions{RequireToken: true})
	c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	view, err := c.loadCurrentArtifactState(client, artifactA.ID, "full")
	if err != nil {
		t.Fatal(err)
	}
	if view.Artifact.ID != artifactB.ID || view.Current.Artifact == nil || view.Current.Artifact.ID != artifactB.ID || !view.ConsumersConverged || currentReads < 3 {
		t.Fatalf("state read did not retry to one serving snapshot: reads=%d view=%+v", currentReads, view)
	}
}

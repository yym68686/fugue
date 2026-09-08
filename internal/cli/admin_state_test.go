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
		case "/v1/admin/expected-consumer-sets":
			fmt.Fprint(w, `{"expected_consumer_sets":[]}`)
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
		case "/v1/admin/expected-consumer-sets":
			fmt.Fprint(w, `{"expected_consumer_sets":[]}`)
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

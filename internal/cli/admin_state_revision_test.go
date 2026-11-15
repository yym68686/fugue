package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/model"
)

func TestArtifactStateUsesNewestExactExpectationRevision(t *testing.T) {
	for _, scenario := range []string{"latest-pass", "latest-fail", "latest-missing", "ambiguous-revision", "inventory-truncated", "revision-changes", "continuous-revision-churn", "out-of-order", "wrong-assessment-scope", "wrong-assessment-generation", "wrong-expectation-release", "expectation-read-fails", "final-read-fails"} {
		t.Run(scenario, func(t *testing.T) {
			artifact := model.PlatformArtifact{ID: "release-set", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "global", Generation: "generation", Content: map[string]any{"artifact_kinds": []any{model.PlatformArtifactKindDNSAnswerBundle}}}
			release := model.PlatformArtifactRelease{ID: "release", ArtifactID: artifact.ID, Status: model.PlatformArtifactReleaseStatusActive}
			old := model.PlatformExpectedConsumerSet{ID: "expect-old", ReleaseSetID: artifact.ID, ArtifactReleaseID: release.ID, ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: artifact.ScopeKey, ExpectedGeneration: "dns-generation", Revision: 1}
			latest := old
			latest.ID, latest.Revision = "expect-new", 2
			newest := latest
			newest.ID, newest.Revision = "expect-newest", 3
			setReads, convergenceReads := 0, 0
			status := func(set model.PlatformExpectedConsumerSet, pass bool) model.PlatformConsumerConvergenceStatus {
				return model.PlatformConsumerConvergenceStatus{ExpectedConsumerSetID: set.ID, ArtifactKind: set.ArtifactKind, ScopeKey: set.ScopeKey, ExpectedGeneration: set.ExpectedGeneration, Pass: pass}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("state inspection mutated: %s", r.Method)
				}
				switch r.URL.Path {
				case "/v1/admin/artifacts/release-set":
					json.NewEncoder(w).Encode(map[string]any{"artifact": artifact})
				case "/v1/platform-state/artifacts/release_set":
					json.NewEncoder(w).Encode(platformStateArtifactEnvelope{Artifact: &artifact, Release: &release})
				case "/v1/admin/artifacts/release-set/consumers":
					json.NewEncoder(w).Encode(map[string]any{"consumers": []any{}})
				case "/v1/admin/expected-consumer-sets":
					setReads++
					if scenario == "expectation-read-fails" || (scenario == "final-read-fails" && setReads%2 == 0) {
						// Do not let transport retries turn a failed final read
						// into the next successful first-read fixture.
						w.WriteHeader(http.StatusForbidden)
						return
					}
					if r.URL.Query().Get("release_set_id") != artifact.ID || r.URL.Query().Get("artifact_release_id") != release.ID || r.URL.Query().Get("scope_key") != artifact.ScopeKey {
						t.Errorf("expectation read lost the exact release: %s", r.URL.RawQuery)
					}
					sets := []model.PlatformExpectedConsumerSet{old, latest}
					switch scenario {
					case "ambiguous-revision":
						newest.Revision = latest.Revision
						sets = append(sets, newest)
					case "inventory-truncated":
						for len(sets) < 200 {
							sets = append(sets, old)
						}
					case "revision-changes":
						if setReads > 1 {
							sets = append(sets, newest)
						}
					case "continuous-revision-churn":
						latest.Revision = int64(setReads + 1)
						sets = []model.PlatformExpectedConsumerSet{old, latest}
					case "out-of-order":
						sets = []model.PlatformExpectedConsumerSet{latest, old}
					case "wrong-expectation-release":
						sets[1].ArtifactReleaseID = "another-release"
					}
					json.NewEncoder(w).Encode(map[string]any{"expected_consumer_sets": sets})
				case "/v1/admin/platform-state/convergence":
					convergenceReads++
					statuses := []model.PlatformConsumerConvergenceStatus{status(old, scenario == "latest-fail")}
					if scenario != "latest-missing" {
						statuses = append(statuses, status(latest, scenario != "latest-fail"))
					}
					if scenario == "wrong-assessment-scope" {
						statuses[1].ScopeKey = "another-scope"
					}
					if scenario == "wrong-assessment-generation" {
						statuses[1].ExpectedGeneration = "another-generation"
					}
					if scenario == "revision-changes" && setReads > 1 {
						statuses = append(statuses, status(newest, true))
						statuses[1].Pass = false
					}
					json.NewEncoder(w).Encode(map[string]any{"convergence": statuses})
				default:
					t.Errorf("unexpected read %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, _ := newClientWithOptions(server.URL, "test", clientOptions{RequireToken: true})
			c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
			view, err := c.loadCurrentArtifactState(client, artifact.ID, "gray")
			if scenario == "continuous-revision-churn" || scenario == "final-read-fails" {
				if ExitCodeForError(err) != ExitCodeIndeterminate || view.ConsumersConverged || convergenceReads > artifactStateReadAttempts {
					t.Fatalf("revision churn became success or unbounded retry: err=%v reads=%d view=%+v", err, convergenceReads, view)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := scenario == "latest-pass" || scenario == "revision-changes" || scenario == "out-of-order"
			if view.ConsumersConverged != want {
				t.Fatalf("historical/missing/ambiguous evidence changed newest convergence: want=%v view=%+v", want, view)
			}
			if want && (len(view.Convergence) != 1 || view.Convergence[0].ExpectedConsumerSetID == old.ID) {
				t.Fatal("history remained in the current state projection", view)
			}
			if scenario == "revision-changes" && (convergenceReads != 2 || view.Convergence[0].ExpectedConsumerSetID != newest.ID) {
				t.Fatal("revision change was not reread within a bound", view)
			}
		})
	}
}

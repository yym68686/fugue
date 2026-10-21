package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestArtifactStateConsistencySafety(t *testing.T) {
	for _, scenario := range []string{"release-revoked", "lkg-revoked", "lkg-expires-during-read", "final-read-unavailable", "continuous-churn", "stable-authoritative-failure"} {
		t.Run(scenario, func(t *testing.T) {
			artifact := model.PlatformArtifact{ID: "artifact-fixed", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ScopeKey: "global", Generation: "generation-fixed", ContentHash: "hash-fixed"}
			release := model.PlatformArtifactRelease{ID: "release-fixed", ArtifactID: artifact.ID, Status: model.PlatformArtifactReleaseStatusActive, VerificationState: model.PlatformArtifactVerificationStateVerified}
			expires := time.Now().Add(time.Hour)
			if scenario == "lkg-expires-during-read" {
				expires = time.Now().Add(100 * time.Millisecond)
			}
			lkg := model.PlatformLKGSnapshot{ID: "lkg-fixed", ArtifactID: artifact.ID, Generation: artifact.Generation, ContentHash: artifact.ContentHash, GenerationSequence: 1, VerifiedByReleaseID: release.ID, VerificationEvidenceHash: "evidence", ExpiresAt: expires, SnapshotProvenance: model.PlatformArtifactProvenance{Signature: "snapshot-signature"}, ArtifactProvenance: model.PlatformArtifactProvenance{Signature: "artifact-signature"}}
			reads, convergences := 0, 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected mutation %s", r.Method)
				}
				switch r.URL.Path {
				case "/v1/admin/artifacts/artifact-fixed":
					json.NewEncoder(w).Encode(map[string]any{"artifact": artifact})
				case "/v1/platform-state/artifacts/edge_route_bundle":
					reads++
					currentRelease, currentLKG := release, lkg
					if reads > 1 {
						switch scenario {
						case "release-revoked":
							currentRelease.Status = "superseded"
						case "lkg-revoked":
							currentLKG.ExpiresAt = time.Now().Add(-time.Minute).Truncate(time.Hour)
						case "final-read-unavailable":
							if reads%2 == 0 {
								http.Error(w, "observation unavailable", http.StatusForbidden)
								return
							}
						case "continuous-churn":
							currentRelease.FencingToken = int64(reads)
						}
					}
					json.NewEncoder(w).Encode(platformStateArtifactEnvelope{Artifact: &artifact, Release: &currentRelease, LKG: &currentLKG})
				case "/v1/admin/artifacts/artifact-fixed/consumers":
					fmt.Fprint(w, `{"consumers":[]}`)
				case "/v1/admin/platform-state/convergence":
					convergences++
					if scenario == "lkg-expires-during-read" {
						time.Sleep(max(0, time.Until(expires.Add(20*time.Millisecond))))
					}
					json.NewEncoder(w).Encode(map[string]any{"convergence": []model.PlatformConsumerConvergenceStatus{{ArtifactKind: artifact.ArtifactKind, ExpectedGeneration: artifact.Generation, Pass: scenario != "stable-authoritative-failure"}}})
				default:
					t.Errorf("unexpected read %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer srv.Close()
			client, _ := newClientWithOptions(srv.URL, "test", clientOptions{RequireToken: true})
			c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
			view, err := c.loadCurrentArtifactState(client, artifact.ID, "full")
			if scenario == "continuous-churn" || scenario == "final-read-unavailable" {
				if ExitCodeForError(err) != ExitCodeIndeterminate || view.ConsumersConverged || view.VerifiedLKG || convergences != artifactStateReadAttempts {
					t.Fatalf("unestablished read became success or retried without bounds: err=%v reads=%d convergence=%d view=%+v", err, reads, convergences, view)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "stable-authoritative-failure" {
				if view.ConsumersConverged || convergences != 1 || !view.VerifiedLKG {
					t.Fatal("stable negative evidence was retried away or conflated with LKG", view)
				}
			} else if view.VerifiedLKG {
				t.Fatal("revoked or expired LKG was returned as verified", view)
			}
		})
	}
}

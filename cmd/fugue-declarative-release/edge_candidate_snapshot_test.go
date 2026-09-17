package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestStageCandidateRefreshesWorkerFactsWithoutRecoveringHealthyConfig(t *testing.T) {
	for _, scenario := range []string{"fresh publication", "changed grant", "changed code", "stale facts", "regressed publication", "snapshot unavailable", "conflict budget"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST", "sha256:"+strings.Repeat("7", 64))
			now := time.Now().UTC()
			old, target := edgeTargetFixture("1", "a"), edgeTargetFixture("2", "b")
			transition := edgeTransitionFixture()
			transition.GroupID = "edge-group-test"
			activation := edgeActivationState{Schema: edgeActivationStateSchema, GroupID: transition.GroupID,
				Generation: 1, ActiveSlot: "a", BundleGeneration: "old-config.p10.r2", WorkerSourceCommit: old.ConfigSHA,
				WorkerImageDigest: digestFromTarget(t, old), Authority: edgeActivationAuthority}
			before := edgeStateFixture("a", old, edgeFrontHealthFromActivation(activation))
			before.FrontActivation = &activation
			worker := before.WorkerA["node-1"]
			worker.BundleGeneration, worker.ServingGeneration, worker.PublicationSequence = "old-config.p10.r2", "old-config", 10
			before.WorkerA["node-1"] = worker
			current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
				GroupID: transition.GroupID, AuthorityEpoch: 1, CurrentRecordDigest: "sha256:" + strings.Repeat("a", 64),
				CurrentWorkerSlot: releaseguardian.AuthoritySlotA, CurrentWorkerSourceSHA: old.ConfigSHA, CurrentWorkerImageDigest: activation.WorkerImageDigest,
				CurrentFrontGeneration: 1, CurrentBundleGeneration: activation.BundleGeneration}
			raw, _ := json.Marshal(current)
			object := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]interface{}{"name": "fugue-current-authority-" + transition.GroupID, "namespace": "test-system", "uid": "current-uid", "resourceVersion": "42"},
				"data":     map[string]interface{}{"authority.json": string(raw)}}}
			client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
			keyring := edgeCandidateKeyring{Schema: "edge-control-group-recovery-keyring/v1", Generation: 1,
				GroupID: transition.GroupID, Keys: []edgeCandidateKey{{KeyID: "test-key", Secret: base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("s", 32))), NotBeforeUnix: now.Add(-time.Hour).Unix(), NotAfterUnix: now.Add(time.Hour).Unix()}}}
			raw, _ = json.Marshal(keyring)
			transition.CandidateKeyring = filepath.Join(t.TempDir(), "keyring.json")
			if err := os.WriteFile(transition.CandidateKeyring, raw, 0600); err != nil {
				t.Fatal(err)
			}
			posts, recoveryPosts, snapshots := 0, 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method == http.MethodGet {
					_ = json.NewEncoder(w).Encode(edgeCandidateStageStatus{GroupID: transition.GroupID, Ready: true, ServingHealthy: true,
						PublicationDecision: "published", LKGState: "current", AuthoritySequence: 11, CurrentPublicationSequence: 11,
						RecoveryEpoch: 2, BundleGeneration: "new-config", PublishedBundleDigest: "sha256:" + strings.Repeat("b", 64)})
					return
				}
				if req.URL.Path != edgeCandidateStagePath {
					recoveryPosts++
					http.Error(w, "healthy configuration must not be recovered", http.StatusConflict)
					return
				}
				posts++
				var staged edgeCandidateStageRequest
				if err := json.NewDecoder(req.Body).Decode(&staged); err != nil {
					t.Error(err)
				}
				if staged.ServingAuthority == nil || staged.ServingAuthority.WorkerSourceSHA != old.ConfigSHA || staged.ServingAuthority.WorkerSlot != "a" ||
					staged.ServingAuthority.WorkerImageDigest != activation.WorkerImageDigest || staged.ExpectedRecoveryEpoch != 2 {
					t.Error("retry changed the active code or recovery grant")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if staged.ServingAuthority.BundleVersion != "new-config.p11.r2" || scenario == "conflict budget" {
					w.WriteHeader(http.StatusConflict)
					_, _ = w.Write([]byte(`{"schema":"edge-control-error/v1","error":"sequence_conflict"}`))
					return
				}
				_ = json.NewEncoder(w).Encode(edgeCandidateStageReceipt{Schema: edgeCandidateReceiptSchema, GroupID: staged.GroupID,
					CandidateEpoch: 12, CandidateRecordDigest: "sha256:" + strings.Repeat("8", 64), CandidateBundleGeneration: "new-config",
					ReleaseRecordDigest: staged.ReleaseRecordDigest, WorkerSourceSHA: staged.WorkerSourceSHA, WorkerImageDigest: staged.WorkerImageDigest,
					WorkerSlot: staged.TargetWorkerSlot, CurrentWorkerSlot: staged.ExpectedCurrentWorkerSlot,
					CurrentPublishedBundleDigest: staged.ExpectedPublishedBundleDigest, CurrentPublicationSequence: 11, CurrentRecoveryEpoch: 2, AllowDegradedPrevious: true})
			}))
			defer server.Close()
			transition.CandidateStageURL = server.URL + edgeCandidateStagePath
			r := kubectlEdgeGroupRuntime{client: client, transition: transition, release: declarativerelease.PlanRelease{
				SupersedesFailedConfigSHA: strings.Repeat("f", 40), Workload: declarativerelease.Workload{Namespace: "test-system"}}}
			readSnapshot := func(context.Context) (edgeGroupState, error) {
				snapshots++
				if scenario == "snapshot unavailable" {
					return edgeGroupState{}, errors.New("snapshot unavailable")
				}
				observed := before
				state := activation
				observed.FrontActivation = &state
				fresh := worker
				fresh.BundleGeneration, fresh.ServingGeneration, fresh.PublicationSequence = "new-config.p11.r2", "new-config", 11
				if scenario == "changed grant" {
					state.Generation++
				} else if scenario == "changed code" {
					fresh.SourceCommit = target.ConfigSHA
				} else if scenario == "stale facts" {
					fresh.InventoryHeartbeatAt = now.Add(-time.Hour)
				} else if scenario == "regressed publication" {
					fresh.BundleGeneration, fresh.PublicationSequence = "new-config.p9.r2", 9
				}
				observed.WorkerA = map[string]edgeGroupPod{"node-1": fresh}
				return observed, nil
			}
			_, err := r.stageCandidate(context.Background(), before, "b", target, false, readSnapshot)
			if (err == nil) != (scenario == "fresh publication") || recoveryPosts != 0 {
				t.Fatalf("scenario=%s error=%v stages=%d recoveries=%d snapshots=%d", scenario, err, posts, recoveryPosts, snapshots)
			}
			expectedPosts, expectedSnapshots := 1, 1
			if scenario == "fresh publication" {
				expectedPosts = 2
			} else if scenario == "conflict budget" {
				expectedPosts, expectedSnapshots = edgeCandidateStageAttempts, edgeCandidateStageAttempts-1
			}
			if posts != expectedPosts || snapshots != expectedSnapshots {
				t.Fatalf("unbounded or stale retry: stages=%d snapshots=%d", posts, snapshots)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" {
					t.Fatal("candidate retry mutated code authority", action.GetVerb())
				}
			}
		})
	}
}

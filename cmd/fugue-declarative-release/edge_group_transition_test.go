package main

import (
	"context"
	"encoding/base64"
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

	"fugue/internal/declarativerelease"
	"fugue/internal/edgecontrol"
	"fugue/internal/releaseguardian"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestReadEdgeCandidateStageStatusAcceptsFullAuthorityResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"edge_group_id":"edge-group-country-us","status":"serving_lkg","ready":true,"authority_sequence":12,"publication_sequence":12,"current_publication_sequence":10,"candidate_epoch":13,"candidate_worker_source_sha":"9999999999999999999999999999999999999999","published_bundle_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","recovery_epoch":2,"lkg_state":"preserved"}`))
	}))
	defer server.Close()
	endpoint := server.URL + edgeCandidateStagePath
	status, err := readEdgeCandidateStageStatus(context.Background(), endpoint, "edge-group-country-us")
	if err != nil || status.AuthoritySequence != 12 || status.CurrentPublicationSequence != 10 || status.CandidateEpoch != 13 ||
		status.CandidateWorkerSourceSHA != strings.Repeat("9", 40) {
		t.Fatalf("full status response: status=%+v err=%v", status, err)
	}
}

func TestPostEdgeCandidateStageReportsTrustedControlErrorCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"schema":"edge-control-error/v1","error":"sequence_conflict"}`))
	}))
	defer server.Close()
	_, err := postEdgeCandidateStage(context.Background(), server.URL, edgeCandidateStageRequest{})
	if !errors.Is(err, errEdgeCandidateStageSequenceConflict) || err.Error() != "stage edge Worker candidate: HTTP 409 (sequence_conflict)" {
		t.Fatalf("trusted edge-control error code was lost: %v", err)
	}
}

func TestCurrentAuthorityMustConvergeToExactStagedCandidate(t *testing.T) {
	groupID := "edge-group-country-de"
	staged := edgeCandidateStageReceipt{
		GroupID: groupID, CandidateRecordDigest: "sha256:" + strings.Repeat("1", 64),
		CandidateBundleGeneration: "routes", WorkerSlot: "b", WorkerSourceSHA: strings.Repeat("2", 40),
		WorkerImageDigest: "sha256:" + strings.Repeat("3", 64),
	}
	current := releaseguardian.CurrentAuthority{
		APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind, GroupID: groupID,
		CurrentRecordDigest: staged.CandidateRecordDigest, CurrentWorkerSlot: releaseguardian.AuthoritySlotB,
		CurrentFrontGeneration: 12, CurrentBundleGeneration: "routes.p20.r0", CurrentWorkerSourceSHA: staged.WorkerSourceSHA,
		CurrentWorkerImageDigest: staged.WorkerImageDigest, PreviousRecordDigest: "sha256:" + strings.Repeat("4", 64),
		PreviousWorkerSlot: releaseguardian.AuthoritySlotA, PreviousFrontGeneration: 11, PreviousBundleGeneration: "routes.p19.r0",
		PreviousWorkerSourceSHA: strings.Repeat("5", 40), PreviousWorkerImageDigest: "sha256:" + strings.Repeat("6", 64), AuthorityEpoch: 7,
	}
	if !edgeCurrentAuthorityMatchesCandidate(current, staged) {
		t.Fatal("exact staged candidate authority did not converge")
	}
	current.CurrentWorkerSourceSHA = strings.Repeat("7", 40)
	if edgeCurrentAuthorityMatchesCandidate(current, staged) {
		t.Fatal("stale Guardian source identity was accepted")
	}
	current.CurrentWorkerSourceSHA = staged.WorkerSourceSHA
	current.CurrentBundleGeneration = "other.p20.r0"
	if edgeCurrentAuthorityMatchesCandidate(current, staged) {
		t.Fatal("cross-generation Guardian authority was accepted")
	}
}

func TestEdgeSharedResourcesExcludeOnlyDeclaredABWorkloads(t *testing.T) {
	transition := edgeTransitionFixture()
	item := func(apiVersion, kind, name string) map[string]any {
		return map[string]any{"apiVersion": apiVersion, "kind": kind,
			"metadata": map[string]any{"name": name, "namespace": "fugue-system"}}
	}
	manifest, _ := json.Marshal(map[string]any{"apiVersion": "release.fugue.dev/v2", "kind": "ComponentResourceSet", "items": []any{
		item("apps/v1", "DaemonSet", transition.FrontName), item("apps/v1", "DaemonSet", transition.WorkerAName),
		item("apps/v1", "DaemonSet", transition.WorkerBName), item("v1", "ServiceAccount", "edge-worker-us"),
	}})
	shared, err := edgeSharedResourceIdentities(manifest, transition)
	if err != nil || len(shared) != 1 || shared[0].Kind != "ServiceAccount" || shared[0].Name != "edge-worker-us" {
		t.Fatalf("shared identities=%+v err=%v", shared, err)
	}
	manifest, _ = json.Marshal(map[string]any{"apiVersion": "release.fugue.dev/v2", "kind": "ComponentResourceSet", "items": []any{
		item("apps/v1", "DaemonSet", "undeclared-worker"),
	}})
	if _, err := edgeSharedResourceIdentities(manifest, transition); err == nil || !strings.Contains(err.Error(), "undeclared DaemonSet") {
		t.Fatalf("undeclared workload was accepted: %v", err)
	}
}

func TestStageCandidateRefreshesCASStateAfterSequenceConflict(t *testing.T) {
	t.Setenv("FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST", "sha256:"+strings.Repeat("7", 64))
	now := time.Now().UTC()
	keyringPath := filepath.Join(t.TempDir(), "keyring.json")
	keyring := edgeCandidateKeyring{Schema: "edge-control-group-recovery-keyring/v1", Generation: 1,
		GroupID: "edge-group-country-de", Keys: []edgeCandidateKey{{KeyID: "key-1",
			Secret:        base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("s", 32))),
			NotBeforeUnix: now.Add(-time.Hour).Unix(), NotAfterUnix: now.Add(time.Hour).Unix()}}}
	rawKeyring, _ := json.Marshal(keyring)
	if err := os.WriteFile(keyringPath, rawKeyring, 0o600); err != nil {
		t.Fatal(err)
	}

	statusReads := 0
	postedEpochs := make([]uint64, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.Method == http.MethodGet {
			statusReads++
			_, _ = fmt.Fprintf(writer, `{"edge_group_id":"edge-group-country-de","authority_sequence":12,"current_publication_sequence":10,"candidate_epoch":%d,"published_bundle_digest":"sha256:%s","recovery_epoch":2}`,
				6+statusReads, strings.Repeat("4", 64))
			return
		}
		var staged edgeCandidateStageRequest
		if err := json.NewDecoder(request.Body).Decode(&staged); err != nil {
			t.Errorf("decode staged request: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		postedEpochs = append(postedEpochs, staged.ExpectedCandidateEpoch)
		if len(postedEpochs) == 1 {
			writer.WriteHeader(http.StatusConflict)
			_, _ = writer.Write([]byte(`{"schema":"edge-control-error/v1","error":"sequence_conflict"}`))
			return
		}
		receipt := edgeCandidateStageReceipt{Schema: edgeCandidateReceiptSchema, GroupID: staged.GroupID,
			CandidateEpoch: staged.ExpectedCandidateEpoch + 1, CandidateRecordDigest: "sha256:" + strings.Repeat("8", 64),
			ReleaseRecordDigest: staged.ReleaseRecordDigest, WorkerSourceSHA: staged.WorkerSourceSHA,
			WorkerImageDigest: staged.WorkerImageDigest, WorkerSlot: staged.TargetWorkerSlot,
			CurrentWorkerSlot: staged.ExpectedCurrentWorkerSlot, CurrentPublishedBundleDigest: staged.ExpectedPublishedBundleDigest,
			CurrentPublicationSequence: staged.ExpectedPublicationSequence, CurrentRecoveryEpoch: staged.ExpectedRecoveryEpoch,
			AllowDegradedPrevious: staged.AllowDegradedPrevious, StandbyOnly: staged.StandbyOnly}
		_ = json.NewEncoder(writer).Encode(receipt)
	}))
	defer server.Close()

	runtime := kubectlEdgeGroupRuntime{client: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		release: declarativerelease.PlanRelease{Workload: declarativerelease.Workload{Namespace: "fugue-system"}},
		transition: declarativerelease.EdgeGroupABTransition{GroupID: "edge-group-country-de",
			CandidateStageURL: server.URL + edgeCandidateStagePath, CandidateKeyring: keyringPath}}
	target := declarativerelease.TargetIdentity{ConfigSHA: strings.Repeat("5", 40),
		ImageRef: "ghcr.io/yym68686/fugue-edge@sha256:" + strings.Repeat("6", 64)}
	receipt, err := runtime.StageCandidate(context.Background(), edgeGroupState{ActiveSlot: "a"}, "b", target)
	if err != nil || receipt.WorkerSlot != "b" || statusReads != 2 || len(postedEpochs) != 2 || postedEpochs[0] != 7 || postedEpochs[1] != 8 {
		t.Fatalf("candidate CAS retry did not refresh state: receipt=%+v reads=%d epochs=%v err=%v", receipt, statusReads, postedEpochs, err)
	}
}

func TestStageCandidateRecoversPublishedLKGAfterSequenceConflict(t *testing.T) {
	t.Setenv("FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST", "sha256:"+strings.Repeat("7", 64))
	now := time.Now().UTC()
	keyringPath := filepath.Join(t.TempDir(), "keyring.json")
	keyring := edgeCandidateKeyring{Schema: "edge-control-group-recovery-keyring/v1", Generation: 1,
		GroupID: "edge-group-country-de", Keys: []edgeCandidateKey{{KeyID: "key-1",
			Secret:        base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("s", 32))),
			NotBeforeUnix: now.Add(-time.Hour).Unix(), NotAfterUnix: now.Add(time.Hour).Unix()}}}
	rawKeyring, _ := json.Marshal(keyring)
	if err := os.WriteFile(keyringPath, rawKeyring, 0o600); err != nil {
		t.Fatal(err)
	}

	statusReads := 0
	stagePosts := 0
	recoveryPosts := 0
	candidateRecoveryPosts := 0
	operations := make([]string, 0, 4)
	failedSourceSHA := strings.Repeat("9", 40)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet:
			statusReads++
			publicationSequence, recoveryEpoch, candidateEpoch := uint64(10), uint64(2), uint64(7)
			candidateSource := failedSourceSHA
			if statusReads >= 3 {
				publicationSequence, recoveryEpoch, candidateEpoch, candidateSource = 11, 3, 0, ""
			}
			_, _ = fmt.Fprintf(writer, `{"edge_group_id":"edge-group-country-de","authority_sequence":12,"current_publication_sequence":%d,"candidate_epoch":%d,"candidate_worker_source_sha":%q,"bundle_generation":"routes.p160.r1","published_bundle_digest":"sha256:%s","recovery_epoch":%d}`,
				publicationSequence, candidateEpoch, candidateSource, strings.Repeat("4", 64), recoveryEpoch)
		case request.URL.Path == edgeCandidateRecoveryPath:
			candidateRecoveryPosts++
			operations = append(operations, "fence")
			var recovery edgeCandidateRecoveryRequest
			if err := json.NewDecoder(request.Body).Decode(&recovery); err != nil {
				t.Errorf("decode candidate recovery request: %v", err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if recovery.Schema != edgeCandidateRecoverySchema || recovery.GroupID != "edge-group-country-de" ||
				recovery.ExpectedAuthoritySequence != 12 || recovery.ExpectedPublicationSequence != 10 ||
				recovery.ExpectedRecoveryEpoch != 2 || recovery.ExpectedCandidateEpoch != 7 ||
				recovery.ExpectedWorkerSourceSHA != failedSourceSHA || recovery.Signature == "" {
				t.Errorf("candidate recovery request is not bound to failed candidate: %+v", recovery)
			}
			_ = json.NewEncoder(writer).Encode(edgeCandidateRecoveryReceipt{Schema: edgeCandidateRecoveryReceiptSchema,
				GroupID: "edge-group-country-de", FencedCandidateEpoch: 7, FencedWorkerSourceSHA: failedSourceSHA,
				CurrentPublicationSequence: 10, CurrentRecoveryEpoch: 2,
				PublishedBundleDigest: "sha256:" + strings.Repeat("4", 64), CandidateCleared: true})
		case request.URL.Path == edgeGroupRecoveryPath:
			recoveryPosts++
			operations = append(operations, "recover")
			var recovery edgeGroupRecoveryRequest
			if err := json.NewDecoder(request.Body).Decode(&recovery); err != nil {
				t.Errorf("decode recovery request: %v", err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if recovery.Schema != edgeGroupRecoverySchema || recovery.GroupID != "edge-group-country-de" ||
				recovery.ExpectedPublicationSequence != 10 || recovery.ExpectedRecoveryEpoch != 2 ||
				recovery.TargetBundleGeneration != "routes.p160.r1" || recovery.Signature == "" {
				t.Errorf("recovery request is not bound to published LKG: %+v", recovery)
			}
			_ = json.NewEncoder(writer).Encode(edgeGroupRecoveryReceipt{Schema: edgeGroupRecoveryReceiptSchema,
				GroupID: "edge-group-country-de", PublicationSequence: 11, RecoveryEpoch: 3,
				BundleGeneration: "routes", PublishedBundleDigest: "sha256:" + strings.Repeat("4", 64),
				Authority: edgeActivationAuthority, PublicationEnabled: true})
		default:
			stagePosts++
			operations = append(operations, "stage")
			var staged edgeCandidateStageRequest
			if err := json.NewDecoder(request.Body).Decode(&staged); err != nil {
				t.Errorf("decode staged request: %v", err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if stagePosts == 1 {
				writer.WriteHeader(http.StatusConflict)
				_, _ = writer.Write([]byte(`{"schema":"edge-control-error/v1","error":"sequence_conflict"}`))
				return
			}
			_ = json.NewEncoder(writer).Encode(edgeCandidateStageReceipt{Schema: edgeCandidateReceiptSchema,
				GroupID: staged.GroupID, CandidateEpoch: staged.ExpectedCandidateEpoch + 1,
				CandidateRecordDigest: "sha256:" + strings.Repeat("8", 64), ReleaseRecordDigest: staged.ReleaseRecordDigest,
				WorkerSourceSHA: staged.WorkerSourceSHA, WorkerImageDigest: staged.WorkerImageDigest,
				WorkerSlot: staged.TargetWorkerSlot, CurrentWorkerSlot: staged.ExpectedCurrentWorkerSlot,
				CurrentPublishedBundleDigest: staged.ExpectedPublishedBundleDigest,
				CurrentPublicationSequence:   staged.ExpectedPublicationSequence, CurrentRecoveryEpoch: staged.ExpectedRecoveryEpoch,
				AllowDegradedPrevious: staged.AllowDegradedPrevious})
		}
	}))
	defer server.Close()

	runtime := kubectlEdgeGroupRuntime{client: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		release: declarativerelease.PlanRelease{SupersedesFailedConfigSHA: strings.Repeat("f", 40), Workload: declarativerelease.Workload{Namespace: "fugue-system"}},
		transition: declarativerelease.EdgeGroupABTransition{GroupID: "edge-group-country-de",
			CandidateStageURL: server.URL + edgeCandidateStagePath, CandidateKeyring: keyringPath}}
	target := declarativerelease.TargetIdentity{ConfigSHA: strings.Repeat("5", 40),
		ImageRef: "ghcr.io/yym68686/fugue-edge@sha256:" + strings.Repeat("6", 64)}
	receipt, err := runtime.StageCandidate(context.Background(), edgeGroupState{ActiveSlot: "a"}, "b", target)
	if err != nil || receipt.WorkerSlot != "b" || statusReads != 3 || stagePosts != 2 || recoveryPosts != 1 || candidateRecoveryPosts != 1 ||
		strings.Join(operations, ",") != "stage,fence,recover,stage" ||
		receipt.CurrentPublicationSequence != 11 || receipt.CurrentRecoveryEpoch != 3 {
		t.Fatalf("candidate recovery flow did not settle: receipt=%+v reads=%d stages=%d fences=%d recoveries=%d operations=%v err=%v",
			receipt, statusReads, stagePosts, candidateRecoveryPosts, recoveryPosts, operations, err)
	}
}

func TestStageCandidateRecoversExistingFailedCandidateBeforeFirstStage(t *testing.T) {
	t.Setenv("FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST", "sha256:"+strings.Repeat("7", 64))
	now := time.Now().UTC()
	keyringPath := filepath.Join(t.TempDir(), "keyring.json")
	keyring := edgeCandidateKeyring{Schema: "edge-control-group-recovery-keyring/v1", Generation: 1,
		GroupID: "edge-group-country-de", Keys: []edgeCandidateKey{{KeyID: "key-1",
			Secret:        base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("s", 32))),
			NotBeforeUnix: now.Add(-time.Hour).Unix(), NotAfterUnix: now.Add(time.Hour).Unix()}}}
	rawKeyring, _ := json.Marshal(keyring)
	if err := os.WriteFile(keyringPath, rawKeyring, 0o600); err != nil {
		t.Fatal(err)
	}
	lkgSource, lkgImage := strings.Repeat("1", 40), "sha256:"+strings.Repeat("2", 64)
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: "edge-group-country-de", CurrentRecordDigest: "sha256:" + strings.Repeat("a", 64), CurrentWorkerSlot: releaseguardian.AuthoritySlotA,
		CurrentFrontGeneration: 137, CurrentBundleGeneration: "routes.p10.r2", CurrentWorkerSourceSHA: strings.Repeat("c", 40),
		CurrentWorkerImageDigest: "sha256:" + strings.Repeat("d", 64), PreviousRecordDigest: "sha256:" + strings.Repeat("e", 64),
		PreviousWorkerSlot: releaseguardian.AuthoritySlotB, PreviousFrontGeneration: 136, PreviousBundleGeneration: "routes.p5.r1",
		PreviousWorkerSourceSHA: lkgSource, PreviousWorkerImageDigest: lkgImage, AuthorityEpoch: 23}
	currentRaw, _ := json.Marshal(current)
	if err := current.Validate(); err != nil {
		t.Fatalf("authority fixture invalid: %v", err)
	}
	authorityObject := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]interface{}{"name": "fugue-current-authority-edge-group-country-de", "namespace": "fugue-system", "uid": "authority-uid", "resourceVersion": "41"},
		"data":     map[string]interface{}{"authority.json": string(currentRaw)},
	}}
	statusReads, stagePosts, fencePosts, recoveryPosts := 0, 0, 0, 0
	operations := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet:
			statusReads++
			candidateEpoch, publicationSequence, recoveryEpoch := uint64(7), uint64(10), uint64(2)
			bundleGeneration := "failed-routes"
			if statusReads > 1 {
				candidateEpoch, publicationSequence, recoveryEpoch = 0, 11, 3
				bundleGeneration = "routes"
			}
			_, _ = fmt.Fprintf(writer, `{"edge_group_id":"edge-group-country-de","authority_sequence":12,"current_publication_sequence":%d,"candidate_epoch":%d,"candidate_worker_source_sha":%q,"bundle_generation":%q,"published_bundle_digest":"sha256:%s","recovery_epoch":%d}`,
				publicationSequence, candidateEpoch, strings.Repeat("9", 40), bundleGeneration, strings.Repeat("4", 64), recoveryEpoch)
		case request.URL.Path == edgeCandidateRecoveryPath:
			fencePosts++
			operations = append(operations, "fence")
			_ = json.NewEncoder(writer).Encode(edgeCandidateRecoveryReceipt{Schema: edgeCandidateRecoveryReceiptSchema,
				GroupID: "edge-group-country-de", FencedCandidateEpoch: 7, FencedWorkerSourceSHA: strings.Repeat("9", 40),
				CurrentPublicationSequence: 10, CurrentRecoveryEpoch: 2, PublishedBundleDigest: "sha256:" + strings.Repeat("4", 64), CandidateCleared: true})
		case request.URL.Path == edgeGroupRecoveryPath:
			recoveryPosts++
			operations = append(operations, "recover")
			var recovery edgeGroupRecoveryRequest
			if err := json.NewDecoder(request.Body).Decode(&recovery); err != nil || recovery.TargetBundleGeneration != "routes.p5.r1" {
				t.Fatalf("recovery request target=%+v err=%v", recovery, err)
			}
			_ = json.NewEncoder(writer).Encode(edgeGroupRecoveryReceipt{Schema: edgeGroupRecoveryReceiptSchema,
				GroupID: "edge-group-country-de", PublicationSequence: 11, RecoveryEpoch: 3, BundleGeneration: "routes",
				PublishedBundleDigest: "sha256:" + strings.Repeat("4", 64), Authority: edgeActivationAuthority, PublicationEnabled: true})
		default:
			stagePosts++
			operations = append(operations, "stage")
			var staged edgeCandidateStageRequest
			if err := json.NewDecoder(request.Body).Decode(&staged); err != nil {
				t.Fatalf("decode staged request: %v", err)
			}
			_ = json.NewEncoder(writer).Encode(edgeCandidateStageReceipt{Schema: edgeCandidateReceiptSchema, GroupID: staged.GroupID,
				CandidateEpoch: staged.ExpectedCandidateEpoch + 1, CandidateRecordDigest: "sha256:" + strings.Repeat("8", 64),
				ReleaseRecordDigest: staged.ReleaseRecordDigest, WorkerSourceSHA: staged.WorkerSourceSHA, WorkerImageDigest: staged.WorkerImageDigest,
				WorkerSlot: staged.TargetWorkerSlot, CurrentWorkerSlot: staged.ExpectedCurrentWorkerSlot,
				CurrentPublishedBundleDigest: staged.ExpectedPublishedBundleDigest, CurrentPublicationSequence: staged.ExpectedPublicationSequence,
				CurrentRecoveryEpoch: staged.ExpectedRecoveryEpoch, AllowDegradedPrevious: staged.AllowDegradedPrevious})
		}
	}))
	defer server.Close()

	runtime := kubectlEdgeGroupRuntime{client: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), authorityObject),
		release: declarativerelease.PlanRelease{SupersedesFailedConfigSHA: strings.Repeat("f", 40), ExpectedPreviousConfigSHA: lkgSource,
			ExpectedPreviousImageDigest: lkgImage, Workload: declarativerelease.Workload{Namespace: "fugue-system"}},
		transition: declarativerelease.EdgeGroupABTransition{GroupID: "edge-group-country-de", CandidateStageURL: server.URL + edgeCandidateStagePath, CandidateKeyring: keyringPath}}
	target := declarativerelease.TargetIdentity{ConfigSHA: strings.Repeat("5", 40), ImageRef: "ghcr.io/yym68686/fugue-edge@sha256:" + strings.Repeat("6", 64)}
	receipt, err := runtime.StageCandidate(context.Background(), edgeGroupState{ActiveSlot: "b", FrontActivation: &edgeActivationState{
		Schema: edgeActivationStateSchema, GroupID: "edge-group-country-de", Generation: 138, ActiveSlot: "b", PreviousSlot: "a",
		BundleGeneration: "routes.p5.r1", WorkerSourceCommit: lkgSource, WorkerImageDigest: lkgImage, Authority: edgeActivationAuthority}}, "a", target)
	if err != nil || receipt.WorkerSlot != "a" || statusReads != 3 || stagePosts != 1 || fencePosts != 1 || recoveryPosts != 1 || strings.Join(operations, ",") != "fence,recover,stage" {
		t.Fatalf("existing failed candidate recovery did not run before staging: receipt=%+v reads=%d stages=%d fences=%d recoveries=%d operations=%v err=%v",
			receipt, statusReads, stagePosts, fencePosts, recoveryPosts, operations, err)
	}
}

func TestServingLKGRecoverySkipsAlreadyRenewedBundleFamily(t *testing.T) {
	lkgSource, lkgImage := strings.Repeat("1", 40), "sha256:"+strings.Repeat("2", 64)
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: "edge-group-country-de", CurrentRecordDigest: "sha256:" + strings.Repeat("a", 64), CurrentWorkerSlot: releaseguardian.AuthoritySlotA,
		CurrentFrontGeneration: 137, CurrentBundleGeneration: "routes.p10.r2", CurrentWorkerSourceSHA: strings.Repeat("c", 40),
		CurrentWorkerImageDigest: "sha256:" + strings.Repeat("d", 64), PreviousRecordDigest: "sha256:" + strings.Repeat("e", 64),
		PreviousWorkerSlot: releaseguardian.AuthoritySlotB, PreviousFrontGeneration: 136,
		PreviousBundleGeneration: "routes.p5.r1", PreviousWorkerSourceSHA: lkgSource, PreviousWorkerImageDigest: lkgImage, AuthorityEpoch: 23}
	raw, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	authorityObject := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]interface{}{"name": "fugue-current-authority-edge-group-country-de", "namespace": "fugue-system", "uid": "authority-uid", "resourceVersion": "41"},
		"data":     map[string]interface{}{"authority.json": string(raw)},
	}}
	runtime := kubectlEdgeGroupRuntime{client: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), authorityObject),
		release:    declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: lkgSource, ExpectedPreviousImageDigest: lkgImage, Workload: declarativerelease.Workload{Namespace: "fugue-system"}},
		transition: declarativerelease.EdgeGroupABTransition{GroupID: "edge-group-country-de"}}
	before := edgeGroupState{ActiveSlot: "b", FrontActivation: &edgeActivationState{Schema: edgeActivationStateSchema,
		GroupID: "edge-group-country-de", Generation: 138, ActiveSlot: "b", PreviousSlot: "a", BundleGeneration: "routes.p5.r1",
		WorkerSourceCommit: lkgSource, WorkerImageDigest: lkgImage, Authority: edgeActivationAuthority}}
	status := edgeCandidateStageStatus{GroupID: "edge-group-country-de", Ready: true, ServingHealthy: true, PublicationDecision: "published", LKGState: "current", CurrentPublicationSequence: 11, CandidateEpoch: 7,
		CandidateWorkerSourceSHA: strings.Repeat("9", 40), BundleGeneration: "routes", RecoveryEpoch: 3,
		PublishedBundleDigest: "sha256:" + strings.Repeat("4", 64)}
	if target, needed, err := runtime.servingLKGRecoveryTarget(context.Background(), before, status); err != nil || needed || target != "" {
		t.Fatalf("already renewed LKG family requested another recovery: target=%q needed=%t err=%v", target, needed, err)
	}
}

func TestServingLKGRecoveryDoesNotSkipFailedPublication(t *testing.T) {
	lkgSource, lkgImage := strings.Repeat("1", 40), "sha256:"+strings.Repeat("2", 64)
	raw, err := json.Marshal(releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: "edge-group-country-de", CurrentRecordDigest: "sha256:" + strings.Repeat("a", 64), CurrentWorkerSlot: releaseguardian.AuthoritySlotA,
		CurrentFrontGeneration: 137, CurrentBundleGeneration: "routes.p10.r2", CurrentWorkerSourceSHA: strings.Repeat("c", 40),
		CurrentWorkerImageDigest: "sha256:" + strings.Repeat("d", 64), PreviousRecordDigest: "sha256:" + strings.Repeat("e", 64),
		PreviousWorkerSlot: releaseguardian.AuthoritySlotB, PreviousFrontGeneration: 136, PreviousBundleGeneration: "routes.p5.r1",
		PreviousWorkerSourceSHA: lkgSource, PreviousWorkerImageDigest: lkgImage, AuthorityEpoch: 23})
	if err != nil {
		t.Fatal(err)
	}
	authorityObject := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]interface{}{"name": "fugue-current-authority-edge-group-country-de", "namespace": "fugue-system", "uid": "authority-uid", "resourceVersion": "41"},
		"data": map[string]interface{}{"authority.json": string(raw)},
	}}
	runtime := kubectlEdgeGroupRuntime{client: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), authorityObject), release: declarativerelease.PlanRelease{
		ExpectedPreviousConfigSHA: lkgSource, ExpectedPreviousImageDigest: lkgImage, Workload: declarativerelease.Workload{Namespace: "fugue-system"}}, transition: declarativerelease.EdgeGroupABTransition{GroupID: "edge-group-country-de"}}
	before := edgeGroupState{ActiveSlot: "b", FrontActivation: &edgeActivationState{Schema: edgeActivationStateSchema, GroupID: "edge-group-country-de", Generation: 138, ActiveSlot: "b", PreviousSlot: "a", BundleGeneration: "routes.p5.r1", WorkerSourceCommit: lkgSource, WorkerImageDigest: lkgImage, Authority: edgeActivationAuthority}}
	status := edgeCandidateStageStatus{GroupID: "edge-group-country-de", Ready: true, ServingHealthy: false, PublicationDecision: "failed", LKGState: "preserved", CurrentPublicationSequence: 11, CandidateEpoch: 7, CandidateWorkerSourceSHA: strings.Repeat("9", 40), BundleGeneration: "routes", RecoveryEpoch: 3, PublishedBundleDigest: "sha256:" + strings.Repeat("4", 64)}
	if target, needed, err := runtime.servingLKGRecoveryTarget(context.Background(), before, status); err != nil || !needed || target != before.FrontActivation.BundleGeneration {
		t.Fatalf("failed LKG publication was skipped: target=%q needed=%t err=%v", target, needed, err)
	}
}

func TestRefreshPublishedLKGReconcilesTransportFailure(t *testing.T) {
	now := time.Now().UTC()
	keyringPath := filepath.Join(t.TempDir(), "keyring.json")
	keyring := edgeCandidateKeyring{Schema: "edge-control-group-recovery-keyring/v1", Generation: 1,
		GroupID: "edge-group-country-de", Keys: []edgeCandidateKey{{KeyID: "key-1",
			Secret:        base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("s", 32))),
			NotBeforeUnix: now.Add(-time.Hour).Unix(), NotAfterUnix: now.Add(time.Hour).Unix()}}}
	rawKeyring, _ := json.Marshal(keyring)
	if err := os.WriteFile(keyringPath, rawKeyring, 0o600); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name      string
		committed bool
	}{
		{name: "committed response lost", committed: true},
		{name: "request lost", committed: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			publicationSequence, recoveryEpoch := uint64(10), uint64(2)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				if request.Method == http.MethodGet {
					_, _ = fmt.Fprintf(writer, `{"edge_group_id":"edge-group-country-de","authority_sequence":12,"current_publication_sequence":%d,"bundle_generation":"routes.p160.r1","published_bundle_digest":"sha256:%s","recovery_epoch":%d}`,
						publicationSequence, strings.Repeat("4", 64), recoveryEpoch)
					return
				}
				if test.committed {
					publicationSequence, recoveryEpoch = 11, 3
				}
				panic(http.ErrAbortHandler)
			}))
			defer server.Close()

			runtime := kubectlEdgeGroupRuntime{transition: declarativerelease.EdgeGroupABTransition{
				GroupID: "edge-group-country-de", CandidateStageURL: server.URL + edgeCandidateStagePath, CandidateKeyring: keyringPath}}
			status := edgeCandidateStageStatus{GroupID: "edge-group-country-de", CurrentPublicationSequence: 10,
				BundleGeneration: "routes.p160.r1", PublishedBundleDigest: "sha256:" + strings.Repeat("4", 64), RecoveryEpoch: 2}
			err := runtime.refreshPublishedLKG(context.Background(), status)
			if test.committed && err != nil {
				t.Fatalf("committed recovery was not reconciled: %v", err)
			}
			if !test.committed && err == nil {
				t.Fatal("uncommitted transport failure was accepted")
			}
		})
	}
}

func TestStageCandidateFailsClosedWhenPublishedLKGRecoveryFails(t *testing.T) {
	t.Setenv("FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST", "sha256:"+strings.Repeat("7", 64))
	now := time.Now().UTC()
	keyringPath := filepath.Join(t.TempDir(), "keyring.json")
	keyring := edgeCandidateKeyring{Schema: "edge-control-group-recovery-keyring/v1", Generation: 1,
		GroupID: "edge-group-country-de", Keys: []edgeCandidateKey{{KeyID: "key-1",
			Secret:        base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("s", 32))),
			NotBeforeUnix: now.Add(-time.Hour).Unix(), NotAfterUnix: now.Add(time.Hour).Unix()}}}
	rawKeyring, _ := json.Marshal(keyring)
	if err := os.WriteFile(keyringPath, rawKeyring, 0o600); err != nil {
		t.Fatal(err)
	}
	statusReads, recoveryPosts, stagePosts := 0, 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet:
			statusReads++
			_, _ = fmt.Fprintf(writer, `{"edge_group_id":"edge-group-country-de","authority_sequence":12,"current_publication_sequence":10,"candidate_epoch":7,"bundle_generation":"routes.p160.r1","published_bundle_digest":"sha256:%s","recovery_epoch":2}`, strings.Repeat("4", 64))
		case request.URL.Path == edgeGroupRecoveryPath:
			recoveryPosts++
			writer.WriteHeader(http.StatusNotFound)
		default:
			stagePosts++
			writer.WriteHeader(http.StatusConflict)
			_, _ = writer.Write([]byte(`{"schema":"edge-control-error/v1","error":"sequence_conflict"}`))
		}
	}))
	defer server.Close()

	runtime := kubectlEdgeGroupRuntime{client: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		release: declarativerelease.PlanRelease{SupersedesFailedConfigSHA: strings.Repeat("f", 40), Workload: declarativerelease.Workload{Namespace: "fugue-system"}},
		transition: declarativerelease.EdgeGroupABTransition{GroupID: "edge-group-country-de",
			CandidateStageURL: server.URL + edgeCandidateStagePath, CandidateKeyring: keyringPath}}
	target := declarativerelease.TargetIdentity{ConfigSHA: strings.Repeat("5", 40),
		ImageRef: "ghcr.io/yym68686/fugue-edge@sha256:" + strings.Repeat("6", 64)}
	_, err := runtime.StageCandidate(context.Background(), edgeGroupState{ActiveSlot: "a"}, "b", target)
	if err == nil || !strings.Contains(err.Error(), "refresh published Edge Control LKG after candidate sequence conflict: Edge Control recovery HTTP 404") ||
		statusReads != 2 || recoveryPosts != 1 || stagePosts != 1 {
		t.Fatalf("recovery failure was not fail-closed: reads=%d recoveries=%d stages=%d err=%v", statusReads, recoveryPosts, stagePosts, err)
	}
}

func TestPostEdgeCandidateStageDoesNotReflectUntrustedErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"schema":"edge-control-error/v1","error":"sequence conflict: secret"}`))
	}))
	defer server.Close()
	_, err := postEdgeCandidateStage(context.Background(), server.URL, edgeCandidateStageRequest{})
	if err == nil || err.Error() != "stage edge Worker candidate: HTTP 409" {
		t.Fatalf("untrusted edge-control error body was reflected: %v", err)
	}
}

func TestEdgeCandidateStageRequestMatchesControlServingAuthoritySchema(t *testing.T) {
	witness := edgeServingAuthorityWitness{CurrentRecordDigest: "sha256:" + strings.Repeat("1", 64), AuthorityEpoch: 9,
		CurrentAuthorityUID: "current-uid", CurrentAuthorityRV: "123", FrontGeneration: 7,
		BundleVersion: "routes.p5.r2", WorkerSlot: "b", WorkerSourceSHA: strings.Repeat("2", 40), WorkerImageDigest: "sha256:" + strings.Repeat("3", 64)}
	local := edgeCandidateStageRequest{Schema: edgeCandidateStageSchema, KeyID: "key-1", GroupID: "edge-group-country-de",
		ExpectedAuthoritySequence: 11, ExpectedPublicationSequence: 10, ExpectedRecoveryEpoch: 2,
		ExpectedPublishedBundleDigest: "sha256:" + strings.Repeat("4", 64), ExpectedCandidateEpoch: 12,
		ExpectedCurrentWorkerSlot: "b", TargetWorkerSlot: "a", ServingAuthority: &witness, AllowDegradedPrevious: true, StandbyOnly: false,
		WorkerSourceSHA: strings.Repeat("5", 40), WorkerImageDigest: "sha256:" + strings.Repeat("6", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("7", 64), IssuedAtUnix: 100, ExpiresAtUnix: 160,
		Nonce: "nonce", Reason: "stage immutable candidate", Signature: "signature"}
	control := edgecontrol.GroupCandidateStageRequest{Schema: local.Schema, KeyID: local.KeyID, GroupID: local.GroupID,
		ExpectedAuthoritySequence: local.ExpectedAuthoritySequence, ExpectedPublicationSequence: local.ExpectedPublicationSequence,
		ExpectedRecoveryEpoch: local.ExpectedRecoveryEpoch, ExpectedPublishedBundleDigest: local.ExpectedPublishedBundleDigest,
		ExpectedCandidateEpoch: local.ExpectedCandidateEpoch, ExpectedCurrentWorkerSlot: local.ExpectedCurrentWorkerSlot,
		AllowDegradedPrevious: local.AllowDegradedPrevious, StandbyOnly: local.StandbyOnly,
		TargetWorkerSlot: local.TargetWorkerSlot, ServingAuthority: &edgecontrol.GroupServingAuthorityWitness{
			CurrentRecordDigest: witness.CurrentRecordDigest, AuthorityEpoch: witness.AuthorityEpoch,
			CurrentAuthorityUID: witness.CurrentAuthorityUID, CurrentAuthorityRV: witness.CurrentAuthorityRV,
			FrontGeneration: witness.FrontGeneration, BundleVersion: witness.BundleVersion, WorkerSlot: witness.WorkerSlot,
			WorkerSourceSHA: witness.WorkerSourceSHA, WorkerImageDigest: witness.WorkerImageDigest,
		}, WorkerSourceSHA: local.WorkerSourceSHA, WorkerImageDigest: local.WorkerImageDigest,
		ReleaseRecordDigest: local.ReleaseRecordDigest, IssuedAtUnix: local.IssuedAtUnix, ExpiresAtUnix: local.ExpiresAtUnix,
		Nonce: local.Nonce, Reason: local.Reason, Signature: local.Signature}
	localRaw, _ := json.Marshal(local)
	controlRaw, _ := json.Marshal(control)
	if string(localRaw) != string(controlRaw) {
		t.Fatalf("candidate request JSON differs from Edge Control schema:\nlocal=%s\ncontrol=%s", localRaw, controlRaw)
	}
}

func TestServingAuthorityWitnessAcceptsCompensatedFrontGeneration(t *testing.T) {
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: "edge-group-country-de", CurrentRecordDigest: "sha256:" + strings.Repeat("1", 64),
		CurrentWorkerSlot: releaseguardian.AuthoritySlotB, CurrentFrontGeneration: 8, CurrentBundleGeneration: "routes.p5.r2",
		CurrentWorkerSourceSHA: strings.Repeat("2", 40), CurrentWorkerImageDigest: "sha256:" + strings.Repeat("3", 64), AuthorityEpoch: 9}
	health := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 12, BundleGeneration: current.CurrentBundleGeneration,
		WorkerSourceCommit: current.CurrentWorkerSourceSHA, WorkerImageDigest: current.CurrentWorkerImageDigest, RouteAuthority: edgeActivationAuthority}
	before := edgeGroupState{ActiveSlot: "b", FrontHealth: map[string]edgeFrontHealth{"node-1": health}}
	witness, err := edgeServingAuthorityWitnessFromCurrent(before, current, current.GroupID, "current-uid", "123")
	if err != nil || witness == nil || witness.FrontGeneration != current.CurrentFrontGeneration || witness.BundleVersion != current.CurrentBundleGeneration || witness.WorkerSlot != "b" {
		t.Fatalf("serving authority witness=%+v err=%v", witness, err)
	}

	health.Generation = 11
	before.FrontHealth["node-1"] = health
	if _, err := edgeServingAuthorityWitnessFromCurrent(before, current, current.GroupID, "current-uid", "123"); err == nil || !strings.Contains(err.Error(), "Front evidence") {
		t.Fatalf("odd uncompensated Front generation was accepted: %v", err)
	}
}

func TestServingAuthorityWitnessAllowsExplicitDegradedPublicationRefresh(t *testing.T) {
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: "edge-group-country-de", CurrentRecordDigest: "sha256:" + strings.Repeat("1", 64),
		CurrentWorkerSlot: releaseguardian.AuthoritySlotB, CurrentFrontGeneration: 134,
		CurrentBundleGeneration: "routes.p15765.r151", CurrentWorkerSourceSHA: strings.Repeat("2", 40),
		CurrentWorkerImageDigest: "sha256:" + strings.Repeat("3", 64), AuthorityEpoch: 12635}
	health := edgeFrontHealth{ActiveSlot: "a", ActivationPresent: true, Generation: 135,
		BundleGeneration: "routes.p15778.r151", WorkerSourceCommit: current.CurrentWorkerSourceSHA,
		WorkerImageDigest: current.CurrentWorkerImageDigest, RouteAuthority: edgeActivationAuthority}
	before := edgeGroupState{ActiveSlot: "a", FrontHealth: map[string]edgeFrontHealth{"node-1": health}}
	witness, err := edgeServingAuthorityWitnessFromCurrentWithDegradedRecovery(before, current, current.GroupID, "current-uid", "123", true)
	if err != nil || witness == nil || witness.WorkerSlot != "a" || witness.BundleVersion != health.BundleGeneration || witness.FrontGeneration != health.Generation {
		t.Fatalf("degraded publication refresh witness=%+v err=%v", witness, err)
	}
	if _, err := edgeServingAuthorityWitnessFromCurrentWithDegradedRecovery(before, current, current.GroupID, "current-uid", "123", false); err == nil {
		t.Fatal("degraded witness was accepted without explicit authorization")
	}
}

func TestServingAuthorityWitnessAllowsCommittedFailedAuthorityWithExactLKGFront(t *testing.T) {
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: "edge-group-country-de", CurrentRecordDigest: "sha256:" + strings.Repeat("1", 64),
		CurrentWorkerSlot: releaseguardian.AuthoritySlotA, CurrentFrontGeneration: 134,
		CurrentBundleGeneration: "routes.p15765.r151", CurrentWorkerSourceSHA: strings.Repeat("2", 40),
		CurrentWorkerImageDigest: "sha256:" + strings.Repeat("3", 64), AuthorityEpoch: 12635}
	health := edgeFrontHealth{ActiveSlot: "a", ActivationPresent: true, Generation: 135,
		BundleGeneration: "routes.p15778.r151", WorkerSourceCommit: strings.Repeat("4", 40),
		WorkerImageDigest: "sha256:" + strings.Repeat("5", 64), RouteAuthority: edgeActivationAuthority}
	before := edgeGroupState{ActiveSlot: "a", FrontHealth: map[string]edgeFrontHealth{"node-1": health}}
	witness, err := edgeServingAuthorityWitnessFromCurrentWithExpectedLKG(before, current, current.GroupID, "current-uid", "123", true, strings.Repeat("4", 40), "sha256:"+strings.Repeat("5", 64))
	if err != nil || witness == nil || witness.WorkerSlot != "a" || witness.WorkerSourceSHA != health.WorkerSourceCommit || witness.WorkerImageDigest != health.WorkerImageDigest {
		t.Fatalf("exact LKG Front witness was rejected: witness=%+v err=%v", witness, err)
	}
	if _, err := edgeServingAuthorityWitnessFromCurrentWithExpectedLKG(before, current, current.GroupID, "current-uid", "123", true, strings.Repeat("6", 40), "sha256:"+strings.Repeat("6", 64)); err == nil {
		t.Fatal("Front identity outside the declared LKG was accepted")
	}
}

func TestServingAuthorityWitnessAllowsCommittedFailedAuthorityOnPreviousLKGSlot(t *testing.T) {
	current := releaseguardian.CurrentAuthority{
		APIVersion:                releaseguardian.APIVersion,
		Kind:                      releaseguardian.CurrentAuthorityKind,
		GroupID:                   "edge-group-country-de",
		CurrentRecordDigest:       "sha256:" + strings.Repeat("1", 64),
		AuthorityEpoch:            137,
		CurrentFrontGeneration:    137,
		CurrentBundleGeneration:   "routes.p15765.r151",
		CurrentWorkerSlot:         "a",
		CurrentWorkerSourceSHA:    strings.Repeat("2", 40),
		CurrentWorkerImageDigest:  "sha256:" + strings.Repeat("3", 64),
		PreviousRecordDigest:      "sha256:" + strings.Repeat("6", 64),
		PreviousWorkerSlot:        "b",
		PreviousFrontGeneration:   136,
		PreviousBundleGeneration:  "routes.p15710.r150",
		PreviousWorkerSourceSHA:   strings.Repeat("4", 40),
		PreviousWorkerImageDigest: "sha256:" + strings.Repeat("5", 64),
	}
	health := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 138,
		BundleGeneration: "routes.p15778.r151", WorkerSourceCommit: strings.Repeat("4", 40),
		WorkerImageDigest: "sha256:" + strings.Repeat("5", 64), RouteAuthority: edgeActivationAuthority}
	witness, err := edgeServingAuthorityWitnessFromCurrentWithExpectedLKG(edgeGroupState{ActiveSlot: "b", FrontHealth: map[string]edgeFrontHealth{"node": health}}, current, "edge-group-country-de", strings.Repeat("a", 20), strings.Repeat("b", 20), true, strings.Repeat("4", 40), "sha256:"+strings.Repeat("5", 64))
	if err != nil || witness == nil || witness.WorkerSlot != "b" {
		t.Fatalf("previous-slot LKG recovery was rejected: witness=%+v err=%v", witness, err)
	}
}

func TestServingAuthorityWitnessAllowsProductionCommittedAuthorityRollback(t *testing.T) {
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: "edge-group-country-de", CurrentRecordDigest: "sha256:2d1944caf69c601c014df2daf6bae7193aebd7297f827294162971ebace28074",
		CurrentWorkerSlot: releaseguardian.AuthoritySlotA, CurrentFrontGeneration: 137,
		CurrentBundleGeneration: "edgegroupbundle_80ecacaded8a8a2f0442ac723b4aeef5743bde9d9fead155142d84a41392aa36.p22941.r390",
		CurrentWorkerSourceSHA:  "11990495e58630ae634405da4d48dbf6a52687cd", CurrentWorkerImageDigest: "sha256:e0dd98a24e5160e61abb0d6f5faa13c910628c7a4946953636f36af65cd8c6ee",
		PreviousRecordDigest: "sha256:fe4cf177860b6de7203de55f7100776b08b42007496c04c37e39f05fb48acdde", PreviousWorkerSlot: releaseguardian.AuthoritySlotB,
		PreviousFrontGeneration: 136, PreviousBundleGeneration: "edgegroupbundle_80ecacaded8a8a2f0442ac723b4aeef5743bde9d9fead155142d84a41392aa36.p19710.r328",
		PreviousWorkerSourceSHA: "791c67f0824831364c29797425dc7652622672d7", PreviousWorkerImageDigest: "sha256:1a1b1abbf00be60001f59ae99e33e542dfc9ef1064b59d5dd688e361e9f1bfd6", AuthorityEpoch: 12638}
	health := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 138, BundleGeneration: current.PreviousBundleGeneration,
		WorkerSourceCommit: current.PreviousWorkerSourceSHA, WorkerImageDigest: current.PreviousWorkerImageDigest, RouteAuthority: edgeActivationAuthority}
	if witness, err := edgeServingAuthorityWitnessFromCurrentWithExpectedLKG(edgeGroupState{ActiveSlot: "b", FrontHealth: map[string]edgeFrontHealth{"node": health}}, current, current.GroupID, "authority-uid", "77188752", true, current.PreviousWorkerSourceSHA, current.PreviousWorkerImageDigest); err != nil || witness == nil {
		t.Fatalf("production rollback authority witness was rejected: witness=%+v err=%v", witness, err)
	}
}

func TestServingAuthorityWitnessUsesWorkerActivationEvidenceWhenFrontMetadataLags(t *testing.T) {
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: "edge-group-country-de", CurrentRecordDigest: "sha256:" + strings.Repeat("1", 64),
		CurrentWorkerSlot: releaseguardian.AuthoritySlotB, CurrentFrontGeneration: 134,
		CurrentBundleGeneration: "routes.p15765.r151", CurrentWorkerSourceSHA: strings.Repeat("2", 40),
		CurrentWorkerImageDigest: "sha256:" + strings.Repeat("3", 64), AuthorityEpoch: 12635}
	activation := edgeActivationState{Schema: edgeActivationStateSchema, GroupID: current.GroupID, Generation: 135,
		ActiveSlot: "a", BundleGeneration: "routes.p15778.r151", WorkerSourceCommit: current.CurrentWorkerSourceSHA,
		WorkerImageDigest: current.CurrentWorkerImageDigest, Authority: edgeActivationAuthority, Operation: edgeActivationPromote}
	before := edgeGroupState{ActiveSlot: "a", FrontActivation: &activation}
	witness, err := edgeServingAuthorityWitnessFromCurrentWithDegradedRecovery(before, current, current.GroupID, "current-uid", "123", true)
	if err != nil || witness == nil || witness.WorkerSlot != "a" || witness.BundleVersion != activation.BundleGeneration || witness.FrontGeneration != activation.Generation {
		t.Fatalf("worker activation evidence was not accepted for stale Front metadata: witness=%+v err=%v", witness, err)
	}
}

func TestServingAuthorityWitnessAllowsExplicitRuntimeDriftWithActiveWorkerProof(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: "edge-group-country-us", CurrentRecordDigest: "sha256:" + strings.Repeat("1", 64),
		CurrentWorkerSlot: releaseguardian.AuthoritySlotA, CurrentFrontGeneration: 23,
		CurrentBundleGeneration: "edgegroupbundle_old.p14252.r103", CurrentWorkerSourceSHA: strings.Repeat("2", 40),
		CurrentWorkerImageDigest: "sha256:" + strings.Repeat("3", 64), AuthorityEpoch: 14221}
	source, image := strings.Repeat("5", 40), "sha256:"+strings.Repeat("6", 64)
	health := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 24,
		BundleGeneration: "edgegroupbundle_front.p16641.r104", WorkerSourceCommit: source,
		WorkerImageDigest: image, RouteAuthority: edgeActivationAuthority}
	worker := edgeGroupPod{Name: "worker-b", NodeName: "edge-node-us", Ready: true, SourceCommit: source,
		ImageRef: "ghcr.io/example/fugue-edge@" + image, RouteBundleSource: edgeGroupAuthoritySource,
		BundleGeneration: "edgegroupbundle_current.p39215.r120", ServingGeneration: "edgegroupbundle_current",
		PublicationSequence: 39215, InventoryProducerActive: true, InventoryHeartbeatGeneration: 49241,
		InventoryHeartbeatAt: now.Add(-time.Second)}
	state := edgeGroupState{ActiveSlot: "b", Front: map[string]edgeGroupPod{"edge-node-us": {Name: "front", NodeName: "edge-node-us"}},
		WorkerB: map[string]edgeGroupPod{"edge-node-us": worker}}
	if !edgeGroupStateMatchesExplicitServingDrift(state, current, health, now) {
		t.Fatal("exact runtime-drift serving witness was rejected")
	}
	worker.InventoryHeartbeatAt = now.Add(-3 * time.Minute)
	state.WorkerB["edge-node-us"] = worker
	if edgeGroupStateMatchesExplicitServingDrift(state, current, health, now) {
		t.Fatal("stale active Worker inventory witness was accepted")
	}
	worker.InventoryHeartbeatAt = now
	state.WorkerB["edge-node-us"] = worker
	health.Generation = 25
	if edgeGroupStateMatchesExplicitServingDrift(state, current, health, now) {
		t.Fatal("runtime-drift witness with invalid slot parity was accepted")
	}
}

func TestServingAuthorityWitnessUsesCurrentWorkerPublicationAfterAuthorityBundleDrift(t *testing.T) {
	now := time.Date(2026, 8, 27, 18, 0, 0, 0, time.UTC)
	source := strings.Repeat("a", 40)
	image := "sha256:" + strings.Repeat("b", 64)
	witness := &edgeServingAuthorityWitness{WorkerSlot: "a", WorkerSourceSHA: source, WorkerImageDigest: image,
		BundleVersion: "edgegroupbundle_old.p26702.r513"}
	worker := edgeGroupPod{Name: "worker-a", NodeName: "edge-de", Ready: true, SourceCommit: source,
		ImageRef: "ghcr.io/example/fugue-edge@" + image, RouteBundleSource: edgeGroupAuthoritySource,
		BundleGeneration: "edgegroupbundle_current.p26746.r513", ServingGeneration: "edgegroupbundle_current",
		PublicationSequence: 26746, InventoryProducerActive: true, InventoryHeartbeatGeneration: 34953,
		InventoryHeartbeatAt: now.Add(-time.Second)}
	before := edgeGroupState{Front: map[string]edgeGroupPod{"edge-de": {Name: "front", NodeName: "edge-de"}},
		WorkerA: map[string]edgeGroupPod{"edge-de": worker}}
	status := edgeCandidateStageStatus{Ready: true, ServingHealthy: true, PublicationDecision: "published", LKGState: "current",
		BundleGeneration: "edgegroupbundle_current", CurrentPublicationSequence: 26750, RecoveryEpoch: 513,
		PublishedBundleDigest: "sha256:" + strings.Repeat("c", 64)}
	updated, err := edgeServingAuthorityWitnessWithCurrentPublication(before, witness, status, now)
	if err != nil || updated.BundleVersion != worker.BundleGeneration || updated.WorkerSlot != witness.WorkerSlot ||
		updated.WorkerSourceSHA != source || updated.WorkerImageDigest != image {
		t.Fatalf("current publication witness=%+v err=%v", updated, err)
	}
	worker.SourceCommit = strings.Repeat("d", 40)
	before.WorkerA["edge-de"] = worker
	unchanged, err := edgeServingAuthorityWitnessWithCurrentPublication(before, witness, status, now)
	if err != nil || unchanged.BundleVersion != witness.BundleVersion {
		t.Fatalf("mismatched worker identity changed witness: witness=%+v err=%v", unchanged, err)
	}
}

func TestServingAuthorityWitnessOmitsLegacyUnboundFront(t *testing.T) {
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: "edge-group-country-de", CurrentRecordDigest: "sha256:" + strings.Repeat("1", 64),
		CurrentWorkerSlot: releaseguardian.AuthoritySlotB, AuthorityEpoch: 9}
	witness, err := edgeServingAuthorityWitnessFromCurrent(edgeGroupState{ActiveSlot: "b"}, current, current.GroupID, "", "")
	if err != nil || witness != nil {
		t.Fatalf("legacy unbound Front witness=%+v err=%v", witness, err)
	}
}

type fakeEdgeGroupRuntime struct {
	snapshots       []edgeGroupState
	rolls           map[string]map[string]edgeGroupPod
	rollTargets     map[string]declarativerelease.TargetIdentity
	waits           []map[string]edgeFrontHealth
	calls           []string
	requests        []edgeActivationRequest
	rollAuthority   []bool
	rollUnready     []bool
	activationState *edgeActivationState
	standbyErr      error
	applyFailures   map[string]int
	declared        map[string]declarativerelease.TargetIdentity
	stageDegraded   bool
}

func (fake *fakeEdgeGroupRuntime) Snapshot(context.Context) (edgeGroupState, error) {
	fake.calls = append(fake.calls, "snapshot")
	if len(fake.snapshots) == 0 {
		return edgeGroupState{}, fmt.Errorf("no snapshot")
	}
	value := fake.snapshots[0]
	fake.snapshots = fake.snapshots[1:]
	return value, nil
}

func (fake *fakeEdgeGroupRuntime) ApplySharedResources(context.Context) error {
	fake.calls = append(fake.calls, "apply-shared")
	return nil
}

func (fake *fakeEdgeGroupRuntime) ApplyCandidateResources(_ context.Context, selector string) error {
	fake.calls = append(fake.calls, "apply:"+selector)
	if fake.applyFailures[selector] > 0 {
		fake.applyFailures[selector]--
		return errors.New("transient candidate resource apply failure")
	}
	return nil
}

func (fake *fakeEdgeGroupRuntime) StageCandidate(_ context.Context, before edgeGroupState, inactive string, target declarativerelease.TargetIdentity) (edgeCandidateStageReceipt, error) {
	fake.calls = append(fake.calls, "stage:"+inactive)
	digest, _ := immutableDigestFromRef(target.ImageRef)
	return edgeCandidateStageReceipt{Schema: edgeCandidateReceiptSchema,
		WorkerSlot: inactive, CurrentWorkerSlot: before.ActiveSlot, WorkerSourceSHA: target.ConfigSHA, WorkerImageDigest: digest,
		AllowDegradedPrevious: fake.stageDegraded}, nil
}

func (fake *fakeEdgeGroupRuntime) StageStandby(_ context.Context, before edgeGroupState, inactive string, target declarativerelease.TargetIdentity) (edgeCandidateStageReceipt, error) {
	fake.calls = append(fake.calls, "stage-standby:"+inactive)
	if fake.standbyErr != nil {
		return edgeCandidateStageReceipt{}, fake.standbyErr
	}
	digest, _ := immutableDigestFromRef(target.ImageRef)
	return edgeCandidateStageReceipt{Schema: edgeCandidateReceiptSchema,
		WorkerSlot: inactive, CurrentWorkerSlot: before.ActiveSlot, WorkerSourceSHA: target.ConfigSHA, WorkerImageDigest: digest, StandbyOnly: true}, nil
}

func (fake *fakeEdgeGroupRuntime) DeclaredTarget(name string) (declarativerelease.TargetIdentity, error) {
	target, exists := fake.declared[name]
	if !exists {
		return declarativerelease.TargetIdentity{}, fmt.Errorf("undeclared target %s", name)
	}
	return target, nil
}

func (fake *fakeEdgeGroupRuntime) Roll(_ context.Context, name string, target declarativerelease.TargetIdentity, requireGroupAuthority, replaceUnready bool) (map[string]edgeGroupPod, error) {
	fake.calls = append(fake.calls, "roll:"+name)
	fake.rollAuthority = append(fake.rollAuthority, requireGroupAuthority)
	fake.rollUnready = append(fake.rollUnready, replaceUnready)
	if fake.rollTargets != nil {
		fake.rollTargets[name] = target
	}
	value, exists := fake.rolls[name]
	if !exists {
		return nil, fmt.Errorf("unexpected roll %s", name)
	}
	return value, nil
}

func (fake *fakeEdgeGroupRuntime) WaitCandidateWorkerAuthority(_ context.Context, name string, _ declarativerelease.TargetIdentity, _ edgeCandidateStageReceipt) (map[string]edgeGroupPod, error) {
	fake.calls = append(fake.calls, "wait-candidate-authority:"+name)
	value, exists := fake.rolls[name]
	if !exists {
		return nil, fmt.Errorf("unexpected candidate authority wait %s", name)
	}
	return value, nil
}

func (fake *fakeEdgeGroupRuntime) SelectCASExecutor(_ context.Context, candidates ...edgeGroupPod) (edgeGroupPod, error) {
	fake.calls = append(fake.calls, "select-cas")
	for _, candidate := range candidates {
		if candidate.Name != "" {
			return candidate, nil
		}
	}
	return edgeGroupPod{}, fmt.Errorf("no executor")
}

func (fake *fakeEdgeGroupRuntime) ReadActivation(context.Context, edgeGroupPod) (edgeActivationState, bool, error) {
	fake.calls = append(fake.calls, "read-activation")
	if fake.activationState == nil {
		return edgeActivationState{}, false, nil
	}
	return *fake.activationState, true, nil
}

func (fake *fakeEdgeGroupRuntime) ActivationCAS(_ context.Context, _ edgeGroupPod, request edgeActivationRequest) (edgeActivationReceipt, error) {
	fake.calls = append(fake.calls, "cas:"+request.Operation+":"+request.TargetSlot)
	fake.requests = append(fake.requests, request)
	state := edgeActivationState{
		Schema: edgeActivationStateSchema, GroupID: request.GroupID, Generation: request.ExpectedGeneration + 1,
		ActiveSlot: request.TargetSlot, BundleGeneration: request.BundleGeneration, WorkerSourceCommit: request.WorkerSourceCommit,
		WorkerImageDigest: request.WorkerImageDigest, Authority: edgeActivationAuthority, Operation: request.Operation,
		RollbackOfGeneration: request.RollbackOfGeneration,
	}
	fake.activationState = &state
	return edgeActivationReceipt{Schema: edgeActivationReceiptSchema, GroupID: request.GroupID, Current: state}, nil
}

func (fake *fakeEdgeGroupRuntime) WaitFront(_ context.Context, slot, source, digest string) (map[string]edgeFrontHealth, error) {
	fake.calls = append(fake.calls, "wait-front:"+slot)
	if len(fake.waits) == 0 {
		return nil, fmt.Errorf("unexpected front wait")
	}
	value := fake.waits[0]
	fake.waits = fake.waits[1:]
	return value, nil
}

func (fake *fakeEdgeGroupRuntime) WaitCurrentAuthority(_ context.Context, _ edgeCandidateStageReceipt) error {
	fake.calls = append(fake.calls, "wait-current-authority")
	return nil
}

func (fake *fakeEdgeGroupRuntime) WaitActiveWorkerAuthority(_ context.Context, name string, _ declarativerelease.TargetIdentity) error {
	fake.calls = append(fake.calls, "wait-worker-authority:"+name)
	return nil
}

func TestParseEdgeGroupPodsRequiresOneReadyGroupBoundPodPerNode(t *testing.T) {
	pods := map[string]any{"items": []any{
		edgeGroupPodFixture("worker-1", "uid-1", "node-1", "edge-group-country-us", strings.Repeat("1", 40), strings.Repeat("a", 64)),
		edgeGroupPodFixture("worker-2", "uid-2", "node-2", "edge-group-country-us", strings.Repeat("1", 40), strings.Repeat("a", 64)),
	}}
	raw, _ := json.Marshal(pods)
	got, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 2, "edge-group-country-us", true)
	if err != nil || len(got) != 2 || got["node-1"].Name != "worker-1" || !got["node-2"].Ready {
		t.Fatalf("parse edge group pods: got=%+v err=%v", got, err)
	}

	pods["items"].([]any)[1].(map[string]any)["metadata"].(map[string]any)["labels"].(map[string]any)["fugue.io/edge-group-id"] = "edge-group-country-de"
	raw, _ = json.Marshal(pods)
	if _, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 2, "edge-group-country-us", true); err == nil || !strings.Contains(err.Error(), "group identity") {
		t.Fatalf("cross-group pod was accepted: %v", err)
	}

	delete(pods["items"].([]any)[1].(map[string]any)["metadata"].(map[string]any)["labels"].(map[string]any), "fugue.io/edge-group-id")
	raw, _ = json.Marshal(pods)
	if _, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 2, "edge-group-country-us", true); err == nil || !strings.Contains(err.Error(), "group identity") {
		t.Fatalf("pod without group identity was accepted: %v", err)
	}

	pods["items"] = []any{edgeGroupPodFixture("worker-1", "uid-1", "node-1", "edge-group-country-us", strings.Repeat("1", 40), strings.Repeat("a", 64))}
	raw, _ = json.Marshal(pods)
	if _, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 2, "edge-group-country-us", true); err == nil || !strings.Contains(err.Error(), "want 2") {
		t.Fatalf("partial group cohort was accepted: %v", err)
	}
}

func TestParseEdgeGroupPodsSnapshotPreservesUnreadyImmutableIdentity(t *testing.T) {
	pod := edgeGroupPodFixture("worker-b", "uid-b", "node-1", "edge-group-country-us", strings.Repeat("1", 40), strings.Repeat("a", 64))
	pod["status"].(map[string]any)["conditions"] = []any{map[string]any{"type": "Ready", "status": "False"}}
	raw, _ := json.Marshal(map[string]any{"items": []any{pod}})

	if _, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 1, "edge-group-country-us", true); err == nil || !strings.Contains(err.Error(), "readiness") {
		t.Fatalf("strict worker read accepted an unready active slot: %v", err)
	}
	got, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 1, "edge-group-country-us", false)
	if err != nil {
		t.Fatal(err)
	}
	worker := got["node-1"]
	if worker.Ready || worker.UID != "uid-b" || worker.ResourceVersion != "42" || worker.SourceCommit != strings.Repeat("1", 40) ||
		worker.ImageRef != "ghcr.io/example/fugue-edge@sha256:"+strings.Repeat("a", 64) || worker.ImageID == "" {
		t.Fatalf("unready worker identity was not preserved exactly: %+v", worker)
	}

	delete(pod["metadata"].(map[string]any), "uid")
	raw, _ = json.Marshal(map[string]any{"items": []any{pod}})
	if _, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 1, "edge-group-country-us", false); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("lenient snapshot accepted missing immutable identity: %v", err)
	}

	pod = edgeGroupPodFixture("worker-b", "uid-b", "node-1", "edge-group-country-us", "", strings.Repeat("a", 64))
	delete(pod["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any), "imageID")
	delete(pod["metadata"].(map[string]any)["annotations"].(map[string]any), "fugue.pro/source-commit")
	raw, _ = json.Marshal(map[string]any{"items": []any{pod}})
	if _, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 1, "edge-group-country-us", false); err != nil {
		t.Fatalf("snapshot rejected pod without runtime-only evidence: %v", err)
	}
}

func TestParseEdgeGroupPodsAcceptsReadyLKGWithHistoricalRestarts(t *testing.T) {
	pod := edgeGroupPodFixture("worker-1", "uid-1", "node-1", "edge-group-country-de", strings.Repeat("1", 40), strings.Repeat("a", 64))
	pod["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["restartCount"] = 2
	raw, _ := json.Marshal(map[string]any{"items": []any{pod}})

	got, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 1, "edge-group-country-de", true)
	if err != nil || !got["node-1"].Ready || got["node-1"].RestartCount != 2 {
		t.Fatalf("ready LKG with historical restarts was rejected: got=%+v err=%v", got, err)
	}
	target := declarativerelease.TargetIdentity{Present: true, ConfigSHA: strings.Repeat("1", 40), ImageRef: "ghcr.io/example/fugue-edge@sha256:" + strings.Repeat("a", 64)}
	if edgePodMatchesTarget(got["node-1"], target) {
		t.Fatal("new target accepted a restarted pod")
	}
}

func TestEdgeGroupTargetMatchingRequiresExactSourceAndImmutableRef(t *testing.T) {
	target := declarativerelease.TargetIdentity{Present: true, ConfigSHA: strings.Repeat("1", 40), ImageRef: "ghcr.io/example/fugue-edge@sha256:" + strings.Repeat("a", 64)}
	pod := edgeGroupPod{Ready: true, SourceCommit: target.ConfigSHA, ImageRef: target.ImageRef}
	if !edgePodMatchesTarget(pod, target) {
		t.Fatal("exact edge pod target did not match")
	}
	pod.SourceCommit = strings.Repeat("2", 40)
	if edgePodMatchesTarget(pod, target) {
		t.Fatal("wrong edge source commit matched target")
	}
	pod.SourceCommit = target.ConfigSHA
	pod.ImageRef = "ghcr.io/example/fugue-edge:latest"
	if edgePodMatchesTarget(pod, target) {
		t.Fatal("mutable edge image matched target")
	}
	if digest, err := immutableDigestFromRef(target.ImageRef); err != nil || digest != "sha256:"+strings.Repeat("a", 64) {
		t.Fatalf("immutable digest parse=%q err=%v", digest, err)
	}
	if _, err := immutableDigestFromRef("ghcr.io/example/fugue-edge:latest"); err == nil {
		t.Fatal("mutable edge reference yielded a digest")
	}
}

func TestExecuteEdgeGroupABRollsInactiveSwitchesAndThenRollsFormerActive(t *testing.T) {
	transition := edgeTransitionFixture()
	old := edgeTargetFixture("1", "a")
	target := edgeTargetFixture("2", "b")
	before := edgeStateFixture("a", old, edgeFrontHealth{ActiveSlot: "a"})
	final := edgeStateFixture("b", target, edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 2, WorkerSourceCommit: target.ConfigSHA, WorkerImageDigest: digestFromTarget(t, target), RouteAuthority: edgeActivationAuthority})
	final.WorkerA = before.WorkerA
	runtime := &fakeEdgeGroupRuntime{
		snapshots: []edgeGroupState{before, final},
		rolls: map[string]map[string]edgeGroupPod{
			transition.WorkerBName: final.WorkerB,
			transition.FrontName:   final.Front,
			transition.WorkerAName: final.WorkerA,
		},
		waits:    []map[string]edgeFrontHealth{{"node-1": final.FrontHealth["node-1"]}},
		declared: map[string]declarativerelease.TargetIdentity{transition.WorkerAName: old, transition.WorkerBName: target, transition.FrontName: target},
	}
	release := declarativerelease.PlanRelease{
		ExpectedPreviousConfigSHA: old.ConfigSHA,
		Transition:                &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &transition},
	}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, target); err != nil {
		t.Fatal(err)
	}
	want := []string{"snapshot", "apply-shared", "stage:b", "apply:b", "roll:" + transition.WorkerBName, "wait-candidate-authority:" + transition.WorkerBName, "wait-current-authority", "wait-front:b", "apply:" + transition.FrontName, "roll:" + transition.FrontName, "wait-worker-authority:" + transition.WorkerBName, "stage-standby:a", "apply:" + transition.WorkerAName, "roll:" + transition.WorkerAName, "snapshot"}
	if strings.Join(runtime.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("edge forward order=%v want=%v", runtime.calls, want)
	}
	if len(runtime.requests) != 0 {
		t.Fatalf("edge forward performed a direct Front activation CAS: %+v", runtime.requests)
	}
	if got, want := fmt.Sprint(runtime.rollAuthority), "[true true true]"; got != want {
		t.Fatalf("edge authority gates=%s want=%s", got, want)
	}
	if got, want := fmt.Sprint(runtime.rollUnready), "[false false true]"; got != want {
		t.Fatalf("edge replace-unready gates=%s want=%s", got, want)
	}
}

func TestExecuteEdgeGroupABKeepsPreviousAuthoritySlotAtExactLKG(t *testing.T) {
	transition := edgeTransitionFixture()
	old := edgeTargetFixture("1", "a")
	failed := edgeTargetFixture("2", "b")
	target := edgeTargetFixture("3", "c")
	before := edgeStateFixture("a", failed, edgeFrontHealth{ActiveSlot: "a"})
	before.WorkerB = edgeStateFixture("b", old, edgeFrontHealth{ActiveSlot: "b"}).WorkerB
	finalHealth := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 4, WorkerSourceCommit: target.ConfigSHA, WorkerImageDigest: digestFromTarget(t, target), RouteAuthority: edgeActivationAuthority}
	final := edgeStateFixture("b", target, finalHealth)
	final.WorkerA = edgeStateFixture("a", old, edgeFrontHealth{ActiveSlot: "a"}).WorkerA
	runtime := &fakeEdgeGroupRuntime{
		snapshots: []edgeGroupState{before, final},
		rolls: map[string]map[string]edgeGroupPod{
			transition.WorkerBName: final.WorkerB,
			transition.FrontName:   final.Front,
			transition.WorkerAName: final.WorkerA,
		},
		waits:         []map[string]edgeFrontHealth{{"node-1": finalHealth}},
		declared:      map[string]declarativerelease.TargetIdentity{transition.WorkerAName: old, transition.WorkerBName: target, transition.FrontName: target},
		stageDegraded: true,
	}
	release := declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: old.ConfigSHA, SupersedesFailedConfigSHA: failed.ConfigSHA,
		Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &transition}}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, target); err != nil {
		t.Fatal(err)
	}
	if got := final.WorkerA["node-1"].SourceCommit; got != old.ConfigSHA {
		t.Fatalf("previous authority slot changed from LKG: %s", got)
	}
	if got, want := fmt.Sprint(runtime.rollUnready), "[true true]"; got != want {
		t.Fatalf("failed successor replace-unready gates=%s want=%s", got, want)
	}
	if got, want := fmt.Sprint(runtime.rollAuthority), "[false false]"; got != want {
		t.Fatalf("failed successor authority gates=%s want=%s", got, want)
	}
	if joined := strings.Join(runtime.calls, "\n"); strings.Contains(joined, "apply:"+transition.WorkerAName) ||
		strings.Contains(joined, "roll:"+transition.WorkerAName) || strings.Contains(joined, "stage-standby:a") {
		t.Fatalf("stale former-active declaration mutated the previous authority slot: %v", runtime.calls)
	}
}

func TestExecuteEdgeGroupABDoesNotMutateDriftedActiveWorkerBeforePromotion(t *testing.T) {
	transition := edgeTransitionFixture()
	old := edgeTargetFixture("1", "a")
	failed := edgeTargetFixture("2", "b")
	target := edgeTargetFixture("3", "c")
	frontTarget := edgeTargetFixture("4", "front")
	beforeHealth := edgeFrontHealth{ActiveSlot: "a", ActivationPresent: true, Generation: 4,
		BundleGeneration: "bundle-a", WorkerSourceCommit: failed.ConfigSHA, WorkerImageDigest: digestFromTarget(t, failed), RouteAuthority: edgeActivationAuthority}
	before := edgeStateFixture("a", failed, beforeHealth)
	active := before.WorkerA["node-1"]
	active.Ready = false
	active.RouteBundleSource = ""
	before.WorkerA["node-1"] = active
	before.FrontActivation = &edgeActivationState{Schema: edgeActivationStateSchema, GroupID: transition.GroupID,
		Generation: 4, ActiveSlot: "a", BundleGeneration: "bundle-a", WorkerSourceCommit: failed.ConfigSHA,
		WorkerImageDigest: digestFromTarget(t, failed), Authority: edgeActivationAuthority, Operation: edgeActivationPromote}
	finalHealth := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 5,
		BundleGeneration: "bundle-b", WorkerSourceCommit: target.ConfigSHA, WorkerImageDigest: digestFromTarget(t, target), RouteAuthority: edgeActivationAuthority}
	final := edgeStateFixture("b", target, finalHealth)
	for node, pod := range final.Front {
		pod.SourceCommit = frontTarget.ConfigSHA
		pod.ImageRef = frontTarget.ImageRef
		final.Front[node] = pod
	}
	final.WorkerA = edgeStateFixture("a", old, edgeFrontHealth{ActiveSlot: "a"}).WorkerA
	runtime := &fakeEdgeGroupRuntime{
		snapshots: []edgeGroupState{before, final},
		rolls: map[string]map[string]edgeGroupPod{
			transition.WorkerBName: final.WorkerB,
			transition.WorkerAName: final.WorkerA,
			transition.FrontName:   final.Front,
		},
		waits:           []map[string]edgeFrontHealth{{"node-1": finalHealth}},
		declared:        map[string]declarativerelease.TargetIdentity{transition.WorkerAName: old, transition.WorkerBName: target, transition.FrontName: frontTarget},
		rollTargets:     make(map[string]declarativerelease.TargetIdentity),
		activationState: before.FrontActivation,
		stageDegraded:   true,
	}
	release := declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: old.ConfigSHA, SupersedesFailedConfigSHA: failed.ConfigSHA,
		Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &transition}}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, target); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runtime.calls, "\n")
	if strings.Contains(joined, "apply:"+transition.WorkerAName) || strings.Contains(joined, "roll:"+transition.WorkerAName) {
		t.Fatalf("drifted active Worker was rewritten from a stale declaration: %v", runtime.calls)
	}
	if len(runtime.requests) != 0 {
		t.Fatalf("executor wrote traffic activation during promotion: %+v", runtime.requests)
	}
	if got := runtime.rollTargets[transition.FrontName]; got != frontTarget {
		t.Fatalf("Front recovery used Worker target: got=%+v want=%+v", got, frontTarget)
	}
}

func TestExecuteEdgeGroupABRetriesFrontAfterExactAuthorityCommits(t *testing.T) {
	transition := edgeTransitionFixture()
	old := edgeTargetFixture("1", "a")
	target := edgeTargetFixture("2", "b")
	before := edgeStateFixture("a", old, edgeFrontHealth{ActiveSlot: "a"})
	finalHealth := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 2,
		WorkerSourceCommit: target.ConfigSHA, WorkerImageDigest: digestFromTarget(t, target), RouteAuthority: edgeActivationAuthority}
	final := edgeStateFixture("b", target, finalHealth)
	final.WorkerA = before.WorkerA
	runtime := &fakeEdgeGroupRuntime{
		snapshots: []edgeGroupState{before, final},
		rolls: map[string]map[string]edgeGroupPod{
			transition.WorkerBName: final.WorkerB,
			transition.FrontName:   final.Front,
			transition.WorkerAName: final.WorkerA,
		},
		waits:         []map[string]edgeFrontHealth{{"node-1": finalHealth}},
		declared:      map[string]declarativerelease.TargetIdentity{transition.WorkerAName: old, transition.WorkerBName: target, transition.FrontName: target},
		applyFailures: map[string]int{transition.FrontName: 1},
		stageDegraded: true,
	}
	release := declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: old.ConfigSHA, SupersedesFailedConfigSHA: strings.Repeat("f", 40),
		Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &transition}}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, target); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runtime.calls, "\n")
	if strings.Count(joined, "apply:"+transition.FrontName) != 2 || strings.Count(joined, "wait-current-authority") != 1 ||
		strings.Index(joined, "wait-current-authority") > strings.LastIndex(joined, "apply:"+transition.FrontName) {
		t.Fatalf("Front did not retry strictly after the committed authority: %v", runtime.calls)
	}
}

func TestExecuteEdgeGroupABDoesNotRollbackCommittedAuthorityWhenStandbyStagingFails(t *testing.T) {
	transition := edgeTransitionFixture()
	old := edgeTargetFixture("1", "a")
	target := edgeTargetFixture("2", "b")
	before := edgeStateFixture("a", old, edgeFrontHealth{ActiveSlot: "a"})
	finalHealth := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 2,
		WorkerSourceCommit: target.ConfigSHA, WorkerImageDigest: digestFromTarget(t, target), RouteAuthority: edgeActivationAuthority}
	final := edgeStateFixture("b", target, finalHealth)
	final.WorkerA = before.WorkerA
	runtime := &fakeEdgeGroupRuntime{
		snapshots: []edgeGroupState{before, final},
		rolls: map[string]map[string]edgeGroupPod{
			transition.WorkerBName: final.WorkerB,
			transition.FrontName:   final.Front,
		},
		waits:      []map[string]edgeFrontHealth{{"node-1": finalHealth}},
		declared:   map[string]declarativerelease.TargetIdentity{transition.WorkerAName: old, transition.WorkerBName: target, transition.FrontName: target},
		standbyErr: errors.New("standby sequence changed"),
	}
	release := declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: old.ConfigSHA,
		Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &transition}}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, target); err != nil {
		t.Fatal(err)
	}
	want := []string{"snapshot", "apply-shared", "stage:b", "apply:b", "roll:" + transition.WorkerBName, "wait-candidate-authority:" + transition.WorkerBName, "wait-current-authority", "wait-front:b", "apply:" + transition.FrontName,
		"roll:" + transition.FrontName, "wait-worker-authority:" + transition.WorkerBName, "stage-standby:a", "snapshot"}
	if strings.Join(runtime.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("post-commit standby failure changed serving workloads: calls=%v want=%v", runtime.calls, want)
	}
	if got, want := fmt.Sprint(runtime.rollAuthority), "[true true]"; got != want {
		t.Fatalf("post-commit standby failure authority gates=%s want=%s", got, want)
	}
}

func TestExecuteEdgeGroupABDoesNotCompensateGuardianActivationOnObservationFailure(t *testing.T) {
	transition := edgeTransitionFixture()
	old := edgeTargetFixture("1", "a")
	target := edgeTargetFixture("2", "b")
	beforeHealth := edgeFrontHealth{ActiveSlot: "a", ActivationPresent: true, Generation: 4,
		BundleGeneration: "old-bundle", WorkerSourceCommit: old.ConfigSHA, WorkerImageDigest: digestFromTarget(t, old), RouteAuthority: edgeActivationAuthority}
	before := edgeStateFixture("a", old, beforeHealth)
	runtime := &fakeEdgeGroupRuntime{
		snapshots: []edgeGroupState{before},
		rolls:     map[string]map[string]edgeGroupPod{transition.WorkerBName: before.WorkerB},
		declared:  map[string]declarativerelease.TargetIdentity{transition.WorkerAName: old, transition.WorkerBName: target, transition.FrontName: target},
		activationState: &edgeActivationState{Schema: edgeActivationStateSchema, GroupID: transition.GroupID, Generation: 5,
			ActiveSlot: "b", BundleGeneration: "new-bundle", WorkerSourceCommit: target.ConfigSHA, WorkerImageDigest: digestFromTarget(t, target),
			Authority: edgeActivationAuthority, Operation: edgeActivationPromote},
	}
	release := declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: old.ConfigSHA,
		Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &transition}}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, target); err == nil || !strings.Contains(err.Error(), "observe Guardian authority switch") {
		t.Fatalf("Guardian observation failure was not reported: %v", err)
	}
	if len(runtime.requests) != 0 {
		t.Fatalf("executor raced Guardian compensation: %+v", runtime.requests)
	}
	if runtime.activationState == nil || runtime.activationState.ActiveSlot != "b" || runtime.activationState.Generation != 5 {
		t.Fatalf("executor mutated Guardian-owned activation: %+v", runtime.activationState)
	}
}

func TestExecuteEdgeGroupABCompensationSwitchesBeforeRestoringFront(t *testing.T) {
	transition := edgeTransitionFixture()
	lkg := edgeTargetFixture("1", "a")
	current := edgeTargetFixture("2", "b")
	currentHealth := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 4, WorkerSourceCommit: current.ConfigSHA, WorkerImageDigest: digestFromTarget(t, current), RouteAuthority: edgeActivationAuthority}
	before := edgeStateFixture("b", current, currentHealth)
	before.FrontActivation = &edgeActivationState{Schema: edgeActivationStateSchema, GroupID: transition.GroupID, Generation: 4,
		ActiveSlot: "b", BundleGeneration: "bundle-b", WorkerSourceCommit: current.ConfigSHA,
		WorkerImageDigest: digestFromTarget(t, current), Authority: edgeActivationAuthority, Operation: edgeActivationPromote}
	before.WorkerA = edgeStateFixture("a", lkg, edgeFrontHealth{ActiveSlot: "a"}).WorkerA
	finalHealth := edgeFrontHealth{ActiveSlot: "a", ActivationPresent: true, Generation: 5, WorkerSourceCommit: lkg.ConfigSHA, WorkerImageDigest: digestFromTarget(t, lkg), RouteAuthority: edgeActivationAuthority}
	runtime := &fakeEdgeGroupRuntime{
		snapshots: []edgeGroupState{before},
		rolls: map[string]map[string]edgeGroupPod{
			transition.WorkerAName: edgeStateFixture("a", lkg, finalHealth).WorkerA,
			transition.WorkerBName: edgeStateFixture("a", lkg, finalHealth).WorkerB,
			transition.FrontName:   edgeStateFixture("a", lkg, finalHealth).Front,
		},
		waits:           []map[string]edgeFrontHealth{{"node-1": finalHealth}},
		activationState: before.FrontActivation,
		declared:        map[string]declarativerelease.TargetIdentity{transition.WorkerAName: lkg, transition.WorkerBName: lkg, transition.FrontName: lkg},
	}
	release := declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: lkg.ConfigSHA}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, lkg); err != nil {
		t.Fatal(err)
	}
	want := []string{"snapshot", "select-cas", "read-activation", "cas:rollback:a", "wait-front:a", "apply:", "roll:" + transition.WorkerAName, "roll:" + transition.WorkerBName, "roll:" + transition.FrontName}
	if strings.Join(runtime.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("edge compensation order=%v want=%v", runtime.calls, want)
	}
	if len(runtime.requests) != 1 || runtime.requests[0].Operation != edgeActivationRollback || runtime.requests[0].TargetSlot != "a" || runtime.requests[0].RollbackOfGeneration != 4 {
		t.Fatalf("exact LKG compensation did not restore authority first: %+v", runtime.requests)
	}
}

func TestEdgeGroupAuthorityRequiresPublicationOnBothSlotsAndInventoryOnActive(t *testing.T) {
	transition := edgeTransitionFixture()
	target := edgeTargetFixture("2", "b")
	state := edgeStateFixture("a", target, edgeFrontHealth{ActiveSlot: "a"})
	if err := validateEdgeGroupAuthority(state, transition); err != nil {
		t.Fatalf("valid authority evidence: %v", err)
	}
	pod := state.WorkerB["node-1"]
	pod.RouteBundleSource = ""
	state.WorkerB["node-1"] = pod
	if err := validateEdgeGroupAuthority(state, transition); err == nil || !strings.Contains(err.Error(), "publication") {
		t.Fatalf("missing inactive publication was accepted: %v", err)
	}
	state = edgeStateFixture("a", target, edgeFrontHealth{ActiveSlot: "a"})
	pod = state.WorkerA["node-1"]
	pod.InventoryProducerActive = false
	state.WorkerA["node-1"] = pod
	if err := validateEdgeGroupAuthority(state, transition); err == nil || !strings.Contains(err.Error(), "inventory") {
		t.Fatalf("missing active inventory was accepted: %v", err)
	}
}

func TestActiveInventoryUsesFreshVerifiedSuccessAcrossTransientFailure(t *testing.T) {
	now := time.Date(2026, 8, 25, 1, 0, 0, 0, time.UTC)
	pod := edgeGroupPod{RouteBundleSource: edgeGroupAuthoritySource, PublicationSequence: 4, ServingGeneration: "routes",
		InventoryProducerActive: true, InventoryHeartbeatGeneration: 9, InventoryHeartbeatAt: now.Add(-time.Minute),
		InventoryHeartbeatError: "Edge inventory producer heartbeat returned status 409"}
	if !edgePodHasActiveInventoryAt(pod, now) {
		t.Fatal("transient failure erased a still-fresh verified inventory receipt")
	}
	pod.InventoryHeartbeatAt = now.Add(-edgeInventoryHeartbeatMaxAge - time.Nanosecond)
	if edgePodHasActiveInventoryAt(pod, now) {
		t.Fatal("stale inventory receipt remained authoritative")
	}
}

func TestCandidateAuthorityRequiresExactStagedReleaseWitness(t *testing.T) {
	stage := edgeCandidateStageReceipt{WorkerSlot: "a", CandidateBundleGeneration: "routes",
		CandidateRecordDigest: "sha256:" + strings.Repeat("a", 64), ReleaseRecordDigest: "sha256:" + strings.Repeat("b", 64)}
	pod := edgeGroupPod{RouteBundleSource: edgeGroupAuthoritySource, PublicationSequence: 12, ServingGeneration: "routes",
		BundleGeneration: "routes.p12.r0", CandidateBundleLoaded: true, CandidateRecordDigest: stage.CandidateRecordDigest,
		CandidateReleaseRecordDigest: stage.ReleaseRecordDigest, CandidateWorkerSlot: stage.WorkerSlot}
	if !edgePodHasCandidateAuthority(pod, stage) {
		t.Fatal("exact staged candidate authority was rejected")
	}
	for name, mutate := range map[string]func(*edgeGroupPod){
		"generation": func(value *edgeGroupPod) { value.ServingGeneration = "other-routes" },
		"record":     func(value *edgeGroupPod) { value.CandidateRecordDigest = "sha256:" + strings.Repeat("c", 64) },
		"release":    func(value *edgeGroupPod) { value.CandidateReleaseRecordDigest = "sha256:" + strings.Repeat("d", 64) },
		"slot":       func(value *edgeGroupPod) { value.CandidateWorkerSlot = "b" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := pod
			mutate(&changed)
			if edgePodHasCandidateAuthority(changed, stage) {
				t.Fatal("candidate authority drift was accepted")
			}
		})
	}
}

func TestEdgeCASExecutorRequiresBinaryAndWritableSharedStateMount(t *testing.T) {
	transition := edgeTransitionFixture()
	got := edgeCASExecutorProbeArguments(transition)
	want := []string{"sh", "-ceu", `test -x "$1" && test -d "$2" && test -w "$2"`, "sh", transition.CASBinary, "/var/lib/fugue-edge-front"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("CAS executor probe=%q want=%q", got, want)
	}
}

func TestInactiveWorkerRollCollectsBundleHealthBeforeActivationCAS(t *testing.T) {
	transition := edgeTransitionFixture()
	if !edgeRollIncludesWorkerHealth(transition, transition.WorkerBName) {
		t.Fatal("inactive worker roll disabled bundle-health collection")
	}
	if edgeRollIncludesWorkerHealth(transition, transition.FrontName) {
		t.Fatal("front roll unexpectedly requested worker bundle health")
	}
}

func TestEdgeActivationCommitUnknownRequiresExactStateOrPrecondition(t *testing.T) {
	request := edgeActivationRequest{GroupID: "edge-group-country-de", ExpectedGeneration: 3, ExpectedSlot: "a", TargetSlot: "b", BundleGeneration: "bundle-4",
		WorkerSourceCommit: strings.Repeat("1", 40), WorkerImageDigest: "sha256:" + strings.Repeat("a", 64), Operation: edgeActivationPromote}
	precondition := edgeActivationState{Schema: edgeActivationStateSchema, GroupID: request.GroupID, Generation: 3, ActiveSlot: "a", Authority: edgeActivationAuthority}
	committed := edgeActivationState{Schema: edgeActivationStateSchema, GroupID: request.GroupID, Generation: 4, ActiveSlot: "b", BundleGeneration: request.BundleGeneration,
		WorkerSourceCommit: request.WorkerSourceCommit, WorkerImageDigest: request.WorkerImageDigest, Authority: edgeActivationAuthority, Operation: request.Operation}
	if !edgeActivationStateMatchesPrecondition(precondition, true, request) || !edgeActivationStateMatchesRequest(committed, request) {
		t.Fatal("exact activation precondition or committed state was rejected")
	}
	precondition.ActiveSlot = "b"
	committed.WorkerImageDigest = "sha256:" + strings.Repeat("b", 64)
	if edgeActivationStateMatchesPrecondition(precondition, true, request) || edgeActivationStateMatchesRequest(committed, request) {
		t.Fatal("activation CAS drift was accepted")
	}
}

func TestValidateCommittedEdgeGroupStateUsesCurrentAuthorityForPreviousSlot(t *testing.T) {
	transition := edgeTransitionFixture()
	currentTarget := edgeTargetFixture("9", "b")
	previousTarget := edgeTargetFixture("4", "a")
	currentDigest := digestFromTarget(t, currentTarget)
	previousDigest := digestFromTarget(t, previousTarget)
	state := edgeStateFixture("a", currentTarget, edgeFrontHealth{
		ActiveSlot: "a", ActivationPresent: true, Generation: 12,
		BundleGeneration: "bundle.p39769.r150", WorkerSourceCommit: currentTarget.ConfigSHA,
		WorkerImageDigest: currentDigest, RouteAuthority: edgeActivationAuthority,
	})
	state.WorkerB = map[string]edgeGroupPod{"node-1": {
		Name: "worker-b-pod", UID: "worker-b-uid", ResourceVersion: "43", NodeName: "node-1",
		SourceCommit: previousTarget.ConfigSHA, ImageRef: previousTarget.ImageRef, ImageID: previousTarget.ImageRef,
		RouteBundleSource: edgeGroupAuthoritySource, PublicationSequence: 39768, ServingGeneration: "generation-previous", Ready: true,
	}}
	current := releaseguardian.CurrentAuthority{
		APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind, GroupID: transition.GroupID,
		CurrentRecordDigest: "sha256:" + strings.Repeat("1", 64), CurrentWorkerSlot: releaseguardian.AuthoritySlotA,
		CurrentFrontGeneration: 12, CurrentBundleGeneration: "bundle.p39769.r150", CurrentWorkerSourceSHA: currentTarget.ConfigSHA,
		CurrentWorkerImageDigest: currentDigest, PreviousRecordDigest: "sha256:" + strings.Repeat("2", 64),
		PreviousWorkerSlot: releaseguardian.AuthoritySlotB, PreviousFrontGeneration: 10,
		PreviousBundleGeneration: "bundle.p39768.r150", PreviousWorkerSourceSHA: previousTarget.ConfigSHA,
		PreviousWorkerImageDigest: previousDigest, AuthorityEpoch: 39559,
	}

	if err := validateCommittedEdgeGroupState(state, current, transition, currentTarget); err != nil {
		t.Fatalf("authority-bound previous slot was rejected: %v", err)
	}
	state.WorkerB["node-1"] = state.WorkerA["node-1"]
	if err := validateCommittedEdgeGroupState(state, current, transition, currentTarget); err == nil || !strings.Contains(err.Error(), "previous Edge Worker") {
		t.Fatalf("static-forward previous slot was accepted: %v", err)
	}
}

func edgeGroupPodFixture(name, uid, node, group, source, digest string) map[string]any {
	return map[string]any{
		"metadata": map[string]any{
			"name": name, "uid": uid, "resourceVersion": "42",
			"labels":      map[string]any{"fugue.io/edge-group-id": group},
			"annotations": map[string]any{"fugue.pro/source-commit": source},
		},
		"spec": map[string]any{
			"nodeName":   node,
			"containers": []any{map[string]any{"name": "edge", "image": "ghcr.io/example/fugue-edge@sha256:" + digest}},
		},
		"status": map[string]any{
			"conditions":        []any{map[string]any{"type": "Ready", "status": "True"}},
			"containerStatuses": []any{map[string]any{"name": "edge", "imageID": "containerd://sha256:" + digest, "restartCount": 0}},
		},
	}
}

func edgeTransitionFixture() declarativerelease.EdgeGroupABTransition {
	return declarativerelease.EdgeGroupABTransition{GroupID: "edge-group-country-us", CandidateStageURL: "http://edge-control-us:8092/v1/authority/group-worker-candidates", CandidateKeyring: "/var/run/secrets/fugue-authority-recovery-us/keyring.json", FrontName: "front", WorkerAName: "worker-a", WorkerBName: "worker-b", WorkerContainer: "edge", ActivationStatePath: "/var/lib/fugue-edge-front/activation.json", CASBinary: "/usr/local/bin/fugue-edge-front-cas", ExpectedNodes: 1, SoakSeconds: 180}
}

func edgeTargetFixture(sourceDigit, digestDigit string) declarativerelease.TargetIdentity {
	return declarativerelease.TargetIdentity{Present: true, ConfigSHA: strings.Repeat(sourceDigit, 40), OCIRevision: strings.Repeat(sourceDigit, 40), ImageRef: "ghcr.io/example/fugue-edge@sha256:" + strings.Repeat(digestDigit, 64)}
}

func edgeStateFixture(active string, target declarativerelease.TargetIdentity, health edgeFrontHealth) edgeGroupState {
	pod := func(name string) map[string]edgeGroupPod {
		return map[string]edgeGroupPod{"node-1": {Name: name + "-pod", UID: name + "-uid", ResourceVersion: "42", NodeName: "node-1", SourceCommit: target.ConfigSHA, ImageRef: target.ImageRef, ImageID: target.ImageRef, BundleGeneration: "bundle-" + active,
			RouteBundleSource: edgeGroupAuthoritySource, PublicationSequence: 1, ServingGeneration: "generation-one",
			InventoryProducerActive: true, InventoryHeartbeatGeneration: 1, InventoryHeartbeatAt: time.Now().UTC(), Ready: true}}
	}
	return edgeGroupState{Front: pod("front"), FrontHealth: map[string]edgeFrontHealth{"node-1": health}, WorkerA: pod("worker-a"), WorkerB: pod("worker-b"), ActiveSlot: active}
}

func digestFromTarget(t *testing.T, target declarativerelease.TargetIdentity) string {
	t.Helper()
	digest, err := immutableDigestFromRef(target.ImageRef)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

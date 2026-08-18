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
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestReadEdgeCandidateStageStatusAcceptsFullAuthorityResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"edge_group_id":"edge-group-country-us","status":"serving_lkg","ready":true,"authority_sequence":12,"publication_sequence":12,"current_publication_sequence":10,"candidate_epoch":13,"published_bundle_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","recovery_epoch":2,"lkg_state":"preserved"}`))
	}))
	defer server.Close()
	endpoint := server.URL + edgeCandidateStagePath
	status, err := readEdgeCandidateStageStatus(context.Background(), endpoint, "edge-group-country-us")
	if err != nil || status.AuthoritySequence != 12 || status.CurrentPublicationSequence != 10 || status.CandidateEpoch != 13 {
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
	waits           []map[string]edgeFrontHealth
	calls           []string
	requests        []edgeActivationRequest
	rollAuthority   []bool
	rollUnready     []bool
	activationState *edgeActivationState
	standbyErr      error
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

func (fake *fakeEdgeGroupRuntime) ApplyCandidateResources(_ context.Context, selector string) error {
	fake.calls = append(fake.calls, "apply:"+selector)
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

func (fake *fakeEdgeGroupRuntime) Roll(_ context.Context, name string, _ declarativerelease.TargetIdentity, requireGroupAuthority, replaceUnready bool) (map[string]edgeGroupPod, error) {
	fake.calls = append(fake.calls, "roll:"+name)
	fake.rollAuthority = append(fake.rollAuthority, requireGroupAuthority)
	fake.rollUnready = append(fake.rollUnready, replaceUnready)
	value, exists := fake.rolls[name]
	if !exists {
		return nil, fmt.Errorf("unexpected roll %s", name)
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
	return edgeActivationReceipt{Schema: edgeActivationReceiptSchema, GroupID: request.GroupID, Current: edgeActivationState{
		Schema: edgeActivationStateSchema, GroupID: request.GroupID, Generation: request.ExpectedGeneration + 1,
		ActiveSlot: request.TargetSlot, BundleGeneration: request.BundleGeneration, WorkerSourceCommit: request.WorkerSourceCommit,
		WorkerImageDigest: request.WorkerImageDigest, Authority: edgeActivationAuthority, Operation: request.Operation,
	}}, nil
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
	got, err := parseEdgeGroupPods(raw, "edge", 2, "edge-group-country-us", false, "")
	if err != nil || len(got) != 2 || got["node-1"].Name != "worker-1" || !got["node-2"].Ready {
		t.Fatalf("parse edge group pods: got=%+v err=%v", got, err)
	}

	pods["items"].([]any)[1].(map[string]any)["metadata"].(map[string]any)["labels"].(map[string]any)["fugue.io/edge-group-id"] = "edge-group-country-de"
	raw, _ = json.Marshal(pods)
	if _, err := parseEdgeGroupPods(raw, "edge", 2, "edge-group-country-us", false, ""); err == nil || !strings.Contains(err.Error(), "group identity") {
		t.Fatalf("cross-group pod was accepted: %v", err)
	}

	pods["items"] = []any{edgeGroupPodFixture("worker-1", "uid-1", "node-1", "edge-group-country-us", strings.Repeat("1", 40), strings.Repeat("a", 64))}
	raw, _ = json.Marshal(pods)
	if _, err := parseEdgeGroupPods(raw, "edge", 2, "edge-group-country-us", false, ""); err == nil || !strings.Contains(err.Error(), "want 2") {
		t.Fatalf("partial group cohort was accepted: %v", err)
	}
}

func TestParseEdgeGroupPodsSnapshotPreservesUnreadyImmutableIdentity(t *testing.T) {
	pod := edgeGroupPodFixture("worker-b", "uid-b", "node-1", "edge-group-country-us", strings.Repeat("1", 40), strings.Repeat("a", 64))
	pod["status"].(map[string]any)["conditions"] = []any{map[string]any{"type": "Ready", "status": "False"}}
	raw, _ := json.Marshal(map[string]any{"items": []any{pod}})

	if _, err := parseEdgeGroupPods(raw, "edge", 1, "edge-group-country-us", false, ""); err == nil || !strings.Contains(err.Error(), "readiness") {
		t.Fatalf("strict worker read accepted an unready active slot: %v", err)
	}
	got, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 1, "edge-group-country-us", false, "", false)
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
	if _, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 1, "edge-group-country-us", false, "", false); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("lenient snapshot accepted missing immutable identity: %v", err)
	}

	pod = edgeGroupPodFixture("worker-b", "uid-b", "node-1", "edge-group-country-us", "", strings.Repeat("a", 64))
	delete(pod["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any), "imageID")
	delete(pod["metadata"].(map[string]any)["annotations"].(map[string]any), "fugue.pro/source-commit")
	raw, _ = json.Marshal(map[string]any{"items": []any{pod}})
	if _, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 1, "edge-group-country-us", false, "", false); err != nil {
		t.Fatalf("snapshot rejected pod without runtime-only evidence: %v", err)
	}
}

func TestParseEdgeGroupPodsAcceptsReadyLKGWithHistoricalRestarts(t *testing.T) {
	pod := edgeGroupPodFixture("worker-1", "uid-1", "node-1", "edge-group-country-de", strings.Repeat("1", 40), strings.Repeat("a", 64))
	pod["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["restartCount"] = 2
	raw, _ := json.Marshal(map[string]any{"items": []any{pod}})

	got, err := parseEdgeGroupPods(raw, "edge", 1, "edge-group-country-de", false, "")
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
	want := []string{"snapshot", "stage:b", "apply:b", "roll:" + transition.WorkerBName, "wait-front:b", "apply:" + transition.FrontName, "roll:" + transition.FrontName, "wait-worker-authority:" + transition.WorkerBName, "stage-standby:a", "apply:" + transition.WorkerAName, "roll:" + transition.WorkerAName, "snapshot"}
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
	if got, want := fmt.Sprint(runtime.rollUnready), "[true false true]"; got != want {
		t.Fatalf("failed successor replace-unready gates=%s want=%s", got, want)
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
	want := []string{"snapshot", "stage:b", "apply:b", "roll:" + transition.WorkerBName, "wait-front:b", "apply:" + transition.FrontName,
		"roll:" + transition.FrontName, "wait-worker-authority:" + transition.WorkerBName, "stage-standby:a", "snapshot"}
	if strings.Join(runtime.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("post-commit standby failure changed serving workloads: calls=%v want=%v", runtime.calls, want)
	}
	if got, want := fmt.Sprint(runtime.rollAuthority), "[true true]"; got != want {
		t.Fatalf("post-commit standby failure authority gates=%s want=%s", got, want)
	}
}

func TestExecuteEdgeGroupABCompensationSwitchesBeforeRestoringFront(t *testing.T) {
	transition := edgeTransitionFixture()
	lkg := edgeTargetFixture("1", "a")
	current := edgeTargetFixture("2", "b")
	currentHealth := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 4, WorkerSourceCommit: current.ConfigSHA, WorkerImageDigest: digestFromTarget(t, current), RouteAuthority: edgeActivationAuthority}
	before := edgeStateFixture("b", current, currentHealth)
	finalHealth := edgeFrontHealth{ActiveSlot: "a", ActivationPresent: true, Generation: 5, WorkerSourceCommit: lkg.ConfigSHA, WorkerImageDigest: digestFromTarget(t, lkg), RouteAuthority: edgeActivationAuthority}
	runtime := &fakeEdgeGroupRuntime{
		snapshots: []edgeGroupState{before},
		rolls: map[string]map[string]edgeGroupPod{
			transition.WorkerAName: edgeStateFixture("a", lkg, finalHealth).WorkerA,
			transition.WorkerBName: edgeStateFixture("a", lkg, finalHealth).WorkerB,
			transition.FrontName:   edgeStateFixture("a", lkg, finalHealth).Front,
		},
		declared: map[string]declarativerelease.TargetIdentity{transition.WorkerAName: lkg, transition.WorkerBName: lkg, transition.FrontName: lkg},
	}
	release := declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: lkg.ConfigSHA}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, lkg); err != nil {
		t.Fatal(err)
	}
	want := []string{"snapshot", "apply:", "roll:" + transition.WorkerAName, "roll:" + transition.WorkerBName, "roll:" + transition.FrontName}
	if strings.Join(runtime.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("edge compensation order=%v want=%v", runtime.calls, want)
	}
	if len(runtime.requests) != 0 {
		t.Fatalf("exact LKG compensation performed direct authority CAS: %+v", runtime.requests)
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
			InventoryProducerActive: true, InventoryHeartbeatGeneration: 1, InventoryHeartbeatAt: time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC), Ready: true}}
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

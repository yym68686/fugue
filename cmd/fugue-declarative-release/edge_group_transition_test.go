package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

type fakeEdgeGroupRuntime struct {
	snapshots       []edgeGroupState
	rolls           map[string]map[string]edgeGroupPod
	waits           []map[string]edgeFrontHealth
	calls           []string
	requests        []edgeActivationRequest
	rollAuthority   []bool
	activationState *edgeActivationState
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

func (fake *fakeEdgeGroupRuntime) ApplyResources(context.Context) error {
	fake.calls = append(fake.calls, "apply")
	return nil
}

func (fake *fakeEdgeGroupRuntime) Roll(_ context.Context, name string, _ declarativerelease.TargetIdentity, requireGroupAuthority bool) (map[string]edgeGroupPod, error) {
	fake.calls = append(fake.calls, "roll:"+name)
	fake.rollAuthority = append(fake.rollAuthority, requireGroupAuthority)
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
	runtime := &fakeEdgeGroupRuntime{
		snapshots: []edgeGroupState{before, final},
		rolls: map[string]map[string]edgeGroupPod{
			transition.WorkerBName: final.WorkerB,
			transition.FrontName:   final.Front,
			transition.WorkerAName: final.WorkerA,
		},
		waits: []map[string]edgeFrontHealth{
			{"node-1": {ActiveSlot: "a", ActivationPresent: true, Generation: 1, WorkerSourceCommit: old.ConfigSHA, WorkerImageDigest: digestFromTarget(t, old), RouteAuthority: edgeActivationAuthority}},
			{"node-1": final.FrontHealth["node-1"]},
		},
	}
	release := declarativerelease.PlanRelease{
		ExpectedPreviousConfigSHA: old.ConfigSHA,
		Transition:                &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &transition},
	}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, target); err != nil {
		t.Fatal(err)
	}
	want := []string{"snapshot", "apply", "roll:" + transition.WorkerBName, "read-activation", "cas:initialize:a", "roll:" + transition.FrontName, "wait-front:a", "select-cas", "cas:promote:b", "wait-front:b", "roll:" + transition.WorkerAName, "wait-worker-authority:" + transition.WorkerBName, "snapshot"}
	if strings.Join(runtime.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("edge forward order=%v want=%v", runtime.calls, want)
	}
	if len(runtime.requests) != 2 || runtime.requests[1].WorkerSourceCommit != target.ConfigSHA || runtime.requests[1].Operation != edgeActivationPromote {
		t.Fatalf("edge forward CAS requests are not target-bound: %+v", runtime.requests)
	}
	if got, want := fmt.Sprint(runtime.rollAuthority), "[true true true]"; got != want {
		t.Fatalf("edge authority gates=%s want=%s", got, want)
	}
}

func TestExecuteEdgeGroupABContinuesFromExistingGroupBoundActivation(t *testing.T) {
	transition := edgeTransitionFixture()
	old := edgeTargetFixture("1", "a")
	target := edgeTargetFixture("2", "b")
	before := edgeStateFixture("a", old, edgeFrontHealth{ActiveSlot: "a"})
	finalHealth := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 4, WorkerSourceCommit: target.ConfigSHA, WorkerImageDigest: digestFromTarget(t, target), RouteAuthority: edgeActivationAuthority}
	final := edgeStateFixture("b", target, finalHealth)
	runtime := &fakeEdgeGroupRuntime{
		snapshots: []edgeGroupState{before, final},
		rolls: map[string]map[string]edgeGroupPod{
			transition.WorkerBName: final.WorkerB,
			transition.FrontName:   final.Front,
			transition.WorkerAName: final.WorkerA,
		},
		waits:           []map[string]edgeFrontHealth{{"node-1": finalHealth}},
		activationState: &edgeActivationState{Schema: edgeActivationStateSchema, GroupID: transition.GroupID, Generation: 3, ActiveSlot: "a", Authority: edgeActivationAuthority},
	}
	release := declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: old.ConfigSHA, Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &transition}}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, target); err != nil {
		t.Fatal(err)
	}
	if len(runtime.requests) != 1 || runtime.requests[0].Operation != edgeActivationPromote || runtime.requests[0].ExpectedGeneration != 3 {
		t.Fatalf("existing activation was not continued by exact CAS: %+v", runtime.requests)
	}
	if strings.Contains(strings.Join(runtime.calls, "\n"), "wait-front:a") {
		t.Fatalf("existing exact activation performed redundant pre-CAS wait: %v", runtime.calls)
	}
}

func TestExecuteEdgeGroupABCompensationSwitchesBeforeRestoringFront(t *testing.T) {
	transition := edgeTransitionFixture()
	lkg := edgeTargetFixture("1", "a")
	current := edgeTargetFixture("2", "b")
	currentHealth := edgeFrontHealth{ActiveSlot: "b", ActivationPresent: true, Generation: 4, WorkerSourceCommit: current.ConfigSHA, WorkerImageDigest: digestFromTarget(t, current), RouteAuthority: edgeActivationAuthority}
	before := edgeStateFixture("b", current, currentHealth)
	finalHealth := edgeFrontHealth{ActiveSlot: "a", ActivationPresent: true, Generation: 5, WorkerSourceCommit: lkg.ConfigSHA, WorkerImageDigest: digestFromTarget(t, lkg), RouteAuthority: edgeActivationAuthority}
	final := edgeStateFixture("a", lkg, finalHealth)
	runtime := &fakeEdgeGroupRuntime{
		snapshots: []edgeGroupState{before, final},
		rolls: map[string]map[string]edgeGroupPod{
			transition.WorkerAName: final.WorkerA,
			transition.WorkerBName: final.WorkerB,
			transition.FrontName:   final.Front,
		},
		waits: []map[string]edgeFrontHealth{{"node-1": finalHealth}},
	}
	release := declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: lkg.ConfigSHA}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, lkg); err != nil {
		t.Fatal(err)
	}
	want := []string{"snapshot", "apply", "roll:" + transition.WorkerAName, "select-cas", "cas:rollback:a", "wait-front:a", "roll:" + transition.WorkerBName, "roll:" + transition.FrontName, "wait-worker-authority:" + transition.WorkerAName, "snapshot"}
	if strings.Join(runtime.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("edge compensation order=%v want=%v", runtime.calls, want)
	}
	if len(runtime.requests) != 1 || runtime.requests[0].Operation != edgeActivationRollback || runtime.requests[0].RollbackOfGeneration != 4 {
		t.Fatalf("edge compensation CAS is not generation-bound: %+v", runtime.requests)
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
	return declarativerelease.EdgeGroupABTransition{GroupID: "edge-group-country-us", FrontName: "front", WorkerAName: "worker-a", WorkerBName: "worker-b", WorkerContainer: "edge", ActivationStatePath: "/var/lib/fugue-edge-front/activation.json", CASBinary: "/usr/local/bin/fugue-edge-front-cas", ExpectedNodes: 1, SoakSeconds: 180}
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

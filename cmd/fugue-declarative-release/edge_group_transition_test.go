package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

type fakeEdgeGroupRuntime struct {
	snapshots []edgeGroupState
	rolls     map[string]map[string]edgeGroupPod
	waits     []map[string]edgeFrontHealth
	calls     []string
	requests  []edgeActivationRequest
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

func (fake *fakeEdgeGroupRuntime) Roll(_ context.Context, name string, _ declarativerelease.TargetIdentity) (map[string]edgeGroupPod, error) {
	fake.calls = append(fake.calls, "roll:"+name)
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

func TestAdoptingEdgeGroupAloneMayReadLegacyPodIdentity(t *testing.T) {
	pod := edgeGroupPodFixture("worker-legacy", "uid-legacy", "node-1", "edge-group-country-us", strings.Repeat("1", 40), strings.Repeat("a", 64))
	metadata := pod["metadata"].(map[string]any)
	delete(metadata["labels"].(map[string]any), "fugue.io/edge-group-id")
	delete(metadata["annotations"].(map[string]any), "fugue.pro/source-commit")
	raw, _ := json.Marshal(map[string]any{"items": []any{pod}})
	legacySource := strings.Repeat("2", 40)
	got, err := parseEdgeGroupPods(raw, "edge", 1, "edge-group-country-us", true, legacySource)
	if err != nil || got["node-1"].SourceCommit != legacySource {
		t.Fatalf("explicit adoption did not recover the reviewed legacy identity: got=%+v err=%v", got, err)
	}
	if _, err := parseEdgeGroupPods(raw, "edge", 1, "edge-group-country-us", false, ""); err == nil {
		t.Fatal("independent edge group accepted legacy pod identity")
	}
	metadata["labels"].(map[string]any)["fugue.io/edge-group-id"] = "edge-group-country-de"
	raw, _ = json.Marshal(map[string]any{"items": []any{pod}})
	if _, err := parseEdgeGroupPods(raw, "edge", 1, "edge-group-country-us", true, legacySource); err == nil {
		t.Fatal("adoption accepted an explicit cross-group identity")
	}
}

func TestAdoptingEdgeGroupCanInspectOnlyAnUnreadyInactivePodForExactRecovery(t *testing.T) {
	pod := edgeGroupPodFixture("worker-broken", "uid-broken", "node-1", "edge-group-country-us", strings.Repeat("1", 40), strings.Repeat("a", 64))
	status := pod["status"].(map[string]any)
	status["conditions"] = []any{map[string]any{"type": "Ready", "status": "False"}}
	status["containerStatuses"] = []any{map[string]any{"name": "edge", "imageID": "containerd://sha256:" + strings.Repeat("a", 64), "restartCount": 7}}
	raw, _ := json.Marshal(map[string]any{"items": []any{pod}})
	if _, err := parseEdgeGroupPods(raw, "edge", 1, "edge-group-country-us", true, ""); err == nil {
		t.Fatal("strict edge state accepted an unready Pod")
	}
	got, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 1, "edge-group-country-us", true, "", false)
	if err != nil || got["node-1"].Ready || got["node-1"].UID != "uid-broken" || got["node-1"].ResourceVersion != "42" {
		t.Fatalf("degraded adoption recovery lost exact Pod preconditions: got=%+v err=%v", got, err)
	}
}

func TestWorkloadLegacySourceFallsBackToBoundTemplateSourceAfterAdoption(t *testing.T) {
	raw := []byte(`{"spec":{"template":{"metadata":{"annotations":{"fugue.pro/source-commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},"spec":{"containers":[{"name":"edge","image":"ghcr.io/example/edge@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]}}}}`)
	got, err := workloadLegacySource(raw, "edge")
	if err != nil || got != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("bound adopting source was not recovered: got=%q err=%v", got, err)
	}
	if _, err := workloadLegacySource(bytes.ReplaceAll(raw, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("invalid")), "edge"); err == nil {
		t.Fatal("invalid bound adopting source was accepted")
	}
}

func TestAdoptingLegacyPodIdentityComesFromExactBootstrapLKG(t *testing.T) {
	oldSource := strings.Repeat("7", 40)
	oldDigest := strings.Repeat("d", 64)
	newSource := strings.Repeat("f", 40)
	release := declarativerelease.PlanRelease{
		MigrationState: "adopting", HeterogeneousBootstrapLKG: true, BootstrapLKGPath: "bootstrap.json",
		ExpectedPreviousPresent: true, OwnershipAdoption: &declarativerelease.OwnershipAdoption{},
		Workload:   declarativerelease.Workload{Namespace: "fugue-system", FieldManager: "edge-declarative"},
		Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{}},
	}
	bootstrap := []byte(fmt.Sprintf(`{"apiVersion":"release.fugue.dev/v2","kind":"ComponentResourceSet","items":[{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"worker-a","namespace":"fugue-system"},"spec":{"selector":{"matchLabels":{"app":"edge"}},"template":{"metadata":{"labels":{"app":"edge"},"annotations":{"fugue.pro/source-commit":"%s","fugue.pro/oci-revision":"%s"}},"spec":{"containers":[{"name":"edge","image":"ghcr.io/example/fugue-edge@sha256:%s"}]}},"updateStrategy":{"type":"OnDelete"}}}]}`, oldSource, oldSource, oldDigest))
	target, err := declaredEdgeDaemonSetTarget(bootstrap, release, "worker-a", "edge")
	if err != nil || target.ConfigSHA != oldSource {
		t.Fatalf("bind bootstrap LKG target: target=%+v err=%v", target, err)
	}
	currentDesired := []byte(fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"fugue.pro/source-commit":"%s"}},"spec":{"containers":[{"name":"edge","image":"ghcr.io/example/edge@sha256:%s"}]}}}}`, newSource, strings.Repeat("e", 64)))
	if got, err := workloadLegacySource(currentDesired, "edge"); err != nil || got != newSource {
		t.Fatalf("fixture no longer reproduces the misleading desired-template source: got=%q err=%v", got, err)
	}
	pod := edgeGroupPodFixture("worker-a-old", "uid-old", "node-1", "edge-group-country-de", oldSource, oldDigest)
	metadata := pod["metadata"].(map[string]any)
	delete(metadata["annotations"].(map[string]any), "fugue.pro/source-commit")
	raw, _ := json.Marshal(map[string]any{"items": []any{pod}})
	parsed, err := parseEdgeGroupPodsWithReadiness(raw, "edge", 1, "edge-group-country-de", true, target.ConfigSHA, true)
	if err != nil || validateBootstrapLegacyPodImages(parsed, "sha256:"+oldDigest) != nil || !parsed["node-1"].LegacyIdentity || parsed["node-1"].SourceCommit != oldSource || !edgePodsMatchTarget(parsed, target) {
		t.Fatalf("legacy running Pod was not bound to the exact bootstrap LKG: pods=%+v err=%v", parsed, err)
	}
	tampered := parsed["node-1"]
	tampered.ImageID = "containerd://sha256:" + strings.Repeat("a", 64)
	parsed["node-1"] = tampered
	if err := validateBootstrapLegacyPodImages(parsed, "sha256:"+oldDigest); err == nil {
		t.Fatal("legacy Pod with a non-LKG image was accepted")
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
	release := declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: old.ConfigSHA}
	if err := executeEdgeGroupAB(context.Background(), runtime, release, transition, target); err != nil {
		t.Fatal(err)
	}
	want := []string{"snapshot", "apply", "roll:" + transition.WorkerBName, "cas:initialize:a", "roll:" + transition.FrontName, "wait-front:a", "select-cas", "cas:promote:b", "wait-front:b", "roll:" + transition.WorkerAName, "wait-worker-authority:" + transition.WorkerBName, "snapshot"}
	if strings.Join(runtime.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("edge forward order=%v want=%v", runtime.calls, want)
	}
	if len(runtime.requests) != 2 || runtime.requests[1].WorkerSourceCommit != target.ConfigSHA || runtime.requests[1].Operation != edgeActivationPromote {
		t.Fatalf("edge forward CAS requests are not target-bound: %+v", runtime.requests)
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

func TestLegacyRouteBootstrapIsAdoptionOnlyAndNeverSatisfiesFinalAuthority(t *testing.T) {
	release := declarativerelease.PlanRelease{
		MigrationState: "adopting", HeterogeneousBootstrapLKG: true, BootstrapLKGPath: "deploy/releases/edge-worker-us/lkg.json",
		ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: strings.Repeat("1", 40), OwnershipAdoption: &declarativerelease.OwnershipAdoption{},
		Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{}},
	}
	target := edgeTargetFixture("2", "b")
	legacy := edgeGroupPod{Ready: true, BundleGeneration: "legacy-generation", ServingGeneration: "legacy-generation"}
	if !allowsAdoptionRouteBootstrap(release, target) || !edgePodHasAdoptionLegacyRoute(legacy) {
		t.Fatal("explicit adopting target could not use the bounded bootstrap route")
	}
	release.MigrationState = "independent"
	if allowsAdoptionRouteBootstrap(release, target) {
		t.Fatal("independent release retained legacy route bootstrap")
	}
	state := edgeStateFixture("a", target, edgeFrontHealth{ActiveSlot: "a"})
	pod := state.WorkerB["node-1"]
	pod.RouteBundleSource = ""
	pod.PublicationSequence = 0
	state.WorkerB["node-1"] = pod
	if err := validateEdgeGroupAuthority(state, edgeTransitionFixture()); err == nil {
		t.Fatal("legacy bootstrap route satisfied final group authority")
	}
}

func TestBootstrapLKGSelectorRequiresExactAdoptionIdentity(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	previous := strings.Repeat("1", 40)
	release := declarativerelease.PlanRelease{
		MigrationState: "adopting", HeterogeneousBootstrapLKG: true, BootstrapLKGPath: "deploy/releases/edge-worker-us/lkg.json",
		ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: previous, ExpectedPreviousManifestSHA: previous, ExpectedPreviousOCIRevision: previous,
		ExpectedPreviousImageDigest: digest, OwnershipAdoption: &declarativerelease.OwnershipAdoption{},
		Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{}},
	}
	target := declarativerelease.TargetIdentity{Present: true, ConfigSHA: previous, ManifestSHA: previous, OCIRevision: previous, ImageRef: "ghcr.io/example/edge@" + digest}
	if !isEdgeBootstrapLKGTarget(release, target) {
		t.Fatal("exact adoption bootstrap LKG did not select heterogeneous compensation")
	}
	tampered := target
	tampered.OCIRevision = strings.Repeat("2", 40)
	if isEdgeBootstrapLKGTarget(release, tampered) {
		t.Fatal("tampered bootstrap LKG selected heterogeneous compensation")
	}
	release.MigrationState = "independent"
	if isEdgeBootstrapLKGTarget(release, target) {
		t.Fatal("independent release selected adoption compensation")
	}
}

func TestBootstrapLKGCompensationAcceptsExactPreRolloutWorkloadsOnly(t *testing.T) {
	frontTarget := edgeTargetFixture("1", "a")
	workerATarget := edgeTargetFixture("2", "b")
	workerBTarget := edgeTargetFixture("3", "c")
	pods := func(name string, target declarativerelease.TargetIdentity) map[string]edgeGroupPod {
		return map[string]edgeGroupPod{"node-1": {
			Name: name, UID: name + "-uid", ResourceVersion: "42", NodeName: "node-1", Ready: true,
			SourceCommit: target.ConfigSHA, ImageRef: target.ImageRef, ImageID: target.ImageRef,
		}}
	}
	front := pods("front", frontTarget)
	workerA := pods("worker-a", workerATarget)
	workerB := pods("worker-b", workerBTarget)
	if !bootstrapWorkloadsMatchLKG(front, workerA, workerB, frontTarget, workerATarget, workerBTarget) {
		t.Fatal("exact heterogeneous pre-rollout LKG was not accepted for compensation")
	}
	tampered := workerB["node-1"]
	tampered.ImageID = "ghcr.io/example/fugue-edge@sha256:" + strings.Repeat("d", 64)
	workerB["node-1"] = tampered
	if bootstrapWorkloadsMatchLKG(front, workerA, workerB, frontTarget, workerATarget, workerBTarget) {
		t.Fatal("non-LKG workload was accepted for direct compensation")
	}
	legacy := workerA["node-1"]
	legacy.SourceCommit = workerATarget.ConfigSHA
	legacy.ImageRef = "ghcr.io/example/fugue-edge:bde0e5e99fd9cc4fc8b9adfc2aa99510273061fa"
	legacy.ImageID = workerATarget.ImageRef
	workerA["node-1"] = legacy
	if !edgePodsMatchBootstrapTarget(workerA, workerATarget) {
		t.Fatal("legacy tag with exact immutable ImageID was rejected")
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

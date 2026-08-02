package releasedomain

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type hotfixExecutionRuntime struct {
	observations      []ControlPlaneHotfixObservation
	forwardResult     ControlPlaneHotfixCommitResult
	compensateResult  ControlPlaneHotfixCommitResult
	observeCalls      int
	acquireCalls      int
	forwardCalls      int
	compensateCalls   int
	releaseCalls      int
	persistedRecovery []bool
	acquireErr        error
	verifyLeaseErrAt  int
	verifyLeaseCalls  int
	forwardErr        error
	compensateErr     error
}

func (runtime *hotfixExecutionRuntime) Observe(context.Context) (ControlPlaneHotfixObservation, error) {
	if runtime.observeCalls >= len(runtime.observations) {
		return ControlPlaneHotfixObservation{}, context.DeadlineExceeded
	}
	observation := runtime.observations[runtime.observeCalls]
	runtime.observeCalls++
	return observation, nil
}

func (runtime *hotfixExecutionRuntime) AcquireLease(context.Context, ControlPlaneHotfixAdoptionPlan) error {
	runtime.acquireCalls++
	return runtime.acquireErr
}

func (runtime *hotfixExecutionRuntime) VerifyLease(context.Context, ControlPlaneHotfixAdoptionPlan) error {
	runtime.verifyLeaseCalls++
	if runtime.verifyLeaseErrAt == runtime.verifyLeaseCalls {
		return errors.New("lease ownership lost")
	}
	return nil
}

func (runtime *hotfixExecutionRuntime) PersistWAL(_ context.Context, wal ControlPlaneHotfixAdoptionWAL) error {
	runtime.persistedRecovery = append(runtime.persistedRecovery, wal.RecoveryRequired)
	return nil
}

func (runtime *hotfixExecutionRuntime) Forward(context.Context, ControlPlaneHotfixAdoptionPlan) (ControlPlaneHotfixCommitResult, error) {
	runtime.forwardCalls++
	return runtime.forwardResult, runtime.forwardErr
}

func (runtime *hotfixExecutionRuntime) Compensate(context.Context, ControlPlaneHotfixAdoptionPlan) (ControlPlaneHotfixCommitResult, error) {
	runtime.compensateCalls++
	return runtime.compensateResult, runtime.compensateErr
}

func TestExecuteControlPlaneHotfixAdoptionHappyPathAndCommitUnknownReadback(t *testing.T) {
	t.Parallel()

	for _, result := range []ControlPlaneHotfixCommitResult{
		ControlPlaneHotfixCommitAcknowledged,
		ControlPlaneHotfixCommitUnknown,
	} {
		result := result
		t.Run(string(result), func(t *testing.T) {
			t.Parallel()
			plan := validControlPlaneHotfixExecutionPlan()
			base := baseControlPlaneHotfixObservation(plan)
			target := targetControlPlaneHotfixObservation(plan)
			runtime := &hotfixExecutionRuntime{
				observations:  []ControlPlaneHotfixObservation{base, base, target},
				forwardResult: result,
			}
			execution, err := ExecuteControlPlaneHotfixAdoption(context.Background(), plan, runtime, ControlPlaneHotfixExecutionOptions{})
			if err != nil || execution.Status != "verified" || execution.WAL.RecoveryRequired {
				t.Fatalf("expected verified execution, result=%+v err=%v", execution, err)
			}
			if runtime.forwardCalls != 1 || runtime.compensateCalls != 0 || runtime.releaseCalls != 1 {
				t.Fatalf("unexpected transaction calls: %+v", runtime)
			}
		})
	}
}

func TestExecuteControlPlaneHotfixAdoptionDryRunHasNoWrites(t *testing.T) {
	t.Parallel()

	plan := validControlPlaneHotfixExecutionPlan()
	base := baseControlPlaneHotfixObservation(plan)
	runtime := &hotfixExecutionRuntime{observations: []ControlPlaneHotfixObservation{base, base}}
	result, err := ExecuteControlPlaneHotfixAdoption(context.Background(), plan, runtime, ControlPlaneHotfixExecutionOptions{DryRun: true})
	if err != nil || result.Status != "dry-run-verified" {
		t.Fatalf("dry-run failed: result=%+v err=%v", result, err)
	}
	if runtime.acquireCalls != 0 || runtime.forwardCalls != 0 || runtime.compensateCalls != 0 || runtime.releaseCalls != 0 || len(runtime.persistedRecovery) != 0 {
		t.Fatalf("dry-run performed a write: %+v", runtime)
	}
}

func TestExecuteControlPlaneHotfixAdoptionRejectsPrewriteTOCTOU(t *testing.T) {
	t.Parallel()

	plan := validControlPlaneHotfixExecutionPlan()
	first := baseControlPlaneHotfixObservation(plan)
	second := first
	second.Kubernetes.APIResourceVersion = "101"
	runtime := &hotfixExecutionRuntime{observations: []ControlPlaneHotfixObservation{first, second}}
	_, err := ExecuteControlPlaneHotfixAdoption(context.Background(), plan, runtime, ControlPlaneHotfixExecutionOptions{})
	if err == nil || !strings.Contains(err.Error(), "second prewrite sample") || runtime.forwardCalls != 0 {
		t.Fatalf("TOCTOU was not rejected before forward mutation: err=%v runtime=%+v", err, runtime)
	}
}

func TestExecuteControlPlaneHotfixAdoptionLeaseLossBlocksForward(t *testing.T) {
	t.Parallel()

	plan := validControlPlaneHotfixExecutionPlan()
	base := baseControlPlaneHotfixObservation(plan)
	runtime := &hotfixExecutionRuntime{
		observations:     []ControlPlaneHotfixObservation{base, base},
		verifyLeaseErrAt: 2,
	}
	result, err := ExecuteControlPlaneHotfixAdoption(context.Background(), plan, runtime, ControlPlaneHotfixExecutionOptions{})
	if err == nil || runtime.forwardCalls != 0 || result.WAL.RecoveryRequired {
		t.Fatalf("Lease loss did not block forward transaction safely: result=%+v err=%v runtime=%+v", result, err, runtime)
	}
}

func TestExecuteControlPlaneHotfixAdoptionReplayIsRejectedByLease(t *testing.T) {
	t.Parallel()

	plan := validControlPlaneHotfixExecutionPlan()
	base := baseControlPlaneHotfixObservation(plan)
	runtime := &hotfixExecutionRuntime{
		observations: []ControlPlaneHotfixObservation{base},
		acquireErr:   errors.New("nonce already fenced"),
	}
	_, err := ExecuteControlPlaneHotfixAdoption(context.Background(), plan, runtime, ControlPlaneHotfixExecutionOptions{})
	if err == nil || !strings.Contains(err.Error(), "acquire hotfix Lease") || runtime.forwardCalls != 0 {
		t.Fatalf("replay reached forward mutation: err=%v runtime=%+v", err, runtime)
	}
}

func TestExecuteControlPlaneHotfixAdoptionRolloutFailureUsesExactHybridCompensation(t *testing.T) {
	t.Parallel()

	plan := validControlPlaneHotfixExecutionPlan()
	base := baseControlPlaneHotfixObservation(plan)
	failed := targetControlPlaneHotfixObservation(plan)
	failed.Kubernetes.APIReady = 1
	hybrid := hybridControlPlaneHotfixObservation(plan)
	runtime := &hotfixExecutionRuntime{
		observations:     []ControlPlaneHotfixObservation{base, base, failed, hybrid},
		forwardResult:    ControlPlaneHotfixCommitAcknowledged,
		compensateResult: ControlPlaneHotfixCommitAcknowledged,
	}
	result, err := ExecuteControlPlaneHotfixAdoption(context.Background(), plan, runtime, ControlPlaneHotfixExecutionOptions{})
	if err == nil || result.Status != "compensated" || result.WAL.RecoveryRequired {
		t.Fatalf("expected exact compensated failure, result=%+v err=%v", result, err)
	}
	if runtime.forwardCalls != 1 || runtime.compensateCalls != 1 || runtime.releaseCalls != 1 {
		t.Fatalf("unexpected compensation calls: %+v", runtime)
	}
}

func TestBuildControlPlaneHotfixAdoptionPlanAllowsOnlyTheTwoAPIPointers(t *testing.T) {
	t.Parallel()

	plan, input := validBuiltControlPlaneHotfixPlan(t)
	if err := VerifyControlPlaneHotfixAdoptionPlan(plan); err != nil {
		t.Fatalf("verify plan: %v", err)
	}
	forward, err := RenderControlPlaneHotfixTransaction(input.TargetManifest, plan, "forward")
	if err != nil || hotfixDigest(forward) != plan.TargetManifestDigest {
		t.Fatalf("forward render: digest=%s err=%v", hotfixDigest(forward), err)
	}
	compensated, err := RenderControlPlaneHotfixTransaction(input.BaseManifest, plan, "compensate")
	if err != nil || hotfixDigest(compensated) != plan.HybridManifestDigest {
		t.Fatalf("compensation render: digest=%s err=%v", hotfixDigest(compensated), err)
	}

	input.TargetManifest = hotfixManifest(t, input.AdoptedSource, input.LiveImageRef, "third-pointer")
	input.RepeatedTarget = input.TargetManifest
	if _, err := BuildControlPlaneHotfixAdoptionPlan(input); err == nil || !strings.Contains(err.Error(), "third API pointer") {
		t.Fatalf("third target difference was accepted: %v", err)
	}
}

func TestVerifyControlPlaneHotfixAdoptionPlanBindsEveryAuthority(t *testing.T) {
	t.Parallel()

	plan, _ := validBuiltControlPlaneHotfixPlan(t)
	tests := map[string]func(*ControlPlaneHotfixAdoptionPlan){
		"Helm status": func(value *ControlPlaneHotfixAdoptionPlan) { value.BaseStatus = "pending-upgrade" },
		"Helm record": func(value *ControlPlaneHotfixAdoptionPlan) { value.HelmRecordDigest = "invalid" },
		"revision":    func(value *ControlPlaneHotfixAdoptionPlan) { value.TargetRevision++ },
		"artifact index": func(value *ControlPlaneHotfixAdoptionPlan) {
			value.Provenance.IndexDigest = "sha256:" + strings.Repeat("9", 64)
		},
		"API UID":               func(value *ControlPlaneHotfixAdoptionPlan) { value.Kubernetes.APIUID = "" },
		"Service binding":       func(value *ControlPlaneHotfixAdoptionPlan) { value.Kubernetes.ServiceSelectorDigest = "invalid" },
		"Endpoint binding":      func(value *ControlPlaneHotfixAdoptionPlan) { value.Kubernetes.EndpointServiceName = "other" },
		"Lease resourceVersion": func(value *ControlPlaneHotfixAdoptionPlan) { value.Lease.ResourceVersion = "" },
		"fence":                 func(value *ControlPlaneHotfixAdoptionPlan) { value.Fence = "short" },
		"nonce":                 func(value *ControlPlaneHotfixAdoptionPlan) { value.Nonce = "short" },
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed := plan
			mutate(&changed)
			changed.Digest = controlPlaneHotfixPlanDigest(changed)
			if err := VerifyControlPlaneHotfixAdoptionPlan(changed); err == nil {
				t.Fatalf("mutated %s binding was accepted", name)
			}
		})
	}
}

func TestBuildControlPlaneHotfixAdoptionPlanRejectsMalformedManifestWithoutPanic(t *testing.T) {
	t.Parallel()

	_, input := validBuiltControlPlaneHotfixPlan(t)
	input.BaseManifest = []byte("apiVersion: apps/v1\nkind: Deployment\nmetadata: []\n")
	if _, err := BuildControlPlaneHotfixAdoptionPlan(input); err == nil {
		t.Fatal("malformed manifest was accepted")
	}

	_, input = validBuiltControlPlaneHotfixPlan(t)
	objects, err := decodeHotfixObjects(input.TargetManifest)
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := exactHotfixDeployment(objects, input.Namespace, input.Kubernetes.APIName)
	if err != nil {
		t.Fatal(err)
	}
	delete(deployment, "spec")
	input.TargetManifest, err = encodeHotfixObjects(objects)
	if err != nil {
		t.Fatal(err)
	}
	input.RepeatedTarget = input.TargetManifest
	if _, err := BuildControlPlaneHotfixAdoptionPlan(input); err == nil {
		t.Fatal("manifest with a missing pod-template path was accepted")
	}
}

func (runtime *hotfixExecutionRuntime) ReleaseLease(context.Context, ControlPlaneHotfixAdoptionPlan) error {
	runtime.releaseCalls++
	return nil
}

func TestExecuteControlPlaneHotfixAdoptionKeepsFenceWhenForwardAndCompensationAreUnknown(t *testing.T) {
	t.Parallel()

	plan := validControlPlaneHotfixExecutionPlan()
	base := baseControlPlaneHotfixObservation(plan)
	failed := targetControlPlaneHotfixObservation(plan)
	failed.Kubernetes.APIReady = 1
	runtime := &hotfixExecutionRuntime{
		observations:     []ControlPlaneHotfixObservation{base, base, failed, failed},
		forwardResult:    ControlPlaneHotfixCommitUnknown,
		compensateResult: ControlPlaneHotfixCommitUnknown,
	}

	result, err := ExecuteControlPlaneHotfixAdoption(context.Background(), plan, runtime, ControlPlaneHotfixExecutionOptions{})
	if err == nil || !strings.Contains(err.Error(), "recovery fence") {
		t.Fatalf("expected a fenced commit-unknown failure, result=%+v err=%v", result, err)
	}
	if runtime.acquireCalls != 1 || runtime.forwardCalls != 1 || runtime.compensateCalls != 1 || runtime.releaseCalls != 0 {
		t.Fatalf("unexpected execution calls: %+v", runtime)
	}
	if len(runtime.persistedRecovery) == 0 || !runtime.persistedRecovery[len(runtime.persistedRecovery)-1] {
		t.Fatalf("unknown compensation must preserve a recovery-required WAL: %+v", runtime.persistedRecovery)
	}
}

func validControlPlaneHotfixExecutionPlan() ControlPlaneHotfixAdoptionPlan {
	digest := "sha256:" + strings.Repeat("a", 64)
	plan := ControlPlaneHotfixAdoptionPlan{
		APIVersion:              ControlPlaneHotfixAdoptionAPIVersion,
		Kind:                    ControlPlaneHotfixAdoptionPlanKind,
		Policy:                  ControlPlaneHotfixAdoptionPolicy,
		ExpectedSHA:             strings.Repeat("1", 40),
		RunID:                   "30733955954",
		RunAttempt:              1,
		Namespace:               "fugue-system",
		ReleaseName:             "fugue",
		ReleaseFullname:         "fugue-fugue",
		BaseRevision:            806,
		TargetRevision:          807,
		BaseStatus:              "deployed",
		HelmRecordDigest:        digest,
		BaseValuesDigest:        digest,
		TargetValuesDigest:      "sha256:" + strings.Repeat("e", 64),
		ChartTreeDigest:         digest,
		BaseManifestDigest:      "sha256:" + strings.Repeat("b", 64),
		TargetManifestDigest:    "sha256:" + strings.Repeat("c", 64),
		HybridManifestDigest:    "sha256:" + strings.Repeat("d", 64),
		TargetAPITemplateDigest: "sha256:" + strings.Repeat("6", 64),
		HybridAPITemplateDigest: "sha256:" + strings.Repeat("7", 64),
		CurrentSource:           strings.Repeat("2", 40),
		AdoptedSource:           strings.Repeat("3", 40),
		LiveImageRef:            "ghcr.io/example/fugue-api@" + digest,
		Fence:                   "fence-token-1234567890",
		Nonce:                   "nonce-token-1234567890",
		Provenance: ControlPlaneHotfixProvenance{
			BuildRunID: "30733955954", BuildRunAttempt: 1,
			ArtifactName: "fugue-api-artifact", ArtifactDigest: digest,
			Repository: "ghcr.io/example/fugue-api", IndexDigest: digest,
			PlatformManifestDigest: digest, ConfigDigest: digest,
			OCIRevision: strings.Repeat("3", 40), Verified: true,
		},
		Kubernetes: ControlPlaneHotfixKubernetesEvidence{
			APIName: "fugue-fugue-api", APIUID: "api-uid", APIResourceVersion: "100",
			APIGeneration: 9, APIObservedGeneration: 9, APITemplateDigest: digest,
			APIImageRef: "ghcr.io/example/fugue-api@" + digest,
			APIImageID:  "containerd://" + strings.Repeat("f", 64), APIHealthDigest: digest,
			APIReplicas: 2, APIReady: 2, APIUpdated: 2, APIAvailable: 2,
			ServiceName: "fugue-fugue", ServiceUID: "service-uid", ServiceResourceVersion: "200", ServiceSelectorDigest: digest,
			EndpointSliceName: "fugue-fugue-abc", EndpointSliceUID: "slice-uid",
			EndpointSliceResourceVersion: "300", EndpointServiceName: "fugue-fugue", EndpointBindingDigest: digest,
			ReadyServingEndpoints: 2,
		},
		Lease: ControlPlaneHotfixLeaseEvidence{
			Namespace: "fugue-system", Name: "fugue-fugue-control-plane-db-backup",
			UID: "lease-uid", ResourceVersion: "400",
		},
	}
	plan.Digest = controlPlaneHotfixPlanDigest(plan)
	return plan
}

func baseControlPlaneHotfixObservation(plan ControlPlaneHotfixAdoptionPlan) ControlPlaneHotfixObservation {
	return ControlPlaneHotfixObservation{
		HelmRevision: plan.BaseRevision, HelmStatus: "deployed",
		HelmRecordDigest: plan.HelmRecordDigest,
		ManifestDigest:   plan.BaseManifestDigest, ValuesDigest: plan.BaseValuesDigest,
		ChartTreeDigest: plan.ChartTreeDigest, Source: plan.CurrentSource,
		LiveImageRef: plan.LiveImageRef, APIImageID: plan.Kubernetes.APIImageID,
		Kubernetes: plan.Kubernetes, APIHealthStatus: 200, APIHealthDigest: plan.Kubernetes.APIHealthDigest,
	}
}

func targetControlPlaneHotfixObservation(plan ControlPlaneHotfixAdoptionPlan) ControlPlaneHotfixObservation {
	observation := baseControlPlaneHotfixObservation(plan)
	observation.HelmRevision = plan.TargetRevision
	observation.ManifestDigest = plan.TargetManifestDigest
	observation.ValuesDigest = plan.TargetValuesDigest
	observation.Source = plan.AdoptedSource
	observation.Kubernetes.APIGeneration++
	observation.Kubernetes.APIObservedGeneration++
	observation.Kubernetes.APITemplateDigest = plan.TargetAPITemplateDigest
	observation.Kubernetes.APIResourceVersion = "101"
	observation.Kubernetes.EndpointSliceResourceVersion = "301"
	observation.Kubernetes.EndpointBindingDigest = "sha256:" + strings.Repeat("8", 64)
	return observation
}

func hybridControlPlaneHotfixObservation(plan ControlPlaneHotfixAdoptionPlan) ControlPlaneHotfixObservation {
	observation := targetControlPlaneHotfixObservation(plan)
	observation.HelmRevision++
	observation.ManifestDigest = plan.HybridManifestDigest
	observation.ValuesDigest = plan.BaseValuesDigest
	observation.Source = plan.CurrentSource
	observation.Kubernetes.APIGeneration++
	observation.Kubernetes.APIObservedGeneration++
	observation.Kubernetes.APITemplateDigest = plan.HybridAPITemplateDigest
	observation.Kubernetes.APIResourceVersion = "102"
	observation.Kubernetes.EndpointSliceResourceVersion = "302"
	return observation
}

func validBuiltControlPlaneHotfixPlan(t *testing.T) (ControlPlaneHotfixAdoptionPlan, ControlPlaneHotfixAdoptionInput) {
	t.Helper()
	seed := validControlPlaneHotfixExecutionPlan()
	baseImage := seed.Provenance.Repository + ":" + seed.CurrentSource
	input := ControlPlaneHotfixAdoptionInput{
		ExpectedSHA: seed.ExpectedSHA, RunID: seed.RunID, RunAttempt: seed.RunAttempt,
		Namespace: seed.Namespace, ReleaseName: seed.ReleaseName, ReleaseFullname: seed.ReleaseFullname,
		HelmRevision: seed.BaseRevision, HelmStatus: seed.BaseStatus,
		HelmRecordDigest: seed.HelmRecordDigest, BaseValuesDigest: seed.BaseValuesDigest,
		TargetValuesDigest: seed.TargetValuesDigest, ChartTreeDigest: seed.ChartTreeDigest,
		CurrentSource: seed.CurrentSource, AdoptedSource: seed.AdoptedSource,
		LiveImageRef: seed.LiveImageRef, Fence: seed.Fence, Nonce: seed.Nonce,
		Confirm:    "CONFIRM_CONTROL_PLANE_HOTFIX_BASELINE_ADOPTION",
		Provenance: seed.Provenance, Kubernetes: seed.Kubernetes, Lease: seed.Lease,
	}
	input.BaseManifest = hotfixManifest(t, input.CurrentSource, baseImage, "")
	input.TargetManifest = hotfixManifest(t, input.AdoptedSource, input.LiveImageRef, "")
	input.RepeatedTarget = append([]byte(nil), input.TargetManifest...)
	input.HybridManifest = hotfixManifest(t, input.CurrentSource, input.LiveImageRef, "")
	baseTemplateDigest, err := hotfixManifestTemplateDigest(input.BaseManifest, input.Namespace, input.Kubernetes.APIName)
	if err != nil {
		t.Fatalf("digest base template: %v", err)
	}
	input.Kubernetes.APITemplateDigest = baseTemplateDigest
	plan, err := BuildControlPlaneHotfixAdoptionPlan(input)
	if err != nil {
		t.Fatalf("build valid hotfix plan: %v", err)
	}
	return plan, input
}

func hotfixManifest(t *testing.T, source, image, extra string) []byte {
	t.Helper()
	objects := hotfixObjects{}
	annotations := map[string]any{"fugue.pro/source-commit": source}
	if extra != "" {
		annotations["fugue.pro/unexpected"] = extra
	}
	deployment := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "fugue-fugue-api", "namespace": "fugue-system"},
		"spec": map[string]any{
			"replicas": 2,
			"strategy": map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxUnavailable": 0, "maxSurge": 1}},
			"template": map[string]any{
				"metadata": map[string]any{"annotations": annotations},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "api", "image": image}}},
			},
		},
	}
	objects[hotfixObjectKey(deployment)] = deployment
	for index := 0; index < controlPlaneHotfixManifestObjects-1; index++ {
		name := fmt.Sprintf("fixture-%02d", index)
		object := map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": name, "namespace": "fugue-system"},
			"data":     map[string]any{"value": name},
		}
		objects[hotfixObjectKey(object)] = object
	}
	rendered, err := encodeHotfixObjects(objects)
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	return rendered
}

package releasedomain

import (
	"context"
	"encoding/json"
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

func TestControlPlaneAPIHotfixRolloutV2BindsDistinctForwardAndHybridImages(t *testing.T) {
	t.Parallel()

	plan, input := validBuiltControlPlaneAPIHotfixV2Plan(t)
	if err := VerifyControlPlaneHotfixAdoptionPlan(plan); err != nil {
		t.Fatalf("verify v2 plan: %v", err)
	}
	if plan.PlanVersion != 2 || plan.Policy != ControlPlaneAPIHotfixRolloutPolicyV2 || plan.TargetAPIImageRef == plan.LiveHybridAPIImageRef {
		t.Fatalf("v2 image identities are not distinct: %+v", plan)
	}
	forward, err := RenderControlPlaneHotfixTransaction(input.TargetManifest, plan, "forward")
	if err != nil || hotfixDigest(forward) != plan.TargetManifestDigest {
		t.Fatalf("v2 forward render failed: digest=%s err=%v", hotfixDigest(forward), err)
	}
	compensated, err := RenderControlPlaneHotfixTransaction(input.BaseManifest, plan, "compensate")
	if err != nil || hotfixDigest(compensated) != plan.HybridManifestDigest {
		t.Fatalf("v2 compensation did not restore the live hybrid: digest=%s err=%v", hotfixDigest(compensated), err)
	}

	base := baseControlPlaneHotfixObservation(plan)
	target := targetControlPlaneHotfixObservation(plan)
	hybrid := hybridControlPlaneHotfixObservation(plan)
	if target.LiveImageRef != plan.TargetAPIImageRef || target.APIImageID != plan.TargetAPIImageID ||
		hybrid.LiveImageRef != plan.LiveHybridAPIImageRef || hybrid.APIImageID != plan.Kubernetes.APIImageID {
		t.Fatalf("v2 observations do not bind phase-specific images: target=%+v hybrid=%+v", target, hybrid)
	}
	runtime := &hotfixExecutionRuntime{
		observations:     []ControlPlaneHotfixObservation{base, base, target, hybrid},
		forwardResult:    ControlPlaneHotfixCommitUnknown,
		compensateResult: ControlPlaneHotfixCommitUnknown,
	}
	target.Kubernetes.APIReady = 1
	runtime.observations[2] = target
	result, err := ExecuteControlPlaneHotfixAdoption(context.Background(), plan, runtime, ControlPlaneHotfixExecutionOptions{})
	if err == nil || result.Status != "compensated" || result.WAL.RecoveryRequired || runtime.forwardCalls != 1 || runtime.compensateCalls != 1 {
		t.Fatalf("v2 commit-unknown did not reconcile through exact hybrid compensation: result=%+v err=%v runtime=%+v", result, err, runtime)
	}

	target = targetControlPlaneHotfixObservation(plan)
	target.Kubernetes.APIReady = 1
	hybrid = hybridControlPlaneHotfixObservation(plan)
	hybrid.Kubernetes.APIReady = 1
	runtime = &hotfixExecutionRuntime{
		observations:     []ControlPlaneHotfixObservation{base, base, target, hybrid},
		forwardResult:    ControlPlaneHotfixCommitUnknown,
		compensateResult: ControlPlaneHotfixCommitUnknown,
	}
	result, err = ExecuteControlPlaneHotfixAdoption(context.Background(), plan, runtime, ControlPlaneHotfixExecutionOptions{})
	if err == nil || result.Status != "recovery-required" || !result.WAL.RecoveryRequired || result.WAL.Phase != "compensation-started" || runtime.forwardCalls != 1 || runtime.compensateCalls != 1 {
		t.Fatalf("v2 ambiguous compensation did not retain the exact recovery fence: result=%+v err=%v runtime=%+v", result, err, runtime)
	}
}

func TestControlPlaneAPIHotfixRolloutV2RejectsIdentityAndManifestDrift(t *testing.T) {
	t.Parallel()

	_, input := validBuiltControlPlaneAPIHotfixV2Plan(t)
	tests := map[string]func(*ControlPlaneHotfixAdoptionInput){
		"same image":          func(value *ControlPlaneHotfixAdoptionInput) { value.TargetAPIImageRef = value.LiveHybridAPIImageRef },
		"wrong target source": func(value *ControlPlaneHotfixAdoptionInput) { value.AdoptedSource = strings.Repeat("4", 40) },
		"wrong hybrid source": func(value *ControlPlaneHotfixAdoptionInput) { value.CurrentSource = strings.Repeat("5", 40) },
		"wrong Helm base":     func(value *ControlPlaneHotfixAdoptionInput) { value.HelmRevision = 818 },
		"live prestate drift": func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.APIImageRef = value.TargetAPIImageRef },
		"non-API drift":       func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.FrozenNonAPIWorkloadDigest = "" },
		"target imageID drift": func(value *ControlPlaneHotfixAdoptionInput) {
			value.TargetAPIImageID = "containerd://sha256:" + strings.Repeat("1", 64)
		},
		"platform manifest is not the CRI imageID": func(value *ControlPlaneHotfixAdoptionInput) {
			value.TargetAPIImageID = "docker-pullable://ghcr.io/yym68686/fugue-api@" + value.Provenance.PlatformManifestDigest
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed := input
			mutate(&changed)
			if _, err := BuildControlPlaneHotfixAdoptionPlan(changed); err == nil {
				t.Fatalf("v2 %s drift was accepted", name)
			}
		})
	}

	changed := input
	changed.HybridManifest = hotfixManifest(t, changed.CurrentSource, changed.LiveHybridAPIImageRef, "third-pointer")
	if _, err := BuildControlPlaneHotfixAdoptionPlan(changed); err == nil || !strings.Contains(err.Error(), "non-image pointer") {
		t.Fatalf("v2 hybrid third pointer was accepted: %v", err)
	}
	changed = input
	changed.TargetManifest = hotfixManifestWithForeignObjectDrift(t, changed.TargetManifest)
	changed.RepeatedTarget = changed.TargetManifest
	if _, err := BuildControlPlaneHotfixAdoptionPlan(changed); err == nil || !strings.Contains(err.Error(), "non-API object") {
		t.Fatalf("v2 non-API drift was accepted: %v", err)
	}
}

func TestControlPlaneAPIHotfixRolloutV2WALReopensWithDistinctImageBindings(t *testing.T) {
	t.Parallel()

	plan, _ := validBuiltControlPlaneAPIHotfixV2Plan(t)
	wal, err := NewControlPlaneHotfixAdoptionWAL(plan)
	if err != nil {
		t.Fatalf("new v2 WAL: %v", err)
	}
	for _, phase := range []string{"prewrite-verified", "forward-started", "compensation-started"} {
		encoded, marshalErr := json.Marshal(wal)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		var reopened ControlPlaneHotfixAdoptionWAL
		if unmarshalErr := json.Unmarshal(encoded, &reopened); unmarshalErr != nil {
			t.Fatal(unmarshalErr)
		}
		if verifyErr := VerifyControlPlaneHotfixAdoptionWAL(reopened); verifyErr != nil || reopened.Policy != ControlPlaneAPIHotfixRolloutPolicyV2 || reopened.PlanDigest != plan.Digest {
			t.Fatalf("reopened v2 WAL lost its exact plan binding: wal=%+v err=%v", reopened, verifyErr)
		}
		wal, err = AdvanceControlPlaneHotfixAdoptionWAL(reopened, phase)
		if err != nil {
			t.Fatalf("advance reopened v2 WAL to %s: %v", phase, err)
		}
	}
	if wal.Phase != "compensation-started" || wal.ForwardAttempts != 1 || wal.CompensationAttempts != 1 || !wal.RecoveryRequired {
		t.Fatalf("v2 WAL reopen lost action fencing: %+v", wal)
	}
}

func TestControlPlaneAPIHotfixPostRenderBindsRawRestoreAndEffectiveBytes(t *testing.T) {
	t.Parallel()

	plan, input := validBuiltControlPlaneAPIHotfixV2Plan(t)
	rawTarget := []byte("raw-target-render\n")
	rawHybrid := []byte("raw-hybrid-render\n")
	restore := []byte("sealed-helm819-nine-object-restore-plan\n")
	plan.RawTargetManifestDigest = hotfixDigest(rawTarget)
	plan.RawHybridManifestDigest = hotfixDigest(rawHybrid)
	plan.TargetPostRenderDigest = hotfixDigest(input.TargetManifest)
	plan.HybridPostRenderDigest = hotfixDigest(input.HybridManifest)
	plan.NonAPIEdgeRestorePlanDigest = hotfixDigest(restore)
	plan.Digest = controlPlaneHotfixPlanDigest(plan)
	if err := VerifyControlPlaneAPIHotfixPostRender(plan, "forward", rawTarget, restore, input.TargetManifest); err != nil {
		t.Fatalf("verify forward post-render: %v", err)
	}
	if err := VerifyControlPlaneAPIHotfixPostRender(plan, "compensate", rawHybrid, restore, input.HybridManifest); err != nil {
		t.Fatalf("verify compensation post-render: %v", err)
	}
	tests := map[string]struct{ raw, restore, output []byte }{
		"raw":     {append(append([]byte(nil), rawTarget...), 'x'), restore, input.TargetManifest},
		"restore": {rawTarget, append(append([]byte(nil), restore...), 'x'), input.TargetManifest},
		"output":  {rawTarget, restore, append(append([]byte(nil), input.TargetManifest...), 'x')},
	}
	for name, test := range tests {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			if err := VerifyControlPlaneAPIHotfixPostRender(plan, "forward", test.raw, test.restore, test.output); err == nil {
				t.Fatalf("post-render accepted %s drift", name)
			}
		})
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
		LiveImageRef: controlPlaneHotfixHybridImage(plan), APIImageID: plan.Kubernetes.APIImageID,
		Kubernetes: plan.Kubernetes, APIHealthStatus: 200, APIHealthDigest: plan.Kubernetes.APIHealthDigest,
	}
}

func targetControlPlaneHotfixObservation(plan ControlPlaneHotfixAdoptionPlan) ControlPlaneHotfixObservation {
	observation := baseControlPlaneHotfixObservation(plan)
	observation.HelmRevision = plan.TargetRevision
	observation.ManifestDigest = plan.TargetManifestDigest
	observation.ValuesDigest = plan.TargetValuesDigest
	observation.Source = plan.AdoptedSource
	observation.LiveImageRef = controlPlaneHotfixTargetImage(plan)
	if plan.PlanVersion == 2 {
		observation.APIImageID = plan.TargetAPIImageID
		observation.Kubernetes.APIImageRef = plan.TargetAPIImageRef
		observation.Kubernetes.APIImageID = plan.TargetAPIImageID
	}
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
	if plan.PlanVersion == 2 {
		observation.ValuesDigest = plan.HybridValuesDigest
	}
	observation.Source = plan.CurrentSource
	observation.LiveImageRef = controlPlaneHotfixHybridImage(plan)
	observation.APIImageID = plan.Kubernetes.APIImageID
	observation.Kubernetes.APIImageRef = controlPlaneHotfixHybridImage(plan)
	observation.Kubernetes.APIImageID = plan.Kubernetes.APIImageID
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

func validBuiltControlPlaneAPIHotfixV2Plan(t *testing.T) (ControlPlaneHotfixAdoptionPlan, ControlPlaneHotfixAdoptionInput) {
	t.Helper()
	seed := validControlPlaneHotfixExecutionPlan()
	targetIndex := "sha256:" + strings.Repeat("8", 64)
	targetPlatform := "sha256:" + strings.Repeat("9", 64)
	targetImage := "ghcr.io/yym68686/fugue-api@" + targetIndex
	targetImageID := "docker-pullable://ghcr.io/yym68686/fugue-api@" + targetIndex
	input := ControlPlaneHotfixAdoptionInput{
		PlanVersion: 2,
		ExpectedSHA: seed.ExpectedSHA, RunID: seed.RunID, RunAttempt: seed.RunAttempt,
		Namespace: seed.Namespace, ReleaseName: seed.ReleaseName, ReleaseFullname: seed.ReleaseFullname,
		HelmRevision: controlPlaneAPIHotfixBaseRevisionV2, HelmStatus: seed.BaseStatus,
		HelmRecordDigest: seed.HelmRecordDigest, BaseValuesDigest: seed.BaseValuesDigest,
		TargetValuesDigest: seed.TargetValuesDigest, HybridValuesDigest: "sha256:" + strings.Repeat("b", 64),
		ChartTreeDigest: seed.ChartTreeDigest,
		CurrentSource:   controlPlaneAPIHotfixHybridSourceV2, AdoptedSource: controlPlaneAPIHotfixTargetSourceV2,
		TargetAPIImageRef: targetImage, LiveHybridAPIImageRef: controlPlaneAPIHotfixHybridImageV2,
		TargetAPIImageID: targetImageID, Fence: seed.Fence, Nonce: seed.Nonce,
		Confirm: "CONFIRM_CONTROL_PLANE_API_HOTFIX_ROLLOUT_V2",
		Provenance: ControlPlaneHotfixProvenance{
			BuildRunID: seed.Provenance.BuildRunID, BuildRunAttempt: 1,
			ArtifactName: "fugue-api-hotfix-v2", ArtifactDigest: seed.Provenance.ArtifactDigest,
			Repository: "ghcr.io/yym68686/fugue-api", IndexDigest: targetIndex,
			PlatformManifestDigest: targetPlatform, ConfigDigest: seed.Provenance.ConfigDigest,
			OCIRevision: controlPlaneAPIHotfixTargetSourceV2, Verified: true,
		},
		Kubernetes: seed.Kubernetes,
		Lease:      seed.Lease,
	}
	input.Kubernetes.APIImageRef = input.LiveHybridAPIImageRef
	input.Kubernetes.FrozenNonAPIWorkloadDigest = "sha256:" + strings.Repeat("e", 64)
	input.RawTargetManifestDigest = "sha256:" + strings.Repeat("1", 64)
	input.RawHybridManifestDigest = "sha256:" + strings.Repeat("2", 64)
	input.TargetPostRenderDigest = "sha256:" + strings.Repeat("3", 64)
	input.HybridPostRenderDigest = "sha256:" + strings.Repeat("4", 64)
	input.NonAPIEdgeRestorePlanDigest = "sha256:" + strings.Repeat("5", 64)
	// V2 uses the frozen live LKG PodTemplate as its effective transaction
	// base. The emergency annotation models live-only material that is absent
	// from the Helm819 chart render and must survive both directions.
	input.BaseManifest = hotfixManifest(t, input.CurrentSource, input.LiveHybridAPIImageRef, "preserved-live")
	input.TargetManifest = hotfixManifest(t, input.AdoptedSource, input.TargetAPIImageRef, "preserved-live")
	input.HybridManifest = hotfixManifest(t, input.CurrentSource, input.LiveHybridAPIImageRef, "preserved-live")
	input.RepeatedTarget = append([]byte(nil), input.TargetManifest...)
	hybridTemplateDigest, err := hotfixManifestTemplateDigest(input.HybridManifest, input.Namespace, input.Kubernetes.APIName)
	if err != nil {
		t.Fatalf("digest v2 hybrid template: %v", err)
	}
	input.Kubernetes.APITemplateDigest = hybridTemplateDigest
	plan, err := BuildControlPlaneHotfixAdoptionPlan(input)
	if err != nil {
		t.Fatalf("build valid v2 hotfix plan: %v", err)
	}
	return plan, input
}

func hotfixManifestWithForeignObjectDrift(t *testing.T, manifest []byte) []byte {
	t.Helper()
	objects, err := decodeHotfixObjects(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for key, object := range objects {
		if object["kind"] != "ConfigMap" {
			continue
		}
		object["data"] = map[string]any{"value": "drifted"}
		objects[key] = object
		break
	}
	rendered, err := encodeHotfixObjects(objects)
	if err != nil {
		t.Fatal(err)
	}
	return rendered
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

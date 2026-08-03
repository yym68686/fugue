package releasedomain

import (
	"bytes"
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

func TestControlPlaneControllerM16RolloutV3BindsPlanRenderWALAndCompensation(t *testing.T) {
	t.Parallel()

	plan, input := validBuiltControlPlaneControllerM16V3Plan(t)
	if err := VerifyControlPlaneHotfixAdoptionPlan(plan); err != nil {
		t.Fatalf("verify Controller M16 v3 plan: %v", err)
	}
	if plan.PlanVersion != 3 || plan.Policy != ControlPlaneControllerM16PolicyV3 ||
		plan.BaseRevision != 820 || plan.TargetRevision != 821 ||
		plan.CurrentSource != controlPlaneControllerM16HybridSourceV3 || plan.AdoptedSource != controlPlaneControllerM16TargetSourceV3 ||
		plan.TargetControllerImageRef != controlPlaneControllerM16TargetImageV3 || plan.LiveHybridControllerImageRef != controlPlaneControllerM16HybridImageV3 ||
		plan.TargetControllerImageID != input.TargetControllerImageID || plan.TargetAPIImageRef != "" || plan.LiveHybridAPIImageRef != "" || plan.TargetAPIImageID != "" ||
		plan.TargetControllerTemplateDigest == plan.HybridControllerTemplateDigest || plan.TargetAPITemplateDigest != "" || plan.HybridAPITemplateDigest != "" {
		t.Fatalf("Controller M16 v3 plan lost its fixed placement identity: %+v", plan)
	}
	if err := VerifyControlPlaneHotfixRenderSet(plan, input.BaseManifest, input.TargetManifest, input.RepeatedTarget, input.HybridManifest); err != nil {
		t.Fatalf("verify Controller M16 render set: %v", err)
	}
	forward, err := RenderControlPlaneHotfixTransaction(input.TargetManifest, plan, "forward")
	if err != nil || hotfixDigest(forward) != plan.TargetManifestDigest {
		t.Fatalf("Controller M16 forward render: digest=%s err=%v", hotfixDigest(forward), err)
	}
	compensated, err := RenderControlPlaneHotfixTransaction(input.BaseManifest, plan, "compensate")
	if err != nil || hotfixDigest(compensated) != plan.HybridManifestDigest || !bytes.Equal(compensated, input.HybridManifest) {
		t.Fatalf("Controller M16 compensation did not restore exact d1e/e636 bytes: digest=%s err=%v", hotfixDigest(compensated), err)
	}

	wal, err := NewControlPlaneHotfixAdoptionWAL(plan)
	if err != nil {
		t.Fatalf("new Controller M16 WAL: %v", err)
	}
	for _, phase := range []string{"prewrite-verified", "forward-started", "compensation-started", "compensated"} {
		encoded, marshalErr := json.Marshal(wal)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		var reopened ControlPlaneHotfixAdoptionWAL
		if unmarshalErr := json.Unmarshal(encoded, &reopened); unmarshalErr != nil {
			t.Fatal(unmarshalErr)
		}
		if verifyErr := VerifyControlPlaneHotfixAdoptionWAL(reopened); verifyErr != nil ||
			reopened.Policy != ControlPlaneControllerM16PolicyV3 || reopened.PlanDigest != plan.Digest {
			t.Fatalf("reopened Controller M16 WAL lost its exact plan binding: wal=%+v err=%v", reopened, verifyErr)
		}
		wal, err = AdvanceControlPlaneHotfixAdoptionWAL(reopened, phase)
		if err != nil {
			t.Fatalf("advance Controller M16 WAL to %s: %v", phase, err)
		}
	}
	if wal.Phase != "compensated" || wal.ForwardAttempts != 1 || wal.CompensationAttempts != 1 || wal.RecoveryRequired {
		t.Fatalf("Controller M16 WAL lost exact compensation fencing: %+v", wal)
	}
	if _, err := ExecuteControlPlaneHotfixAdoption(context.Background(), plan, &hotfixExecutionRuntime{}, ControlPlaneHotfixExecutionOptions{}); err == nil ||
		!strings.Contains(err.Error(), "sealed production shell runtime") {
		t.Fatalf("Controller M16 escaped its fixed sealed runtime: %v", err)
	}
}

func TestControlPlaneControllerM16RolloutV3RejectsIdentityRenderAndCompensationDrift(t *testing.T) {
	t.Parallel()

	_, input := validBuiltControlPlaneControllerM16V3Plan(t)
	inputTests := map[string]func(*ControlPlaneHotfixAdoptionInput){
		"wrong target source": func(value *ControlPlaneHotfixAdoptionInput) { value.AdoptedSource = strings.Repeat("4", 40) },
		"wrong hybrid source": func(value *ControlPlaneHotfixAdoptionInput) { value.CurrentSource = strings.Repeat("5", 40) },
		"wrong Helm base":     func(value *ControlPlaneHotfixAdoptionInput) { value.HelmRevision = 819 },
		"same image": func(value *ControlPlaneHotfixAdoptionInput) {
			value.TargetControllerImageRef = value.LiveHybridControllerImageRef
		},
		"wrong target image": func(value *ControlPlaneHotfixAdoptionInput) {
			value.TargetControllerImageRef = "ghcr.io/yym68686/fugue-controller@sha256:" + strings.Repeat("1", 64)
		},
		"live image drift": func(value *ControlPlaneHotfixAdoptionInput) {
			value.Kubernetes.ControllerImageRef = value.TargetControllerImageRef
		},
		"target imageID drift": func(value *ControlPlaneHotfixAdoptionInput) {
			value.TargetControllerImageID = "containerd://sha256:" + strings.Repeat("1", 64)
		},
		"API target pointer": func(value *ControlPlaneHotfixAdoptionInput) {
			value.TargetAPIImageRef = "ghcr.io/example/api@sha256:" + strings.Repeat("1", 64)
		},
		"Controller identity":   func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.ControllerUID = "" },
		"Controller generation": func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.ControllerObservedGeneration-- },
		"Controller readiness":  func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.ControllerReady = 1 },
		"leader Lease":          func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.ControllerLeaderHolder = "" },
		"metrics witness":       func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.ControllerMetricsDigest = "" },
		"LKG witness":           func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.ControllerLKGDigest = "" },
		"non-Controller freeze": func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.FrozenNonControllerDigest = "" },
		"nondeterministic target": func(value *ControlPlaneHotfixAdoptionInput) {
			value.RepeatedTarget = controllerM16ManifestWithForeignObjectDrift(t, value.RepeatedTarget)
		},
		"target foreign drift": func(value *ControlPlaneHotfixAdoptionInput) {
			value.TargetManifest = controllerM16ManifestWithForeignObjectDrift(t, value.TargetManifest)
			value.RepeatedTarget = value.TargetManifest
		},
		"hybrid foreign drift": func(value *ControlPlaneHotfixAdoptionInput) {
			value.HybridManifest = controllerM16ManifestWithForeignObjectDrift(t, value.HybridManifest)
		},
		"wrong rolling strategy": func(value *ControlPlaneHotfixAdoptionInput) {
			value.BaseManifest = mutateControllerM16Deployment(t, value.BaseManifest, func(deployment map[string]any) {
				deployment["spec"].(map[string]any)["strategy"].(map[string]any)["rollingUpdate"].(map[string]any)["maxSurge"] = 1
			})
		},
		"target third pointer": func(value *ControlPlaneHotfixAdoptionInput) {
			value.TargetManifest = mutateControllerM16Deployment(t, value.TargetManifest, func(deployment map[string]any) {
				deployment["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)["fugue.pro/unexpected"] = "drift"
			})
			value.RepeatedTarget = value.TargetManifest
		},
		"compensation third pointer": func(value *ControlPlaneHotfixAdoptionInput) {
			value.HybridManifest = mutateControllerM16Deployment(t, value.HybridManifest, func(deployment map[string]any) {
				deployment["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)["fugue.pro/unexpected"] = "drift"
			})
		},
	}
	for name, mutate := range inputTests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed := input
			mutate(&changed)
			if _, err := BuildControlPlaneHotfixAdoptionPlan(changed); err == nil {
				t.Fatalf("Controller M16 accepted %s drift", name)
			}
		})
	}

	plan, validInput := validBuiltControlPlaneControllerM16V3Plan(t)
	planTests := map[string]func(*ControlPlaneHotfixAdoptionPlan){
		"revision":        func(value *ControlPlaneHotfixAdoptionPlan) { value.TargetRevision++ },
		"target template": func(value *ControlPlaneHotfixAdoptionPlan) { value.TargetControllerTemplateDigest = "" },
		"hybrid image": func(value *ControlPlaneHotfixAdoptionPlan) {
			value.LiveHybridControllerImageRef = value.TargetControllerImageRef
		},
		"API image authority": func(value *ControlPlaneHotfixAdoptionPlan) {
			value.TargetAPIImageRef = "ghcr.io/example/api@sha256:" + strings.Repeat("1", 64)
		},
		"provenance": func(value *ControlPlaneHotfixAdoptionPlan) {
			value.Provenance.IndexDigest = "sha256:" + strings.Repeat("1", 64)
		},
		"leader identity":       func(value *ControlPlaneHotfixAdoptionPlan) { value.Kubernetes.ControllerLeaderLeaseUID = "" },
		"frozen non-Controller": func(value *ControlPlaneHotfixAdoptionPlan) { value.Kubernetes.FrozenNonControllerDigest = "" },
	}
	for name, mutate := range planTests {
		name, mutate := name, mutate
		t.Run("verify plan "+name, func(t *testing.T) {
			t.Parallel()
			changed := plan
			mutate(&changed)
			changed.Digest = controlPlaneHotfixPlanDigest(changed)
			if err := VerifyControlPlaneHotfixAdoptionPlan(changed); err == nil {
				t.Fatalf("Controller M16 plan accepted %s drift", name)
			}
		})
	}
	if _, err := RenderControlPlaneHotfixTransaction(validInput.BaseManifest, plan, "forward"); err == nil {
		t.Fatal("Controller M16 forward accepted base bytes")
	}
	if _, err := RenderControlPlaneHotfixTransaction(validInput.TargetManifest, plan, "compensate"); err == nil {
		t.Fatal("Controller M16 compensation accepted target bytes as its base")
	}
	if err := VerifyControlPlaneHotfixRenderSet(plan, validInput.BaseManifest, validInput.TargetManifest, controllerM16ManifestWithForeignObjectDrift(t, validInput.RepeatedTarget), validInput.HybridManifest); err == nil {
		t.Fatal("Controller M16 render verifier accepted nondeterministic target bytes")
	}
	wal, err := NewControlPlaneHotfixAdoptionWAL(plan)
	if err != nil {
		t.Fatal(err)
	}
	wal.Policy = "control-plane-generic-component-rollout"
	wal.Digest = controlPlaneHotfixWALDigest(wal)
	if err := VerifyControlPlaneHotfixAdoptionWAL(wal); err == nil {
		t.Fatal("Controller M16 WAL accepted an unreviewed generic policy")
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

func validBuiltControlPlaneControllerM16V3Plan(t *testing.T) (ControlPlaneHotfixAdoptionPlan, ControlPlaneHotfixAdoptionInput) {
	t.Helper()
	seed := validControlPlaneHotfixExecutionPlan()
	targetIndex := "sha256:444bca23386cc0f19012fcbaba20d71db1b9863ee80d50d1bde6d87376e190df"
	input := ControlPlaneHotfixAdoptionInput{
		PlanVersion: 3,
		ExpectedSHA: seed.ExpectedSHA, RunID: "30824899056", RunAttempt: 1,
		Namespace: seed.Namespace, ReleaseName: seed.ReleaseName, ReleaseFullname: seed.ReleaseFullname,
		HelmRevision: controlPlaneControllerM16BaseRevisionV3, HelmStatus: seed.BaseStatus,
		HelmRecordDigest: seed.HelmRecordDigest, BaseValuesDigest: seed.BaseValuesDigest,
		TargetValuesDigest: "sha256:" + strings.Repeat("1", 64), HybridValuesDigest: "sha256:" + strings.Repeat("2", 64),
		RawTargetManifestDigest: "sha256:" + strings.Repeat("3", 64), RawHybridManifestDigest: "sha256:" + strings.Repeat("4", 64),
		TargetPostRenderDigest: "sha256:" + strings.Repeat("5", 64), HybridPostRenderDigest: "sha256:" + strings.Repeat("6", 64),
		NonAPIEdgeRestorePlanDigest: "sha256:" + strings.Repeat("7", 64), ChartTreeDigest: seed.ChartTreeDigest,
		CurrentSource: controlPlaneControllerM16HybridSourceV3, AdoptedSource: controlPlaneControllerM16TargetSourceV3,
		TargetControllerImageRef: controlPlaneControllerM16TargetImageV3, LiveHybridControllerImageRef: controlPlaneControllerM16HybridImageV3,
		TargetControllerImageID: "docker-pullable://ghcr.io/yym68686/fugue-controller@" + targetIndex,
		Fence:                   seed.Fence, Nonce: seed.Nonce, Confirm: "CONFIRM_CONTROL_PLANE_CONTROLLER_M16_ROLLOUT_V1",
		Provenance: ControlPlaneHotfixProvenance{
			BuildRunID: "30824899056", BuildRunAttempt: 1,
			ArtifactName: "fugue-historical-controller-build-only-58fc", ArtifactDigest: "sha256:" + strings.Repeat("8", 64),
			Repository: "ghcr.io/yym68686/fugue-controller", IndexDigest: targetIndex,
			PlatformManifestDigest: "sha256:7fa0ec2c4dbe4d7570ef595b006411efba9f4fbba1caf1571611265a018fbc00",
			ConfigDigest:           "sha256:7db86a97c096224cae83a3865dccdc7973ca71cef359653d76e31bc22aee7b06",
			OCIRevision:            controlPlaneControllerM16TargetSourceV3, Verified: true,
		},
		Kubernetes: seed.Kubernetes,
		Lease:      seed.Lease,
	}
	input.Kubernetes.ControllerName = input.ReleaseFullname + "-controller"
	input.Kubernetes.ControllerUID = "controller-uid"
	input.Kubernetes.ControllerResourceVersion = "67714132"
	input.Kubernetes.ControllerGeneration = 691
	input.Kubernetes.ControllerObservedGeneration = 691
	input.Kubernetes.ControllerImageRef = input.LiveHybridControllerImageRef
	input.Kubernetes.ControllerImageID = "containerd://sha256:e636b35fe8718e1f20895c0a290924a0d48a6cb7d1072d741612df18483fa13d"
	input.Kubernetes.ControllerReplicas = 2
	input.Kubernetes.ControllerReady = 2
	input.Kubernetes.ControllerUpdated = 2
	input.Kubernetes.ControllerAvailable = 2
	input.Kubernetes.ControllerUnavailable = 0
	input.Kubernetes.ControllerLeaderLeaseName = input.ReleaseFullname + "-controller"
	input.Kubernetes.ControllerLeaderLeaseUID = "controller-leader-lease-uid"
	input.Kubernetes.ControllerLeaderLeaseVersion = "67714200"
	input.Kubernetes.ControllerLeaderHolder = "fugue-fugue-controller-7c7785b56-abcde"
	input.Kubernetes.ControllerMetricsDigest = "sha256:" + strings.Repeat("9", 64)
	input.Kubernetes.ControllerLKGDigest = "sha256:" + strings.Repeat("a", 64)
	input.Kubernetes.FrozenNonControllerDigest = "sha256:" + strings.Repeat("b", 64)
	input.BaseManifest = controllerM16Manifest(t, input.CurrentSource, input.LiveHybridControllerImageRef)
	input.TargetManifest = controllerM16Manifest(t, input.AdoptedSource, input.TargetControllerImageRef)
	input.RepeatedTarget = append([]byte(nil), input.TargetManifest...)
	input.HybridManifest = controllerM16Manifest(t, input.CurrentSource, input.LiveHybridControllerImageRef)
	hybridTemplateDigest, err := hotfixManifestTemplateDigest(input.HybridManifest, input.Namespace, input.Kubernetes.ControllerName)
	if err != nil {
		t.Fatalf("digest Controller M16 hybrid template: %v", err)
	}
	input.Kubernetes.ControllerTemplateDigest = hybridTemplateDigest
	plan, err := BuildControlPlaneHotfixAdoptionPlan(input)
	if err != nil {
		t.Fatalf("build valid Controller M16 v3 plan: %v", err)
	}
	return plan, input
}

func controllerM16Manifest(t *testing.T, source, image string) []byte {
	t.Helper()
	objects := hotfixObjects{}
	deployment := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "fugue-fugue-controller", "namespace": "fugue-system"},
		"spec": map[string]any{
			"replicas": 2,
			"strategy": map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxUnavailable": 0, "maxSurge": 2}},
			"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": source}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "controller", "image": image}}},
			},
		},
	}
	objects[hotfixObjectKey(deployment)] = deployment
	for index := 0; index < controlPlaneHotfixManifestObjects-1; index++ {
		name := fmt.Sprintf("controller-m16-fixture-%02d", index)
		object := map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": name, "namespace": "fugue-system"},
			"data":     map[string]any{"value": name},
		}
		objects[hotfixObjectKey(object)] = object
	}
	rendered, err := encodeHotfixObjects(objects)
	if err != nil {
		t.Fatalf("encode Controller M16 manifest: %v", err)
	}
	return rendered
}

func mutateControllerM16Deployment(t *testing.T, manifest []byte, mutate func(map[string]any)) []byte {
	t.Helper()
	objects, err := decodeHotfixObjects(manifest)
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := exactHotfixDeployment(objects, "fugue-system", "fugue-fugue-controller")
	if err != nil {
		t.Fatal(err)
	}
	mutate(deployment)
	objects[hotfixObjectKey(deployment)] = deployment
	rendered, err := encodeHotfixObjects(objects)
	if err != nil {
		t.Fatal(err)
	}
	return rendered
}

func controllerM16ManifestWithForeignObjectDrift(t *testing.T, manifest []byte) []byte {
	t.Helper()
	objects, err := decodeHotfixObjects(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for key, object := range objects {
		if object["kind"] == "ConfigMap" {
			object["data"] = map[string]any{"value": "drifted"}
			objects[key] = object
			break
		}
	}
	rendered, err := encodeHotfixObjects(objects)
	if err != nil {
		t.Fatal(err)
	}
	return rendered
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

func TestBuildControlPlaneControllerM16ObservedRecoveryPlan(t *testing.T) {
	t.Parallel()

	plan, _ := validBuiltControlPlaneControllerM16ObservedRecoveryPlan(t)
	if plan.Kind != ControlPlaneControllerM16ObservedRecoveryPlanKind || plan.Policy != ControlPlaneControllerM16ObservedRecoveryPolicyV4 {
		t.Fatalf("unexpected observed recovery identity: %+v", plan)
	}
	if plan.BaseRevision != 822 || plan.TargetRevision != 823 || plan.ArchivedRevision != 820 {
		t.Fatalf("unexpected observed recovery revisions: %+v", plan)
	}
	if plan.RecoveryBasis != "independent-observed-state" || plan.OriginRunID != "30836591717" {
		t.Fatalf("new plan could masquerade as the missing original WAL: %+v", plan)
	}
	if plan.ObservedAPITemplateDigest == plan.TargetAPITemplateDigest || plan.TargetAPITemplateDigest != plan.HybridAPITemplateDigest {
		t.Fatalf("API recovery template binding is invalid: %+v", plan)
	}
	if plan.ObservedControllerTemplateDigest != plan.TargetControllerTemplateDigest {
		t.Fatalf("Controller would roll during observed recovery: %+v", plan)
	}
}

func TestControlPlaneControllerM16ObservedRecoveryRejectsBindingDrift(t *testing.T) {
	t.Parallel()

	_, valid := validBuiltControlPlaneControllerM16ObservedRecoveryPlan(t)
	tests := map[string]func(*ControlPlaneHotfixAdoptionInput){
		"origin run":     func(value *ControlPlaneHotfixAdoptionInput) { value.OriginRunID = "30836591718" },
		"origin attempt": func(value *ControlPlaneHotfixAdoptionInput) { value.OriginRunAttempt = 2 },
		"origin SHA":     func(value *ControlPlaneHotfixAdoptionInput) { value.OriginSourceSHA = strings.Repeat("9", 40) },
		"API UID":        func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.APIUID = "wrong" },
		"API RV":         func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.APIResourceVersion = "1" },
		"Controller UID": func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.ControllerUID = "wrong" },
		"Controller RV":  func(value *ControlPlaneHotfixAdoptionInput) { value.Kubernetes.ControllerResourceVersion = "1" },
		"active operation": func(value *ControlPlaneHotfixAdoptionInput) {
			value.Kubernetes.ActiveOperationsDigest = "sha256:" + strings.Repeat("3", 64)
		},
		"Lease UID": func(value *ControlPlaneHotfixAdoptionInput) { value.Lease.UID = "wrong" },
		"Lease RV":  func(value *ControlPlaneHotfixAdoptionInput) { value.Lease.ResourceVersion = "1" },
		"Lease token": func(value *ControlPlaneHotfixAdoptionInput) {
			value.Lease.CoordinationTokenDigest = "sha256:" + strings.Repeat("1", 64)
		},
		"Helm revision": func(value *ControlPlaneHotfixAdoptionInput) { value.HelmRevision = 821 },
		"archive digest": func(value *ControlPlaneHotfixAdoptionInput) {
			value.ArchivedManifestDigest = "sha256:" + strings.Repeat("2", 64)
		},
		"old WAL claim": func(value *ControlPlaneHotfixAdoptionInput) { value.RecoveryBasis = "prior-wal" },
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			changed := valid
			mutate(&changed)
			if _, err := BuildControlPlaneHotfixAdoptionPlan(changed); err == nil {
				t.Fatalf("observed recovery accepted drifted %s", name)
			}
		})
	}
}

func TestControlPlaneControllerM16ObservedRecoveryWALDurability(t *testing.T) {
	t.Parallel()

	plan, _ := validBuiltControlPlaneControllerM16ObservedRecoveryPlan(t)
	wal, err := NewControlPlaneHotfixAdoptionWAL(plan)
	if err != nil {
		t.Fatal(err)
	}
	if wal.Kind != ControlPlaneControllerM16ObservedRecoveryWALKind || wal.Phase != "prepared" || !wal.RecoveryRequired || wal.HelmAttempts != 0 {
		t.Fatalf("prepared observed recovery WAL is invalid: %+v", wal)
	}
	for _, phase := range []string{"fence-persisted", "helm-started", "commit-unknown", "helm-committed", "verified"} {
		wal, err = AdvanceControlPlaneHotfixAdoptionWAL(wal, phase)
		if err != nil {
			t.Fatalf("advance to %s: %v", phase, err)
		}
		if !wal.RecoveryRequired {
			t.Fatalf("fence cleared before terminal verification at %s", phase)
		}
	}
	wal, err = AdvanceControlPlaneHotfixAdoptionWAL(wal, "sealed")
	if err != nil {
		t.Fatal(err)
	}
	if wal.RecoveryRequired || wal.HelmAttempts != 1 {
		t.Fatalf("sealed observed recovery WAL is invalid: %+v", wal)
	}
	if _, err := AdvanceControlPlaneHotfixAdoptionWAL(wal, "helm-started"); err == nil {
		t.Fatal("sealed recovery could execute Helm again")
	}
}

func TestControlPlaneControllerM16ObservedRecoveryRawDocuments(t *testing.T) {
	t.Parallel()

	observed := observedRecoveryRawFixture("current-api", "current-controller", "stable")
	target := observedRecoveryRawFixture("helm820-api", "current-controller", "stable")
	if err := verifyObservedRecoveryRawDocumentIsolation(observed, target); err != nil {
		t.Fatalf("valid raw recovery isolation failed: %v", err)
	}
	if err := verifyObservedRecoveryRawDocumentIsolation(observed, observedRecoveryRawFixture("helm820-api", "changed-controller", "stable")); err == nil {
		t.Fatal("Controller raw document drift was accepted")
	}
	if err := verifyObservedRecoveryRawDocumentIsolation(observed, observedRecoveryRawFixture("helm820-api", "current-controller", "changed")); err == nil {
		t.Fatal("non-API raw document drift was accepted")
	}
}

func validBuiltControlPlaneControllerM16ObservedRecoveryPlan(t *testing.T) (ControlPlaneHotfixAdoptionPlan, ControlPlaneHotfixAdoptionInput) {
	t.Helper()
	digest := "sha256:" + strings.Repeat("a", 64)
	observed := observedRecoveryManifest(t, "current-api", "current-controller")
	archived := observedRecoveryManifest(t, "helm820-api", "old-controller")
	target := observedRecoveryManifest(t, "helm820-api", "current-controller")
	controllerDigest, err := hotfixManifestTemplateDigest(observed, "fugue-system", "fugue-fugue-controller")
	if err != nil {
		t.Fatal(err)
	}
	input := ControlPlaneHotfixAdoptionInput{
		PlanVersion: 4, ExpectedSHA: strings.Repeat("1", 40), RunID: "30900000001", RunAttempt: 1,
		Namespace: "fugue-system", ReleaseName: "fugue", ReleaseFullname: "fugue-fugue",
		HelmRevision: 822, HelmStatus: "deployed", HelmRecordDigest: digest,
		BaseValuesDigest:        controlPlaneControllerM16ObservedRecoveryValues,
		TargetValuesDigest:      controlPlaneControllerM16ObservedRecoveryValues,
		HybridValuesDigest:      controlPlaneControllerM16ObservedRecoveryValues,
		RawTargetManifestDigest: digest, TargetPostRenderDigest: digest, NonAPIEdgeRestorePlanDigest: digest,
		ChartTreeDigest: digest, Fence: "observed-recovery-fence-token", Nonce: "observed-recovery-nonce-token",
		Confirm:     "CONFIRM_CONTROL_PLANE_CONTROLLER_M16_OBSERVED_RECOVERY_V1_30836591717",
		OriginRunID: controlPlaneControllerM16ObservedRecoveryOriginRunID, OriginRunAttempt: 1,
		OriginSourceSHA:  controlPlaneControllerM16ObservedRecoveryOriginSource,
		ArchivedRevision: 820, ArchivedManifestDigest: controlPlaneControllerM16ObservedRecoveryManifest820,
		ObservedManifestDigest: controlPlaneControllerM16ObservedRecoveryManifest822,
		ArchivedValuesDigest:   controlPlaneControllerM16ObservedRecoveryValues,
		ObservedValuesDigest:   controlPlaneControllerM16ObservedRecoveryValues,
		RecoveryBasis:          "independent-observed-state", RecoveryConfigMapName: controlPlaneControllerM16ObservedRecoveryConfigMap,
		Kubernetes: ControlPlaneHotfixKubernetesEvidence{
			APIName: "fugue-fugue-api", APIUID: controlPlaneControllerM16ObservedRecoveryAPIUID,
			APIResourceVersion: controlPlaneControllerM16ObservedRecoveryAPIRV, APIGeneration: 718, APIObservedGeneration: 718,
			APITemplateDigest: controlPlaneControllerM16ObservedRecoveryAPILiveTemplate,
			APIImageRef:       controlPlaneControllerM16ObservedRecoveryAPIImage, APIImageID: "containerd://api",
			APIHealthDigest: digest, APIReplicas: 2, APIReady: 2, APIUpdated: 2, APIAvailable: 2,
			ServiceName: "fugue-fugue", ServiceUID: "service-uid", ServiceResourceVersion: "200", ServiceSelectorDigest: digest,
			EndpointSliceName: "fugue-fugue-abc", EndpointSliceUID: "slice-uid", EndpointSliceResourceVersion: "300",
			EndpointServiceName: "fugue-fugue", EndpointBindingDigest: digest, ReadyServingEndpoints: 2,
			ControllerName: "fugue-fugue-controller", ControllerUID: controlPlaneControllerM16ObservedRecoveryControllerUID,
			ControllerResourceVersion: controlPlaneControllerM16ObservedRecoveryControllerRV, ControllerGeneration: 693, ControllerObservedGeneration: 693,
			ControllerTemplateDigest: controllerDigest, ControllerImageRef: controlPlaneControllerM16HybridImageV3, ControllerImageID: "containerd://controller",
			ControllerReplicas: 2, ControllerReady: 2, ControllerUpdated: 2, ControllerAvailable: 2,
			ControllerLeaderLeaseName: "fugue-fugue-controller", ControllerLeaderLeaseUID: "leader-uid", ControllerLeaderLeaseVersion: "400",
			ControllerLeaderHolder: "fugue-fugue-controller-abc", ControllerMetricsDigest: digest, ControllerLKGDigest: digest,
			FrozenNonAPIControllerDigest: controlPlaneControllerM16ObservedRecoveryOtherWorkloads,
			HealthWitnessDigest:          digest,
			ActiveOperationsDigest:       controlPlaneControllerM16ObservedRecoveryNoActiveOperations,
		},
		Lease: ControlPlaneHotfixLeaseEvidence{
			Namespace: "fugue-system", Name: "fugue-fugue-control-plane-db-backup",
			UID: controlPlaneControllerM16ObservedRecoveryLeaseUID, ResourceVersion: controlPlaneControllerM16ObservedRecoveryLeaseRV,
			HolderIdentity: controlPlaneControllerM16ObservedRecoveryLeaseHolder, RecoveryRequired: true,
			CoordinationTokenDigest: controlPlaneControllerM16ObservedRecoveryLeaseTokenDigest,
		},
		BaseManifest: observed, TargetManifest: target, RepeatedTarget: append([]byte(nil), target...), HybridManifest: archived,
	}
	plan, err := BuildControlPlaneHotfixAdoptionPlan(input)
	if err != nil {
		t.Fatalf("build valid observed recovery plan: %v", err)
	}
	return plan, input
}

func observedRecoveryManifest(t *testing.T, apiMarker, controllerMarker string) []byte {
	t.Helper()
	objects := hotfixObjects{}
	api := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "fugue-fugue-api", "namespace": "fugue-system"},
		"spec": map[string]any{"replicas": 2, "strategy": map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxUnavailable": 0, "maxSurge": 1}}, "template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": controlPlaneAPIHotfixTargetSourceV2, "fixture": apiMarker}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "api", "image": controlPlaneControllerM16ObservedRecoveryAPIImage}}}}},
	}
	controller := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "fugue-fugue-controller", "namespace": "fugue-system"},
		"spec": map[string]any{"replicas": 2, "strategy": map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxUnavailable": 0, "maxSurge": 2}}, "template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": controlPlaneControllerM16HybridSourceV3, "fixture": controllerMarker}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "controller", "image": controlPlaneControllerM16HybridImageV3}}}}},
	}
	objects[hotfixObjectKey(api)] = api
	objects[hotfixObjectKey(controller)] = controller
	for index := 0; index < controlPlaneHotfixManifestObjects-2; index++ {
		name := fmt.Sprintf("observed-recovery-%02d", index)
		object := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": name, "namespace": "fugue-system"}, "data": map[string]any{"value": name}}
		objects[hotfixObjectKey(object)] = object
	}
	rendered, err := encodeHotfixObjects(objects)
	if err != nil {
		t.Fatal(err)
	}
	return rendered
}

func observedRecoveryRawFixture(apiMarker, controllerMarker, foreignMarker string) []byte {
	docs := []string{
		"apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: fugue-fugue-api\nspec:\n  template:\n    metadata:\n      annotations:\n        fixture: " + apiMarker + "\n",
		"apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: fugue-fugue-controller\nspec:\n  template:\n    metadata:\n      annotations:\n        fixture: " + controllerMarker + "\n",
	}
	for index := 0; index < 83; index++ {
		marker := "stable"
		if index == 0 {
			marker = foreignMarker
		}
		docs = append(docs, fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: raw-%02d\ndata:\n  value: %s\n", index, marker))
	}
	return []byte("---\n" + strings.Join(docs, "---\n"))
}

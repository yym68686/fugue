package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestExecuteManagedPostgresResizeStagesConvergesOrderedStagesWithoutRestart(t *testing.T) {
	fixture := newManagedPostgresResizeExecutorFixture(t)
	target := kubeResourceRequirements{
		Requests: map[string]string{"cpu": "300m", "memory": "1280Mi"},
		Limits:   map[string]string{"cpu": "500m", "memory": "1792Mi"},
	}
	gates := config.ManagedPostgresInPlaceResizeConfig{
		Enabled:                     true,
		CPURequestUpscaleEnabled:    true,
		MemoryRequestUpscaleEnabled: true,
		CPULimitUpscaleEnabled:      true,
		MemoryLimitUpscaleEnabled:   true,
	}

	final, err := executeManagedPostgresResizeStages(
		context.Background(),
		target,
		gates,
		5*time.Second,
		time.Millisecond,
		fixture.hooks(),
	)
	if err != nil {
		t.Fatalf("execute resize stages: %v", err)
	}
	if fixture.patchCalls != 2 {
		t.Fatalf("patch calls = %d, want one limit and one request stage", fixture.patchCalls)
	}
	if fixture.probeCalls != 3 {
		t.Fatalf("probe calls = %d, want preflight plus each stage", fixture.probeCalls)
	}
	if final.Observation.PodUID != fixture.initialPodUID ||
		final.Observation.RestartCount != fixture.initialRestartCount ||
		final.Observation.ContainerStartedAt != fixture.initialStartedAt {
		t.Fatalf("resize changed immutable Pod/container identity: %+v", final.Observation)
	}
	if final.Observation.Generation != fixture.initialGeneration+int64(fixture.patchCalls) ||
		final.Observation.ObservedGeneration != final.Observation.Generation {
		t.Fatalf("unexpected terminal generations: generation=%d observed=%d", final.Observation.Generation, final.Observation.ObservedGeneration)
	}
	if final.Observation.ActualResources == nil ||
		!managedPostgresResizeResourcesEqual(managedPostgresCPUAndMemoryEnvelope(*final.Observation.ActualResources), target) {
		t.Fatalf("terminal resources did not reach target: %+v", final.Observation.ActualResources)
	}
	if got := fixture.eventStageNames(model.OperationEvidenceTypePostgresResizeApplying); strings.Join(got, ",") != "limit_upscale,request_upscale" {
		t.Fatalf("applied stages = %v", got)
	}
	if got := fixture.eventCount(model.OperationEvidenceTypePostgresResizeVerified); got != 2 {
		t.Fatalf("verified event count = %d, want 2", got)
	}
}

func TestExecuteManagedPostgresResizeStagesConvergesMixedDirectionsInSafeOrder(t *testing.T) {
	fixture := newManagedPostgresResizeExecutorFixture(t)
	target := kubeResourceRequirements{
		Requests: map[string]string{"cpu": "300m", "memory": "768Mi"},
		Limits:   map[string]string{"cpu": "500m", "memory": "1024Mi"},
	}
	gates := config.ManagedPostgresInPlaceResizeConfig{
		Enabled:                       true,
		CPURequestUpscaleEnabled:      true,
		MemoryRequestDownscaleEnabled: true,
		CPULimitUpscaleEnabled:        true,
		MemoryLimitDownscaleEnabled:   true,
	}

	final, err := executeManagedPostgresResizeStages(
		context.Background(), target, gates, 5*time.Second, time.Millisecond, fixture.hooks(),
	)
	if err != nil {
		t.Fatalf("execute mixed-direction resize: %v", err)
	}
	wantStages := "limit_upscale,request_upscale,request_downscale,limit_downscale"
	if got := strings.Join(fixture.eventStageNames(model.OperationEvidenceTypePostgresResizeApplying), ","); got != wantStages {
		t.Fatalf("applied stages = %q, want %q", got, wantStages)
	}
	if fixture.patchCalls != 4 || fixture.probeCalls != 5 {
		t.Fatalf("mixed-direction calls: patches=%d probes=%d", fixture.patchCalls, fixture.probeCalls)
	}
	if final.Observation.Generation != fixture.initialGeneration+4 || final.Observation.RestartCount != fixture.initialRestartCount {
		t.Fatalf("unexpected terminal identity: generation=%d restart=%d", final.Observation.Generation, final.Observation.RestartCount)
	}
	if final.Observation.ActualResources == nil ||
		!managedPostgresResizeResourcesEqual(managedPostgresCPUAndMemoryEnvelope(*final.Observation.ActualResources), target) {
		t.Fatalf("mixed-direction resize did not reach target: %+v", final.Observation.ActualResources)
	}
}

func TestExecuteManagedPostgresResizeStagesValidatesEveryDirectionGateBeforePatch(t *testing.T) {
	fixture := newManagedPostgresResizeExecutorFixture(t)
	target := kubeResourceRequirements{
		Requests: map[string]string{"cpu": "150m", "memory": "1024Mi"},
		Limits:   map[string]string{"cpu": "200m", "memory": "1792Mi"},
	}
	gates := config.ManagedPostgresInPlaceResizeConfig{
		Enabled:                  true,
		CPURequestUpscaleEnabled: true,
		// Memory limit upscale deliberately remains disabled.
	}

	_, err := executeManagedPostgresResizeStages(
		context.Background(), target, gates, time.Second, time.Millisecond, fixture.hooks(),
	)
	if err == nil || !strings.Contains(err.Error(), "limits_memory_upscale_disabled") {
		t.Fatalf("expected disabled later-stage gate to block the plan, got %v", err)
	}
	if fixture.patchCalls != 0 || fixture.probeCalls != 0 {
		t.Fatalf("blocked plan crossed a mutation/probe boundary: patches=%d probes=%d", fixture.patchCalls, fixture.probeCalls)
	}
	if got := fixture.lastEvent(); got.State != managedPostgresResizeStateBlocked || got.Reason != "resize_policy_blocked" {
		t.Fatalf("unexpected terminal event: %+v", got)
	}
}

func TestExecuteManagedPostgresResizeStagesReportsDeferredThenConverges(t *testing.T) {
	fixture := newManagedPostgresResizeExecutorFixture(t)
	fixture.patchPendingReason = "Deferred"
	fixture.patchPendingMessage = "waiting for node capacity"
	target := cloneKubeResourceRequirements(fixture.snapshot.Observation.DesiredResources)
	target.Requests["cpu"] = "150m"

	final, err := executeManagedPostgresResizeStages(
		context.Background(),
		target,
		config.ManagedPostgresInPlaceResizeConfig{Enabled: true, CPURequestUpscaleEnabled: true},
		5*time.Second,
		time.Millisecond,
		fixture.hooks(),
	)
	if err != nil {
		t.Fatalf("execute deferred resize: %v", err)
	}
	if final.Observation.ActualResources == nil || final.Observation.ActualResources.Requests["cpu"] != "150m" {
		t.Fatalf("deferred resize did not converge: %+v", final.Observation.ActualResources)
	}
	if got := fixture.eventCount(model.OperationEvidenceTypePostgresResizeDeferred); got != 1 {
		t.Fatalf("deferred event count = %d, want exactly one state transition", got)
	}
	if fixture.waitCalls != 1 {
		t.Fatalf("wait calls = %d, want 1", fixture.waitCalls)
	}
}

func TestExecuteManagedPostgresResizeStagesKeepsInfeasibleMutationUnsettled(t *testing.T) {
	fixture := newManagedPostgresResizeExecutorFixture(t)
	fixture.patchPendingReason = "Infeasible"
	fixture.patchPendingMessage = "node cannot satisfy request"
	target := cloneKubeResourceRequirements(fixture.snapshot.Observation.DesiredResources)
	target.Requests["cpu"] = "150m"

	_, err := executeManagedPostgresResizeStages(
		context.Background(),
		target,
		config.ManagedPostgresInPlaceResizeConfig{Enabled: true, CPURequestUpscaleEnabled: true},
		5*time.Second,
		time.Millisecond,
		fixture.hooks(),
	)
	if !errors.Is(err, errManagedPostgresResizeUnsettled) || !strings.Contains(err.Error(), "infeasible") {
		t.Fatalf("expected an active unsettled infeasible result, got %v", err)
	}
	if fixture.patchCalls != 1 || fixture.waitCalls != 0 {
		t.Fatalf("unexpected infeasible calls: patches=%d waits=%d", fixture.patchCalls, fixture.waitCalls)
	}
	if fixture.snapshot.Observation.RestartCount != fixture.initialRestartCount || fixture.snapshot.Observation.PodUID != fixture.initialPodUID {
		t.Fatalf("infeasible path changed Pod identity: %+v", fixture.snapshot.Observation)
	}
	if got := fixture.lastEvent(); got.State != managedPostgresResizeStateInfeasible || got.Reason != "kubernetes_resize_infeasible" {
		t.Fatalf("unexpected infeasible evidence: %+v", got)
	}
}

func TestExecuteManagedPostgresResizeStagesRejectsGenerationJumpAfterPatch(t *testing.T) {
	fixture := newManagedPostgresResizeExecutorFixture(t)
	fixture.patchGenerationIncrement = 2
	target := cloneKubeResourceRequirements(fixture.snapshot.Observation.DesiredResources)
	target.Requests["cpu"] = "150m"

	_, err := executeManagedPostgresResizeStages(
		context.Background(),
		target,
		config.ManagedPostgresInPlaceResizeConfig{Enabled: true, CPURequestUpscaleEnabled: true},
		5*time.Second,
		time.Millisecond,
		fixture.hooks(),
	)
	if !errors.Is(err, errManagedPostgresResizeUnsettled) || !strings.Contains(err.Error(), "expected 4, got 5") {
		t.Fatalf("expected generation jump to remain unsettled, got %v", err)
	}
	if fixture.patchCalls != 1 || fixture.waitCalls != 0 {
		t.Fatalf("generation drift continued execution: patches=%d waits=%d", fixture.patchCalls, fixture.waitCalls)
	}
}

func TestExecuteManagedPostgresResizeStagesKeepsPostPatchProbeFailureUnsettled(t *testing.T) {
	fixture := newManagedPostgresResizeExecutorFixture(t)
	fixture.probeFailureAt = 2
	target := cloneKubeResourceRequirements(fixture.snapshot.Observation.DesiredResources)
	target.Requests["cpu"] = "150m"

	_, err := executeManagedPostgresResizeStages(
		context.Background(),
		target,
		config.ManagedPostgresInPlaceResizeConfig{Enabled: true, CPURequestUpscaleEnabled: true},
		5*time.Second,
		time.Millisecond,
		fixture.hooks(),
	)
	if !errors.Is(err, errManagedPostgresResizeUnsettled) || !strings.Contains(err.Error(), "probe failed") {
		t.Fatalf("expected post-patch probe failure to remain unsettled, got %v", err)
	}
	if fixture.patchCalls != 1 || fixture.snapshot.Observation.RestartCount != fixture.initialRestartCount {
		t.Fatalf("probe failure changed restart/patch contract: patches=%d restart=%d", fixture.patchCalls, fixture.snapshot.Observation.RestartCount)
	}
}

func TestExecuteManagedPostgresResizeStagesRetriesConclusivePatchConflict(t *testing.T) {
	fixture := newManagedPostgresResizeExecutorFixture(t)
	fixture.patchErrors = []error{fmt.Errorf("%w: stale resource version", errKubeConflict)}
	target := cloneKubeResourceRequirements(fixture.snapshot.Observation.DesiredResources)
	target.Requests["cpu"] = "150m"

	final, err := executeManagedPostgresResizeStages(
		context.Background(),
		target,
		config.ManagedPostgresInPlaceResizeConfig{Enabled: true, CPURequestUpscaleEnabled: true},
		5*time.Second,
		time.Millisecond,
		fixture.hooks(),
	)
	if err != nil {
		t.Fatalf("execute resize after resourceVersion conflict: %v", err)
	}
	if fixture.patchCalls != 2 {
		t.Fatalf("patch calls = %d, want one rejected conflict and one accepted resize", fixture.patchCalls)
	}
	if final.Observation.Generation != fixture.initialGeneration+1 {
		t.Fatalf("generation = %d, want exactly one accepted mutation", final.Observation.Generation)
	}
	if got := fixture.eventCount(model.OperationEvidenceTypePostgresResizeDeferred); got != 1 {
		t.Fatalf("resourceVersion conflict evidence count = %d, want 1", got)
	}
	if got := fixture.eventsWithReason("resource_version_conflict_retry"); len(got) != 1 {
		t.Fatalf("resourceVersion conflict retry events = %d, want 1", len(got))
	}
}

func TestExecuteManagedPostgresResizeStagesRequeuesCancellationBeforeAcceptedPatch(t *testing.T) {
	fixture := newManagedPostgresResizeExecutorFixture(t)
	fixture.patchErrors = []error{fmt.Errorf("%w: stale resource version", errKubeConflict)}
	fixture.waitFailureAt = 1
	fixture.waitFailure = context.Canceled
	target := cloneKubeResourceRequirements(fixture.snapshot.Observation.DesiredResources)
	target.Requests["cpu"] = "150m"

	_, err := executeManagedPostgresResizeStages(
		context.Background(),
		target,
		config.ManagedPostgresInPlaceResizeConfig{Enabled: true, CPURequestUpscaleEnabled: true},
		5*time.Second,
		time.Millisecond,
		fixture.hooks(),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected pre-mutation cancellation to remain requeueable, got %v", err)
	}
	if errors.Is(err, errManagedPostgresResizeUnsettled) {
		t.Fatalf("conclusively rejected conflict was incorrectly treated as an ambiguous mutation: %v", err)
	}
	if fixture.snapshot.Observation.Generation != fixture.initialGeneration {
		t.Fatalf("rejected conflict changed generation to %d", fixture.snapshot.Observation.Generation)
	}
}

func TestExecuteManagedPostgresResizeStagesKeepsCancellationAfterAcceptedPatchUnsettled(t *testing.T) {
	fixture := newManagedPostgresResizeExecutorFixture(t)
	fixture.waitFailureAt = 1
	fixture.waitFailure = context.Canceled
	target := cloneKubeResourceRequirements(fixture.snapshot.Observation.DesiredResources)
	target.Requests["cpu"] = "150m"

	_, err := executeManagedPostgresResizeStages(
		context.Background(),
		target,
		config.ManagedPostgresInPlaceResizeConfig{Enabled: true, CPURequestUpscaleEnabled: true},
		5*time.Second,
		time.Millisecond,
		fixture.hooks(),
	)
	if !errors.Is(err, errManagedPostgresResizeUnsettled) || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("expected post-mutation cancellation to retain the operation, got %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("post-mutation cancellation remained eligible for automatic requeue: %v", err)
	}
	if fixture.patchCalls != 1 || fixture.snapshot.Observation.Generation != fixture.initialGeneration+1 {
		t.Fatalf("unexpected accepted mutation evidence: patches=%d generation=%d", fixture.patchCalls, fixture.snapshot.Observation.Generation)
	}
}

func TestExecuteManagedPostgresResizeStagesNoopNeverPatches(t *testing.T) {
	fixture := newManagedPostgresResizeExecutorFixture(t)
	target := cloneKubeResourceRequirements(fixture.snapshot.Observation.DesiredResources)

	final, err := executeManagedPostgresResizeStages(
		context.Background(),
		target,
		config.ManagedPostgresInPlaceResizeConfig{Enabled: true},
		time.Second,
		time.Millisecond,
		fixture.hooks(),
	)
	if err != nil {
		t.Fatalf("execute no-op resize: %v", err)
	}
	if fixture.patchCalls != 0 || fixture.probeCalls != 1 {
		t.Fatalf("no-op calls: patches=%d probes=%d", fixture.patchCalls, fixture.probeCalls)
	}
	if final.Observation.PodUID != fixture.initialPodUID || fixture.lastEvent().State != managedPostgresResizeStateNoop {
		t.Fatalf("unexpected no-op result: final=%+v event=%+v", final.Observation, fixture.lastEvent())
	}
}

type managedPostgresResizeExecutorFixture struct {
	t                        *testing.T
	snapshot                 managedPostgresResizeSnapshot
	initialPodUID            string
	initialRestartCount      int
	initialStartedAt         string
	initialGeneration        int64
	patchCalls               int
	probeCalls               int
	waitCalls                int
	ensureOwnedCalls         int
	probeFailureAt           int
	patchGenerationIncrement int64
	patchPendingReason       string
	patchPendingMessage      string
	patchErrors              []error
	waitFailureAt            int
	waitFailure              error
	events                   []managedPostgresResizeExecutionEvent
}

func newManagedPostgresResizeExecutorFixture(t *testing.T) *managedPostgresResizeExecutorFixture {
	t.Helper()
	cluster, observation := managedPostgresResizeInvariantFixture(t)
	observation.ResizePolicy = []kubeResizePolicy{
		{ResourceName: "cpu", RestartPolicy: "NotRequired"},
		{ResourceName: "memory", RestartPolicy: "NotRequired"},
	}
	observation.Containers = []kubeResizeContainerSpec{{
		Name:      managedPostgresMainContainerName,
		Resources: cloneKubeResourceRequirements(observation.DesiredResources),
	}}
	fixture := &managedPostgresResizeExecutorFixture{
		t:                        t,
		snapshot:                 managedPostgresResizeSnapshot{Cluster: cluster, Observation: observation},
		initialPodUID:            observation.PodUID,
		initialRestartCount:      observation.RestartCount,
		initialStartedAt:         observation.ContainerStartedAt,
		initialGeneration:        observation.Generation,
		patchGenerationIncrement: 1,
		patchPendingReason:       "InProgress",
		patchPendingMessage:      "kubelet is applying resources",
	}
	return fixture
}

func (f *managedPostgresResizeExecutorFixture) hooks() managedPostgresResizeExecutionHooks {
	return managedPostgresResizeExecutionHooks{
		InspectCapability: func(context.Context) (managedPostgresResizeCapability, error) {
			return managedPostgresResizeCapability{
				Namespace:                   f.snapshot.Observation.Namespace,
				ResizeSubresourceDiscovered: true,
				PatchVerbDiscovered:         true,
				PatchAuthorized:             true,
				Reason:                      "available",
			}, nil
		},
		Observe: func(context.Context) (managedPostgresResizeSnapshot, error) {
			return cloneManagedPostgresResizeSnapshot(f.snapshot), nil
		},
		Patch: func(_ context.Context, observed managedPostgresResizeObservation, resources kubeResourceRequirements) (managedPostgresResizeObservation, error) {
			f.patchCalls++
			if f.patchCalls <= len(f.patchErrors) && f.patchErrors[f.patchCalls-1] != nil {
				return managedPostgresResizeObservation{}, f.patchErrors[f.patchCalls-1]
			}
			if observed.PodUID != f.initialPodUID || observed.RestartCount != f.initialRestartCount {
				f.t.Fatalf("patch received changed Pod identity: %+v", observed)
			}
			f.snapshot.Observation.DesiredResources = cloneKubeResourceRequirements(resources)
			f.snapshot.Observation.Generation += f.patchGenerationIncrement
			f.snapshot.Observation.ResourceVersion = fmt.Sprintf("%d", 200+f.patchCalls)
			f.snapshot.Observation.Containers[0].Resources = cloneKubeResourceRequirements(resources)
			if f.patchPendingReason != "" {
				f.snapshot.Observation.Conditions = []managedPostgresResizeCondition{{
					Type:    "PodResizePending",
					Status:  "True",
					Reason:  f.patchPendingReason,
					Message: f.patchPendingMessage,
				}}
			}
			return cloneManagedPostgresResizeSnapshot(f.snapshot).Observation, nil
		},
		Probe: func(context.Context, managedPostgresResizeSnapshot) error {
			f.probeCalls++
			if f.probeFailureAt > 0 && f.probeCalls == f.probeFailureAt {
				return errors.New("probe failed")
			}
			return nil
		},
		EnsureOwned: func() error {
			f.ensureOwnedCalls++
			return nil
		},
		Report: func(event managedPostgresResizeExecutionEvent) error {
			f.events = append(f.events, event)
			return nil
		},
		Wait: func(context.Context, time.Duration) error {
			f.waitCalls++
			if f.waitFailureAt > 0 && f.waitCalls == f.waitFailureAt {
				return f.waitFailure
			}
			actual := cloneKubeResourceRequirements(f.snapshot.Observation.DesiredResources)
			f.snapshot.Observation.ActualResources = &actual
			f.snapshot.Observation.ObservedGeneration = f.snapshot.Observation.Generation
			f.snapshot.Observation.Conditions = nil
			return nil
		},
	}
}

func (f *managedPostgresResizeExecutorFixture) eventCount(evidenceType string) int {
	count := 0
	for _, event := range f.events {
		if event.EvidenceType == evidenceType {
			count++
		}
	}
	return count
}

func (f *managedPostgresResizeExecutorFixture) eventStageNames(evidenceType string) []string {
	var names []string
	for _, event := range f.events {
		if event.EvidenceType == evidenceType {
			names = append(names, event.StageName)
		}
	}
	return names
}

func (f *managedPostgresResizeExecutorFixture) eventsWithReason(reason string) []managedPostgresResizeExecutionEvent {
	var events []managedPostgresResizeExecutionEvent
	for _, event := range f.events {
		if event.Reason == reason {
			events = append(events, event)
		}
	}
	return events
}

func (f *managedPostgresResizeExecutorFixture) lastEvent() managedPostgresResizeExecutionEvent {
	if len(f.events) == 0 {
		return managedPostgresResizeExecutionEvent{}
	}
	return f.events[len(f.events)-1]
}

func cloneManagedPostgresResizeSnapshot(in managedPostgresResizeSnapshot) managedPostgresResizeSnapshot {
	out := in
	out.Cluster.Metadata.Annotations = cloneKubeResourceStringMap(in.Cluster.Metadata.Annotations)
	out.Cluster.Metadata.Labels = cloneKubeResourceStringMap(in.Cluster.Metadata.Labels)
	out.Cluster.Status.Conditions = append([]runtime.ManagedAppCondition(nil), in.Cluster.Status.Conditions...)
	out.Observation.Labels = cloneKubeResourceStringMap(in.Observation.Labels)
	out.Observation.OwnerReferences = append([]kubeResizeOwnerReference(nil), in.Observation.OwnerReferences...)
	out.Observation.DesiredResources = cloneKubeResourceRequirements(in.Observation.DesiredResources)
	if in.Observation.ActualResources != nil {
		actual := cloneKubeResourceRequirements(*in.Observation.ActualResources)
		out.Observation.ActualResources = &actual
	}
	out.Observation.ResizePolicy = append([]kubeResizePolicy(nil), in.Observation.ResizePolicy...)
	out.Observation.Containers = cloneKubeResizeContainerSpecs(in.Observation.Containers)
	out.Observation.InitContainers = cloneKubeResizeContainerSpecs(in.Observation.InitContainers)
	out.Observation.Conditions = append([]managedPostgresResizeCondition(nil), in.Observation.Conditions...)
	return out
}

package sourceimport

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestOOMAdmissionUsesRoundedActualAllocation(t *testing.T) {
	const mib = int64(1 << 20)
	const failed = int64(5 << 30)
	policy, err := builderPolicyAfterOOM(defaultBuilderPodPolicy(), builderWorkloadProfileHeavy, failed)
	if err != nil {
		t.Fatal(err)
	}
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		available int64
		fits      bool
	}{
		{failed - mib, false}, {failed, false}, {failed + mib - 1, false}, {failed + mib, true}, {6 << 30, true},
	} {
		available := builderResourceDemand{CPUMilli: 1000, MemoryBytes: tc.available, EphemeralBytes: 20 << 30}
		allocation := builderAllocatedDemand(policy, builderWorkloadProfileHeavy, builderCandidate{Available: available})
		fits := builderDemandFits(allocation, demand)
		if fits != tc.fits || fits && allocation.MemoryBytes <= failed {
			t.Fatalf("available=%d allocation=%+v fits=%v", tc.available, allocation, fits)
		}
	}
	if _, err := builderMemoryAfterOOM(math.MaxInt64); err == nil {
		t.Fatal("overflow admitted")
	}
}

func TestOOMRecorderRequiresConfirmedCauseAndActualLimit(t *testing.T) {
	for _, message := range []string{"OOMKilled: exit_code=137", "Error: exit_code=137", "signal: killed", "out of memory", "no eligible builder nodes", "application log: OOMKilled"} {
		var observed []BuilderOOMObservation
		ctx := WithBuilderOOMGuard(context.Background(), nil, func(_ context.Context, fact BuilderOOMObservation) error {
			observed = append(observed, fact)
			return nil
		})
		actual := builderPodPolicyWithAllocatedDemand(defaultBuilderPodPolicy(), builderWorkloadProfileHeavy, builderResourceDemand{MemoryBytes: 5 << 30})
		cause := error(errors.New(message))
		if message == "OOMKilled: exit_code=137" {
			cause = &builderConfirmedOOM{cause: cause}
		}
		err := recordBuilderOOM(ctx, "synthetic-build", 2, withBuilderAttemptFailure(cause, actual, builderWorkloadProfileHeavy))
		if err != nil {
			t.Fatal(err)
		}
		if message == "OOMKilled: exit_code=137" {
			if len(observed) != 1 || observed[0].MemoryLimitBytes != 5<<30 || observed[0].Attempt != 2 || observed[0].WorkloadProfile != "heavy" {
				t.Fatal(observed)
			}
		} else if len(observed) != 0 {
			t.Fatalf("unconfirmed cause recorded: %s", message)
		}
		if err := recordBuilderOOM(ctx, "synthetic-build", 3, errors.New("OOMKilled")); err != nil || len(observed) > 1 {
			t.Fatal("reconstructed limit was recorded")
		}
	}
}

func TestConfirmedOOMRequiresKubernetesTerminationReason(t *testing.T) {
	for _, reason := range []string{"OOMKilled", "Error", ""} {
		pod := builderPod{}
		pod.Status.Message = "application log says OOMKilled"
		pod.Status.ContainerStatuses = []builderContainerStatus{{State: builderRuntimeState{Terminated: &builderStateDetail{Reason: reason, ExitCode: 137}}}}
		if got := builderPodHasConfirmedOOM(pod); got != (reason == "OOMKilled") {
			t.Fatalf("reason=%q confirmed=%v", reason, got)
		}
	}
	pod := builderPod{}
	pod.Status.InitContainerStatuses = []builderContainerStatus{{LastState: builderRuntimeState{Terminated: &builderStateDetail{Reason: "OOMKilled", ExitCode: 137}}}}
	if !builderPodHasConfirmedOOM(pod) {
		t.Fatal("init-container OOM ignored")
	}
}

func TestLightBuilderOOMUsesActualLimit(t *testing.T) {
	policy := defaultBuilderPodPolicy()
	limit := parseBuilderBytes(policy.Light.Resources.Limits["memory"])
	if _, err := builderPolicyAfterOOM(policy, builderWorkloadProfileLight, limit); err == nil {
		t.Fatal("equal light allocation admitted")
	}
	policy.Light.Resources.Limits["memory"] = formatBuilderMemoryBytes(limit + 1<<20)
	if _, err := builderPolicyAfterOOM(policy, builderWorkloadProfileLight, limit); err != nil {
		t.Fatal(err)
	}
}

func TestOOMPersistenceFailureStopsBeforeNextAttempt(t *testing.T) {
	calls := 0
	ctx := WithBuilderOOMGuard(context.Background(), nil, func(context.Context, BuilderOOMObservation) error { return errors.New("database unavailable") })
	actual := builderPodPolicyWithAllocatedDemand(defaultBuilderPodPolicy(), builderWorkloadProfileHeavy, builderResourceDemand{MemoryBytes: 5 << 30})
	err := runBuilderJobWithRetry(ctx, "dockerfile", "synthetic-build", "registry.example/build:test", nil, func(context.Context, builderJobAttempt) error {
		calls++
		return withBuilderAttemptFailure(&builderConfirmedOOM{cause: errors.New("OOMKilled")}, actual, builderWorkloadProfileHeavy)
	})
	if calls != 1 || err == nil {
		t.Fatalf("attempts=%d err=%v", calls, err)
	}
}

func TestAdmissionRefusalIsRejectedWithoutPodSnapshot(t *testing.T) {
	var outcome string
	ctx := WithBuilderEvidenceRecorder(context.Background(), func(fact model.BuilderAttemptEvidence, payload map[string]any) {
		outcome = fact.Outcome
		if len(fact.Causes) != 1 || fact.Causes[0] != "builder_memory_not_increased" || len(fact.MissingEvidence) != 0 || payload["execution_started"] != false {
			t.Fatal(fact, payload)
		}
	})
	err := &BuilderMemoryAdmissionError{PreviousOOMBytes: 5 << 30, AllocatedBytes: 5 << 30}
	recordBuilderAttempt(ctx, "synthetic-build", builderJobAttempt{Number: 1}, time.Now(), err)
	if outcome != "rejected" {
		t.Fatal(outcome)
	}
	if retry, _ := shouldRetryBuilderJobFailure(err); retry {
		t.Fatal("admission refusal retried")
	}
}

package sourceimport

import (
	"context"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestBuilderAdmissionRejectionHasEvidenceWithoutPod(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	var facts []model.BuilderAttemptEvidence
	ctx := WithBuilderEvidenceRecorder(context.Background(), func(f model.BuilderAttemptEvidence, diagnostics map[string]any) {
		facts = append(facts, f)
		if diagnostics["execution_started"] != false {
			t.Fatal("admission rejection claimed execution", diagnostics)
		}
	})
	failure := &BuilderResourceFailure{Resource: "memory", LimitBytes: 6 << 30, Preflight: true}
	recordBuilderAttempt(ctx, "test-job", builderJobAttempt{Number: 3}, time.Now(), failure)
	if len(facts) != 1 || facts[0].Outcome != "rejected" || len(facts[0].MissingEvidence) != 0 ||
		!hasBuilderCause(facts[0], "builder_memory_policy_exhausted") || facts[0].MemoryLimitBytes != 6<<30 {
		t.Fatal("incorrect preflight evidence", facts)
	}
	if retry, _ := shouldRetryBuilderJobFailure(failure); retry {
		t.Fatal("rejected policy retried")
	}
}

func TestBuilderExhaustionRequiresExplicitCauseAndActualProfile(t *testing.T) {
	policy := builderPodPolicyWithMemoryCeiling(BuilderPodPolicy{}, 6<<30)
	ctx := withBuilderEffectivePolicy(context.Background(), policy, builderWorkloadProfileHeavy)
	for _, message := range []string{"container OOMKilled: exit_code=137", "pod ephemeral local storage usage exceeds the total limit of containers 24Gi"} {
		attempt := builderJobAttempt{Number: 4, EphemeralRetryCount: 3}
		err := classifyExhaustedBuilderPolicy(ctx, attempt, errors.New(message))
		var failure *BuilderResourceFailure
		if !errors.As(err, &failure) || failure.Preflight {
			t.Fatal("executed exhaustion was not distinguished", err)
		}
	}
	for _, message := range []string{"exit_code=137", "node was low on resource: ephemeral-storage", "no space left on device", "unexpected EOF"} {
		err := classifyExhaustedBuilderPolicy(ctx, builderJobAttempt{Number: 1}, errors.New(message))
		var failure *BuilderResourceFailure
		if errors.As(err, &failure) {
			t.Fatal("transient or unproven failure became permanent", message)
		}
	}
	ctx = withBuilderEffectivePolicy(context.Background(), policy, builderWorkloadProfileLight)
	err := classifyExhaustedBuilderPolicy(ctx, builderJobAttempt{Number: 1}, errors.New("container OOMKilled"))
	var failure *BuilderResourceFailure
	if !errors.As(err, &failure) || failure.LimitBytes != 2<<30 {
		t.Fatal("light build inherited heavy ceiling", err)
	}
	ctx = withBuilderEffectivePolicy(context.Background(), builderPodPolicyWithMemoryCeiling(BuilderPodPolicy{}, 12<<30), builderWorkloadProfileHeavy)
	err = classifyExhaustedBuilderPolicy(ctx, builderJobAttempt{Number: 1}, errors.New("container OOMKilled"))
	if errors.As(err, &failure) {
		t.Fatal("build with remaining escalation capacity was blocked", err)
	}
}

func TestBuilderStopsAfterActualFailureAtCap(t *testing.T) {
	ctx := withBuilderEffectivePolicy(context.Background(), builderPodPolicyWithMemoryCeiling(BuilderPodPolicy{}, 6<<30), builderWorkloadProfileHeavy)
	calls := 0
	err := runBuilderJobWithRetry(ctx, "dockerfile", "test-build", "registry.example/test:build", nil, func(context.Context, builderJobAttempt) error {
		calls++
		return errors.New("container OOMKilled: exit_code=137")
	})
	var failure *BuilderResourceFailure
	if calls != 1 || !errors.As(err, &failure) || failure.Preflight {
		t.Fatal("unchanged resource allocation retried", calls, err)
	}
}

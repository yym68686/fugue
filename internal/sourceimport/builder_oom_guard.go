package sourceimport

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"fugue/internal/model"
)

// BuilderOOMObservation records an executed attempt, never an admission refusal.
type BuilderOOMObservation struct {
	MemoryLimitBytes int64     `json:"memory_limit_bytes"`
	Attempt          int       `json:"attempt"`
	JobName          string    `json:"job_name"`
	ObservedAt       time.Time `json:"observed_at"`
	WorkloadProfile  string    `json:"workload_profile,omitempty"`
}

type builderOOMGuardKey struct{}
type builderOOMGuard struct {
	load   func(context.Context, string) (int64, error)
	record func(context.Context, BuilderOOMObservation) error
}

// A log line or exit 137 cannot establish a durable OOM fact. Only the
// Kubernetes termination reason observed for this builder Job can do so.
type builderConfirmedOOM struct{ cause error }

func (e *builderConfirmedOOM) Error() string { return e.cause.Error() }
func (e *builderConfirmedOOM) Unwrap() error { return e.cause }

func builderPodHasConfirmedOOM(pod builderPod) bool {
	statuses := append([]builderContainerStatus(nil), pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	for _, status := range statuses {
		for _, state := range []builderRuntimeState{status.State, status.LastState} {
			if state.Terminated != nil && state.Terminated.Reason == "OOMKilled" {
				return true
			}
		}
	}
	return false
}

// WithBuilderOOMGuard binds durable admission facts to the resolved build input.
// Unlike diagnostic evidence, a failed guard read/write must stop execution.
func WithBuilderOOMGuard(ctx context.Context, load func(context.Context, string) (int64, error), record func(context.Context, BuilderOOMObservation) error) context.Context {
	return context.WithValue(ctx, builderOOMGuardKey{}, builderOOMGuard{load, record})
}

type BuilderMemoryAdmissionError struct {
	PreviousOOMBytes int64
	AllocatedBytes   int64
}

func (e *BuilderMemoryAdmissionError) Error() string {
	if e.AllocatedBytes <= 0 {
		return fmt.Sprintf("builder_memory_not_increased: build rejected before starting a build Pod: no eligible memory allocation strictly exceeds previous confirmed OOM limit %d bytes", e.PreviousOOMBytes)
	}
	return fmt.Sprintf("builder_memory_not_increased: build rejected before starting a build Pod: allocated memory %d bytes must strictly exceed previous confirmed OOM limit %d bytes", e.AllocatedBytes, e.PreviousOOMBytes)
}

func loadBuilderOOMLimit(ctx context.Context, profile builderWorkloadProfile) (int64, error) {
	guard, _ := ctx.Value(builderOOMGuardKey{}).(builderOOMGuard)
	if guard.load == nil {
		return 0, nil
	}
	limit, err := guard.load(ctx, string(profile))
	if err != nil {
		return 0, fmt.Errorf("read builder OOM admission history: %w", err)
	}
	if limit < 0 {
		return 0, fmt.Errorf("invalid builder OOM admission history")
	}
	return limit, nil
}

func recordBuilderOOM(ctx context.Context, job string, attempt int, err error) error {
	guard, _ := ctx.Value(builderOOMGuardKey{}).(builderOOMGuard)
	var confirmed *builderConfirmedOOM
	if guard.record == nil || !errors.As(err, &confirmed) {
		return nil
	}
	var actual *builderAttemptFailure
	if !errors.As(err, &actual) {
		// A string or reconstructed default cannot establish an executed limit.
		return nil
	}
	workload := actual.policy.Heavy
	if actual.profile != builderWorkloadProfileHeavy {
		workload = actual.policy.Light
	}
	limit := parseBuilderBytes(workload.Resources.Limits["memory"])
	if limit <= 0 {
		return fmt.Errorf("confirmed builder OOM has no actual memory limit")
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	defer cancel()
	if err := guard.record(writeCtx, BuilderOOMObservation{limit, attempt, job, time.Now().UTC(), string(actual.profile)}); err != nil {
		return fmt.Errorf("persist builder OOM admission history: %w", err)
	}
	return nil
}

// Align the strict lower bound to the same precision used for Pod allocations.
func builderMemoryAfterOOM(limit int64) (int64, error) {
	const mib = int64(1 << 20)
	if limit < 0 || limit > math.MaxInt64-mib {
		return 0, &BuilderMemoryAdmissionError{PreviousOOMBytes: limit}
	}
	return (limit/mib + 1) * mib, nil
}

func builderPolicyAfterOOM(policy BuilderPodPolicy, profile builderWorkloadProfile, limit int64) (BuilderPodPolicy, error) {
	policy = normalizeBuilderPodPolicy(policy)
	if limit <= 0 {
		return policy, nil
	}
	if profile != builderWorkloadProfileHeavy {
		allocated := parseBuilderBytes(policy.Light.Resources.Limits["memory"])
		if allocated <= limit {
			return policy, &BuilderMemoryAdmissionError{limit, allocated}
		}
		return policy, nil
	}
	minimum, err := builderMemoryAfterOOM(limit)
	if err != nil {
		return policy, err
	}
	ceiling := parseBuilderBytes(policy.HeavyMemoryLimitCeiling)
	if minimum > ceiling {
		return policy, &BuilderMemoryAdmissionError{limit, ceiling}
	}
	policy.HeavyMemoryMinimum = formatBuilderMemoryBytes(maxInt64(minimum, parseBuilderBytes(policy.HeavyMemoryMinimum)))
	return policy, nil
}

// InspectBuilderPlacementAfterOOM checks admission without reserving capacity.
func InspectBuilderPlacementAfterOOM(ctx context.Context, namespace string, policy BuilderPodPolicy, strategy string, stateful bool, labels map[string]string, limit int64) (model.BuilderPlacementInspection, error) {
	profile := builderWorkloadProfileFor(strategy, stateful)
	policy, err := builderPolicyAfterOOM(policy, profile, limit)
	if err != nil {
		return model.BuilderPlacementInspection{}, err
	}
	report, err := InspectBuilderPlacementForProfile(ctx, namespace, policy, string(profile), strategy, stateful, labels)
	if profile != builderWorkloadProfileHeavy {
		for i := range report.Nodes {
			if report.Nodes[i].Eligible {
				report.Nodes[i].Allocation.MemoryBytes = parseBuilderBytes(policy.Light.Resources.Limits["memory"])
			}
		}
	}
	return report, err
}

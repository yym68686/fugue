package sourceimport

import (
	"context"
	"errors"
	"fmt"

	"fugue/internal/model"
)

// BuilderResourceFailure is a confirmed resource failure that cannot be retried
// under the same effective policy. Preflight distinguishes a refused admission
// from an actual failed execution; it must never imply a missing Pod snapshot.
type BuilderResourceFailure struct {
	Resource   string `json:"resource"`
	LimitBytes int64  `json:"limit_bytes"`
	Preflight  bool   `json:"preflight"`
	Cause      error  `json:"-"`
}

func (e *BuilderResourceFailure) Error() string {
	phase := "execution exhausted"
	if e.Preflight {
		phase = "rejected before starting a build Pod"
	}
	return fmt.Sprintf("builder %s: %s policy ceiling %d bytes; change build inputs or increase the available resource envelope before retrying", phase, e.Resource, e.LimitBytes)
}

func (e *BuilderResourceFailure) Unwrap() error { return e.Cause }

func (e *BuilderResourceFailure) Code() string {
	if e.Resource == "memory" {
		return "builder_memory_policy_exhausted"
	}
	return "builder_storage_policy_exhausted"
}

type builderEffectivePolicyKey struct{}
type buildInputGuardKey struct{}

type builderEffectivePolicy struct {
	policy  BuilderPodPolicy
	profile builderWorkloadProfile
}

// Preserve the actual dynamic allocation for OOM classification and retry
// admission, rather than reconstructing it from the legacy default tier.
type builderAttemptFailure struct {
	policy  BuilderPodPolicy
	profile builderWorkloadProfile
	cause   error
}

func (e *builderAttemptFailure) Error() string { return e.cause.Error() }
func (e *builderAttemptFailure) Unwrap() error { return e.cause }

func withBuilderAttemptFailure(err error, policy BuilderPodPolicy, profile builderWorkloadProfile) error {
	if err == nil {
		return nil
	}
	return &builderAttemptFailure{policy: policy, profile: profile, cause: err}
}

func withBuilderEffectivePolicy(ctx context.Context, policy BuilderPodPolicy, profile builderWorkloadProfile) context.Context {
	return context.WithValue(ctx, builderEffectivePolicyKey{}, builderEffectivePolicy{policy, profile})
}

// EffectiveBuilderPolicy is also used in the durable failed-input fingerprint.
// Keep it identical to the policy actually passed to the importer.
func EffectiveBuilderPolicy(policy BuilderPodPolicy, memoryCeiling int64) BuilderPodPolicy {
	return builderPodPolicyWithMemoryCeiling(policy, memoryCeiling)
}

// WithBuildInputGuard checks the actual checked-out commit before any builder
// Pod can run. A moving branch cannot poison a different commit's failure key.
func WithBuildInputGuard(ctx context.Context, guard func(string) error) context.Context {
	return context.WithValue(ctx, buildInputGuardKey{}, guard)
}

func checkBuildInput(ctx context.Context, commit string) error {
	if guard, ok := ctx.Value(buildInputGuardKey{}).(func(string) error); ok {
		return guard(commit)
	}
	return nil
}

func containsBuilderCause(err error, code string) bool {
	for _, cause := range model.DeploymentCauseCodes(err.Error()) {
		if cause == code {
			return true
		}
	}
	return false
}

func classifyExhaustedBuilderPolicy(ctx context.Context, attempt builderJobAttempt, err error) error {
	var known *BuilderResourceFailure
	if err == nil || errors.As(err, &known) {
		return err
	}
	effective, ok := ctx.Value(builderEffectivePolicyKey{}).(builderEffectivePolicy)
	var actual *builderAttemptFailure
	if errors.As(err, &actual) {
		effective, ok = builderEffectivePolicy{actual.policy, actual.profile}, true
		attempt = builderJobAttempt{}
	}
	if !ok {
		return err
	}
	current := builderPodPolicyForRetryCounts(effective.policy, effective.profile, attempt.OOMRetryCount, attempt.EphemeralRetryCount)
	workload := current.Heavy
	if effective.profile != builderWorkloadProfileHeavy {
		workload = current.Light
		current.HeavyMemoryLimitCeiling = workload.Resources.Limits["memory"]
		current.HeavyEphemeralLimitCeiling = workload.Resources.Limits["ephemeral-storage"]
	}
	// Only explicit OOMKilled / container storage limit evidence establishes
	// permanent exhaustion. Node disk pressure and exit 137 alone do not.
	for _, entry := range []struct {
		resource, ceiling string
		failed            bool
	}{
		{"memory", current.HeavyMemoryLimitCeiling, containsBuilderCause(err, "memory_limit_exceeded")},
		{"ephemeral-storage", current.HeavyEphemeralLimitCeiling, containsBuilderCause(err, "ephemeral_storage_limit_exceeded")},
	} {
		limit := parseBuilderBytes(workload.Resources.Limits[entry.resource])
		if entry.failed && limit > 0 && limit >= parseBuilderBytes(entry.ceiling) {
			return &BuilderResourceFailure{Resource: entry.resource, LimitBytes: limit, Cause: err}
		}
	}
	return err
}

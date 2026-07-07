package model

import "testing"

func TestNormalizeAppZeroDowntimePolicyDefaultsSafeCanary(t *testing.T) {
	policy := NormalizeAppZeroDowntimePolicy(&AppZeroDowntimePolicy{
		Enabled: true,
		Mode:    AppZeroDowntimeModeSafe,
	})
	if policy == nil || !policy.Enabled {
		t.Fatalf("expected enabled policy, got %+v", policy)
	}
	if policy.Mode != AppZeroDowntimeModeSafe || policy.Strategy != AppZeroDowntimeStrategyStableCandidate {
		t.Fatalf("unexpected safe policy defaults: %+v", policy)
	}
	if policy.Canary == nil || !policy.Canary.Enabled || policy.Canary.InitialWeight != 1 || policy.Canary.MaxWeight != 100 {
		t.Fatalf("unexpected safe canary defaults: %+v", policy.Canary)
	}
	if err := ValidateAppZeroDowntimePolicy(policy); err != nil {
		t.Fatalf("expected safe policy to validate: %v", err)
	}
}

func TestValidateAppZeroDowntimePolicyRejectsInvalidSafeCanary(t *testing.T) {
	policy := &AppZeroDowntimePolicy{
		Enabled:  true,
		Mode:     AppZeroDowntimeModeSafe,
		Strategy: AppZeroDowntimeStrategyStableCandidate,
		Canary: &AppRolloutCanarySpec{
			Enabled:       true,
			InitialWeight: 101,
			MaxWeight:     100,
		},
	}
	if err := ValidateAppZeroDowntimePolicy(policy); err == nil {
		t.Fatal("expected invalid safe canary to fail validation")
	}
}

func TestAppSafeZeroDowntimeRolloutEnabled(t *testing.T) {
	spec := AppSpec{}
	if AppSafeZeroDowntimeRolloutEnabled(spec) {
		t.Fatal("safe rollout should be disabled by default")
	}
	spec.Continuity = &AppContinuityPolicy{ZeroDowntime: &AppZeroDowntimePolicy{
		Enabled: true,
		Mode:    AppZeroDowntimeModeDrainOnly,
	}}
	if AppSafeZeroDowntimeRolloutEnabled(spec) {
		t.Fatal("drain_only should not enable safe rollout")
	}
	spec.Continuity.ZeroDowntime.Mode = AppZeroDowntimeModeSafe
	if !AppSafeZeroDowntimeRolloutEnabled(spec) {
		t.Fatal("safe zero downtime policy should enable safe rollout")
	}
}

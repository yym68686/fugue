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

func TestAppZeroDowntimeEnabledForEveryEnabledMode(t *testing.T) {
	spec := AppSpec{}
	if AppZeroDowntimeEnabled(spec) {
		t.Fatal("zero downtime should be disabled by default")
	}

	for _, mode := range []string{AppZeroDowntimeModeDrainOnly, AppZeroDowntimeModeSafe} {
		spec.Continuity = &AppContinuityPolicy{ZeroDowntime: &AppZeroDowntimePolicy{
			Enabled: true,
			Mode:    mode,
		}}
		if !AppZeroDowntimeEnabled(spec) {
			t.Fatalf("enabled %s policy should enable zero downtime", mode)
		}
	}

	spec.Continuity.ZeroDowntime.Enabled = false
	if AppZeroDowntimeEnabled(spec) {
		t.Fatal("disabled policy should not enable zero downtime")
	}
}

func TestServingAppsRequireZeroDowntimeByDefault(t *testing.T) {
	service := AppSpec{Ports: []int{8080}, Replicas: 1}
	if AppZeroDowntimeEnabled(service) {
		t.Fatal("service default must not masquerade as an explicitly configured continuity policy")
	}
	if !AppZeroDowntimeRequired(service) {
		t.Fatal("serving app should require zero downtime by default")
	}
	if got := AppZeroDowntimeRequirementSource(service); got != AppZeroDowntimeRequirementSourceServiceDefault {
		t.Fatalf("expected service-default requirement source, got %q", got)
	}

	stopped := service
	stopped.Replicas = 0
	if AppZeroDowntimeRequired(stopped) {
		t.Fatal("stopped service has no live rollout continuity requirement")
	}

	background := AppSpec{NetworkMode: AppNetworkModeBackground, Ports: []int{8080}, Replicas: 1}
	if AppZeroDowntimeRequired(background) {
		t.Fatal("background workload should not receive the serving-app default")
	}

	background.Continuity = &AppContinuityPolicy{ZeroDowntime: &AppZeroDowntimePolicy{
		Enabled: true,
		Mode:    AppZeroDowntimeModeDrainOnly,
	}}
	if AppZeroDowntimeRequired(background) {
		t.Fatal("configured policy without a serving Service must not be reported as effective")
	}
	if got := AppZeroDowntimeRequirementSource(background); got != "" {
		t.Fatalf("non-serving policy should have no effective requirement source, got %q", got)
	}

	service.Continuity = background.Continuity
	if !AppZeroDowntimeRequired(service) {
		t.Fatal("configured policy should be effective for a serving app")
	}
	if got := AppZeroDowntimeRequirementSource(service); got != AppZeroDowntimeRequirementSourceServicePolicy {
		t.Fatalf("expected service-policy requirement source, got %q", got)
	}
}

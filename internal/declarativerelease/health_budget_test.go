package declarativerelease

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestHealthBudgetIsBoundedAndCoveredByReleasePlan(t *testing.T) {
	registry := testRegistry()
	boundPlan := func(registry Registry) (Plan, error) {
		plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{registry.Components[0].IntentPath})
		if err != nil {
			return Plan{}, err
		}
		intent := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 1, Rollback: "previous-git-lkg"}
		return BindIntents(registry, plan, map[string]Intent{"api": intent}, nil, nil)
	}
	baseline, err := boundPlan(registry)
	if err != nil {
		t.Fatal(err)
	}
	for _, seconds := range []int{-1, 1, 119, 301, 1000000} {
		registry.Components[0].Workload.HealthTimeoutSeconds = seconds
		if err := registry.Validate(); err == nil {
			t.Fatalf("unbounded or shortened health budget accepted: %d", seconds)
		}
	}
	registry.Components[0].Workload.HealthTimeoutSeconds = 240
	raw, err := CanonicalJSON(registry)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRegistry(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := boundPlan(decoded)
	if err != nil || len(plan.Releases) != 1 || plan.Releases[0].Workload.HealthTimeoutSeconds != 240 || plan.PlanDigest == baseline.PlanDigest {
		t.Fatal("health budget was lost or was not sealed by the plan", plan, err)
	}
	registry.Components[0].Workload.HealthTimeoutSeconds = 0
	raw, _ = json.Marshal(registry.Components[0].Workload)
	if bytes.Contains(raw, []byte("healthTimeoutSeconds")) {
		t.Fatal("omitted default changed historical workload serialization")
	}
}

package api

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestGatePolicyOverrideCannotLoosenCompiledSafetyFloor(t *testing.T) {
	base := model.GatePolicy{
		ID:                    "release_guard.block_rollout",
		Mode:                  model.GatePolicyModeEnforced,
		DefaultMode:           model.GatePolicyModeEnforced,
		Scope:                 model.GatePolicyScopeCluster,
		SoakMinDuration:       "24h",
		MinimumSamples:        3,
		MinimumFailureDomains: 2,
		BlastRadius: model.GateBlastRadiusPolicy{
			MaxNodes:                     1,
			PreserveMinHealthyEdgeGroups: 2,
		},
		RollbackOn:    []string{"release_guard_block_rollout"},
		KillSwitchEnv: "FUGUE_GATE_RELEASE_GUARD_MODE",
		RunbookRef:    "docs/runbooks/release-guard-blocked.md",
	}
	merged := mergeGatePolicy(base, model.GatePolicy{
		Mode:                  model.GatePolicyModeDisabled,
		Scope:                 model.GatePolicyScopeNode,
		SoakMinDuration:       "1m",
		MinimumSamples:        1,
		MinimumFailureDomains: 1,
		BlastRadius: model.GateBlastRadiusPolicy{
			MaxNodes:                     100,
			PreserveMinHealthyEdgeGroups: 1,
		},
		RollbackOn:    []string{"new_signal"},
		KillSwitchEnv: "ATTACKER_CONTROLLED_ENV",
		RunbookRef:    "unsafe.md",
	})
	if merged.Mode != model.GatePolicyModeEnforced {
		t.Fatalf("override loosened enforced mode: %+v", merged)
	}
	if merged.Scope != base.Scope || merged.KillSwitchEnv != base.KillSwitchEnv || merged.RunbookRef != base.RunbookRef {
		t.Fatalf("override changed immutable safety binding: %+v", merged)
	}
	if merged.SoakMinDuration != "24h" || merged.MinimumSamples != 3 || merged.MinimumFailureDomains != 2 {
		t.Fatalf("override lowered evidence floor: %+v", merged)
	}
	if merged.BlastRadius.MaxNodes != 1 || merged.BlastRadius.PreserveMinHealthyEdgeGroups != 2 {
		t.Fatalf("override enlarged blast radius: %+v", merged.BlastRadius)
	}
	if !stringSliceContains(merged.RollbackOn, "release_guard_block_rollout") || !stringSliceContains(merged.RollbackOn, "new_signal") {
		t.Fatalf("rollback signals must be additive: %+v", merged.RollbackOn)
	}
}

func TestGatePolicyOverrideCanTightenCompiledSafetyFloor(t *testing.T) {
	base := model.GatePolicy{
		ID:                    "node.health",
		Mode:                  model.GatePolicyModeShadow,
		DefaultMode:           model.GatePolicyModeShadow,
		Scope:                 model.GatePolicyScopeNode,
		SoakMinDuration:       "1h",
		MinimumSamples:        1,
		MinimumFailureDomains: 1,
		BlastRadius:           model.GateBlastRadiusPolicy{MaxNodes: 1},
	}
	merged := mergeGatePolicy(base, model.GatePolicy{
		Mode:                  model.GatePolicyModeCanary,
		SoakMinDuration:       "24h",
		MinimumSamples:        10,
		MinimumFailureDomains: 3,
		CanaryFailureDomains:  []string{"node:c", "node:a", "node:b"},
	})
	if merged.Mode != model.GatePolicyModeCanary ||
		merged.SoakMinDuration != "24h" ||
		merged.MinimumSamples != 10 ||
		merged.MinimumFailureDomains != 3 {
		t.Fatalf("expected stricter policy to be retained: %+v", merged)
	}
	if len(merged.CanaryFailureDomains) != 1 || merged.CanaryFailureDomains[0] != "node:a" {
		t.Fatalf("compiled one-node blast cap must bound canary cohort: %+v", merged.CanaryFailureDomains)
	}
}

func TestUnknownGatePolicyIsForcedToShadow(t *testing.T) {
	policies := mergeGatePolicies(nil, []model.GatePolicy{{
		ID:         "extension.unreviewed",
		Mode:       model.GatePolicyModeEnforced,
		Scope:      model.GatePolicyScopeNode,
		RunbookRef: "docs/runbooks/extension.md",
	}})
	if len(policies) != 1 {
		t.Fatalf("expected one policy, got %+v", policies)
	}
	policy := policies[0]
	if policy.Mode != model.GatePolicyModeShadow || policy.DefaultMode != model.GatePolicyModeShadow {
		t.Fatalf("new uncompiled gate must start in shadow: %+v", policy)
	}
	if policy.BlastRadius.MaxNodes != 1 {
		t.Fatalf("new node gate must receive the compiled one-node cap: %+v", policy)
	}
}

func TestGateKillSwitchOnlyDowngradesMode(t *testing.T) {
	const envName = "FUGUE_TEST_GATE_KILL_SWITCH"
	policy := model.GatePolicy{
		Mode:          model.GatePolicyModeEnforced,
		DefaultMode:   model.GatePolicyModeEnforced,
		KillSwitchEnv: envName,
	}
	t.Setenv(envName, model.GatePolicyModeShadow)
	if got := effectiveGatePolicyMode(policy); got != model.GatePolicyModeShadow {
		t.Fatalf("expected shadow kill switch to downgrade enforced gate, got %s", got)
	}
	policy.Mode = model.GatePolicyModeShadow
	t.Setenv(envName, model.GatePolicyModeEnforced)
	if got := effectiveGatePolicyMode(policy); got != model.GatePolicyModeShadow {
		t.Fatalf("kill switch must not promote shadow gate, got %s", got)
	}
}

func TestGateKillSwitchConcurrentPrecedence(t *testing.T) {
	const workers = 64
	var wg sync.WaitGroup
	errors := make(chan error, workers)
	for index := 0; index < workers; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iteration := 0; iteration < 100; iteration++ {
				if got := resolveGatePolicyMode(model.GatePolicyModeEnforced, model.GatePolicyModeDisabled); got != model.GatePolicyModeDisabled {
					errors <- fmt.Errorf("disabled kill switch lost precedence: %s", got)
					return
				}
				if got := resolveGatePolicyMode(model.GatePolicyModeShadow, model.GatePolicyModeEnforced); got != model.GatePolicyModeShadow {
					errors <- fmt.Errorf("kill switch promoted shadow gate: %s", got)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func TestLongerGatePolicyDurationRejectsInvalidOrShorterOverride(t *testing.T) {
	if got := longerGatePolicyDuration("24h", "1h"); got != "24h" {
		t.Fatalf("shorter duration loosened floor: %s", got)
	}
	if got := longerGatePolicyDuration("1h", "24h"); got != "24h" {
		t.Fatalf("longer duration was not retained: %s", got)
	}
	if got := longerGatePolicyDuration("24h", "not-a-duration"); got != "24h" {
		t.Fatalf("invalid duration replaced valid floor: %s", got)
	}
	if got := longerGatePolicyDuration("", (48 * time.Hour).String()); got != "48h0m0s" {
		t.Fatalf("expected valid override for empty floor, got %s", got)
	}
}

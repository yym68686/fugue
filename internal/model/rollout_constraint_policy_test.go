package model

import "testing"

func TestRolloutConstraintPolicyRejectsMixedExecutableChanges(t *testing.T) {
	for _, scenario := range []string{"gate", "canary", "retention", "image", "env", "replicas", "source", "origin", "mode", "disable", "unchanged", "invalid"} {
		t.Run(scenario, func(t *testing.T) {
			current := AppSpec{Image: "registry.example/service:v1", Replicas: 1, Continuity: &AppContinuityPolicy{ZeroDowntime: &AppZeroDowntimePolicy{
				Enabled: true, Mode: AppZeroDowntimeModeSafe, Strategy: AppZeroDowntimeStrategyStableCandidate,
			}}}
			desired := current
			desired.Continuity = CloneAppContinuityPolicy(current.Continuity)
			desired.Continuity.ZeroDowntime.Gate = &AppReleaseGatePolicy{LatencyCohortModel: true}
			op := Operation{Type: OperationTypeDeploy, DesiredSpec: &desired}
			switch scenario {
			case "canary":
				desired.Continuity.ZeroDowntime.Canary = &AppRolloutCanarySpec{StepWeights: []int{1, 25, 100}}
			case "retention":
				desired.Continuity.ZeroDowntime.RetireGraceSeconds = 600
			case "image":
				desired.Image = "registry.example/service:v2"
			case "env":
				desired.Env = map[string]string{"NEW": "value"}
			case "replicas":
				desired.Replicas++
			case "source":
				op.DesiredSource = &AppSource{}
			case "origin":
				op.DesiredOriginSource = &AppSource{}
			case "mode":
				desired.Continuity.ZeroDowntime.Mode = AppZeroDowntimeModeDrainOnly
			case "disable":
				desired.Continuity = nil
			case "unchanged":
				desired = current
			case "invalid":
				desired.Continuity.ZeroDowntime.Gate.LatencyComparisonWindowSeconds = -1
			}
			want := scenario == "gate" || scenario == "canary" || scenario == "retention"
			if got := RolloutConstraintPolicyOnlyChange(current, op); got != want {
				t.Fatalf("classification=%v want=%v", got, want)
			}
			if current.Continuity.ZeroDowntime.Gate != nil {
				t.Fatal("changed source policy")
			}
		})
	}
}

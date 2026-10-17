package model

import "reflect"

// RolloutConstraintPolicyOnlyChange identifies edits to future rollout checks,
// traffic steps and retention. Enabling protection or changing its execution
// mode still requires the ordinary workload reconciliation path.
func RolloutConstraintPolicyOnlyChange(current AppSpec, op Operation) bool {
	if op.Type != OperationTypeDeploy || op.DesiredSpec == nil || op.DesiredSource != nil || op.DesiredOriginSource != nil ||
		!AppSafeZeroDowntimeRolloutEnabled(current) || !AppSafeZeroDowntimeRolloutEnabled(*op.DesiredSpec) {
		return false
	}
	desired := *op.DesiredSpec
	before, after := current.Continuity.ZeroDowntime, desired.Continuity.ZeroDowntime
	if before.Enabled != after.Enabled || before.Mode != after.Mode || before.Strategy != after.Strategy ||
		reflect.DeepEqual(before, after) || ValidateAppZeroDowntimePolicy(after) != nil {
		return false
	}
	// The entire remaining AppSpec must be identical. Do not normalize away
	// image, environment, storage, placement, restart or other executable edits.
	desired.Continuity = current.Continuity
	return reflect.DeepEqual(current, desired)
}

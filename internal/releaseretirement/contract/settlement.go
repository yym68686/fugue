package contract

import (
	"reflect"
	"time"

	"fugue/internal/model"
)

// Snapshot holds the two releases and traffic policy from the same app.
type Snapshot struct {
	Previous, Stable model.AppRelease
	Traffic          model.AppTrafficPolicy
}

// SettleDrained produces a tombstone only when the observations still match
// the rows locked by the caller. The caller supplies the snapshots used to
// verify drain evidence and commits within the same write transaction.
func SettleDrained(current, observed Snapshot, now time.Time) (model.AppRelease, bool) {
	if !reflect.DeepEqual(current, observed) {
		return model.AppRelease{}, false
	}
	previous, stable, policy := current.Previous, current.Stable, current.Traffic
	if previous.ID == "" || stable.ID == "" || previous.ID == stable.ID || policy.AppID == "" ||
		previous.AppID != policy.AppID || previous.TenantID != policy.TenantID ||
		stable.AppID != policy.AppID || stable.TenantID != policy.TenantID ||
		!AwaitingDrain(previous) ||
		stable.Role != model.AppReleaseRoleStable || stable.Status != model.AppReleaseStatusServing ||
		policy.Mode != model.AppTrafficModeSingle || policy.StableReleaseID != stable.ID ||
		policy.CandidateReleaseID != "" || policy.StableWeight != 100 || policy.CandidateWeight != 0 ||
		previous.RetentionUntil != nil && now.Before(*previous.RetentionUntil) {
		return model.AppRelease{}, false
	}
	reason := "safe rollout previous stable drained"
	if previous.Status == model.AppReleaseStatusFailed {
		reason = "failed candidate drained after rollback"
	}
	now = now.Truncate(time.Microsecond)
	previous.Role, previous.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
	previous.RetiredAt, previous.UpdatedAt = &now, now
	previous.StatusReason = reason
	previous.ReleaseMessage = "retired after verified traffic withdrawal and Pod drain"
	return previous, true
}

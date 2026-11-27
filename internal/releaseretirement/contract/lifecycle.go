// Package contract defines release retirement facts shared by the scheduler and
// the persistence boundary. These predicates do not authorize resource changes.
package contract

import "fugue/internal/model"

// AwaitingDrain identifies a withdrawn release that still needs proof.
func AwaitingDrain(release model.AppRelease) bool {
	return release.Role == model.AppReleaseRolePrevious && release.Status == model.AppReleaseStatusDraining ||
		release.Role == model.AppReleaseRoleCandidate && release.Status == model.AppReleaseStatusFailed
}

// HasAlignedRevision identifies a superseded revision workload on a release
// that is still serving. Only the redundant workload can be drained.
func HasAlignedRevision(release model.AppRelease) bool {
	b := release.RevisionWorkload
	return release.Role == model.AppReleaseRoleStable && release.Status == model.AppReleaseStatusServing && b != nil &&
		release.DeploymentName != "" && release.ServiceName != "" && b.DeploymentName != "" && b.ServiceName != "" &&
		release.DeploymentName != b.DeploymentName && release.ServiceName != b.ServiceName
}

func Candidate(release model.AppRelease) bool {
	return AwaitingDrain(release) || HasAlignedRevision(release) || release.Status == model.AppReleaseStatusRetired && release.RevisionWorkload != nil
}

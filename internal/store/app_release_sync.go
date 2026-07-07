package store

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

const stableReleaseSyncActorID = "stable-release-sync"

func syncStableReleaseForCompletedDeployInState(state *model.State, app model.App, op model.Operation, now time.Time) error {
	if state == nil || !shouldSyncStableReleaseForCompletedDeploy(app, op) {
		return nil
	}
	policyIndex := findAppTrafficPolicyByApp(state.AppTrafficPolicies, app.ID)
	if policyIndex < 0 {
		return nil
	}
	policy := state.AppTrafficPolicies[policyIndex]
	if !trafficPolicyAllowsStableReleaseAutoSync(app, policy) {
		return nil
	}

	desired, ok := currentStableReleaseForApp(app, now)
	if !ok {
		return nil
	}
	releaseIndex := findReusableCurrentStableRelease(state.AppReleases, desired)
	if releaseIndex >= 0 {
		desired = state.AppReleases[releaseIndex]
		desired.Role = model.AppReleaseRoleStable
		desired.Status = model.AppReleaseStatusServing
		desired.StatusReason = ""
		desired.ReadyAt = firstNonNilTime(desired.ReadyAt, &now)
		desired.PromotedAt = &now
		desired.UpdatedAt = now
		state.AppReleases[releaseIndex] = desired
	} else {
		var err error
		desired, err = normalizeAppReleaseForStore(desired)
		if err != nil {
			return err
		}
		state.AppReleases = append(state.AppReleases, desired)
	}

	if oldStableID := strings.TrimSpace(policy.StableReleaseID); oldStableID != "" && oldStableID != desired.ID {
		if oldIndex := findAppReleaseByID(state.AppReleases, oldStableID); oldIndex >= 0 && state.AppReleases[oldIndex].AppID == app.ID {
			state.AppReleases[oldIndex].Role = model.AppReleaseRolePrevious
			state.AppReleases[oldIndex].UpdatedAt = now
		}
	}

	state.AppTrafficPolicies[policyIndex] = stableReleaseSyncedTrafficPolicy(policy, desired.ID, now)
	return nil
}

func shouldSyncStableReleaseForCompletedDeploy(app model.App, op model.Operation) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	if strings.TrimSpace(app.ID) == "" || strings.TrimSpace(app.TenantID) == "" || strings.TrimSpace(app.Spec.Image) == "" {
		return false
	}
	return model.AppExposesPublicService(app.Spec)
}

func trafficPolicyAllowsStableReleaseAutoSync(app model.App, policy model.AppTrafficPolicy) bool {
	if model.AppSafeZeroDowntimeRolloutEnabled(app.Spec) {
		return false
	}
	return policy.CandidateWeight == 0
}

func currentStableReleaseForApp(app model.App, now time.Time) (model.AppRelease, bool) {
	upstreamURL := currentAppServiceURL(app)
	if upstreamURL == "" {
		return model.AppRelease{}, false
	}
	specSnapshot := cloneAppSpec(&app.Spec)
	return model.AppRelease{
		TenantID:         app.TenantID,
		AppID:            app.ID,
		Role:             model.AppReleaseRoleStable,
		SourceRef:        appStableReleaseSourceRef(app),
		ResolvedImageRef: strings.TrimSpace(app.Spec.Image),
		UpstreamURL:      upstreamURL,
		RuntimeID:        strings.TrimSpace(app.Spec.RuntimeID),
		Status:           model.AppReleaseStatusServing,
		SpecSnapshot:     specSnapshot,
		ReadyAt:          &now,
		PromotedAt:       &now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}, true
}

func currentAppServiceURL(app model.App) string {
	namespace := strings.TrimSpace(runtimepkg.NamespaceForTenant(app.TenantID))
	serviceName := strings.TrimSpace(runtimepkg.RuntimeAppServiceName(app))
	if namespace == "" || serviceName == "" {
		return ""
	}
	port := 80
	if app.Route != nil && app.Route.ServicePort > 0 {
		port = app.Route.ServicePort
	} else if len(app.Spec.Ports) > 0 && app.Spec.Ports[0] > 0 {
		port = app.Spec.Ports[0]
	}
	return "http://" + serviceName + "." + namespace + ".svc.cluster.local:" + strconv.Itoa(port)
}

func appStableReleaseSourceRef(app model.App) string {
	if app.Source != nil {
		return firstNonEmpty(app.Source.ImageRef, app.Source.RepoURL, app.Spec.Image)
	}
	return strings.TrimSpace(app.Spec.Image)
}

func findReusableCurrentStableRelease(releases []model.AppRelease, desired model.AppRelease) int {
	for idx, release := range releases {
		if appReleaseMatchesCurrentStable(release, desired) {
			return idx
		}
	}
	return -1
}

func appReleaseMatchesCurrentStable(release, desired model.AppRelease) bool {
	return strings.TrimSpace(release.TenantID) == strings.TrimSpace(desired.TenantID) &&
		strings.TrimSpace(release.AppID) == strings.TrimSpace(desired.AppID) &&
		strings.TrimSpace(release.SourceRef) == strings.TrimSpace(desired.SourceRef) &&
		strings.TrimSpace(release.ResolvedImageRef) == strings.TrimSpace(desired.ResolvedImageRef) &&
		strings.TrimSpace(release.UpstreamURL) == strings.TrimSpace(desired.UpstreamURL) &&
		strings.TrimSpace(release.RuntimeID) == strings.TrimSpace(desired.RuntimeID) &&
		reflect.DeepEqual(release.SpecSnapshot, desired.SpecSnapshot)
}

func stableReleaseSyncedTrafficPolicy(policy model.AppTrafficPolicy, stableReleaseID string, now time.Time) model.AppTrafficPolicy {
	policy.Mode = model.AppTrafficModeSingle
	policy.StableReleaseID = strings.TrimSpace(stableReleaseID)
	policy.CandidateReleaseID = ""
	policy.StableWeight = 100
	policy.CandidateWeight = 0
	policy.UpdatedByType = model.ActorTypeSystem
	policy.UpdatedByID = stableReleaseSyncActorID
	policy.UpdatedAt = now
	return policy
}

func firstNonNilTime(values ...*time.Time) *time.Time {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

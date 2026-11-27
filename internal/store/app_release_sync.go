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

func appReleaseMatchesCurrentStable(release, desired model.AppRelease) bool {
	return !AppReleaseIsRetired(release) && strings.TrimSpace(release.TenantID) == strings.TrimSpace(desired.TenantID) &&
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

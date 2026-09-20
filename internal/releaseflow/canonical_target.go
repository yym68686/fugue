package releaseflow

import (
	"strconv"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// CanonicalStableTarget resolves an older implicit stable workload reference.
// It never declares readiness or mutates durable release state. Partial names,
// foreign owners, different runtimes/images and noncanonical upstreams retain
// their explicit identity and cannot borrow the app's canonical workload.
func CanonicalStableTarget(app model.App, release model.AppRelease) model.AppRelease {
	if release.DeploymentName != "" || release.ServiceName != "" || app.ID == "" || app.TenantID == "" || release.AppID != app.ID || release.TenantID != app.TenantID || release.Role != model.AppReleaseRoleStable || release.RuntimeID == "" || release.RuntimeID != app.Spec.RuntimeID || release.ResolvedImageRef == "" || release.ResolvedImageRef != app.Spec.Image {
		return release
	}
	if release.SpecSnapshot != nil && (release.SpecSnapshot.Image != release.ResolvedImageRef || release.SpecSnapshot.RuntimeID != release.RuntimeID) {
		return release
	}
	port := 80
	if app.Route != nil && app.Route.ServicePort > 0 {
		port = app.Route.ServicePort
	} else if len(app.Spec.Ports) > 0 && app.Spec.Ports[0] > 0 {
		port = app.Spec.Ports[0]
	}
	expected := "http://" + runtime.RuntimeAppServiceName(app) + "." + runtime.NamespaceForTenant(app.TenantID) + ".svc.cluster.local:" + strconv.Itoa(port)
	if strings.TrimSpace(release.UpstreamURL) != expected {
		return release
	}
	release.DeploymentName = runtime.RuntimeAppResourceName(app)
	release.ServiceName = runtime.RuntimeAppServiceName(app)
	return release
}

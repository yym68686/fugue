// Package routeartifact exposes the shared compiler projection to executors.
package routeartifact

import (
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"regexp"
)

var platformRouteArtifactGroupID = regexp.MustCompile(`^edge-group-[a-z0-9]+(?:-[a-z0-9]+)*$`)

func Project(artifact model.PlatformArtifact) (model.EdgeRouteIntentSnapshot, error) {
	return platformconfig.ProjectRouteArtifact(artifact)
}

func NormalizePlatformRoute(route model.PlatformRoute) (model.PlatformRoute, bool) {
	return platformconfig.NormalizePlatformRoute(route)
}

func IntentFromPlatformRoute(route model.PlatformRoute, minimumHealthy int) model.EdgeRouteIntent {
	return platformconfig.IntentFromPlatformRoute(route, minimumHealthy)
}

func IntentGeneration(intent model.EdgeRouteIntent) string {
	return platformconfig.IntentGeneration(intent)
}

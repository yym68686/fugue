package api

import (
	"context"
	"fugue/internal/routeprobe"
	"net/http"
	"time"
)

type placementRouteProof = routeprobe.Proof

type placementRouteProbe func(context.Context, string, string, string, string) (placementRouteProof, error)

func probePlacementRoute(ctx context.Context, host, path, address string) (placementRouteProof, error) {
	return probePlacementRouteState(ctx, host, path, address, "")
}
func probePlacementRouteState(ctx context.Context, host, path, address, state string) (placementRouteProof, error) {
	return routeprobe.Probe(ctx, host, path, address, state, trafficOverrideProbeTimeout)
}
func parsePlacementRouteProof(response *http.Response, nonce string, now time.Time) (placementRouteProof, error) {
	return routeprobe.ParseResponse(response, nonce, now)
}

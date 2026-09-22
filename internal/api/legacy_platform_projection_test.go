package api

import (
	"context"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

// Historical comparison adapter. Production capture always starts with pinned
// producer configuration; migration equivalence tests retain the old input view.
// capturePlatformIntent freezes business input and captures runtime observations
// independently. This entry point can be called without an HTTP request; it
// neither publishes artifacts nor changes serving or recovery state.
func (s *Server) capturePlatformIntent(ctx context.Context, principal model.Principal) (platformIntentProjectionResponse, error) {
	return s.capturePlatformIntentWithStatic(ctx, principal, platformproducer.StaticIntentInput{Routes: s.platformRoutes, DNS: s.dnsStaticRecords})
}

func (s *Server) capturePlatformIntentWithStatic(ctx context.Context, principal model.Principal, static platformproducer.StaticIntentInput) (platformIntentProjectionResponse, error) {
	return s.capturePlatformIntentWithInputs(ctx, principal, static, nil, nil)
}

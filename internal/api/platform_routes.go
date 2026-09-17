package api

import (
	"encoding/json"
	"log"
	"strings"

	"fugue/internal/model"
	"fugue/internal/routeartifact"
)

type platformRoutesEnvelope struct {
	Routes []model.PlatformRoute `json:"routes"`
}

func parsePlatformRoutes(raw string, logger *log.Logger) []model.PlatformRoute {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var routes []model.PlatformRoute
	if err := json.Unmarshal([]byte(raw), &routes); err != nil {
		var envelope platformRoutesEnvelope
		if envelopeErr := json.Unmarshal([]byte(raw), &envelope); envelopeErr != nil {
			if logger != nil {
				logger.Printf("ignoring FUGUE_PLATFORM_ROUTES_JSON: %v", err)
			}
			return nil
		}
		routes = envelope.Routes
	}
	out := make([]model.PlatformRoute, 0, len(routes))
	for _, route := range routes {
		normalized, ok := normalizePlatformRoute(route)
		if ok {
			out = append(out, normalized)
		}
	}
	return out
}

func normalizePlatformRoute(route model.PlatformRoute) (model.PlatformRoute, bool) {
	return routeartifact.NormalizePlatformRoute(route)
}

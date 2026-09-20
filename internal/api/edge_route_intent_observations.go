package api

import (
	"encoding/json"
	"strings"

	"fugue/internal/model"
)

// Record the facts used by the business projection. This is diagnostic evidence,
// not an Edge Control decision or proof that an intent was activated. In
// particular it must not change intent hashes, freshness, gates or serving LKG.
func (s *Server) logEdgeRouteIntentObservations(snapshot model.EdgeRouteIntentSnapshot, apps map[string]model.App, provenance map[string]managedAppObservationProvenance) {
	if s == nil || s.log == nil {
		return
	}
	for _, intent := range snapshot.Routes {
		app, found := apps[strings.TrimSpace(intent.AppID)]
		if !found {
			continue
		}
		p := provenance[app.ID]
		binding := model.EdgeRouteBinding{
			Hostname: intent.Hostname, PathPrefix: intent.PathPrefix,
			RouteKind: intent.RouteKind, RouteGeneration: intent.Generation,
			DeploymentGeneration: intent.DeploymentGeneration,
			Status:               intent.OriginStatus, StatusReason: intent.OriginStatusReason,
		}
		material := edgeRouteDecisionMaterialFor(app, binding, p)
		encoded, err := json.Marshal(material)
		if err != nil {
			continue
		}
		// Refresh bookkeeping alone must not turn every polling cycle into a
		// full fleet of duplicate events. Preserve those times in the evidence
		// body without treating them as a change in the serving facts.
		identityMaterial := material
		identityMaterial.CacheExpired = false
		identityMaterial.ImageLocationObservedAt = ""
		observationID := "intent_observation_" + strings.TrimPrefix(edgeRouteDecisionID(identityMaterial), "decision_")
		key := strings.Join([]string{"intent_observation", app.ID, normalizeExternalAppDomain(intent.Hostname), model.NormalizeAppRoutePathPrefix(intent.PathPrefix), intent.RouteKind}, "\x00")
		if !s.edgeRouteObservationChanged(key, observationID) {
			continue
		}
		s.logStructuredEvent(map[string]any{
			"event_type":               "edge_route_intent_observation",
			"fugue_table":              "app_events",
			"source_stage":             "business_route_projection",
			"app_id":                   app.ID,
			"tenant_id":                app.TenantID,
			"project_id":               app.ProjectID,
			"hostname":                 intent.Hostname,
			"path_prefix":              model.NormalizeAppRoutePathPrefix(intent.PathPrefix),
			"source_intent_generation": snapshot.Generation,
			"intent_route_generation":  intent.Generation,
			"observation_id":           observationID,
			"observation_refreshed_at": formatObservationTime(p.refreshedAt),
			"observation_expires_at":   formatObservationTime(p.expiresAt),
			"final_status":             material.Status,
			"final_reason":             material.StatusReason,
			"material_json":            string(encoded),
		})
	}
}

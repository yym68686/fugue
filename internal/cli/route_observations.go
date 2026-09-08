package cli

import (
	"context"
	"fmt"
	"fugue/internal/model"
	"strings"
	"time"
)

// Route decisions are bounded historical evidence. They do not establish the
// currently loaded bundle on every edge or a measured global traffic split.
type routeObservation struct {
	State                 string                 `json:"state"`
	EvidenceKind          string                 `json:"evidence_kind"`
	Source                evidenceSource         `json:"source"`
	Window                appObservabilityWindow `json:"window"`
	Decisions             []map[string]any       `json:"decisions"`
	MissingLinks          []map[string]any       `json:"missing_links"`
	GlobalServingVerified bool                   `json:"global_serving_verified"`
}

func (c *CLI) observeAppRoute(client *Client, app model.App) routeObservation {
	result := routeObservation{State: "unknown", EvidenceKind: "edge_route_decision_samples", Decisions: []map[string]any{}, MissingLinks: []map[string]any{}}
	if app.Route == nil || app.Route.Hostname == "" {
		result.State = "not_configured"
		result.Source = makeEvidenceSource(nil, true)
		return result
	}
	parent := client.context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	scoped := *client
	scoped.context = ctx
	response, err := scoped.ListAppEdgeRouteDecisions(app.ID, appEdgeRouteDecisionOptions{Domain: app.Route.Hostname, Limit: 100, appObservabilityWindowOptions: appObservabilityWindowOptions{Since: "5m"}})
	if err == nil && !response.Source.Available {
		err = errEvidenceUnavailable
	}
	result.Source = makeEvidenceSource(err, len(response.Decisions) == 0)
	result.Window = response.Window
	result.Decisions = response.Decisions
	result.MissingLinks = response.MissingLinks
	if err == nil && len(response.Decisions) > 0 {
		result.State = "sampled"
	}
	return result
}
func (c *CLI) appendRouteDrift(client *Client, app model.App, result *model.AppRuntimeState) routeObservation {
	observed := c.observeAppRoute(client, app)
	if observed.State == "not_configured" {
		result.Checks = append(result.Checks, model.AppRuntimeCheck{Kind: "route", State: "not_configured", Source: "app_route_intent"})
		return observed
	}
	check := model.AppRuntimeCheck{Kind: "route", Key: app.Route.Hostname + model.NormalizeAppRoutePathPrefix(app.Route.PathPrefix), State: "unknown", Source: observed.EvidenceKind, Reason: "decision samples cannot prove current serving state on every edge; runtime scope is independently verifiable"}
	// A recent explicit failed route decision is actionable negative evidence.
	for _, decision := range observed.Decisions {
		if fmt.Sprint(decision["app_id"]) != app.ID || !strings.EqualFold(fmt.Sprint(decision["hostname"]), app.Route.Hostname) {
			continue
		}
		stamp, err := time.Parse(time.RFC3339Nano, fmt.Sprint(decision["ts"]))
		if err != nil {
			stamp, err = time.Parse("2006-01-02 15:04:05.999999999", fmt.Sprint(decision["ts"]))
		}
		if err != nil || stamp.Before(time.Now().Add(-time.Minute)) || stamp.Before(app.UpdatedAt) || stamp.After(time.Now().Add(15*time.Second)) {
			continue
		}
		if state := fmt.Sprint(decision["final_status"]); state == model.EdgeRouteStatusUnavailable || state == model.EdgeRouteStatusRuntimeMissing {
			check.State = "drifted"
			check.Reason = "recent route decision explicitly reports unavailable serving"
		}
	}
	result.Checks = append(result.Checks, check)
	if check.State == "unknown" {
		result.MissingEvidence = appendUniqueString(result.MissingEvidence, "current_edge_serving_state")
		if result.State != "drifted" {
			result.State = "inconclusive"
		}
	} else {
		result.State = "drifted"
	}
	return observed
}

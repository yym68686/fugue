package cli

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

type routeDelivery struct {
	At          string `json:"observed_at"`
	Workload    string `json:"workload"`
	Stable      string `json:"target_stable,omitempty"`
	Publication string `json:"route_publication"`
	Edge        string `json:"edge_apply"`
	HTTP        int    `json:"public_http"`
	RequestID   string `json:"edge_request_id,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

// The existing admin explain operation is bound to a published release and
// checks actual consumer ACKs. Neither a Pod probe nor a reachable older
// release is sufficient evidence that the requested rollout is public.
func checkPublicDelivery(ctx context.Context, c *Client, a model.App, r *appRouteShowResult) {
	d := &routeDelivery{At: time.Now().UTC().Format(time.RFC3339), Workload: r.ConclusionCode, Publication: "unknown", Edge: "unknown"}
	r.Delivery = d
	if a.Route == nil || a.Route.PublicURL == "" {
		r.ConclusionCode = "inconclusive"
		r.Conclusion = "no declared public HTTP route"
		return
	}
	policy, pe := c.GetAppTrafficPolicy(a.ID)
	explain, ee := c.ExplainRoute(a.Route.Hostname)
	response := httpx.ObservePublicRoute(ctx, a.Route.PublicURL, nil)
	if pe == nil && policy.Traffic.AppID == a.ID && policy.Traffic.TenantID == a.TenantID {
		d.Stable = policy.Traffic.StableReleaseID
	}
	if d.Stable != "" && ee == nil {
		current, err := c.GetAppTrafficPolicy(a.ID)
		if err != nil || current.Traffic != policy.Traffic {
			d.Reason = "stable traffic selection changed or could not be reconfirmed"
			ee = fmt.Errorf("%s", d.Reason)
		}
	}
	finishPublicDelivery(a, r, explain, ee, response)
}

func finishPublicDelivery(a model.App, r *appRouteShowResult, explain model.RouteExplainResponse, explainErr error, response httpx.PublicRouteObservation) {
	d := r.Delivery
	d.HTTP, d.RequestID = response.StatusCode, response.RequestID
	if explainErr == nil && d.Stable != "" {
		routes := explain.Routes
		if len(routes) == 0 && explain.Route != nil {
			routes = []model.EdgeRouteBinding{*explain.Route}
		}
		for _, b := range routes {
			if b.AppID != a.ID || b.TenantID != a.TenantID || b.Hostname != a.Route.Hostname || model.NormalizeAppRoutePathPrefix(b.PathPrefix) != model.NormalizeAppRoutePathPrefix(a.Route.PathPrefix) {
				continue
			}
			d.Reason = b.StatusReason
			d.Publication, d.Edge = "pending", "unconfirmed"
			if b.StatusReason == "published traffic consumers have not provided fresh matching serving evidence" {
				d.Publication, d.Edge = "published; target not confirmed", "awaiting matching consumers"
			}
			for _, u := range b.Upstreams {
				if u.ReleaseID == d.Stable && u.Weight > 0 && b.Status == model.EdgeRouteStatusActive {
					d.Publication, d.Edge = "target published", "confirmed"
				}
			}
			break
		}
	}
	switch {
	case d.Workload != "in_sync": // Preserve the independently diagnosed workload failure.
	case !response.Reachable:
		r.ConclusionCode = "public_unverified"
		r.Conclusion = fmt.Sprintf("workload ready; public probe not successful: %s", response.Reason)
	case d.Edge != "confirmed":
		r.ConclusionCode = "inconclusive"
		r.Conclusion = "public path reachable; publication and Edge evidence do not yet confirm the target release"
	default:
		r.Conclusion = "workload ready, stable target published, matching Edge evidence and reachable public HTTP path"
	}
}

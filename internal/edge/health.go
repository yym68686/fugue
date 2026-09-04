package edge

import "time"

type edgeRouteHealthMode uint8

const (
	edgeRouteHealthCurrent edgeRouteHealthMode = iota
	edgeRouteHealthSyncFailed
	edgeRouteHealthStandbyWithoutCandidate
)

type edgeRouteHealthInput struct {
	BundlePresent bool
	ValidUntil    time.Time
	Now           time.Time
	MaxStale      time.Duration
	Stale         bool
	Mode          edgeRouteHealthMode
}

type edgeRouteHealthDecision struct {
	Status           string
	Ready            bool
	Stale            bool
	MaxStaleExceeded bool
	DegradedReason   string
}

func evaluateEdgeRouteHealth(input edgeRouteHealthInput) edgeRouteHealthDecision {
	if !input.BundlePresent {
		return edgeRouteHealthDecision{Status: "unhealthy"}
	}
	now := input.Now.UTC()
	validUntil := input.ValidUntil.UTC()
	expired := !validUntil.IsZero() && now.After(validUntil)
	maxStaleExceeded := input.MaxStale > 0 && expired && now.Sub(validUntil) > input.MaxStale
	status := "ok"
	if input.Stale {
		status = "stale"
	}
	decision := edgeRouteHealthDecision{Status: status, Ready: true, Stale: input.Stale, MaxStaleExceeded: maxStaleExceeded}
	if input.Mode == edgeRouteHealthSyncFailed {
		decision.Status = "stale"
		decision.Stale = true
		decision.DegradedReason = "route bundle sync failed; serving cache"
	}
	if expired {
		decision.Status = "degraded"
		decision.Stale = true
		decision.DegradedReason = "route bundle valid_until expired"
	}
	if maxStaleExceeded {
		decision.Status = "unhealthy"
		decision.Ready = false
		decision.Stale = true
		decision.DegradedReason = "route bundle valid_until exceeded max_stale"
	}
	if input.Mode == edgeRouteHealthStandbyWithoutCandidate && maxStaleExceeded &&
		now.Sub(validUntil) <= edgeEmergencyLKGMaxAge {
		decision.Status = "degraded"
		decision.Ready = true
		decision.DegradedReason = "inactive candidate is empty; retaining standby LKG beyond max_stale"
	}
	return decision
}

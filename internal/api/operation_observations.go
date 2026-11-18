package api

import (
	"context"
	"time"

	"fugue/internal/runtimeobservation"
)

func (s *Server) operationObservations() *runtimeobservation.Operations {
	s.operationOnce.Do(func() {
		var err error
		s.operations, err = runtimeobservation.NewOperations(
			"consumer-assignment", "consumer-artifact", "consumer-resolve",
			"consumer-assignment-retry", "consumer-assignment-retry-ok", "consumer-assignment-retry-empty", "consumer-assignment-retry-exhausted",
			"traffic-source", "discovery-bundle", "dns-serving-facts", "node-policy", "node-desired-state",
			"dns-snapshot-replacement-retry", "dns-snapshot-replacement-retry-verified",
			"agent-grant", "agent-cell-authorization", "agent-observation",
			"agent-cell-unavailable", "agent-observation-unavailable", "agent-authority-changed",
			"agent-evidence-expired", "agent-grant-constraint-unmet",
			"agent-capacity-node-unavailable", "agent-capacity-summary-unavailable", "agent-capacity-time-invalid",
			"agent-capacity-expired", "agent-capacity-pressure", "agent-capacity-node-changed",
			"agent-heartbeat-unavailable", "agent-route-proof-unavailable", "agent-route-binding-mismatch", "agent-observation-lease-short",
			"agent-response-ok", "agent-response-capture-failed", "agent-response-keyring-unavailable",
			"agent-response-signing-key-invalid", "agent-response-lease-short", "agent-response-authority-recheck-failed", "agent-response-sign-failed",
			"agent-response-context-deadline", "agent-response-context-canceled",
		)
		if err != nil {
			panic(err) // Fixed source-defined names are a programmer invariant.
		}
	})
	return s.operations
}

func (s *Server) observeOperation(stage string) func() {
	observations := s.operationObservations()
	start := time.Now()
	return func() { observations.Observe(stage, time.Since(start)) }
}

// OperationSnapshot is consumed by the local Live Diagnostics endpoint. Stage
// names are fixed; no request values, credentials or tenant IDs are recorded.
func (s *Server) OperationSnapshot(ctx context.Context) (any, error) {
	value, err := s.operationObservations().Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	value.(map[string]any)["captures"] = s.captureSnapshot()
	return value, nil
}

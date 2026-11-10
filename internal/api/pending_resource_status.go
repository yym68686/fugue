package api

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Keep only fixed classifications. Wrapped transport/SQL errors may contain
// credentials, object bodies or statements and must never be serialized.
type pendingResourceRefusal struct {
	phase, stage, reason string
	cause                error
}

func (e *pendingResourceRefusal) Error() string { return errPendingResourceObservation.Error() }
func (e *pendingResourceRefusal) Unwrap() error { return e.cause }
func (e *pendingResourceRefusal) Is(target error) bool {
	return target == errPendingResourceObservation
}

func pendingResourceRefuse(stage, reason string, cause error) error {
	if errors.Is(cause, context.DeadlineExceeded) {
		reason = "deadline"
	} else if errors.Is(cause, context.Canceled) {
		reason = "canceled"
	}
	return &pendingResourceRefusal{stage: stage, reason: reason, cause: cause}
}

func pendingResourcePhase(phase string, err error) error {
	var refusal *pendingResourceRefusal
	if errors.As(err, &refusal) {
		copy := *refusal
		copy.phase = phase
		return &copy
	}
	return &pendingResourceRefusal{phase: phase, stage: "unknown", reason: "unavailable", cause: err}
}

type pendingResourceLastResult struct {
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	Succeeded   bool      `json:"succeeded"`
	Phase       string    `json:"phase,omitempty"`
	Stage       string    `json:"stage,omitempty"`
	Reason      string    `json:"reason,omitempty"`
	SQLCategory string    `json:"sql_category,omitempty"`
}

type pendingResourceObservationStatus struct {
	mu   sync.Mutex
	last *pendingResourceLastResult
}

func (s *pendingResourceObservationStatus) record(started time.Time, err error) {
	result := pendingResourceLastResult{StartedAt: started, Succeeded: err == nil}
	if err != nil {
		result.Stage, result.Reason = "unknown", "unavailable"
		var refusal *pendingResourceRefusal
		if errors.As(err, &refusal) {
			result.Phase, result.Stage, result.Reason = refusal.phase, refusal.stage, refusal.reason
		}
		var sqlState interface{ SQLState() string }
		if errors.As(err, &sqlState) {
			// Deliberately exclude raw SQLSTATE and driver error text. Unknown
			// states share one category, so cardinality and output stay bounded.
			switch sqlState.SQLState() {
			case "42501":
				result.SQLCategory = "privilege"
			case "53100":
				result.SQLCategory = "disk-full"
			case "53400":
				result.SQLCategory = "configuration-limit"
			case "57014":
				result.SQLCategory = "query-canceled"
			case "55P03":
				result.SQLCategory = "lock-unavailable"
			default:
				result.SQLCategory = "other"
			}
		}
	}
	s.mu.Lock()
	result.CompletedAt = time.Now().UTC()
	s.last = &result
	s.mu.Unlock()
}

// PendingResourceObservationStatus reads only the most recently completed
// attempt. It never retries the provider or touches Kubernetes/the database.
// Concurrent callers may replace the last result; timestamps identify its scope.
func (s *Server) PendingResourceObservationStatus(context.Context) (any, error) {
	s.pendingResourceStatus.mu.Lock()
	var last *pendingResourceLastResult
	if s.pendingResourceStatus.last != nil {
		copy := *s.pendingResourceStatus.last
		last = &copy
	}
	s.pendingResourceStatus.mu.Unlock()
	return map[string]any{"schema": "fugue.pending-root-resource.status.v1", "last_completed_attempt": last,
		"observation_only": true, "admission_allowed": false, "whole_operation_budget_proven": false,
		"scope": "last completed observation in this process; not an operation outcome or resource bound"}, nil
}

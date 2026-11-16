package api

import (
	"context"
	"errors"
	"time"

	"fugue/internal/runtimeobservation"
	"fugue/internal/store"
)

const consumerFailureLimit = 24

// These are the original failing calls, never a replay of mutable authority.
// Fixed operation/stage names and error categories are the only retained data.
type consumerRequestFailure struct {
	Sequence          uint64                               `json:"sequence"`
	StartedAt         time.Time                            `json:"started_at"`
	FinishedAt        time.Time                            `json:"finished_at"`
	Operation         string                               `json:"operation"`
	DatabaseOperation runtimeobservation.DatabaseOperation `json:"database_operation"`
	Stage             string                               `json:"stage"`
	ErrorClass        string                               `json:"error_class"`
	Conflict          bool                                 `json:"store_conflict"`
	SQLState          string                               `json:"sql_state,omitempty"`
	InputRead         *consumerInputReadFailure            `json:"input_read,omitempty"`
}

func (s *Server) retainConsumerRequestFailure(ctx context.Context, start time.Time, operation, stage string, err error) {
	if err == nil {
		return
	}
	switch operation {
	case "assignment", "trusted_heartbeat", "shadow_heartbeat":
	default:
		operation = "unrecognized"
	}
	switch stage {
	case "resolve", "serving_selection", "input_recheck", "expected_set", "list_expectations", "backend_validation", "store_admission":
	default:
		stage = "unrecognized"
	}
	receipt := consumerRequestFailure{StartedAt: start, FinishedAt: time.Now().UTC(), Operation: operation,
		DatabaseOperation: runtimeobservation.DatabaseOperationFromContext(ctx), Stage: stage, ErrorClass: captureErrorClass(err), Conflict: errors.Is(err, store.ErrConflict)}
	if stage == "input_recheck" {
		receipt.InputRead = consumerInputReadFailureFromContext(ctx)
	}
	var databaseError interface{ SQLState() string }
	if errors.As(err, &databaseError) {
		switch state := databaseError.SQLState(); state {
		case "55P03", "57014", "40001", "40P01":
			receipt.SQLState = state
		default:
			receipt.SQLState = "other"
		}
	}
	s.captures.mu.Lock()
	defer s.captures.mu.Unlock()
	s.captures.consumerFailureTotal++
	receipt.Sequence = s.captures.consumerFailureTotal
	if len(s.captures.consumerFailures) == consumerFailureLimit {
		copy(s.captures.consumerFailures, s.captures.consumerFailures[1:])
		s.captures.consumerFailures = s.captures.consumerFailures[:consumerFailureLimit-1]
		s.captures.consumerFailureEvicted++
	}
	s.captures.consumerFailures = append(s.captures.consumerFailures, receipt)
}

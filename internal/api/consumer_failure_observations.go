package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/runtimeobservation"
	"fugue/internal/store"
)

const consumerFailureLimit = 24

type consumerFailureCorrelationKey struct{}

// The holder is private to the synchronous handler and only the final failed
// assignment attempt is echoed. Never read a caller's correlation header.
func withConsumerFailureCorrelation(ctx context.Context, component string) context.Context {
	if component != model.PlatformConsumerComponentDNSServer {
		return ctx
	}
	return context.WithValue(ctx, consumerFailureCorrelationKey{}, new(string))
}

func writeConsumerFailureCorrelation(w http.ResponseWriter, ctx context.Context) {
	if id, _ := ctx.Value(consumerFailureCorrelationKey{}).(*string); id != nil && *id != "" {
		w.Header().Set(platformconsumer.FailureIDHeader, *id)
	}
}

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
	FailureID         string                               `json:"failure_id,omitempty"`
	InputRead         *consumerInputReadFailure            `json:"input_read,omitempty"`
	StoreAdmission    *store.PlatformInputReadFailure      `json:"store_admission,omitempty"`
}

func (s *Server) retainConsumerRequestFailure(ctx context.Context, start time.Time, operation, stage string, err error, observed ...store.PlatformInputReadFailure) {
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
	if stage == "store_admission" && len(observed) == 1 && observed[0].Phase != "" {
		detail := observed[0]
		detail.ChangedFields = append([]string(nil), detail.ChangedFields...)
		receipt.StoreAdmission = &detail
	}
	if id, _ := ctx.Value(consumerFailureCorrelationKey{}).(*string); id != nil {
		var random [16]byte
		if _, err := rand.Read(random[:]); err == nil {
			receipt.FailureID = hex.EncodeToString(random[:])
		}
		*id = receipt.FailureID
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

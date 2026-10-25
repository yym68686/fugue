package dnsserver

import (
	"crypto/sha256"
	"fmt"
	"time"
)

// Diagnostic only. Neither readiness nor selection consults these fields.
// Each existing bounded history entry retains one fixed-size observation;
// records/receipts receive detached copies without an unbounded event list.
type dnsProbeHistoryObservation struct {
	KeyDigest            string    `json:"key_digest"`
	ResetReason          string    `json:"reset_reason"`
	ResetAt              time.Time `json:"reset_at"`
	PreviousSuccessCount int       `json:"previous_success_count"`
	PreviousSuccessAt    string    `json:"previous_success_at,omitempty"`
	ObservationReason    string    `json:"observation_reason,omitempty"`
}

func dnsHistoryObservation(key, reason string, at time.Time, previous []time.Time, observationReason string) dnsProbeHistoryObservation {
	sum := sha256.Sum256([]byte(key))
	out := dnsProbeHistoryObservation{KeyDigest: fmt.Sprintf("sha256:%x", sum), ResetReason: reason, ResetAt: at, PreviousSuccessCount: min(len(previous), dnsProbeHistoryLimit), ObservationReason: observationReason}
	if len(out.ObservationReason) > 64 {
		out.ObservationReason = out.ObservationReason[:64]
	}
	if len(previous) > 0 {
		out.PreviousSuccessAt = previous[len(previous)-1].UTC().Format(time.RFC3339Nano)
	}
	return out
}
func cloneDNSHistoryObservation(in *dnsProbeHistoryObservation) *dnsProbeHistoryObservation {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

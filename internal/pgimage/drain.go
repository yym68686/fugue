package pgimage

import (
	"encoding/json"
	"fmt"
	"time"
)

// DrainMarker is set only in the temporary Cluster spec. CNPG loads it through
// the same instance reconciliation that updates its cached shutdown timeout.
const DrainMarker = "fugue.image_migration_drain_nonce"

const DrainAbortEvidence = "postgres_image_migration_drain_abort"
const DrainSampleEvidence = "postgres_image_migration_drain_sample"
const EndpointTimingEvidence = "postgres_image_migration_endpoint_timing"

func ValidDrainBudget(seconds int) bool { return seconds == 0 || seconds >= 10 && seconds <= 15 }

type DrainObservation struct {
	Status              string     `json:"status"`
	SampledAt           *time.Time `json:"sampled_at,omitempty"`
	Sessions            *int       `json:"sessions,omitempty"`
	ActiveTransactions  *int       `json:"active_transactions,omitempty"`
	IdleInTransaction   *int       `json:"idle_in_transaction,omitempty"`
	OldestTransactionMS *int64     `json:"oldest_transaction_ms,omitempty"`
	Reason              string     `json:"reason,omitempty"`
	// Never expose the acknowledgement as part of a user-facing plan.
	LoadedMarker   string `json:"-"`
	MarkerFromFile bool   `json:"-"`
}

// Use the CNPG local postgres socket with a fresh, read-only superuser
// connection. A tenant login cannot see other users' transaction states. Only
// aggregate counts are returned; SQL text, role names and PIDs never leave the Pod.
func DrainObservationCommand() []string {
	return []string{"env", "PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=1500 -c lock_timeout=250",
		"psql", "-XqAtw", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres", "-c", `
SELECT json_build_object(
 'sampled_at', statement_timestamp(),
 'sessions', count(*),
 'active_transactions', count(*) FILTER (WHERE state = 'active' AND xact_start IS NOT NULL),
 'idle_in_transaction', count(*) FILTER (WHERE state IN ('idle in transaction', 'idle in transaction (aborted)')),
 'unknown', count(*) FILTER (WHERE state IS NULL OR state NOT IN ('idle', 'active', 'idle in transaction', 'idle in transaction (aborted)')),
 'oldest_transaction_ms', COALESCE(GREATEST(0, floor(EXTRACT(EPOCH FROM (statement_timestamp()-min(xact_start)))*1000)), 0)::bigint,
 'valid', NOT pg_is_in_recovery() AND current_setting('track_activities') = 'on',
 'marker', COALESCE(current_setting('` + DrainMarker + `', true), ''),
 'marker_from_file', EXISTS (SELECT 1 FROM pg_file_settings WHERE name = '` + DrainMarker + `' AND applied IS TRUE
                             AND setting = current_setting('` + DrainMarker + `', true))
) FROM pg_stat_activity
WHERE backend_type IN ('client backend', 'walsender')
  AND pid <> pg_backend_pid();`}
}

func UnavailableDrainObservation() DrainObservation {
	return DrainObservation{Status: "unavailable", Reason: "complete primary session visibility is unavailable"}
}

func DecodeDrainObservation(raw []byte, budget int) DrainObservation {
	var observed struct {
		SampledAt           *time.Time `json:"sampled_at"`
		Sessions            *int       `json:"sessions"`
		ActiveTransactions  *int       `json:"active_transactions"`
		IdleInTransaction   *int       `json:"idle_in_transaction"`
		OldestTransactionMS *int64     `json:"oldest_transaction_ms"`
		Unknown             *int       `json:"unknown"`
		Valid               bool       `json:"valid"`
		Marker              string     `json:"marker"`
		MarkerFromFile      bool       `json:"marker_from_file"`
	}
	if json.Unmarshal(raw, &observed) != nil || !observed.Valid || observed.SampledAt == nil || observed.SampledAt.IsZero() ||
		observed.Sessions == nil || observed.ActiveTransactions == nil || observed.IdleInTransaction == nil ||
		observed.OldestTransactionMS == nil || observed.Unknown == nil || *observed.Unknown != 0 ||
		*observed.Sessions < 0 || *observed.ActiveTransactions < 0 || *observed.IdleInTransaction < 0 ||
		*observed.ActiveTransactions+*observed.IdleInTransaction > *observed.Sessions || *observed.OldestTransactionMS < 0 {
		return UnavailableDrainObservation()
	}
	sample := DrainObservation{Status: "eligible", SampledAt: observed.SampledAt, Sessions: observed.Sessions,
		ActiveTransactions: observed.ActiveTransactions, IdleInTransaction: observed.IdleInTransaction,
		OldestTransactionMS: observed.OldestTransactionMS, LoadedMarker: observed.Marker, MarkerFromFile: observed.MarkerFromFile}
	if *observed.IdleInTransaction > 0 {
		sample.Status, sample.Reason = "blocked", "sessions are idle in a transaction"
	} else if *observed.OldestTransactionMS >= int64(budget)*1000 {
		sample.Status, sample.Reason = "blocked", fmt.Sprintf("a transaction already exceeds the %ds drain budget", budget)
	}
	return sample
}

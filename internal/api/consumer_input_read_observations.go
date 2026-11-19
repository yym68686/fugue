package api

import (
	"context"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

// One synchronous assignment request owns this metadata. It is copied into the
// existing failure ring, never exposed by the public API or reused as authority.
type consumerInputReadFailure struct {
	Stage          string                         `json:"stage"`
	Store          store.PlatformInputReadFailure `json:"store,omitempty"`
	ChangedFields  []string                       `json:"changed_fields,omitempty"`
	Selection      *capturePublicationPair        `json:"selection,omitempty"`
	SelectionReads *consumerSelectionReadPair     `json:"selection_reads,omitempty"`

	initialReads   [4]consumerInitialSelectionRead
	initialCount   int
	initialOmitted uint64
}

type consumerSelectionLaneRead struct {
	Started     time.Time `json:"started_at"`
	Returned    time.Time `json:"returned_at"`
	ReleaseHash string    `json:"release_hash,omitempty"`
}

type consumerSelectionRead struct {
	CohortHash string                    `json:"cohort_hash"`
	Full       consumerSelectionLaneRead `json:"full"`
	Gray       consumerSelectionLaneRead `json:"gray"`
}

type consumerSelectionReadPair struct {
	Initial        *consumerSelectionRead `json:"initial,omitempty"`
	Recheck        consumerSelectionRead  `json:"recheck"`
	InitialOmitted uint64                 `json:"initial_omitted"`
}

type consumerInitialSelectionRead struct {
	setID, releaseID string
	read             consumerSelectionRead
}

func recordConsumerSelectionRead(ctx context.Context, assignment model.PlatformConsumerAssignment, read consumerSelectionRead, recheck bool) {
	o, _ := ctx.Value(consumerInputReadObservationKey{}).(*consumerInputReadFailure)
	// This records only the DNS-only branch that read both original lanes.
	if o == nil || o.Stage != "" || read.Full.Returned.IsZero() || read.Gray.Returned.IsZero() || recheck && o.Selection == nil {
		return
	}
	if recheck {
		o.SelectionReads = &consumerSelectionReadPair{Recheck: read, InitialOmitted: o.initialOmitted}
	}
	for i := 0; i < o.initialCount; i++ {
		entry := &o.initialReads[i]
		if entry.setID == assignment.ExpectedConsumerSetID && entry.releaseID == assignment.ArtifactReleaseID {
			if recheck {
				initial := entry.read
				o.SelectionReads.Initial = &initial
			} else {
				// Retain the last successful check for this exact assignment.
				entry.read = read
			}
			return
		}
	}
	if recheck {
		return
	}
	if o.initialCount == len(o.initialReads) {
		o.initialOmitted++
		return
	}
	o.initialReads[o.initialCount] = consumerInitialSelectionRead{assignment.ExpectedConsumerSetID, assignment.ArtifactReleaseID, read}
	o.initialCount++
}

type consumerInputSelectionObserver func(*capturePublicationPair, ...string)

type consumerInputReadObservationKey struct{}

func withConsumerInputReadObservation(ctx context.Context) context.Context {
	return context.WithValue(ctx, consumerInputReadObservationKey{}, &consumerInputReadFailure{})
}

func observeConsumerInputReadFailure(ctx context.Context, stage string, detail store.PlatformInputReadFailure, before, after *consumerArtifactLookup) {
	failure, _ := ctx.Value(consumerInputReadObservationKey{}).(*consumerInputReadFailure)
	if failure == nil || failure.Stage != "" {
		return
	}
	failure.Stage, failure.Store = stage, detail
	if before != nil && after != nil {
		// Only field names from these three fixed Go structs are recorded.
		// No arbitrary map key, value, identifier, proof or raw error is kept.
		for _, pair := range []struct {
			prefix string
			x, y   any
		}{{"artifact", before.Artifact, after.Artifact}, {"assignment", before.Assignment, after.Assignment}, {"release", before.Release, after.Release}} {
			x, y := reflect.ValueOf(pair.x), reflect.ValueOf(pair.y)
			for i := 0; i < x.NumField(); i++ {
				if !reflect.DeepEqual(x.Field(i).Interface(), y.Field(i).Interface()) {
					field := strings.Split(x.Type().Field(i).Tag.Get("json"), ",")[0]
					failure.ChangedFields = append(failure.ChangedFields, pair.prefix+"."+field)
				}
			}
		}
	}
}

// Keep fixed field names and the fingerprints of the exact rejected binding
// in the owner-only receipt. Never retain raw values or projection contents.
func observeConsumerInputSelectionFailure(ctx context.Context, comparison *capturePublicationPair, fields ...string) {
	failure, _ := ctx.Value(consumerInputReadObservationKey{}).(*consumerInputReadFailure)
	if failure == nil || failure.Stage != "" {
		return
	}
	for _, field := range fields {
		switch field {
		case "selection.parent_id", "selection.parent_digest", "selection.release_id",
			"selection.fencing_token", "selection.projection_missing", "selection.traffic_release_missing",
			"selection.parent_read", "selection.current_release_read", "selection.current_release_missing",
			"selection.projection_read", "selection.non_dns_child":
		default:
			continue
		}
		seen := false
		for _, existing := range failure.ChangedFields {
			if existing == field {
				seen = true
				break
			}
		}
		if !seen {
			failure.ChangedFields = append(failure.ChangedFields, field)
		}
	}
	if comparison != nil && failure.Selection == nil {
		failure.Selection = comparison.clone()
	}
}

func consumerInputReadFailureFromContext(ctx context.Context) *consumerInputReadFailure {
	failure, _ := ctx.Value(consumerInputReadObservationKey{}).(*consumerInputReadFailure)
	if failure == nil || failure.Stage == "" {
		return nil
	}
	out := &consumerInputReadFailure{Stage: failure.Stage, Store: failure.Store,
		ChangedFields: append([]string(nil), failure.ChangedFields...), Selection: failure.Selection.clone()}
	out.Store.ChangedFields = append([]string(nil), failure.Store.ChangedFields...)
	if r := failure.SelectionReads; r != nil {
		pair := *r
		if r.Initial != nil {
			initial := *r.Initial
			pair.Initial = &initial
		}
		out.SelectionReads = &pair
	}
	return out
}

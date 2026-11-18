package dnsserver

import (
	"sync/atomic"

	"fugue/internal/dnsfacts"
)

// A publication wraps the exact pointer the serving path already used. Keep
// the writer class and pointer in one atomic read, including a rollback to an
// older pointer. The wrapper never grants readiness or changes pointer identity.
type dnsServingPublication struct {
	state  *dnsServingState
	writer dnsServingWriter
}

type dnsServingWriter string

const (
	dnsServingWriterCheckpoint            dnsServingWriter = dnsfacts.ReplacementCheckpoint
	dnsServingWriterRetainedSource        dnsServingWriter = dnsfacts.ReplacementRetainedSource
	dnsServingWriterRetainedObservation   dnsServingWriter = dnsfacts.ReplacementRetainedObservation
	dnsServingWriterRetainedFallback      dnsServingWriter = dnsfacts.ReplacementRetainedFallback
	dnsServingWriterRetainedBeforePending dnsServingWriter = dnsfacts.ReplacementRetainedBeforePending
	dnsServingWriterClearFallback         dnsServingWriter = dnsfacts.ReplacementClearFallback
	dnsServingWriterReadinessIncomplete   dnsServingWriter = dnsfacts.ReplacementReadinessIncomplete
	dnsServingWriterRecordTransition      dnsServingWriter = dnsfacts.ReplacementRecordTransition
	dnsServingWriterCandidate             dnsServingWriter = dnsfacts.ReplacementCandidate
	dnsServingWriterRollback              dnsServingWriter = dnsfacts.ReplacementRollback
)

type dnsServingPointer struct {
	publication atomic.Pointer[dnsServingPublication]
}

func (p *dnsServingPointer) Load() *dnsServingState {
	if published := p.publication.Load(); published != nil {
		return published.state
	}
	return nil
}

func (p *dnsServingPointer) publish(state *dnsServingState, writer dnsServingWriter) {
	p.publication.Store(&dnsServingPublication{state: state, writer: writer})
}

type dnsServingSelection struct {
	state           *dnsServingState
	base            *dnsServingPublication
	renewal         *dnsServingRenewal
	renewalSelected bool
}

// Describes the observations at the two comparison points, not the first
// writer in an interval that may contain multiple changes. For example a base
// write can invalidate a renewal, so also retain the observed selection path.
func dnsServingReplacement(before, after dnsServingSelection) (baseWriter, selection, renewal string) {
	baseWriter, renewal = dnsfacts.ReplacementUnchanged, dnsfacts.ReplacementUnchanged
	if before.base != after.base {
		baseWriter = dnsfacts.ReplacementUnattributed
		if after.base != nil && after.base.writer != "" {
			baseWriter = string(after.base.writer)
		}
	}
	if before.renewal != after.renewal {
		renewal = dnsfacts.ReplacementChanged
	}
	switch {
	case before.renewalSelected && after.renewalSelected:
		selection = dnsfacts.ReplacementRenewalToRenewal
	case before.renewalSelected:
		selection = dnsfacts.ReplacementRenewalToBase
	case after.renewalSelected:
		selection = dnsfacts.ReplacementBaseToRenewal
	default:
		selection = dnsfacts.ReplacementBaseToBase
	}
	return baseWriter, selection, renewal
}

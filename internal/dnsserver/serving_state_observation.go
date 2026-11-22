package dnsserver

import (
	"context"
	"errors"
	"sync/atomic"

	"fugue/internal/dnsfacts"
	"fugue/internal/platformconsumer"
)

// Clients can return the context's cause instead of ctx.Err(). Preserve the
// ordinary cancellation class as well, so the probe scheduler does not turn a
// discarded read into a negative physical observation.
var errDNSRenewalRetired = errors.Join(platformconsumer.ErrAssignmentChanged, context.Canceled)

// A publication wraps the exact pointer the serving path already used. Keep
// the writer class and pointer in one atomic read, including a rollback to an
// older pointer. The wrapper never grants readiness or changes pointer identity.
type dnsServingPublication struct {
	state  *dnsServingState
	writer dnsServingWriter
	// A publication's lifetime identifies this installation, not the state
	// pointer. Restoring an old state must not revive work for its old install.
	lifetime context.Context
	retire   context.CancelFunc
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
	lifetime, retire := context.WithCancel(context.Background())
	previous := p.publication.Swap(&dnsServingPublication{state: state, writer: writer, lifetime: lifetime, retire: retire})
	if previous != nil && previous.retire != nil {
		previous.retire()
	}
}

// Stop only a speculative sweep whose original installation can no longer
// accept its result. The parent work deadline and the final pointer/source
// checks remain independent; cancellation is not evidence for a successor.
func (p *dnsServingPublication) renewalContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(p.lifetime, func() { cancel(errDNSRenewalRetired) })
	if p.lifetime.Err() != nil {
		cancel(errDNSRenewalRetired)
	}
	return ctx, func() { stop(); cancel(nil) }
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

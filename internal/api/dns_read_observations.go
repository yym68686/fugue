package api

import (
	"context"
	"time"

	"fugue/internal/runtimeobservation"
)

type dnsReadPhase uint8
type dnsReadStage uint8

const (
	dnsReadInitialSource dnsReadPhase = iota
	dnsReadBackend
	dnsReadAuthorityRecheck
	dnsReadValidation
	dnsReadPhaseCount
)

const (
	dnsReadTotal dnsReadStage = iota
	dnsReadConsumerInventory
	dnsReadExpectedInventory
	dnsReadBackendDisambiguation
	dnsReadAssignmentResolution
	dnsReadAssignmentExpectedSet
	dnsReadTrafficReleaseSelection
	dnsReadArtifactDecode
	dnsReadCellPublicationVerification
	dnsReadPreviousTrafficBinding
	dnsReadCellRouteBindings
	dnsReadRouteProjection
	dnsReadStageCount
)

var dnsReadPhaseNames = [...]string{"initial_source", "backend", "authority_recheck", "validation"}
var dnsReadStageNames = [...]string{"total", "consumer_inventory", "expected_inventory", "backend_disambiguation",
	"assignment_resolution", "assignment_expected_set", "traffic_release_selection", "artifact_decode",
	"cell_publication_verification", "previous_traffic_binding", "cell_route_bindings", "route_projection"}

// Fixed aggregate wall-clock partitions of ONE existing DNS read. Nested stages
// overlap the phase total and must not be summed with it. Remaining budget is
// signed (negative means the deadline already elapsed). HasDeadline distinguishes
// an unbounded context, whose remaining-budget fields are both zero.
// No per-consumer identities, extra reads, errors, retries or verdicts are added.
type dnsReadStageObservation struct {
	Phase                string `json:"phase"`
	Stage                string `json:"stage"`
	Calls                uint64 `json:"calls"`
	ElapsedMillis        int64  `json:"elapsed_ms"`
	MaxMillis            int64  `json:"max_ms"`
	HasDeadline          bool   `json:"has_deadline"`
	FirstStartOffset     int64  `json:"first_start_offset_ms"`
	LastFinishOffset     int64  `json:"last_finish_offset_ms"`
	FirstRemainingMillis int64  `json:"first_remaining_ms"`
	LastRemainingMillis  int64  `json:"last_remaining_ms"`
}

type dnsReadTimings [dnsReadPhaseCount][dnsReadStageCount]dnsReadStageObservation
type dnsReadPhaseKey struct{}

// Inventory reads fan out by node, so the HTTP request operation (if any)
// cannot identify a single DNS capture. Attach a child ID before creating any
// readers, including the artifact session that spans both authority reads.
// The ID remains local to these receipts and the database recorder.
func (s *Server) beginDNSRuntimeFactsCapture(ctx context.Context) (context.Context, *captureTrace) {
	ctx = runtimeobservation.WithDatabaseOperation(ctx, "dns-runtime-facts")
	ctx, trace := s.beginCaptureTrace(ctx, "dns-runtime-facts")
	trace.receipt.DatabaseOperationID = runtimeobservation.DatabaseOperationFromContext(ctx).ID
	return ctx, trace
}

func startDNSReadPhase(ctx context.Context, phase dnsReadPhase) (context.Context, func()) {
	ctx = context.WithValue(ctx, dnsReadPhaseKey{}, phase)
	return ctx, startDNSReadStage(ctx, dnsReadTotal)
}

func startDNSReadStage(ctx context.Context, stage dnsReadStage) func() {
	trace, _ := ctx.Value(captureTraceKey{}).(*captureTrace)
	phase, ok := ctx.Value(dnsReadPhaseKey{}).(dnsReadPhase)
	if trace == nil || !ok || phase >= dnsReadPhaseCount || stage >= dnsReadStageCount {
		return func() {}
	}
	started := time.Now()
	deadline, bounded := ctx.Deadline()
	return func() { trace.observeDNSReadStage(phase, stage, started, time.Now(), deadline, bounded) }
}

func (t *captureTrace) observeDNSReadStage(phase dnsReadPhase, stage dnsReadStage, started, finished, deadline time.Time, bounded bool) {
	if phase >= dnsReadPhaseCount || stage >= dnsReadStageCount {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.receipt.Kind != "dns-runtime-facts" || t.dnsReadClosed {
		return
	}
	if t.dnsRead == nil {
		t.dnsRead = &dnsReadTimings{}
	}
	row := &t.dnsRead[phase][stage]
	startOffset := max(int64(0), started.Sub(t.receipt.StartedAt).Milliseconds())
	finishOffset := max(int64(0), finished.Sub(t.receipt.StartedAt).Milliseconds())
	if row.Calls == 0 || startOffset < row.FirstStartOffset {
		row.FirstStartOffset = startOffset
		row.HasDeadline = bounded
		row.FirstRemainingMillis = 0
		if bounded {
			row.FirstRemainingMillis = deadline.Sub(started).Milliseconds()
		}
	}
	if row.Calls == 0 || finishOffset >= row.LastFinishOffset {
		row.LastFinishOffset = finishOffset
		row.LastRemainingMillis = 0
		if bounded {
			row.LastRemainingMillis = deadline.Sub(finished).Milliseconds()
		}
	}
	row.Calls++
	elapsed := max(int64(0), finished.Sub(started).Milliseconds())
	row.ElapsedMillis += elapsed
	row.MaxMillis = max(row.MaxMillis, elapsed)
}

// Caller holds t.mu. The returned slice is detached before receipt retention.
func (t *captureTrace) dnsReadSnapshot() []dnsReadStageObservation {
	if t.dnsRead == nil {
		return nil
	}
	var rows []dnsReadStageObservation
	for phase := range dnsReadPhaseCount {
		for stage := range dnsReadStageCount {
			row := t.dnsRead[phase][stage]
			if row.Calls == 0 {
				continue
			}
			row.Phase, row.Stage = dnsReadPhaseNames[phase], dnsReadStageNames[stage]
			rows = append(rows, row)
		}
	}
	return rows
}

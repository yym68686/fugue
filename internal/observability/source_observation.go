package observability

import (
	"context"
	"sort"
	"strconv"
	"time"
)

type logSourceObservation struct {
	Identity         string                       `json:"identity"`
	Node             string                       `json:"node"`
	Previous         bool                         `json:"previous"`
	ObservedAt       time.Time                    `json:"observed_at"`
	CursorBefore     time.Time                    `json:"cursor_before"`
	CursorAfter      time.Time                    `json:"cursor_after"`
	NextAttempt      time.Time                    `json:"next_attempt,omitempty"`
	PendingAt        time.Time                    `json:"pending_at,omitempty"`
	DrainedThrough   time.Time                    `json:"drained_through,omitempty"`
	OpenMillis       int64                        `json:"open_ms"`
	TotalMillis      int64                        `json:"total_ms"`
	Lines            int                          `json:"lines"`
	Outcome          string                       `json:"outcome"`
	Attempts         uint64                       `json:"attempts"`
	Errors           uint64                       `json:"errors"`
	ErrorClass       string                       `json:"error_class,omitempty"`
	HTTPStatus       int                          `json:"http_status,omitempty"`
	LastErrorAt      time.Time                    `json:"last_error_at,omitempty"`
	LastErrorClass   string                       `json:"last_error_class,omitempty"`
	LastHTTPStatus   int                          `json:"last_http_status,omitempty"`
	LastErrorStage   string                       `json:"last_error_stage,omitempty"`
	LastErrorMillis  int64                        `json:"last_error_ms,omitempty"`
	LastErrorRead    *logReadTransportObservation `json:"last_error_read,omitempty"`
	LastAdmission    *logAdmissionObservation     `json:"last_admission,omitempty"`
	InvalidRecord    *logInvalidRecordObservation `json:"invalid_record,omitempty"`
	LineLimitBytes   int                          `json:"line_limit_bytes"`
	MaxLineBytes     int                          `json:"max_line_bytes"`
	TotalLines       uint64                       `json:"total_lines"`
	RotationFiles    int                          `json:"rotation_files,omitempty"`
	RotationBytes    int64                        `json:"rotation_bytes,omitempty"`
	RecoveryUnproven bool                         `json:"recovery_unproven,omitempty"`
	GapEvents        uint64                       `json:"gap_events"`
	GapReason        string                       `json:"gap_reason,omitempty"`
	GapLowerBound    time.Time                    `json:"gap_lower_bound,omitempty"`
	GapUpperBound    time.Time                    `json:"gap_upper_bound,omitempty"`
}

type sourceErrorSummary struct {
	Node           string    `json:"node"`
	ErrorClass     string    `json:"error_class"`
	Stage          string    `json:"stage"`
	Sources        uint64    `json:"sources"`
	Errors         uint64    `json:"errors"`
	LastErrorAt    time.Time `json:"last_error_at,omitempty"`
	MaxErrorMillis int64     `json:"max_error_ms,omitempty"`
	HTTPStatus     int       `json:"http_status,omitempty"`
}

const logGapReceiptLimit = 64

type logCheckpointObservation struct {
	Sources         int       `json:"sources"`
	SerializedBytes int       `json:"serialized_bytes"`
	MembershipBits  int       `json:"membership_bits_set"`
	ReplayFloor     time.Time `json:"replay_floor"`
	UnprovenReplays uint64    `json:"unproven_replays"`
	Enabled         bool      `json:"enabled"`
	State           string    `json:"state"`
	ObservedAt      time.Time `json:"observed_at"`
	LastSavedAt     time.Time `json:"last_saved_at"`
}

// Keep each newly reported loss separately from the truncated source view.
// Bounds describe unproven coverage, not reconstructed missing log contents.
// Outcome and prior read error are context, not a claim of causal attribution.
type logGapObservation struct {
	Sequence       uint64                       `json:"sequence"`
	Identity       string                       `json:"identity"`
	Node           string                       `json:"node"`
	Previous       bool                         `json:"previous"`
	ObservedAt     time.Time                    `json:"observed_at"`
	Events         uint64                       `json:"events"`
	Reason         string                       `json:"reason"`
	LowerBound     time.Time                    `json:"lower_bound"`
	UpperBound     time.Time                    `json:"upper_bound"`
	Outcome        string                       `json:"outcome"`
	CursorBefore   time.Time                    `json:"cursor_before"`
	CursorAfter    time.Time                    `json:"cursor_after"`
	PendingAt      time.Time                    `json:"pending_at"`
	DrainedThrough time.Time                    `json:"drained_through"`
	Lines          int                          `json:"lines"`
	RotationFiles  int                          `json:"rotation_files"`
	RotationBytes  int64                        `json:"rotation_bytes"`
	LastErrorAt    time.Time                    `json:"last_error_at"`
	LastErrorClass string                       `json:"last_error_class,omitempty"`
	LastErrorStage string                       `json:"last_error_stage,omitempty"`
	LastAdmission  *logAdmissionObservation     `json:"last_admission,omitempty"`
	InvalidRecord  *logInvalidRecordObservation `json:"invalid_record,omitempty"`
}

func (p *Pipeline) observeLogSource(v logSourceObservation) {
	p.sourceObservationMu.Lock()
	defer p.sourceObservationMu.Unlock()
	if p.sourceObservations == nil {
		p.sourceObservations = map[string]logSourceObservation{}
	}
	prior, exists := p.sourceObservations[v.Identity]
	if v.InvalidRecord != nil {
		copy := *v.InvalidRecord
		v.InvalidRecord = &copy
	}
	newGapEvents := v.GapEvents
	if !exists && len(p.sourceObservations) >= 2048 {
		oldest := ""
		var at time.Time
		for k, row := range p.sourceObservations {
			if oldest == "" || row.ObservedAt.Before(at) {
				oldest, at = k, row.ObservedAt
			}
		}
		delete(p.sourceObservations, oldest)
		p.sourceObservationEvicted++
	}
	v.Attempts = prior.Attempts
	if v.Outcome != "node_backoff" {
		v.Attempts++
	}
	v.Errors = prior.Errors
	v.TotalLines = prior.TotalLines + uint64(v.Lines)
	v.LastErrorAt, v.LastErrorClass = prior.LastErrorAt, prior.LastErrorClass
	v.LastHTTPStatus = prior.LastHTTPStatus
	v.LastErrorStage, v.LastErrorMillis = prior.LastErrorStage, prior.LastErrorMillis
	failed := v.Outcome == "open_error" || v.Outcome == "scan_error" || v.Outcome == "rotation_recovery_error"
	if v.LastAdmission == nil {
		v.LastAdmission = prior.LastAdmission
	} else {
		copy := *v.LastAdmission
		v.LastAdmission = &copy
	}
	if !failed {
		v.LastErrorRead = prior.LastErrorRead
	} else if v.LastErrorRead != nil {
		copy := *v.LastErrorRead
		v.LastErrorRead = &copy
	}
	if v.GapReason == "" {
		v.GapReason, v.GapLowerBound, v.GapUpperBound = prior.GapReason, prior.GapLowerBound, prior.GapUpperBound
	}
	v.GapEvents += prior.GapEvents
	if failed {
		v.Errors++
		v.LastErrorAt, v.LastErrorClass = v.ObservedAt, v.ErrorClass
		v.LastHTTPStatus = v.HTTPStatus
		v.LastErrorStage, v.LastErrorMillis = v.Outcome, v.TotalMillis
	}
	p.sourceObservations[v.Identity] = v
	if newGapEvents > 0 {
		p.sourceGapReceiptTotal++
		if len(p.sourceGapReceipts) == logGapReceiptLimit {
			copy(p.sourceGapReceipts, p.sourceGapReceipts[1:])
			p.sourceGapReceipts = p.sourceGapReceipts[:logGapReceiptLimit-1]
			p.sourceGapReceiptEvicted++
		}
		p.sourceGapReceipts = append(p.sourceGapReceipts, logGapObservation{
			Sequence: p.sourceGapReceiptTotal, Identity: v.Identity, Node: v.Node, Previous: v.Previous,
			ObservedAt: v.ObservedAt, Events: newGapEvents, Reason: v.GapReason,
			LowerBound: v.GapLowerBound, UpperBound: v.GapUpperBound, Outcome: v.Outcome,
			CursorBefore: v.CursorBefore, CursorAfter: v.CursorAfter, PendingAt: v.PendingAt, DrainedThrough: v.DrainedThrough,
			Lines: v.Lines, RotationFiles: v.RotationFiles, RotationBytes: v.RotationBytes,
			LastErrorAt: v.LastErrorAt, LastErrorClass: v.LastErrorClass, LastErrorStage: v.LastErrorStage,
			LastAdmission: v.LastAdmission, InvalidRecord: v.InvalidRecord,
		})
	}
}

// DiagnosticSources projects metadata only; neither log contents nor queue
// payloads are retained. It does no I/O and cannot change collection cursors.
func (p *Pipeline) DiagnosticSources(ctx context.Context) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.sourceObservationMu.Lock()
	rows := make([]logSourceObservation, 0, len(p.sourceObservations))
	for _, row := range p.sourceObservations {
		if row.InvalidRecord != nil {
			copy := *row.InvalidRecord
			row.InvalidRecord = &copy
		}
		if row.LastAdmission != nil {
			copy := *row.LastAdmission
			row.LastAdmission = &copy
		}
		if row.LastErrorRead != nil {
			copy := *row.LastErrorRead
			row.LastErrorRead = &copy
		}
		rows = append(rows, row)
	}
	evicted, cycle := p.sourceObservationEvicted, p.sourceObservationCycle
	checkpoint := p.sourceCheckpoint
	gapReceipts := append([]logGapObservation(nil), p.sourceGapReceipts...)
	for i := range gapReceipts {
		row := &gapReceipts[i]
		if row.InvalidRecord != nil {
			copy := *row.InvalidRecord
			row.InvalidRecord = &copy
		}
		if row.LastAdmission != nil {
			copy := *row.LastAdmission
			row.LastAdmission = &copy
		}
	}
	gapHistory := map[string]any{"receipts": gapReceipts,
		"total": p.sourceGapReceiptTotal, "evicted": p.sourceGapReceiptEvicted, "limit": logGapReceiptLimit,
		"coverage": "new_gap_observations_not_cumulative_source_counts_or_reconstructed_logs"}
	p.sourceObservationMu.Unlock()
	byError := map[string]sourceErrorSummary{}
	for _, row := range rows {
		if row.Errors == 0 {
			continue
		}
		class := row.LastErrorClass
		if class == "" {
			class = "unknown"
		}
		stage := row.LastErrorStage
		if stage == "" {
			stage = "unknown"
		}
		key := row.Node + "\x00" + class + "\x00" + stage + "\x00" + strconv.Itoa(row.LastHTTPStatus)
		summary := byError[key]
		summary.Node, summary.ErrorClass, summary.Stage = row.Node, class, stage
		summary.HTTPStatus = row.LastHTTPStatus
		summary.Sources++
		summary.Errors += row.Errors
		if row.LastErrorAt.After(summary.LastErrorAt) {
			summary.LastErrorAt = row.LastErrorAt
		}
		if row.LastErrorMillis > summary.MaxErrorMillis {
			summary.MaxErrorMillis = row.LastErrorMillis
		}
		byError[key] = summary
	}
	errorSummary := make([]sourceErrorSummary, 0, len(byError))
	for _, summary := range byError {
		errorSummary = append(errorSummary, summary)
	}
	sort.Slice(errorSummary, func(i, j int) bool {
		if errorSummary[i].Errors != errorSummary[j].Errors {
			return errorSummary[i].Errors > errorSummary[j].Errors
		}
		if errorSummary[i].Node != errorSummary[j].Node {
			return errorSummary[i].Node < errorSummary[j].Node
		}
		a, b := errorSummary[i], errorSummary[j]
		if a.ErrorClass+"/"+a.Stage != b.ErrorClass+"/"+b.Stage {
			return a.ErrorClass+"/"+a.Stage < b.ErrorClass+"/"+b.Stage
		}
		return a.HTTPStatus < b.HTTPStatus
	})
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.PendingAt.IsZero() != b.PendingAt.IsZero() {
			return !a.PendingAt.IsZero()
		}
		if !a.PendingAt.Equal(b.PendingAt) {
			return a.PendingAt.Before(b.PendingAt)
		}
		if a.Errors != b.Errors {
			return a.Errors > b.Errors
		}
		return a.Identity < b.Identity
	})
	total := len(rows)
	truncated := total > 512
	if truncated {
		rows = rows[:512]
	}
	return map[string]any{"schema": "fugue.pipeline.sources.v1", "observed_at": time.Now().UTC(), "sources": rows, "source_count": total, "truncated": truncated, "evicted_sources": evicted, "error_summary": errorSummary,
		"error_summary_coverage": "retained_source_lifetime_errors_grouped_by_latest_error_not_per_class_event_totals",
		"gap_coverage":           "retained_source_lifetime_gap_counts_with_most_recent_gap_bounds_not_reconstructed_log_contents",
		"gap_history":            gapHistory,
		"checkpoint":             checkpoint,
		"cycle":                  cycle, "queue_depth": p.queueDepth.Load(), "queued_bytes": p.queuedBytes.Load()}, nil
}

type logCycleObservation struct {
	At              time.Time `json:"at"`
	Targets         int       `json:"targets"`
	Scheduled       int       `json:"scheduled"`
	Visited         int64     `json:"visited"`
	CatchupReads    int       `json:"catchup_reads"`
	InitialBudget   int64     `json:"initial_budget"`
	RemainingBudget int64     `json:"remaining_budget"`
	PollMillis      int64     `json:"poll_ms"`
	PerSourceLimit  int64     `json:"per_source_limit"`
	CycleLimit      int       `json:"cycle_limit"`
}

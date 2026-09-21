package observability

import (
	"context"
	"sort"
	"time"
)

type logSourceObservation struct {
	Identity        string    `json:"identity"`
	Node            string    `json:"node"`
	Previous        bool      `json:"previous"`
	ObservedAt      time.Time `json:"observed_at"`
	CursorBefore    time.Time `json:"cursor_before"`
	CursorAfter     time.Time `json:"cursor_after"`
	PendingAt       time.Time `json:"pending_at,omitempty"`
	DrainedThrough  time.Time `json:"drained_through,omitempty"`
	OpenMillis      int64     `json:"open_ms"`
	TotalMillis     int64     `json:"total_ms"`
	Lines           int       `json:"lines"`
	Outcome         string    `json:"outcome"`
	Attempts        uint64    `json:"attempts"`
	Errors          uint64    `json:"errors"`
	ErrorClass      string    `json:"error_class,omitempty"`
	LastErrorAt     time.Time `json:"last_error_at,omitempty"`
	LastErrorClass  string    `json:"last_error_class,omitempty"`
	LastErrorStage  string    `json:"last_error_stage,omitempty"`
	LastErrorMillis int64     `json:"last_error_ms,omitempty"`
	LineLimitBytes  int       `json:"line_limit_bytes"`
	MaxLineBytes    int       `json:"max_line_bytes"`
	TotalLines      uint64    `json:"total_lines"`
}

type sourceErrorSummary struct {
	Node           string    `json:"node"`
	ErrorClass     string    `json:"error_class"`
	Stage          string    `json:"stage"`
	Sources        uint64    `json:"sources"`
	Errors         uint64    `json:"errors"`
	LastErrorAt    time.Time `json:"last_error_at,omitempty"`
	MaxErrorMillis int64     `json:"max_error_ms,omitempty"`
}

func (p *Pipeline) observeLogSource(v logSourceObservation) {
	p.sourceObservationMu.Lock()
	defer p.sourceObservationMu.Unlock()
	if p.sourceObservations == nil {
		p.sourceObservations = map[string]logSourceObservation{}
	}
	prior, exists := p.sourceObservations[v.Identity]
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
	v.Attempts = prior.Attempts + 1
	v.Errors = prior.Errors
	v.TotalLines = prior.TotalLines + uint64(v.Lines)
	v.LastErrorAt, v.LastErrorClass = prior.LastErrorAt, prior.LastErrorClass
	v.LastErrorStage, v.LastErrorMillis = prior.LastErrorStage, prior.LastErrorMillis
	if v.Outcome == "open_error" || v.Outcome == "scan_error" {
		v.Errors++
		v.LastErrorAt, v.LastErrorClass = v.ObservedAt, v.ErrorClass
		v.LastErrorStage, v.LastErrorMillis = v.Outcome, v.TotalMillis
	}
	p.sourceObservations[v.Identity] = v
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
		rows = append(rows, row)
	}
	evicted, cycle := p.sourceObservationEvicted, p.sourceObservationCycle
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
		key := row.Node + "\x00" + class + "\x00" + stage
		summary := byError[key]
		summary.Node, summary.ErrorClass, summary.Stage = row.Node, class, stage
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
		return errorSummary[i].ErrorClass+"/"+errorSummary[i].Stage < errorSummary[j].ErrorClass+"/"+errorSummary[j].Stage
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
	return map[string]any{"schema": "fugue.pipeline.sources.v1", "observed_at": time.Now().UTC(), "sources": rows, "source_count": total, "truncated": truncated, "evicted_sources": evicted, "error_summary": errorSummary, "cycle": cycle, "queue_depth": p.queueDepth.Load(), "queued_bytes": p.queuedBytes.Load()}, nil
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

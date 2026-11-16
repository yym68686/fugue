package observability

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"time"
)

// Fixed-size, per-pipeline attempt counters. No destination, raw error, event
// content, tenant identifier or caller-provided label is retained.
type exportObservations struct {
	stages [5]exportAttemptCounters
}

type exportAttemptCounters struct {
	started      atomic.Uint64
	completed    atomic.Uint64
	retries      atomic.Uint64
	inflight     atomic.Int64
	events       atomic.Uint64
	totalNanos   atomic.Uint64
	maxNanos     atomic.Uint64
	success      atomic.Uint64
	deadline     atomic.Uint64
	canceled     atomic.Uint64
	timeout      atomic.Uint64
	failed       atomic.Uint64
	lastStarted  atomic.Int64
	lastFinished atomic.Int64
}

func exportObservationIndex(name string) int {
	switch name {
	case "metrics":
		return 0
	case "logs":
		return 1
	case "analytics":
		return 2
	case "noop":
		return 3
	default:
		return 4
	}
}

func (o *exportObservations) export(ctx context.Context, exporter Exporter, events []Event, attempt int) error {
	if o == nil {
		return exporter.Export(ctx, events)
	}
	c := &o.stages[exportObservationIndex(exporter.Name())]
	start := time.Now()
	c.lastStarted.Store(start.UnixNano())
	c.started.Add(1)
	c.inflight.Add(1)
	c.events.Add(uint64(len(events)))
	if attempt > 1 {
		c.retries.Add(1)
	}
	err := exporter.Export(ctx, events)
	finished := time.Now()
	nanos := uint64(finished.Sub(start))
	c.totalNanos.Add(nanos)
	for previous := c.maxNanos.Load(); nanos > previous; previous = c.maxNanos.Load() {
		if c.maxNanos.CompareAndSwap(previous, nanos) {
			break
		}
	}
	var timeout net.Error
	switch {
	case err == nil:
		c.success.Add(1)
	case errors.Is(err, context.DeadlineExceeded):
		c.deadline.Add(1)
	case errors.Is(err, context.Canceled):
		c.canceled.Add(1)
	case errors.As(err, &timeout) && timeout.Timeout():
		c.timeout.Add(1)
	default:
		c.failed.Add(1)
	}
	c.lastFinished.Store(finished.UnixNano())
	c.completed.Add(1)
	c.inflight.Add(-1)
	return err
}

type exportAttemptSnapshot struct {
	Exporter       string     `json:"exporter"`
	Started        uint64     `json:"started"`
	Completed      uint64     `json:"completed"`
	Retries        uint64     `json:"retries"`
	Inflight       int64      `json:"inflight"`
	AttemptEvents  uint64     `json:"attempt_events"`
	TotalNanos     uint64     `json:"total_ns"`
	MaxNanos       uint64     `json:"max_ns"`
	Success        uint64     `json:"success"`
	Deadline       uint64     `json:"deadline"`
	Canceled       uint64     `json:"canceled"`
	Timeout        uint64     `json:"timeout"`
	Failed         uint64     `json:"failed"`
	LastStartedAt  *time.Time `json:"last_started_at,omitempty"`
	LastFinishedAt *time.Time `json:"last_finished_at,omitempty"`
}

type exportObservationSnapshot struct {
	Schema     string                  `json:"schema"`
	ObservedAt time.Time               `json:"observed_at"`
	Exporters  []exportAttemptSnapshot `json:"exporters"`
	Coverage   string                  `json:"coverage"`
}

func exportObservedTime(nanos int64) *time.Time {
	if nanos == 0 {
		return nil
	}
	at := time.Unix(0, nanos).UTC()
	return &at
}

// DiagnosticExports is served only by the existing owner-only Unix socket.
// Atomic fields are independent concurrent samples, not a transactional receipt.
// Durations include encoding and network work, but not queue wait or backoff.
func (p *Pipeline) DiagnosticExports(ctx context.Context) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	names := [...]string{"metrics", "logs", "analytics", "noop", "other"}
	rows := make([]exportAttemptSnapshot, len(names))
	for i, name := range names {
		c := &p.exportObservations.stages[i]
		rows[i] = exportAttemptSnapshot{Exporter: name, Started: c.started.Load(), Completed: c.completed.Load(),
			Retries: c.retries.Load(), Inflight: c.inflight.Load(), AttemptEvents: c.events.Load(),
			TotalNanos: c.totalNanos.Load(), MaxNanos: c.maxNanos.Load(), Success: c.success.Load(),
			Deadline: c.deadline.Load(), Canceled: c.canceled.Load(), Timeout: c.timeout.Load(), Failed: c.failed.Load(),
			LastStartedAt: exportObservedTime(c.lastStarted.Load()), LastFinishedAt: exportObservedTime(c.lastFinished.Load())}
	}
	return exportObservationSnapshot{Schema: "fugue.telemetry-export-observation/v1", ObservedAt: time.Now().UTC(), Exporters: rows,
		Coverage: "independent_concurrent_process_counters_attempt_events_include_retries_not_unique_or_persisted_events_durations_exclude_queue_wait_and_backoff"}, nil
}

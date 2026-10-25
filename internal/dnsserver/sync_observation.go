package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"fugue/internal/platformconsumer"
)

type dnsSyncTraceKey struct{}

type dnsSyncTrace struct {
	id     string
	logger *log.Logger
	count  atomic.Uint32
}

type dnsSyncPhase struct {
	trace *dnsSyncTrace
	path  string
}

var dnsSyncTraceSequence atomic.Uint64

// A trace is diagnostic data only. It carries no authority, adds no deadlines,
// and never retains artifacts, proofs, credentials, or unbounded error text.
func startDNSSyncTrace(ctx context.Context, logger *log.Logger, name string) (context.Context, func(error)) {
	if logger == nil {
		return ctx, func(error) {}
	}
	trace := &dnsSyncTrace{id: fmt.Sprintf("%x-%x", time.Now().UnixNano(), dnsSyncTraceSequence.Add(1)), logger: logger}
	ctx = context.WithValue(ctx, dnsSyncTraceKey{}, dnsSyncPhase{trace: trace})
	return observeDNSSyncPhase(ctx, name)
}

func dnsSyncTraceIdentity(ctx context.Context) (string, string) {
	phase, ok := ctx.Value(dnsSyncTraceKey{}).(dnsSyncPhase)
	if !ok || phase.trace == nil {
		return "", ""
	}
	return phase.trace.id, phase.path
}

func observeDNSSyncPhase(ctx context.Context, name string) (context.Context, func(error)) {
	parent, ok := ctx.Value(dnsSyncTraceKey{}).(dnsSyncPhase)
	if !ok || parent.trace == nil || len(name) == 0 || len(name) > 40 || len(parent.path)+len(name) > 200 {
		return ctx, func(error) {}
	}
	path := name
	if parent.path != "" {
		path = parent.path + "/" + name
	}
	phase := dnsSyncPhase{trace: parent.trace, path: path}
	started := time.Now()
	ctx = context.WithValue(ctx, dnsSyncTraceKey{}, phase)
	var finished atomic.Bool
	return ctx, func(err error) {
		if !finished.CompareAndSwap(false, true) || phase.trace.count.Add(1) > 64 {
			return
		}
		outcome := "success"
		switch {
		case errors.Is(err, platformconsumer.ErrAssignmentChanged):
			outcome = "assignment_changed"
		case errors.Is(err, context.DeadlineExceeded):
			outcome = "deadline_exceeded"
		case errors.Is(err, context.Canceled):
			outcome = "canceled"
		case err != nil:
			outcome = "failed"
		}
		report, marshalErr := json.Marshal(struct {
			Event      string    `json:"event"`
			TraceID    string    `json:"sync_trace_id"`
			Phase      string    `json:"sync_phase"`
			StartedAt  time.Time `json:"started_at"`
			FinishedAt time.Time `json:"finished_at"`
			ElapsedMS  int64     `json:"elapsed_ms"`
			Outcome    string    `json:"outcome"`
		}{"dns_sync_phase", phase.trace.id, path, started.UTC(), time.Now().UTC(), time.Since(started).Milliseconds(), outcome})
		if marshalErr == nil {
			phase.trace.logger.Print(string(report))
		}
	}
}

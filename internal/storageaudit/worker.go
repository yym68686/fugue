package storageaudit

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

const workerInterval = 15 * time.Second

type auditStore interface {
	RunPass(context.Context) (PassResult, error)
	ReadProgress(context.Context) (Progress, error)
}

type Snapshot struct {
	Progress    Progress
	ReadAt      time.Time
	Unavailable bool
}

type Worker struct {
	store  auditStore
	status atomic.Pointer[Snapshot]
}

func NewWorker(store auditStore) *Worker { return &Worker{store: store} }

func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		w.refresh(ctx)
		timer := time.NewTimer(workerInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (w *Worker) refresh(ctx context.Context) {
	progress, readErr := w.store.ReadProgress(ctx)
	var passErr error
	if readErr == nil {
		var result PassResult
		result, passErr = w.store.RunPass(ctx)
		if passErr == nil && (result == Sampled || result == ReadFailed) {
			progress, readErr = w.store.ReadProgress(ctx)
		}
	}
	prior := w.status.Load()
	next := Snapshot{Progress: progress, ReadAt: time.Now(), Unavailable: readErr != nil || passErr != nil}
	if readErr != nil {
		next.ReadAt = time.Time{}
		if prior != nil {
			next.Progress, next.ReadAt = prior.Progress, prior.ReadAt
		}
	}
	w.status.Store(&next)
}

func (w *Worker) Ready(now time.Time) bool {
	return w.status.Load().ready(now)
}

func (s *Snapshot) ready(now time.Time) bool {
	return s != nil && !s.Unavailable && !s.ReadAt.IsZero() && now.Sub(s.ReadAt) < 3*workerInterval
}

// WriteMetrics uses only the last locally cached aggregate. Neither scrapes
// nor readiness can cause an observation query or increase business load.
func (w *Worker) WriteMetrics(output io.Writer) {
	gauge := func(metric, help string, value float64) {
		fmt.Fprintf(output, "# HELP %s %s\n# TYPE %s gauge\n%s %g\n", metric, help, metric, metric, value)
	}
	flag := func(value bool) float64 {
		if value {
			return 1
		}
		return 0
	}
	s := w.status.Load()
	now := time.Now()
	ready := s.ready(now)
	gauge("fugue_storage_audit_progress_available", "Whether the independently polled audit capability is available.", flag(ready))
	if s == nil {
		return
	}
	p := s.Progress
	gauge("fugue_storage_audit_executor_owner", "Whether the durable owner is the storage audit worker.", flag(p.Owner == "worker"))
	gauge("fugue_storage_audit_committed_passes", "Durably completed bounded pages; not a global inventory.", float64(p.Completed))
	gauge("fugue_storage_audit_read_failures", "Durably recorded bounded reads that did not publish a page.", float64(p.Failures))
	gauge("fugue_storage_audit_last_read_failed", "Whether the last recorded audit failed without advancing its page.", flag(p.LastFailed))
	if p.LastAttemptAt.Valid {
		gauge("fugue_storage_audit_last_attempt_timestamp_seconds", "Last recorded bounded audit attempt.", float64(p.LastAttemptAt.Time.Unix()))
	}
	if p.Owner != "worker" {
		return
	}
	o := p.Observation()
	o.PolicyReadFailed = !ready
	o.LastRunFailed = o.LastRunFailed || !ready
	WriteLifecycleMetrics(output, o, now)
	if p.Sample != nil {
		gauge("fugue_storage_audit_sample_current_policy", "Whether the last successful sample belongs to the current policy.", flag(p.Sample.PolicySHA256 == p.PolicySHA256))
		gauge("fugue_storage_audit_last_success_timestamp_seconds", "Last successfully committed bounded audit.", float64(p.Sample.ObservedAt.Unix()))
	}
}

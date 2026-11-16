package observability

import (
	"sync"
	"time"
)

// Keep only source positions, never payloads. An admission is not a delivery
// acknowledgement. Pending/failed records bound the durable replay frontier.
type logDeliveryTracker struct {
	mu       sync.Mutex
	next     uint64
	pending  map[uint64]*logDeliveryReceipt
	inflight map[string]int
	failed   map[string]time.Time
}

type logDeliveryReceipt struct {
	tracker *logDeliveryTracker
	id      uint64
	source  string
	at      time.Time
}

func (t *logDeliveryTracker) begin(source string, at time.Time) *logDeliveryReceipt {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending == nil {
		t.pending = map[uint64]*logDeliveryReceipt{}
		t.failed = map[string]time.Time{}
		t.inflight = map[string]int{}
	}
	t.next++
	r := &logDeliveryReceipt{tracker: t, id: t.next, source: source, at: at}
	t.pending[r.id] = r
	t.inflight[source]++
	return r
}

func (r *logDeliveryReceipt) finish(success bool) {
	if r == nil {
		return
	}
	t := r.tracker
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending[r.id] != r {
		return
	}
	delete(t.pending, r.id)
	t.inflight[r.source]--
	if t.inflight[r.source] == 0 {
		delete(t.inflight, r.source)
	}
	if !success {
		t.failed[r.source] = earlierLogTime(t.failed[r.source], r.at)
	}
}

// A rejected admission remains behind the in-memory cursor; it is not an
// exporter failure and must not create a separate replay attempt.
func (r *logDeliveryReceipt) discard() { r.finish(true) }

func earlierLogTime(a, b time.Time) time.Time {
	if a.IsZero() || !b.IsZero() && b.Before(a) {
		return b
	}
	return a
}

func (t *logDeliveryTracker) floors() map[string]time.Time {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]time.Time, len(t.failed))
	for source, at := range t.failed {
		out[source] = at
	}
	for _, r := range t.pending {
		out[r.source] = earlierLogTime(out[r.source], r.at)
	}
	return out
}

// Retry failed exports through the already bounded source collector. Wait for
// the source's other in-flight batches before resetting its inclusive cursor.
func (t *logDeliveryTracker) takeFailure(source string) time.Time {
	if t == nil {
		return time.Time{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inflight[source] > 0 {
		return time.Time{}
	}
	at := t.failed[source]
	delete(t.failed, source)
	return at
}

func (t *logDeliveryTracker) sourceDelivered(source string) bool {
	if t == nil {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inflight[source] == 0 && t.failed[source].IsZero()
}

func (t *logDeliveryTracker) pruneFailures(active map[string]kubernetesLogCursor) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for source := range t.failed {
		if _, exists := active[source]; !exists && t.inflight[source] == 0 {
			delete(t.failed, source)
		}
	}
}

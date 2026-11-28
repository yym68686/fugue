package frontnetwork

import (
	"context"
	"sync"
	"time"
)

// ObservationBudget bounds local per-edge observation admission.
// The zero value is ready to use.
type ObservationBudget struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func (b *ObservationBudget) Wait(ctx context.Context, edgeID string) bool {
	delay, ok := b.reserve(edgeID, time.Now())
	if !ok || ctx.Err() != nil {
		return false
	}
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

func (b *ObservationBudget) reserve(edgeID string, now time.Time) (time.Duration, bool) {
	if !b.mu.TryLock() {
		return 0, false
	}
	defer b.mu.Unlock()
	if b.last == nil {
		b.last = map[string]time.Time{}
	}
	delay := time.Duration(0)
	if previous, found := b.last[edgeID]; found {
		// Reserve only the next slot. A future reservation already belongs to
		// one waiter; further callers cannot build a queue or consume its slot.
		if previous.After(now) {
			return 0, false
		}
		delay = max(0, previous.Add(time.Second).Sub(now))
	}
	for identity, previous := range b.last {
		if now.Sub(previous) > time.Minute {
			delete(b.last, identity)
		}
	}
	if len(b.last) >= 256 {
		return 0, false
	}
	b.last[edgeID] = now.Add(delay)
	return delay, true
}

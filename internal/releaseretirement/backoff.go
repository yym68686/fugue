package releaseretirement

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"fugue/internal/model"
)

const maxRetirementRetries = 8192

type retirementProgressKey struct{}
type retirementProgress struct{ atomic.Bool }

// NoteProgress suppresses backoff for the current attempt. It conveys no
// authority and has no effect outside a scheduled attempt.
func NoteProgress(ctx context.Context) {
	if progress, _ := ctx.Value(retirementProgressKey{}).(*retirementProgress); progress != nil {
		progress.Store(true)
	}
}

type retirementRetryKey struct{ tenant, app, release string }
type retirementRetry struct {
	version    Version
	next, used time.Time
	delay      time.Duration
}

// Scheduling hints only. A changed release, policy or app intent bypasses the
// delay. Runtime-only changes are rediscovered within the bounded retry period;
// no cached observation is used to authorize retirement or deletion.
type retirementRetries struct {
	sync.Mutex
	entries map[retirementRetryKey]retirementRetry
}

// Version is a scheduling fingerprint, not a transaction precondition.
type Version [32]byte

func IntentVersion(app model.App, stable model.AppRelease, policy model.AppTrafficPolicy) Version {
	stable.SpecSnapshot = nil
	raw, _ := json.Marshal(struct {
		Spec   model.AppSpec
		Stable model.AppRelease
		Policy model.AppTrafficPolicy
	}{app.Spec, stable, policy})
	return sha256.Sum256(raw)
}

func releaseVersion(intent Version, release model.AppRelease) Version {
	// UpdatedAt is maintained by the Store for executable snapshot changes.
	release.SpecSnapshot = nil
	raw, _ := json.Marshal(release)
	h := sha256.New()
	h.Write(intent[:])
	h.Write(raw)
	var out Version
	h.Sum(out[:0])
	return out
}

func (s *Sweep) Due(intent Version, release model.AppRelease, now time.Time) bool {
	return s.scheduler.retries.ready(s.retryKey(release), releaseVersion(intent, release), now)
}

func (s *Sweep) Defer(intent Version, release model.AppRelease, now time.Time, bound bool) {
	s.scheduler.retries.attempted(s.retryKey(release), releaseVersion(intent, release), now, bound)
}

func (s *Sweep) retryKey(release model.AppRelease) retirementRetryKey {
	return retirementRetryKey{s.key.tenant, s.key.app, release.ID}
}

func (r *retirementRetries) ready(key retirementRetryKey, version Version, now time.Time) bool {
	r.Lock()
	defer r.Unlock()
	entry, found := r.entries[key]
	return !found || entry.version != version || !now.Before(entry.next)
}

func (r *retirementRetries) attempted(key retirementRetryKey, version Version, now time.Time, bound bool) {
	r.Lock()
	defer r.Unlock()
	if r.entries == nil {
		r.entries = make(map[retirementRetryKey]retirementRetry)
	}
	entry := r.entries[key]
	delay := 10 * time.Second
	if entry.version == version {
		delay = entry.delay * 2
	}
	limit := 2 * time.Minute
	if bound {
		limit = 30 * time.Second
	}
	if delay < 10*time.Second {
		delay = 10 * time.Second
	}
	if delay > limit {
		delay = limit
	}
	if _, found := r.entries[key]; !found && len(r.entries) >= maxRetirementRetries {
		var oldestKey retirementRetryKey
		var oldest time.Time
		for k, e := range r.entries {
			if oldest.IsZero() || e.used.Before(oldest) {
				oldestKey, oldest = k, e.used
			}
		}
		delete(r.entries, oldestKey)
	}
	r.entries[key] = retirementRetry{version: version, next: now.Add(delay), used: now, delay: delay}
}

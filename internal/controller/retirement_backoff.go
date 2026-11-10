package controller

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

func noteRetirementProgress(ctx context.Context) {
	if progress, _ := ctx.Value(retirementProgressKey{}).(*retirementProgress); progress != nil {
		progress.Store(true)
	}
}

type retirementRetryKey struct{ tenant, app, release string }
type retirementRetry struct {
	version    [32]byte
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

func retirementIntentVersion(app model.App, stable model.AppRelease, policy model.AppTrafficPolicy) [32]byte {
	stable.SpecSnapshot = nil
	raw, _ := json.Marshal(struct {
		Spec   model.AppSpec
		Stable model.AppRelease
		Policy model.AppTrafficPolicy
	}{app.Spec, stable, policy})
	return sha256.Sum256(raw)
}

func retirementReleaseVersion(intent [32]byte, release model.AppRelease) [32]byte {
	// UpdatedAt is maintained by the Store for executable snapshot changes.
	release.SpecSnapshot = nil
	raw, _ := json.Marshal(release)
	h := sha256.New()
	h.Write(intent[:])
	h.Write(raw)
	var out [32]byte
	h.Sum(out[:0])
	return out
}

func (r *retirementRetries) ready(key retirementRetryKey, version [32]byte, now time.Time) bool {
	r.Lock()
	defer r.Unlock()
	entry, found := r.entries[key]
	return !found || entry.version != version || !now.Before(entry.next)
}

func (r *retirementRetries) attempted(key retirementRetryKey, version [32]byte, now time.Time, bound bool) {
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

package main

import (
	"sync"
	"time"
)

type failureLogEntry struct {
	message    string
	last       time.Time
	suppressed int
}

// Coalesce unchanged failures, not reconciliation itself: authorization and
// revocation must keep progressing even when a public consumer is unavailable.
type failureLog struct {
	mu      sync.Mutex
	entries map[string]failureLogEntry
}

func (reporter *failureLog) observe(key string, err error, now time.Time) (bool, int) {
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	previous, exists := reporter.entries[key]
	if err == nil {
		delete(reporter.entries, key)
		return exists, previous.suppressed
	}
	if exists && previous.message == err.Error() && now.Sub(previous.last) < 5*time.Minute {
		previous.suppressed++
		reporter.entries[key] = previous
		return false, 0
	}
	if reporter.entries == nil {
		reporter.entries = make(map[string]failureLogEntry)
	}
	reporter.entries[key] = failureLogEntry{message: err.Error(), last: now}
	return true, previous.suppressed
}

func (reporter *failureLog) retain(keys map[string]bool) {
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	for key := range reporter.entries {
		if !keys[key] {
			delete(reporter.entries, key)
		}
	}
}

package store

import "sync/atomic"

// Attempt counters are process-local, not proof of a committed transaction.
// Successful maintenance commits also have durable storage_events receipts.
var separatedInputStats struct {
	writes, failures, fallback, logical, newPackBytes atomic.Uint64
}

func separatedInputObservation() any {
	return map[string]uint64{
		"verified_write_attempts": separatedInputStats.writes.Load(),
		"write_failures":          separatedInputStats.failures.Load(),
		"budget_fallbacks":        separatedInputStats.fallback.Load(),
		"logical_bytes":           separatedInputStats.logical.Load(),
		"new_pack_bytes":          separatedInputStats.newPackBytes.Load(),
	}
}

package store

import (
	"sync/atomic"
	"time"
)

// Fixed aggregate counters only. Never retain digests, SQL arguments, raw
// evidence, credentials or error text in this private observation surface.
type sharedInputCounters struct {
	writes, failures, logical, newPackBytes, newObjects, reusedObjects atomic.Uint64
	writeNanos, reads, readFailures, readNanos                         atomic.Uint64
	selectiveReads, selectiveFailures, selectiveNanos                  atomic.Uint64
}

var sharedInputStats sharedInputCounters

func sharedInputObservation() any {
	s := &sharedInputStats
	return map[string]any{
		"verified_write_attempts": s.writes.Load(), "write_failures": s.failures.Load(),
		"logical_bytes": s.logical.Load(), "new_pack_bytes": s.newPackBytes.Load(),
		"new_objects": s.newObjects.Load(), "reused_objects": s.reusedObjects.Load(),
		"write_seconds": float64(s.writeNanos.Load()) / float64(time.Second),
		"reads":         s.reads.Load(), "read_failures": s.readFailures.Load(), "read_seconds": float64(s.readNanos.Load()) / float64(time.Second),
		"selective_reads": s.selectiveReads.Load(), "selective_failures": s.selectiveFailures.Load(), "selective_seconds": float64(s.selectiveNanos.Load()) / float64(time.Second),
	}
}

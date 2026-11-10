package api

import (
	"hash/fnv"
	"time"
)

// Leadership already coalesces each scope across API replicas. Repeated failed
// transactions back off without changing authority or extending any deadline.
// The outer loop still observes policy changes every 30s and resets this delay.
func platformProducerRetryDelay(scope string, failures int) time.Duration {
	if failures <= 1 {
		return 30 * time.Second
	}
	delay := 30 * time.Second * time.Duration(1<<min(failures-1, 2))
	h := fnv.New32a()
	_, _ = h.Write([]byte(scope))
	return delay + time.Duration(h.Sum32()%10000)*time.Millisecond
}

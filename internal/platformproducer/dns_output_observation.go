package platformproducer

import "time"

// DNSOutputValidationStage partitions the existing validator's wall time.
// Unvisited stages produce no observations.
type DNSOutputValidationStage uint8

const (
	DNSOutputPolicy DNSOutputValidationStage = iota
	DNSOutputFingerprint
	DNSOutputCacheLookup
	DNSOutputChildDecode
	DNSOutputAuthority
	DNSOutputPredecessorDecode
	DNSOutputPolicyComparison
	DNSOutputMembership
	DNSOutputHandoffComparison
	DNSOutputCacheStore
	DNSOutputStageCount
)

func (s DNSOutputValidationStage) String() string {
	names := [DNSOutputStageCount]string{
		"producer_policy", "fingerprint", "cache_lookup", "child_decode", "authority",
		"predecessor_decode", "policy_comparison", "membership", "handoff_comparison", "cache_store",
	}
	if s >= DNSOutputStageCount {
		return ""
	}
	return names[s]
}

type DNSOutputCacheResult uint8

const (
	DNSOutputCacheUnobserved DNSOutputCacheResult = iota
	DNSOutputCacheHit
	DNSOutputCacheMiss
	DNSOutputCacheNotApplicable
)

// DNSOutputObserver receives only fixed phases, cache outcomes and timings.
type DNSOutputObserver func(DNSOutputValidationStage, time.Duration, DNSOutputCacheResult)

func (observe DNSOutputObserver) start() time.Time {
	if observe != nil {
		return time.Now()
	}
	return time.Time{}
}

func (observe DNSOutputObserver) end(stage DNSOutputValidationStage, start time.Time, cache DNSOutputCacheResult) {
	if observe != nil {
		observe(stage, time.Since(start), cache)
	}
}

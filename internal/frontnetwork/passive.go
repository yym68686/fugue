package frontnetwork

import (
	"time"

	"fugue/internal/model"
)

const (
	PassivePath          = "/edge/passive-network-samples"
	PassiveStatsQuery    = "stats=1"
	PassiveSchema        = "public-front-passive-v1"
	PassiveInterval      = 5 * time.Minute
	PassiveBuckets       = 3
	PassiveBucketLimit   = 128
	PassivePrefixLimit   = 8
	PassiveBatchLimit    = PassiveBuckets * PassiveBucketLimit
	PassiveResponseLimit = 512 << 10
)

type PassiveBatch struct {
	Schema  string                          `json:"schema"`
	EdgeID  string                          `json:"edge_id"`
	GroupID string                          `json:"group_id"`
	Samples []model.EdgeClientNetworkSample `json:"samples"`
	Stats   *PassiveStats                   `json:"stats,omitempty"`
}

// PassiveStats contains only process-wide counters, never peer or route
// identifiers. Recent forwarding counts cover the current and previous bucket.
type PassiveStats struct {
	Eligible           uint64 `json:"eligible"`
	NoPayload          uint64 `json:"no_payload"`
	NoRTT              uint64 `json:"no_rtt"`
	Invalid            uint64 `json:"invalid"`
	LockBusy           uint64 `json:"lock_busy"`
	PrefixLimited      uint64 `json:"prefix_limited"`
	BucketLimited      uint64 `json:"bucket_limited"`
	ActiveLimited      uint64 `json:"active_limited"`
	ActiveSamples      uint64 `json:"active_samples"`
	Delivery           uint64 `json:"delivery"`
	Recorded           uint64 `json:"recorded"`
	RecentForwardOK    uint64 `json:"recent_forward_ok"`
	RecentForwardError uint64 `json:"recent_forward_error"`
}

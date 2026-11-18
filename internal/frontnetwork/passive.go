package frontnetwork

import (
	"time"

	"fugue/internal/model"
)

const (
	PassivePath          = "/edge/passive-network-samples"
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
}

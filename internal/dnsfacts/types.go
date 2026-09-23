// Package dnsfacts defines read-only DNS observations shared by executors and
// control-plane readers. An observation is not a configuration or authorization.
package dnsfacts

import (
	"time"

	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

const Schema = "fugue.dns.runtime-facts/v1"

type Probe struct {
	ProbeID string           `json:"probe_id"`
	Ready   bool             `json:"ready"`
	Proof   routeprobe.Proof `json:"proof"`
}

type Snapshot struct {
	Schema               string                           `json:"schema"`
	NodeID               string                           `json:"node_id"`
	EdgeGroupID          string                           `json:"edge_group_id"`
	Assignment           model.PlatformConsumerAssignment `json:"assignment"`
	ParentDigest         string                           `json:"parent_digest"`
	RouteArtifactID      string                           `json:"route_artifact_id"`
	PlanDigest           string                           `json:"plan_digest"`
	ObservedAt           time.Time                        `json:"observed_at"`
	EvaluatedAt          time.Time                        `json:"evaluated_at"`
	CheckpointValidUntil time.Time                        `json:"checkpoint_valid_until"`
	Ready                bool                             `json:"ready"`
	Facts                []Probe                          `json:"facts"`
}

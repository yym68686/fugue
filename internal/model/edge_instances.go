package model

import "time"

const (
	EdgeInstanceFencingSchemaV1 = "edge-instance-fencing/v1"

	EdgeSlotA      = "a"
	EdgeSlotB      = "b"
	EdgeSlotDirect = "direct"

	EdgeInstanceFailureNone             = ""
	EdgeInstanceFailureSignatureInvalid = "bundle_signature_invalid"
	EdgeInstanceFailureMaxStaleExceeded = "max_stale_exceeded"
	EdgeInstanceFailureIdentityDrift    = "identity_drift"
)

// EdgeNodeInstance is one physical edge process identity. Node remains the
// compatibility-facing node snapshot, while the surrounding identity prevents
// two blue/green slots on the same machine from sharing mutable health state.
type EdgeNodeInstance struct {
	EdgeID               string    `json:"edge_id"`
	EdgeGroupID          string    `json:"edge_group_id"`
	Slot                 string    `json:"slot"`
	InstanceUID          string    `json:"instance_uid"`
	ReleaseEpoch         string    `json:"release_epoch"`
	Node                 EdgeNode  `json:"node"`
	FailureClass         string    `json:"failure_class,omitempty"`
	EffectiveHealthy     bool      `json:"effective_healthy"`
	ConsecutiveHealthy   int       `json:"consecutive_healthy"`
	ConsecutiveUnhealthy int       `json:"consecutive_unhealthy"`
	HealthStateSince     time.Time `json:"health_state_since,omitempty"`
	LastHeartbeatAt      time.Time `json:"last_heartbeat_at"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

// EdgeActiveEpoch is the centrally fenced release identity consumed by route
// generation. Heartbeats cannot create or advance it.
type EdgeActiveEpoch struct {
	EdgeGroupID         string    `json:"edge_group_id"`
	Slot                string    `json:"slot"`
	ReleaseEpoch        string    `json:"release_epoch"`
	FenceSequence       uint64    `json:"fence_sequence"`
	MinHealthyInstances int       `json:"min_healthy_instances"`
	ActivatedAt         time.Time `json:"activated_at"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

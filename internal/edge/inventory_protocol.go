package edge

import (
	"strconv"
	"time"
)

// These wire types are the Edge Worker-owned view of the group inventory
// protocol. Keeping them local prevents the Worker binary from importing the
// Edge Control implementation while preserving the versioned JSON contract.
const (
	groupAuthorityInventoryHeartbeatPathV1   = "/v1/authority/group-inventory-heartbeats"
	authorityGroupReadyPrefixV1              = "/v1/authority/groups/"
	groupInventoryHeartbeatSchemaV1          = "edge-control-group-inventory-heartbeat/v1"
	groupInventoryHeartbeatReceiptSchemaV1   = "edge-control-group-inventory-heartbeat-receipt/v1"
	groupInventorySchemaV1                   = "edge-control-group-inventory/v1"
	inventoryPlatformIdentityKeyringSchemaV1 = "edge-inventory-platform-identity-keyring/v1"
)

type groupInventoryHeartbeat struct {
	Schema             string                 `json:"schema"`
	KeyID              string                 `json:"key_id"`
	GroupID            string                 `json:"edge_group_id"`
	ProducerNodeID     string                 `json:"producer_node_id,omitempty"`
	ProducerGeneration uint64                 `json:"producer_generation,omitempty"`
	ExpectedSequence   uint64                 `json:"expected_sequence"`
	IssuedAtUnix       int64                  `json:"issued_at_unix"`
	ExpiresAtUnix      int64                  `json:"expires_at_unix"`
	Nonce              string                 `json:"nonce"`
	Inventory          groupInventorySnapshot `json:"inventory"`
	Signature          string                 `json:"signature"`
}

type groupInventoryHeartbeatReceipt struct {
	Schema             string `json:"schema"`
	GroupID            string `json:"edge_group_id"`
	Sequence           uint64 `json:"sequence"`
	Generation         string `json:"generation"`
	InventoryDigest    string `json:"inventory_digest"`
	Authority          string `json:"authority"`
	Publication        bool   `json:"publication_enabled"`
	ProducerNodeID     string `json:"producer_node_id,omitempty"`
	ProducerGeneration uint64 `json:"producer_generation,omitempty"`
}

type groupInventorySnapshot struct {
	Schema      string           `json:"schema"`
	GroupID     string           `json:"edge_group_id"`
	Sequence    uint64           `json:"sequence"`
	Generation  string           `json:"generation"`
	ActiveEpoch groupActiveEpoch `json:"active_epoch"`
	Instances   []groupInstance  `json:"instances"`
	ObservedAt  time.Time        `json:"observed_at"`
}

type groupActiveEpoch struct {
	GroupID             string `json:"edge_group_id"`
	Slot                string `json:"slot"`
	ReleaseEpoch        string `json:"release_epoch"`
	FenceSequence       uint64 `json:"fence_sequence"`
	MinHealthyInstances int    `json:"min_healthy_instances"`
}

type groupInstance struct {
	EdgeID           string `json:"edge_id"`
	GroupID          string `json:"edge_group_id"`
	Slot             string `json:"slot"`
	InstanceUID      string `json:"instance_uid"`
	ReleaseEpoch     string `json:"release_epoch"`
	EffectiveHealthy bool   `json:"effective_healthy"`
	NodeHealthy      bool   `json:"node_healthy"`
	NodeStatus       string `json:"node_status"`
	Draining         bool   `json:"draining"`
	FailureClass     string `json:"failure_class,omitempty"`
}

type authorityGroupStatus struct {
	GroupID                     string     `json:"edge_group_id"`
	Status                      string     `json:"status"`
	Ready                       bool       `json:"ready"`
	InventorySequence           uint64     `json:"inventory_sequence,omitempty"`
	InventoryGeneration         string     `json:"inventory_generation,omitempty"`
	InventoryProducerGeneration uint64     `json:"inventory_producer_generation,omitempty"`
	InventoryProducerNodes      int        `json:"inventory_producer_nodes,omitempty"`
	InventoryHeartbeatAt        *time.Time `json:"inventory_heartbeat_at,omitempty"`
	PublicationSequence         uint64     `json:"publication_sequence,omitempty"`
	PublicationDecision         string     `json:"publication_decision,omitempty"`
	BundleGeneration            string     `json:"bundle_generation,omitempty"`
	PublishedBundleDigest       string     `json:"published_bundle_digest,omitempty"`
	RecoveryEpoch               uint64     `json:"recovery_epoch"`
	BundleValidUntil            *time.Time `json:"bundle_valid_until,omitempty"`
	LKGState                    string     `json:"lkg_state"`
	FailureCode                 string     `json:"failure_code,omitempty"`
}

func producerInventoryEnvelopeGeneration(generation uint64) string {
	return "producer-" + strconv.FormatUint(generation, 10)
}

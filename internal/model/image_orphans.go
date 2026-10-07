package model

import "time"

type ImageOrphanPolicy struct {
	SweepIntervalSeconds   int       `json:"sweep_interval_seconds"`
	MaxTargetsPerNode      int       `json:"max_targets_per_node"`
	NodeCooldownSeconds    int       `json:"node_cooldown_seconds"`
	Generation             int64     `json:"generation"`
	Mode                   string    `json:"mode"`
	RepositoryPrefixes     []string  `json:"repository_prefixes"`
	ExcludedNodes          []string  `json:"excluded_nodes,omitempty"`
	QuarantineSeconds      int       `json:"quarantine_seconds"`
	MinimumObservations    int       `json:"minimum_observations"`
	InventoryMaxAgeSeconds int       `json:"inventory_max_age_seconds"`
	UpdatedAt              time.Time `json:"updated_at"`
	UpdatedBy              string    `json:"updated_by"`
}
type ImageOrphanDecision struct {
	ID               string    `json:"id"`
	Node             string    `json:"node"`
	Repo             string    `json:"repo"`
	Target           string    `json:"target"`
	Digest           string    `json:"digest"`
	Ownership        string    `json:"ownership"`
	State            string    `json:"state"`
	PolicyGeneration int64     `json:"policy_generation"`
	Observations     int       `json:"observations"`
	FirstObservedAt  time.Time `json:"first_observed_at"`
	LastObservedAt   time.Time `json:"last_observed_at"`
	Reason           string    `json:"reason"`
	GraphHash        string    `json:"graph_hash"`
}

// BuildArtifact is independent of operation/app retention. Registration is
// required before execution. A verified immutable receipt survives failed deploys.
type BuildArtifact struct {
	ID              string     `json:"id"`
	TenantID        string     `json:"tenant_id"`
	AppID           string     `json:"app_id"`
	OperationID     string     `json:"operation_id"`
	JobName         string     `json:"job_name"`
	ImageRef        string     `json:"image_ref"`
	Digest          string     `json:"digest,omitempty"`
	CacheEndpoint   string     `json:"cache_endpoint,omitempty"`
	ClusterNodeName string     `json:"cluster_node_name,omitempty"`
	RegisteredAt    time.Time  `json:"registered_at"`
	VerifiedAt      *time.Time `json:"verified_at,omitempty"`
}

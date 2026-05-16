package model

import "time"

type ServicePersistentStorageOverride struct {
	StorageSize string `json:"storage_size,omitempty"`
}

type OperationDiagnosis struct {
	Category         string                      `json:"category"`
	Summary          string                      `json:"summary"`
	Hint             string                      `json:"hint,omitempty"`
	AppName          string                      `json:"app_name,omitempty"`
	Service          string                      `json:"service,omitempty"`
	Evidence         []string                    `json:"evidence,omitempty"`
	BuilderPlacement *BuilderPlacementInspection `json:"builder_placement,omitempty"`
	DependencyChain  []string                    `json:"dependency_chain,omitempty"`
	BlockedBy        []OperationDiagnosisBlocker `json:"blocked_by,omitempty"`
	ControllerLane   *OperationControllerLane    `json:"controller_lane,omitempty"`
}

type OperationDiagnosisBlocker struct {
	OperationID string `json:"operation_id"`
	AppID       string `json:"app_id,omitempty"`
	AppName     string `json:"app_name,omitempty"`
	Service     string `json:"service,omitempty"`
	Type        string `json:"type,omitempty"`
	Status      string `json:"status,omitempty"`
}

type OperationControllerLane struct {
	Lane                      string                            `json:"lane"`
	QueuePosition             int                               `json:"queue_position,omitempty"`
	Active                    []OperationControllerLaneOccupant `json:"active,omitempty"`
	EstimatedSecondsRemaining *int                              `json:"estimated_seconds_remaining,omitempty"`
	MedianCompletedSeconds    *int                              `json:"median_completed_seconds,omitempty"`
	SampleSize                int                               `json:"sample_size,omitempty"`
}

type OperationControllerLaneOccupant struct {
	OperationID               string                             `json:"operation_id"`
	AppID                     string                             `json:"app_id,omitempty"`
	AppName                   string                             `json:"app_name,omitempty"`
	Service                   string                             `json:"service,omitempty"`
	Type                      string                             `json:"type,omitempty"`
	Status                    string                             `json:"status,omitempty"`
	StartedAt                 *time.Time                         `json:"started_at,omitempty"`
	ElapsedSeconds            int64                              `json:"elapsed_seconds,omitempty"`
	EstimatedSecondsRemaining *int                               `json:"estimated_seconds_remaining,omitempty"`
	ControllerTimingSegments  []OperationControllerTimingSegment `json:"controller_timing_segments,omitempty"`
}

type BuilderResourceSnapshot struct {
	CPUMilli       int64 `json:"cpu_milli,omitempty"`
	MemoryBytes    int64 `json:"memory_bytes,omitempty"`
	EphemeralBytes int64 `json:"ephemeral_bytes,omitempty"`
}

type BuilderPlacementInspection struct {
	Profile            string                                  `json:"profile,omitempty"`
	BuildStrategy      string                                  `json:"build_strategy,omitempty"`
	RequiredNodeLabels map[string]string                       `json:"required_node_labels,omitempty"`
	Demand             BuilderResourceSnapshot                 `json:"demand"`
	Reservations       []BuilderPlacementReservationInspection `json:"reservations,omitempty"`
	Locks              []BuilderPlacementLockInspection        `json:"locks,omitempty"`
	Nodes              []BuilderPlacementNodeInspection        `json:"nodes,omitempty"`
}

type BuilderPlacementReservationInspection struct {
	Name      string                  `json:"name"`
	NodeName  string                  `json:"node_name,omitempty"`
	RenewedAt *time.Time              `json:"renewed_at,omitempty"`
	ExpiresAt *time.Time              `json:"expires_at,omitempty"`
	Demand    BuilderResourceSnapshot `json:"demand"`
}

type BuilderPlacementLockInspection struct {
	Name           string     `json:"name"`
	NodeName       string     `json:"node_name,omitempty"`
	HolderIdentity string     `json:"holder_identity,omitempty"`
	RenewedAt      *time.Time `json:"renewed_at,omitempty"`
	ExpiresAt      *time.Time `json:"expires_at,omitempty"`
}

type BuilderPlacementNodeInspection struct {
	NodeName     string                  `json:"node_name"`
	Hostname     string                  `json:"hostname,omitempty"`
	NodeMode     string                  `json:"node_mode,omitempty"`
	Ready        bool                    `json:"ready"`
	DiskPressure bool                    `json:"disk_pressure"`
	Eligible     bool                    `json:"eligible"`
	Rank         int                     `json:"rank,omitempty"`
	Reasons      []string                `json:"reasons,omitempty"`
	Allocatable  BuilderResourceSnapshot `json:"allocatable,omitempty"`
	Used         BuilderResourceSnapshot `json:"used,omitempty"`
	Reserved     BuilderResourceSnapshot `json:"reserved,omitempty"`
	SafetyBuffer BuilderResourceSnapshot `json:"safety_buffer,omitempty"`
	Available    BuilderResourceSnapshot `json:"available,omitempty"`
	Remaining    BuilderResourceSnapshot `json:"remaining,omitempty"`
}

package model

import "time"

type ProjectHostRouteEdit struct {
	Hostname    string            `json:"hostname"`
	Paths       map[string]string `json:"paths,omitempty"`
	RemovePaths []string          `json:"remove_paths,omitempty"`
}

type ProjectHostRoutePlan struct {
	PlanID             string               `json:"plan_id"`
	ProjectID          string               `json:"project_id"`
	Edit               ProjectHostRouteEdit `json:"edit"`
	HostnameOwnerAppID string               `json:"hostname_owner_app_id"`
	Before             map[string]string    `json:"before"`
	After              map[string]string    `json:"after"`
	State              string               `json:"state"`
}

type ProjectHostRoutePublicationStatus struct {
	State               string                          `json:"state"`
	Reason              string                          `json:"reason"`
	ReleaseID           string                          `json:"release_id,omitempty"`
	HostCompileConflict *ProjectHostCompileConflict     `json:"host_compile_conflict,omitempty"`
	HostPreparation     *ProjectHostPreparationEvidence `json:"host_preparation,omitempty"`
	HostSelection       *ProjectHostSelectionEvidence   `json:"host_selection,omitempty"`
	ApplyPreflight      *ProjectHostRoutePreflight      `json:"apply_preflight,omitempty"`
}

type ProjectHostCompileConflict struct {
	ParentReleaseID    string    `json:"parent_release_id"`
	ObservedAt         time.Time `json:"observed_at"`
	Target             string    `json:"target"`
	RecordType         string    `json:"record_type"`
	Field              string    `json:"field"`
	ExpectedAddresses  int       `json:"expected_addresses"`
	CandidateAddresses int       `json:"candidate_addresses"`
}

type ProjectHostPreparationEvidence struct {
	DraftID         string                          `json:"draft_id"`
	Sequence        int64                           `json:"sequence"`
	ParentReleaseID string                          `json:"parent_release_id"`
	Blockers        []ProjectHostPreparationBlocker `json:"blockers,omitempty"`
}

type ProjectHostPreparationBlocker struct {
	Code         string `json:"code"`
	NodeID       string `json:"node_id,omitempty"`
	ArtifactKind string `json:"artifact_kind,omitempty"`
	Capability   string `json:"capability,omitempty"`
}

type ProjectHostSelectionEvidence struct {
	Decision      string                     `json:"decision"`
	Fence         int64                      `json:"fence"`
	Phase         string                     `json:"phase"`
	Required      int                        `json:"required"`
	Passing       int                        `json:"passing"`
	EvidenceUntil *time.Time                 `json:"evidence_until,omitempty"`
	Members       []ProjectHostMemberReceipt `json:"members,omitempty"`
}

type ProjectHostMemberReceipt struct {
	NodeID       string `json:"node_id"`
	ArtifactKind string `json:"artifact_kind"`
	Zone         string `json:"zone,omitempty"`
	State        string `json:"state"`
}

type ProjectHostRoutePreflight struct {
	State  string `json:"state"`
	Reason string `json:"reason"`
}

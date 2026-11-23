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
	State          string                        `json:"state"`
	Reason         string                        `json:"reason"`
	ReleaseID      string                        `json:"release_id,omitempty"`
	HostSelection  *ProjectHostSelectionEvidence `json:"host_selection,omitempty"`
	ApplyPreflight *ProjectHostRoutePreflight    `json:"apply_preflight,omitempty"`
}

type ProjectHostSelectionEvidence struct {
	Decision      string     `json:"decision"`
	Fence         int64      `json:"fence"`
	Phase         string     `json:"phase"`
	Required      int        `json:"required"`
	Passing       int        `json:"passing"`
	EvidenceUntil *time.Time `json:"evidence_until,omitempty"`
}

type ProjectHostRoutePreflight struct {
	State  string `json:"state"`
	Reason string `json:"reason"`
}

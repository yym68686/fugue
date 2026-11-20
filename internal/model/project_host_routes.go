package model

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
	State     string `json:"state"`
	Reason    string `json:"reason"`
	ReleaseID string `json:"release_id,omitempty"`
}

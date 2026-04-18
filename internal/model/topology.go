package model

type TopologyDeployPlan struct {
	Mode             string                       `json:"mode"`
	DryRun           bool                         `json:"dry_run,omitempty"`
	UpdateExisting   bool                         `json:"update_existing,omitempty"`
	DeleteMissing    bool                         `json:"delete_missing,omitempty"`
	ProjectID        string                       `json:"project_id,omitempty"`
	ProjectName      string                       `json:"project_name,omitempty"`
	PrimaryService   string                       `json:"primary_service,omitempty"`
	Services         []TopologyDeployPlanService  `json:"services,omitempty"`
	DeleteCandidates []TopologyDeployDeleteTarget `json:"delete_candidates,omitempty"`
	Warnings         []string                     `json:"warnings,omitempty"`
}

type TopologyDeployPlanService struct {
	Service         string `json:"service"`
	Kind            string `json:"kind,omitempty"`
	ServiceType     string `json:"service_type,omitempty"`
	ComposeService  string `json:"compose_service,omitempty"`
	BuildStrategy   string `json:"build_strategy,omitempty"`
	Action          string `json:"action"`
	AppID           string `json:"app_id,omitempty"`
	AppName         string `json:"app_name,omitempty"`
	ExistingAppID   string `json:"existing_app_id,omitempty"`
	ExistingAppName string `json:"existing_app_name,omitempty"`
	Hostname        string `json:"hostname,omitempty"`
	PublicURL       string `json:"public_url,omitempty"`
	InternalPort    int    `json:"internal_port,omitempty"`
}

type TopologyDeployDeleteTarget struct {
	AppID   string `json:"app_id,omitempty"`
	AppName string `json:"app_name,omitempty"`
	Service string `json:"service,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

package model

import "time"

const (
	RobustnessSeverityBlockPublish = "block_publish"
	RobustnessSeverityDegraded     = "degraded"
	RobustnessSeverityWarning      = "warning"
	RobustnessSeverityInfo         = "info"

	RobustnessIncidentStatusDetected             = "detected"
	RobustnessIncidentStatusManualActionRequired = "manual_action_required"

	RobustnessRepairPlanStatusPlanned              = "planned"
	RobustnessRepairPlanStatusManualActionRequired = "manual_action_required"
)

type RobustnessCheck struct {
	Name       string            `json:"name"`
	Pass       bool              `json:"pass"`
	Severity   string            `json:"severity"`
	Subject    string            `json:"subject,omitempty"`
	Expected   string            `json:"expected,omitempty"`
	Observed   string            `json:"observed,omitempty"`
	Evidence   map[string]string `json:"evidence,omitempty"`
	RepairHint string            `json:"repair_hint,omitempty"`
	Message    string            `json:"message,omitempty"`
}

type RobustnessIncident struct {
	ID         string            `json:"id"`
	Status     string            `json:"status"`
	Severity   string            `json:"severity"`
	Subject    string            `json:"subject"`
	CheckName  string            `json:"check_name"`
	Title      string            `json:"title"`
	Message    string            `json:"message,omitempty"`
	Expected   string            `json:"expected,omitempty"`
	Observed   string            `json:"observed,omitempty"`
	Evidence   map[string]string `json:"evidence,omitempty"`
	RepairHint string            `json:"repair_hint,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

type RobustnessStatus struct {
	GeneratedAt      time.Time                       `json:"generated_at"`
	Pass             bool                            `json:"pass"`
	BlockRollout     bool                            `json:"block_rollout"`
	Subject          string                          `json:"subject,omitempty"`
	Summary          map[string]string               `json:"summary,omitempty"`
	Checks           []RobustnessCheck               `json:"checks"`
	Incidents        []RobustnessIncident            `json:"incidents"`
	Autonomy         *PlatformAutonomyStatus         `json:"autonomy,omitempty"`
	DNS              *DNSDelegationPreflightResponse `json:"dns,omitempty"`
	RouteExplain     *RouteExplainResponse           `json:"route_explain,omitempty"`
	GeneratedSources []string                        `json:"generated_sources,omitempty"`
}

type RobustnessStatusResponse struct {
	Status RobustnessStatus `json:"status"`
}

type RobustnessIncidentListResponse struct {
	Incidents   []RobustnessIncident `json:"incidents"`
	GeneratedAt time.Time            `json:"generated_at"`
}

type RobustnessIncidentResponse struct {
	Incident RobustnessIncident `json:"incident"`
	Status   RobustnessStatus   `json:"status"`
}

type RobustnessRepairAction struct {
	Kind        string `json:"kind"`
	Subject     string `json:"subject,omitempty"`
	Description string `json:"description"`
	Command     string `json:"command,omitempty"`
	Automatic   bool   `json:"automatic"`
	Risk        string `json:"risk,omitempty"`
}

type RobustnessRepairRequest struct {
	DryRun bool `json:"dry_run"`
}

type RobustnessRepairPlan struct {
	IncidentID  string                   `json:"incident_id"`
	Status      string                   `json:"status"`
	Safe        bool                     `json:"safe"`
	DryRun      bool                     `json:"dry_run"`
	Message     string                   `json:"message,omitempty"`
	Actions     []RobustnessRepairAction `json:"actions"`
	GeneratedAt time.Time                `json:"generated_at"`
}

type RobustnessRepairPlanResponse struct {
	Plan RobustnessRepairPlan `json:"plan"`
}

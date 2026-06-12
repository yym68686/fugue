package viewmodel

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

type Tone string

const (
	ToneNeutral  Tone = "neutral"
	TonePositive Tone = "positive"
	ToneWarning  Tone = "warning"
	ToneDanger   Tone = "danger"
	ToneMuted    Tone = "muted"
)

type RoutePathView struct {
	State          State  `json:"state"`
	Hostname       string `json:"hostname,omitempty"`
	PathPrefix     string `json:"path_prefix,omitempty"`
	BaseDomain     string `json:"base_domain,omitempty"`
	PublicURL      string `json:"public_url,omitempty"`
	ServicePort    int    `json:"service_port,omitempty"`
	DomainName     string `json:"domain_name,omitempty"`
	EntrypointName string `json:"entrypoint_name,omitempty"`
	Tone           Tone   `json:"tone"`
}

type OperationTimelineView struct {
	State       State                   `json:"state"`
	Steps       []OperationTimelineStep `json:"steps,omitempty"`
	ActiveCount int                     `json:"active_count"`
	LatestID    string                  `json:"latest_id,omitempty"`
}

type OperationTimelineStep struct {
	ID        string     `json:"id"`
	AppID     string     `json:"app_id,omitempty"`
	ServiceID string     `json:"service_id,omitempty"`
	Type      string     `json:"type"`
	Status    string     `json:"status"`
	Message   string     `json:"message,omitempty"`
	Tone      Tone       `json:"tone"`
	Active    bool       `json:"active"`
	CreatedAt time.Time  `json:"created_at"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
}

type AppHealthView struct {
	State           State                 `json:"state"`
	ID              string                `json:"id,omitempty"`
	TenantID        string                `json:"tenant_id,omitempty"`
	ProjectID       string                `json:"project_id,omitempty"`
	Name            string                `json:"name,omitempty"`
	Phase           string                `json:"phase,omitempty"`
	Tone            Tone                  `json:"tone"`
	DesiredReplicas int                   `json:"desired_replicas"`
	CurrentReplicas int                   `json:"current_replicas"`
	RuntimeID       string                `json:"runtime_id,omitempty"`
	URL             string                `json:"url,omitempty"`
	LastMessage     string                `json:"last_message,omitempty"`
	LastOperationID string                `json:"last_operation_id,omitempty"`
	Route           RoutePathView         `json:"route"`
	Operations      OperationTimelineView `json:"operations"`
	ResourceUsage   *model.ResourceUsage  `json:"resource_usage,omitempty"`
}

type ServiceStageView struct {
	ID         string `json:"id,omitempty"`
	Name       string `json:"name,omitempty"`
	Type       string `json:"type,omitempty"`
	Status     string `json:"status,omitempty"`
	ProjectID  string `json:"project_id,omitempty"`
	OwnerAppID string `json:"owner_app_id,omitempty"`
	RuntimeID  string `json:"runtime_id,omitempty"`
	Tone       Tone   `json:"tone"`
}

type ProjectWorkbenchView struct {
	State          State                 `json:"state"`
	ID             string                `json:"id,omitempty"`
	TenantID       string                `json:"tenant_id,omitempty"`
	Name           string                `json:"name,omitempty"`
	Description    string                `json:"description,omitempty"`
	AppCount       int                   `json:"app_count"`
	ServiceCount   int                   `json:"service_count"`
	OperationCount int                   `json:"operation_count"`
	Apps           []AppHealthView       `json:"apps,omitempty"`
	Services       []ServiceStageView    `json:"services,omitempty"`
	Operations     OperationTimelineView `json:"operations"`
}

type RuntimeCapacityView struct {
	State           State      `json:"state"`
	ID              string     `json:"id,omitempty"`
	TenantID        string     `json:"tenant_id,omitempty"`
	Name            string     `json:"name,omitempty"`
	Type            string     `json:"type,omitempty"`
	AccessMode      string     `json:"access_mode,omitempty"`
	Status          string     `json:"status,omitempty"`
	Tone            Tone       `json:"tone"`
	ClusterNodeName string     `json:"cluster_node_name,omitempty"`
	LastSeenAt      *time.Time `json:"last_seen_at,omitempty"`
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty"`
}

type DiagnosisEvidenceView struct {
	State        State    `json:"state"`
	Category     string   `json:"category,omitempty"`
	Summary      string   `json:"summary,omitempty"`
	Hint         string   `json:"hint,omitempty"`
	Component    string   `json:"component,omitempty"`
	Scope        string   `json:"scope,omitempty"`
	Tone         Tone     `json:"tone"`
	Evidence     []string `json:"evidence,omitempty"`
	Warnings     []string `json:"warnings,omitempty"`
	NextCommands []string `json:"next_commands,omitempty"`
}

type ActionPlanView struct {
	State         State    `json:"state"`
	Action        string   `json:"action,omitempty"`
	Target        string   `json:"target,omitempty"`
	Scope         string   `json:"scope,omitempty"`
	APICall       string   `json:"api_call,omitempty"`
	OperationType string   `json:"operation_type,omitempty"`
	Risk          string   `json:"risk,omitempty"`
	RollbackHint  string   `json:"rollback_hint,omitempty"`
	Destructive   bool     `json:"destructive,omitempty"`
	ConfirmText   string   `json:"confirm_text,omitempty"`
	NextCommands  []string `json:"next_commands,omitempty"`
}

func NewRoutePathFromApp(app model.App) RoutePathView {
	if app.Route == nil {
		return RoutePathView{State: EmptyState("app has no public route"), Tone: ToneMuted}
	}
	route := *app.Route
	tone := TonePositive
	if strings.TrimSpace(route.PublicURL) == "" {
		tone = ToneWarning
	}
	return RoutePathView{
		State:          ReadyState(),
		Hostname:       strings.TrimSpace(route.Hostname),
		PathPrefix:     model.NormalizeAppRoutePathPrefix(route.PathPrefix),
		BaseDomain:     strings.TrimSpace(route.BaseDomain),
		PublicURL:      strings.TrimSpace(route.PublicURL),
		ServicePort:    route.ServicePort,
		DomainName:     strings.TrimSpace(route.DomainName),
		EntrypointName: strings.TrimSpace(route.EntrypointName),
		Tone:           tone,
	}
}

func NewOperationTimeline(operations []model.Operation) OperationTimelineView {
	if len(operations) == 0 {
		return OperationTimelineView{State: EmptyState("no operations"), Steps: nil}
	}
	sorted := append([]model.Operation(nil), operations...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].CreatedAt.Equal(sorted[j].CreatedAt) {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
	})
	steps := make([]OperationTimelineStep, 0, len(sorted))
	activeCount := 0
	for _, op := range sorted {
		active := operationActive(op.Status)
		if active {
			activeCount++
		}
		steps = append(steps, OperationTimelineStep{
			ID:        strings.TrimSpace(op.ID),
			AppID:     strings.TrimSpace(op.AppID),
			ServiceID: strings.TrimSpace(op.ServiceID),
			Type:      strings.TrimSpace(op.Type),
			Status:    strings.TrimSpace(op.Status),
			Message:   firstNonEmpty(strings.TrimSpace(op.ResultMessage), strings.TrimSpace(op.ErrorMessage)),
			Tone:      ToneForOperationStatus(op.Status),
			Active:    active,
			CreatedAt: op.CreatedAt,
			StartedAt: op.StartedAt,
			UpdatedAt: op.UpdatedAt,
			EndedAt:   op.CompletedAt,
		})
	}
	return OperationTimelineView{
		State:       ReadyState(),
		Steps:       steps,
		ActiveCount: activeCount,
		LatestID:    steps[len(steps)-1].ID,
	}
}

func NewAppHealth(app model.App, activeOperations []model.Operation) AppHealthView {
	return AppHealthView{
		State:           ReadyState(),
		ID:              strings.TrimSpace(app.ID),
		TenantID:        strings.TrimSpace(app.TenantID),
		ProjectID:       strings.TrimSpace(app.ProjectID),
		Name:            strings.TrimSpace(app.Name),
		Phase:           strings.TrimSpace(app.Status.Phase),
		Tone:            ToneForAppPhase(app.Status.Phase),
		DesiredReplicas: app.Spec.Replicas,
		CurrentReplicas: app.Status.CurrentReplicas,
		RuntimeID:       firstNonEmpty(strings.TrimSpace(app.Status.CurrentRuntimeID), strings.TrimSpace(app.Spec.RuntimeID)),
		URL:             routeURL(app),
		LastMessage:     strings.TrimSpace(app.Status.LastMessage),
		LastOperationID: strings.TrimSpace(app.Status.LastOperationID),
		Route:           NewRoutePathFromApp(app),
		Operations:      NewOperationTimeline(activeOperations),
		ResourceUsage:   app.CurrentResourceUsage,
	}
}

func NewProjectWorkbench(project model.Project, apps []model.App, services []model.BackingService, operations []model.Operation) ProjectWorkbenchView {
	appViews := make([]AppHealthView, 0, len(apps))
	for _, app := range apps {
		appViews = append(appViews, NewAppHealth(app, operationsForApp(operations, app.ID)))
	}
	serviceViews := make([]ServiceStageView, 0, len(services))
	for _, service := range services {
		runtimeID := ""
		if service.Spec.Postgres != nil {
			runtimeID = strings.TrimSpace(service.Spec.Postgres.RuntimeID)
		}
		serviceViews = append(serviceViews, ServiceStageView{
			ID:         strings.TrimSpace(service.ID),
			Name:       strings.TrimSpace(service.Name),
			Type:       strings.TrimSpace(service.Type),
			Status:     strings.TrimSpace(service.Status),
			ProjectID:  strings.TrimSpace(service.ProjectID),
			OwnerAppID: strings.TrimSpace(service.OwnerAppID),
			RuntimeID:  runtimeID,
			Tone:       ToneForGenericStatus(service.Status),
		})
	}
	state := ReadyState()
	if len(apps) == 0 && len(services) == 0 && len(operations) == 0 {
		state = EmptyState("project has no apps, services, or operations")
	}
	return ProjectWorkbenchView{
		State:          state,
		ID:             strings.TrimSpace(project.ID),
		TenantID:       strings.TrimSpace(project.TenantID),
		Name:           strings.TrimSpace(project.Name),
		Description:    strings.TrimSpace(project.Description),
		AppCount:       len(apps),
		ServiceCount:   len(services),
		OperationCount: len(operations),
		Apps:           appViews,
		Services:       serviceViews,
		Operations:     NewOperationTimeline(operations),
	}
}

func NewRuntimeCapacity(runtime model.Runtime) RuntimeCapacityView {
	return RuntimeCapacityView{
		State:           ReadyState(),
		ID:              strings.TrimSpace(runtime.ID),
		TenantID:        strings.TrimSpace(runtime.TenantID),
		Name:            strings.TrimSpace(runtime.Name),
		Type:            strings.TrimSpace(runtime.Type),
		AccessMode:      strings.TrimSpace(runtime.AccessMode),
		Status:          strings.TrimSpace(runtime.Status),
		Tone:            ToneForGenericStatus(runtime.Status),
		ClusterNodeName: strings.TrimSpace(runtime.ClusterNodeName),
		LastSeenAt:      runtime.LastSeenAt,
		LastHeartbeatAt: runtime.LastHeartbeatAt,
	}
}

func NewOperationDiagnosisEvidence(diagnosis model.OperationDiagnosis) DiagnosisEvidenceView {
	return DiagnosisEvidenceView{
		State:    ReadyState(),
		Category: strings.TrimSpace(diagnosis.Category),
		Summary:  strings.TrimSpace(diagnosis.Summary),
		Hint:     strings.TrimSpace(diagnosis.Hint),
		Scope:    firstNonEmpty(strings.TrimSpace(diagnosis.Service), strings.TrimSpace(diagnosis.AppName)),
		Tone:     ToneForDiagnosisCategory(diagnosis.Category),
		Evidence: trimStringSlice(diagnosis.Evidence),
	}
}

func NewActionPlan(action, target, scope, apiCall, operationType string, destructive bool) ActionPlanView {
	return ActionPlanView{
		State:         ReadyState(),
		Action:        strings.TrimSpace(action),
		Target:        strings.TrimSpace(target),
		Scope:         strings.TrimSpace(scope),
		APICall:       strings.TrimSpace(apiCall),
		OperationType: strings.TrimSpace(operationType),
		Destructive:   destructive,
	}
}

func ToneForAppPhase(phase string) Tone {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "ready", "live", "running", "healthy", "active":
		return TonePositive
	case "pending", "deploying", "building", "starting", "updating":
		return ToneWarning
	case "failed", "error", "degraded", "crashloop", "crash-loop":
		return ToneDanger
	case "":
		return ToneMuted
	default:
		return ToneNeutral
	}
}

func ToneForOperationStatus(status string) Tone {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "completed", "succeeded", "success":
		return TonePositive
	case "pending", "queued", "running", "in_progress", "started":
		return ToneWarning
	case "failed", "error", "cancelled", "canceled":
		return ToneDanger
	case "":
		return ToneMuted
	default:
		return ToneNeutral
	}
}

func ToneForGenericStatus(status string) Tone {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "ready", "healthy", "active", "verified", "live":
		return TonePositive
	case "pending", "provisioning", "starting", "updating":
		return ToneWarning
	case "failed", "error", "degraded", "unhealthy", "offline":
		return ToneDanger
	case "":
		return ToneMuted
	default:
		return ToneNeutral
	}
}

func ToneForDiagnosisCategory(category string) Tone {
	category = strings.ToLower(strings.TrimSpace(category))
	switch {
	case category == "", category == "available", category == "healthy":
		return TonePositive
	case strings.Contains(category, "failed"), strings.Contains(category, "crash"), strings.Contains(category, "error"):
		return ToneDanger
	default:
		return ToneWarning
	}
}

func operationActive(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pending", "queued", "running", "in_progress", "started":
		return true
	default:
		return false
	}
}

func routeURL(app model.App) string {
	if app.Route == nil {
		return ""
	}
	return strings.TrimSpace(app.Route.PublicURL)
}

func operationsForApp(operations []model.Operation, appID string) []model.Operation {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil
	}
	out := make([]model.Operation, 0)
	for _, op := range operations {
		if strings.TrimSpace(op.AppID) == appID {
			out = append(out, op)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func trimStringSlice(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

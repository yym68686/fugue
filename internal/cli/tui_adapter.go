package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/tui"
)

type tuiProvider struct {
	cli         *CLI
	client      *Client
	mu          sync.Mutex
	principal   authPrincipalContext
	principalAt time.Time
	plans       map[string]tui.Plan
}

func (p *tuiProvider) scoped(ctx context.Context) *Client {
	client := *p.client
	client.context = ctx
	client.readRetryCount = 0
	return &client
}
func (p *tuiProvider) auth(client *Client) (authPrincipalContext, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.principalAt) < time.Minute {
		return p.principal, nil
	}
	response, err := client.GetAuthContext()
	if err == nil {
		p.principal = response.Principal
		p.principalAt = time.Now()
	}
	return response.Principal, err
}
func (p *tuiProvider) Load(ctx context.Context, request tui.Request) (tui.Snapshot, error) {
	client := p.scoped(ctx)
	principal, err := p.auth(client)
	if err != nil {
		return tui.Snapshot{}, err
	}
	if request.Target.Kind == "cluster" || request.Target.Kind == "node" || request.Target.Kind == "component" {
		if !principal.PlatformAdmin {
			return tui.Snapshot{}, fmt.Errorf("platform administrator scope is required")
		}
	}
	var snapshot tui.Snapshot
	switch request.Target.Kind {
	case "app", "pod":
		snapshot, err = p.loadTUIApp(client, request, principal)
	case "operation":
		snapshot, err = p.loadTUIOperation(client, request)
	case "cluster", "node", "component":
		snapshot, err = p.loadTUICluster(client, request)
	case "runtime":
		snapshot, err = p.loadTUIRuntime(client, request)
	case "project":
		snapshot, err = p.loadTUIProject(client, request)
	default:
		snapshot, err = p.loadTUIWorkspace(client, request, principal)
	}
	snapshot.Admin = principal.PlatformAdmin
	// Keep the authentication token and known environment values out of any
	// diagnostic text that an API error or log payload might contain.
	if err != nil {
		return snapshot, fmt.Errorf("%s", redactDiagnosticString(strings.ReplaceAll(err.Error(), client.token, "[redacted]")))
	}
	data, marshalErr := json.Marshal(snapshot)
	if marshalErr != nil {
		return snapshot, marshalErr
	}
	redacted := redactDiagnosticString(string(data))
	if client.token != "" {
		redacted = strings.ReplaceAll(redacted, client.token, "[redacted]")
	}
	if err = json.Unmarshal([]byte(redacted), &snapshot); err != nil {
		return tui.Snapshot{}, err
	}
	return snapshot, nil
}
func (p *tuiProvider) Watch(ctx context.Context, target tui.Target, notify func(tui.Notice)) error {
	if target.Kind != "workspace" && target.Kind != "project" {
		return nil
	}
	client := p.scoped(ctx)
	return client.StreamConsoleGallery(true, func(event sseEvent) error {
		switch strings.TrimSpace(event.Event) {
		case "changed", "ready":
			notify(tui.Notice{})
		case "error":
			notify(tui.Notice{Err: fmt.Errorf("console event stream reported an error")})
		}
		return nil
	})
}
func tuiSource(id string, err error, at time.Time) tui.Source {
	s := tui.Source{ID: id, State: "available", ObservedAt: at}
	if err == nil {
		return s
	}
	s.State = "unavailable"
	s.Message = redactDiagnosticString(err.Error())
	var apiErr *apiServerError
	if errors.As(err, &apiErr) && (apiErr.StatusCode == 401 || apiErr.StatusCode == 403) {
		s.State = "permission_denied"
	}
	return s
}
func tuiAppTarget(app model.App) tui.Target {
	return tui.Target{Kind: "app", ID: app.ID, Name: app.Name, TenantID: app.TenantID, ProjectID: app.ProjectID}
}
func tuiAppRow(app model.App) tui.Row {
	phase, ready, runtimeID, _ := appObservedDisplayProjection(app)
	target := tuiAppTarget(app)
	return tui.Row{ID: app.ID, Cells: []string{app.Name, phase, fmt.Sprintf("%d/%d", ready, app.Spec.Replicas), runtimeID, appRouteURL(app)}, Numbers: map[int]float64{2: float64(ready)}, Target: &target, Tone: phase}
}
func operationTable(ops []model.Operation) tui.Table {
	ops = append([]model.Operation(nil), ops...)
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].UpdatedAt.After(ops[j].UpdatedAt) })
	table := tui.Table{ID: "operations", Title: "Operations", Columns: []string{"Operation", "Status", "Type", "Updated"}}
	for _, op := range ops {
		target := tui.Target{Kind: "operation", ID: op.ID, Name: op.Type, TenantID: op.TenantID}
		table.Rows = append(table.Rows, tui.Row{ID: op.ID, Cells: []string{op.ID, op.Status, op.Type, op.UpdatedAt.Local().Format("15:04:05")}, Target: &target, Tone: op.Status})
	}
	return table
}
func (p *tuiProvider) loadTUIWorkspace(client *Client, request tui.Request, principal authPrincipalContext) (tui.Snapshot, error) {
	s := tui.Snapshot{Target: request.Target, Title: "Workspace", Status: "ready", ObservedAt: time.Now()}
	if request.Section != "overview" && request.Section != "" {
		return s, nil
	}
	tenantID, projectID, err := p.cli.resolveFilterSelections(client)
	if err != nil {
		return s, err
	}
	projects, err := client.ListProjects(tenantID)
	if err != nil {
		return s, err
	}
	table := tui.Table{ID: "projects", Title: "Projects", Columns: []string{"Project", "Tenant", "Description"}}
	for _, project := range projects {
		if projectID != "" && project.ID != projectID {
			continue
		}
		target := tui.Target{Kind: "project", ID: project.ID, Name: project.Name, TenantID: project.TenantID, ProjectID: project.ID}
		table.Rows = append(table.Rows, tui.Row{ID: project.ID, Cells: []string{project.Name, project.TenantID, project.Description}, Target: &target})
	}
	s.Tables = []tui.Table{table}
	s.Fields = []tui.Field{{Label: "Projects", Value: strconv.Itoa(len(table.Rows))}, {Label: "Role", Value: "tenant"}}
	if principal.PlatformAdmin {
		s.Fields[1].Value = "platform administrator"
	}
	s.Sources = []tui.Source{tuiSource("projects", nil, s.ObservedAt)}
	return s, nil
}
func (p *tuiProvider) loadTUIProject(client *Client, request tui.Request) (tui.Snapshot, error) {
	s := tui.Snapshot{Target: request.Target, Title: request.Target.Name, Status: "ready", ObservedAt: time.Now()}
	if request.Section != "overview" && request.Section != "" {
		return s, nil
	}
	id := request.Target.ID
	if id == "" {
		var err error
		id, err = resolveProjectReference(client, request.Target.TenantID, "", request.Target.Name)
		if err != nil {
			return s, err
		}
	}
	detail, err := client.GetConsoleProjectWithLiveStatus(id, true)
	if err != nil {
		return s, err
	}
	s.Target.ID = id
	s.Target.ProjectID = id
	s.Title = detail.ProjectName
	s.Target.Name = detail.ProjectName
	table := tui.Table{ID: "apps", Title: "Applications", Columns: []string{"Application", "Status", "Ready", "Runtime", "Route"}}
	for _, app := range detail.Apps {
		table.Rows = append(table.Rows, tuiAppRow(app))
	}
	s.Tables = []tui.Table{table, operationTable(detail.Operations)}
	s.Fields = []tui.Field{{Label: "Applications", Value: strconv.Itoa(len(detail.Apps))}, {Label: "Operations", Value: strconv.Itoa(len(detail.Operations))}}
	s.Sources = []tui.Source{tuiSource("project", nil, s.ObservedAt)}
	return s, nil
}
func (p *tuiProvider) loadTUIApp(client *Client, request tui.Request, principal authPrincipalContext) (tui.Snapshot, error) {
	s := tui.Snapshot{Target: request.Target, Title: request.Target.Name, Status: "loading", ObservedAt: time.Now()}
	id := request.Target.ID
	if request.Target.Kind == "pod" {
		id = request.Target.ProjectID
	}
	if id == "" {
		app, err := p.cli.resolveNamedApp(client, request.Target.Name)
		if err != nil {
			return s, err
		}
		id = app.ID
	}
	if request.Section == "logs" {
		pod := ""
		if request.Target.Kind == "pod" {
			pod = request.Target.ID
		}
		logs, err := client.GetRuntimeLogs(id, runtimeLogsOptions{Component: "app", Pod: pod, TailLines: 300})
		s.Sources = []tui.Source{tuiSource("logs", err, s.ObservedAt)}
		if err == nil {
			for _, line := range strings.Split(strings.TrimRight(logs.Logs, "\n"), "\n") {
				s.Logs = append(s.Logs, redactDiagnosticString(line))
			}
		}
		return s, nil
	}
	if request.Section == "metrics" {
		window := request.Window
		if window <= 0 {
			window = 15 * time.Minute
		}
		metrics, err := client.GetAppObservabilityMetricsTimeseries(id, appObservabilityMetricsOptions{appObservabilityWindowOptions: appObservabilityWindowOptions{Since: window.String()}}, 10)
		s.Sources = []tui.Source{tuiSource("request metrics", err, s.ObservedAt)}
		if err == nil {
			state := metrics.Source.Status
			if !metrics.Source.Available {
				state = "unavailable"
			}
			s.Sources[0].State = state
			s.Sources[0].Message = metrics.Source.Reason
			for _, metric := range metrics.Series {
				series := tui.Series{ID: metric.Name, Label: strings.ReplaceAll(metric.Name, "_", " "), Unit: metric.Unit, Source: metric.Source, State: firstNonEmptyTrimmed(metric.State, state), Interval: time.Duration(metric.IntervalSeconds) * time.Second}
				for _, point := range metric.Points {
					at, parseErr := time.Parse(time.RFC3339, point.ObservedAt)
					if parseErr == nil {
						series.Points = append(series.Points, tui.Point{At: at, Value: point.Value})
					}
				}
				s.Series = append(s.Series, series)
			}
		}
		return s, nil
	}
	app, err := client.GetApp(id)
	if err != nil {
		return s, err
	}
	s.Target = tuiAppTarget(app)
	if request.Target.Kind == "pod" {
		s.Target = request.Target
	}
	s.Title = app.Name
	s.Status = app.Status.Phase
	s.Subtitle = "Project " + app.ProjectID
	phase, ready, runtimeID, _ := appObservedDisplayProjection(app)
	s.Status = phase
	s.Fields = []tui.Field{{Label: "Ready", Value: fmt.Sprintf("%d / %d", ready, app.Spec.Replicas)}, {Label: "Runtime", Value: runtimeID}, {Label: "Image", Value: app.Spec.Image}, {Label: "Route", Value: appRouteURL(app)}, {Label: "App ID", Value: app.ID}}
	s.Sources = append(s.Sources, tuiSource("app", nil, s.ObservedAt))
	if app.CurrentResourceUsage != nil {
		usage := app.CurrentResourceUsage
		for _, v := range []struct {
			id, label, unit string
			value           *int64
		}{{"cpu", "CPU", "mCPU", usage.CPUMilliCores}, {"memory", "Memory", "bytes", usage.MemoryBytes}, {"storage", "Storage", "bytes", usage.PersistentStorageUsedBytes}} {
			if v.value != nil {
				s.Fields = append(s.Fields, tui.Field{Label: v.label + " now", Value: fmt.Sprintf("%d %s", *v.value, v.unit)})
			}
		}
	}
	pods, podErr := client.GetAppRuntimePods(id, "app")
	s.Sources = append(s.Sources, tuiSource("pods", podErr, s.ObservedAt))
	table := tui.Table{ID: "pods", Title: "Pods", Columns: []string{"Pod", "Phase", "Node", "Ready", "Restarts"}}
	if podErr == nil {
		for _, group := range pods.Groups {
			for _, pod := range group.Pods {
				target := tui.Target{Kind: "pod", ID: pod.Name, Name: pod.Name, TenantID: app.TenantID, ProjectID: app.ID}
				var restarts int64
				for _, container := range pod.Containers {
					restarts += int64(container.RestartCount)
				}
				table.Rows = append(table.Rows, tui.Row{ID: pod.Name, Cells: []string{pod.Name, pod.Phase, pod.NodeName, fmt.Sprint(pod.Ready), strconv.FormatInt(restarts, 10)}, Target: &target})
			}
		}
	}
	s.Tables = append(s.Tables, table)
	ops, opErr := client.ListOperationsFiltered(listOperationsOptions{AppID: app.ID, Limit: 50})
	s.Sources = append(s.Sources, tuiSource("operations", opErr, s.ObservedAt))
	if opErr == nil {
		s.Tables = append(s.Tables, operationTable(ops))
		for _, op := range ops {
			s.Events = append(s.Events, tui.Event{ID: op.ID, At: op.UpdatedAt, Severity: op.Status, Message: redactDiagnosticString(firstNonEmptyTrimmed(op.ResultMessage, op.ErrorMessage, op.Type))})
		}
	}
	if runtimeID != "" {
		target := tui.Target{Kind: "runtime", ID: runtimeID, Name: runtimeID}
		s.Tables = append(s.Tables, tui.Table{ID: "runtime", Title: "Runtime", Columns: []string{"Runtime", "Status"}, Rows: []tui.Row{{ID: runtimeID, Cells: []string{runtimeID, "inspect"}, Target: &target}}})
	}
	canScale := principal.PlatformAdmin
	canDeploy := principal.PlatformAdmin
	for _, scope := range principal.Scopes {
		if scope == "app.scale" || scope == "*" {
			canScale = true
		}
		if scope == "app.deploy" || scope == "*" {
			canDeploy = true
		}
	}
	s.Actions = []tui.Action{{ID: "restart", Label: "Restart application", Enabled: canDeploy, Reason: "app.deploy scope required"}, {ID: "scale", Label: "Scale replicas", Enabled: canScale, Argument: "Desired replica count", Reason: "app.scale scope required"}, {ID: "rollback", Label: "Deploy or roll back image version", Enabled: canDeploy, Argument: "Exact image reference from app image inventory", Reason: "app.deploy scope required"}}
	return s, nil
}
func (p *tuiProvider) cliTUIInterval() time.Duration { return 3 * time.Second }

func (p *tuiProvider) loadTUIOperation(client *Client, request tui.Request) (tui.Snapshot, error) {
	op, err := client.GetOperation(request.Target.ID)
	if err != nil {
		return tui.Snapshot{}, err
	}
	s := tui.Snapshot{Target: request.Target, Title: op.Type + " · " + op.ID, Status: op.Status, ObservedAt: time.Now(), Fields: []tui.Field{{Label: "Operation", Value: op.ID}, {Label: "Status", Value: op.Status}, {Label: "Result", Value: redactDiagnosticString(op.ResultMessage)}, {Label: "Error", Value: redactDiagnosticString(op.ErrorMessage)}}, Tables: []tui.Table{operationTable([]model.Operation{op})}, Sources: []tui.Source{tuiSource("operation", nil, op.UpdatedAt)}}
	if op.AppID != "" {
		target := tui.Target{Kind: "app", ID: op.AppID, Name: op.AppID}
		s.Tables = append(s.Tables, tui.Table{ID: "app", Title: "Application", Columns: []string{"App", "Status"}, Rows: []tui.Row{{ID: op.AppID, Cells: []string{op.AppID, "inspect"}, Target: &target}}})
	}
	return s, nil
}
func (p *tuiProvider) loadTUIRuntime(client *Client, request tui.Request) (tui.Snapshot, error) {
	runtimeObj, err := client.GetRuntime(request.Target.ID)
	if err != nil {
		return tui.Snapshot{}, err
	}
	s := tui.Snapshot{Target: request.Target, Title: runtimeObj.Name, Status: runtimeObj.Status, ObservedAt: time.Now(), Fields: []tui.Field{{Label: "Type", Value: runtimeObj.Type}, {Label: "Access", Value: runtimeObj.AccessMode}, {Label: "Node", Value: runtimeObj.ClusterNodeName}}, Sources: []tui.Source{tuiSource("runtime", nil, time.Now())}}
	return s, nil
}
func (p *tuiProvider) loadTUICluster(client *Client, request tui.Request) (tui.Snapshot, error) {
	s := tui.Snapshot{Target: request.Target, Title: "Cluster", Status: "ready", ObservedAt: time.Now()}
	nodes, err := client.ListClusterNodes()
	if err != nil {
		return s, err
	}
	table := tui.Table{ID: "nodes", Title: "Nodes", Columns: []string{"Node", "Status", "CPU", "Memory", "Region", "Workloads"}}
	for _, node := range nodes {
		if request.Target.Kind == "node" && node.Name != request.Target.ID {
			continue
		}
		target := tui.Target{Kind: "node", ID: node.Name, Name: node.Name}
		cpu, mem := "--", "--"
		numbers := map[int]float64{5: float64(len(node.Workloads))}
		if node.CPU != nil && node.CPU.UsagePercent != nil {
			cpu = fmt.Sprintf("%.1f%%", *node.CPU.UsagePercent)
			numbers[2] = *node.CPU.UsagePercent
		}
		if node.Memory != nil && node.Memory.UsagePercent != nil {
			mem = fmt.Sprintf("%.1f%%", *node.Memory.UsagePercent)
			numbers[3] = *node.Memory.UsagePercent
		}
		table.Rows = append(table.Rows, tui.Row{ID: node.Name, Cells: []string{node.Name, node.Status, cpu, mem, node.Region, strconv.Itoa(len(node.Workloads))}, Numbers: numbers, Target: &target})
	}
	s.Tables = []tui.Table{table}
	s.Sources = []tui.Source{tuiSource("node inventory", nil, s.ObservedAt)}
	if request.Section == "metrics" {
		return s, nil
	}
	control, controlErr := client.GetControlPlaneStatus()
	s.Sources = append(s.Sources, tuiSource("control plane", controlErr, s.ObservedAt))
	if controlErr == nil {
		components := tui.Table{ID: "components", Title: "Control plane", Columns: []string{"Component", "Status", "Ready", "Version"}}
		for _, component := range control.Components {
			target := tui.Target{Kind: "component", ID: component.DeploymentName, Name: component.Component}
			components.Rows = append(components.Rows, tui.Row{ID: component.Component, Cells: []string{component.Component, component.Status, fmt.Sprintf("%d/%d", component.ReadyReplicas, component.DesiredReplicas), component.ImageTag}, Target: &target})
		}
		s.Tables = append(s.Tables, components)
		s.Fields = append(s.Fields, tui.Field{Label: "Control plane", Value: control.Status})
	}
	runtimes, runtimeErr := client.ListRuntimes()
	s.Sources = append(s.Sources, tuiSource("runtimes", runtimeErr, s.ObservedAt))
	if runtimeErr == nil {
		rt := tui.Table{ID: "runtimes", Title: "Runtimes", Columns: []string{"Runtime", "Status", "Type", "Access", "Node"}}
		for _, r := range runtimes {
			target := tui.Target{Kind: "runtime", ID: r.ID, Name: r.Name}
			rt.Rows = append(rt.Rows, tui.Row{ID: r.ID, Cells: []string{r.Name, r.Status, r.Type, r.AccessMode, r.ClusterNodeName}, Target: &target})
		}
		s.Tables = append(s.Tables, rt)
	}
	s.Fields = append(s.Fields, tui.Field{Label: "Nodes", Value: strconv.Itoa(len(table.Rows))})
	return s, nil
}

func (p *tuiProvider) Plan(ctx context.Context, request tui.ActionRequest) (tui.Plan, error) {
	client := p.scoped(ctx)
	if request.Target.Kind != "app" {
		return tui.Plan{}, fmt.Errorf("this resource has no mutating TUI actions")
	}
	app, err := client.GetApp(request.Target.ID)
	if err != nil {
		return tui.Plan{}, err
	}
	state, err := client.GetAppRuntimeState(app.ID)
	if err != nil {
		return tui.Plan{}, err
	}
	plan := tui.Plan{ID: fmt.Sprintf("tui-%d", time.Now().UnixNano()), Request: request, Title: request.Action + " " + app.Name, Confirmation: request.Action + " " + app.Name, ExpiresAt: time.Now().Add(time.Minute), Precondition: state.DesiredSpecHash}
	plan.Effects = []string{"Target: " + app.Name + " in project " + app.ProjectID, "Uses current committed configuration", "Creates an operation; closing this view does not cancel it"}
	if request.Action == "scale" {
		plan.Effects = append(plan.Effects, "Replicas: "+strconv.Itoa(app.Spec.Replicas)+" -> "+request.Argument)
	}
	if request.Action == "rollback" {
		plan.Effects = append(plan.Effects, "Image: "+request.Argument, "Only the image is rolled back; files, env and database remain current")
	}
	p.mu.Lock()
	if p.plans == nil {
		p.plans = map[string]tui.Plan{}
	}
	for id, old := range p.plans {
		if time.Now().After(old.ExpiresAt) {
			delete(p.plans, id)
		}
	}
	p.plans[plan.ID] = plan
	p.mu.Unlock()
	return plan, nil
}
func (p *tuiProvider) Execute(ctx context.Context, plan tui.Plan) (tui.Receipt, error) {
	p.mu.Lock()
	stored, ok := p.plans[plan.ID]
	delete(p.plans, plan.ID)
	p.mu.Unlock()
	if !ok || !stored.ExpiresAt.After(time.Now()) || stored.Request != plan.Request || stored.Precondition != plan.Precondition {
		return tui.Receipt{}, fmt.Errorf("plan is missing, expired or already submitted")
	}
	client := p.scoped(ctx)
	state, err := client.GetAppRuntimeState(plan.Request.Target.ID)
	if err != nil {
		return tui.Receipt{}, err
	}
	if state.DesiredSpecHash != plan.Precondition {
		return tui.Receipt{}, fmt.Errorf("configuration changed; create a fresh plan")
	}
	var op model.Operation
	switch plan.Request.Action {
	case "restart", "redeploy":
		response, err := client.RestartAppForSpec(plan.Request.Target.ID, plan.Precondition)
		if err != nil {
			return tui.Receipt{}, err
		}
		op = response.Operation
	case "scale":
		replicas, err := strconv.Atoi(plan.Request.Argument)
		if err != nil || replicas < 0 {
			return tui.Receipt{}, fmt.Errorf("invalid replica count")
		}
		response, err := client.ScaleAppForSpec(plan.Request.Target.ID, replicas, plan.Precondition)
		if err != nil {
			return tui.Receipt{}, err
		}
		op = response.Operation
	case "rollback":
		response, err := client.RedeployAppImageForSpec(plan.Request.Target.ID, plan.Request.Argument, plan.Precondition)
		if err != nil {
			return tui.Receipt{}, err
		}
		op = response.Operation
	default:
		return tui.Receipt{}, fmt.Errorf("unsupported action")
	}
	return tui.Receipt{Operation: tui.Target{Kind: "operation", ID: op.ID, Name: op.Type}, Message: "Operation submitted: " + op.ID}, nil
}

func (p *tuiProvider) readJSON(client *Client, path string, out any) error {
	return client.doJSON(http.MethodGet, path, nil, out)
}
func tuiResourcePath(kind, id string) string { return "/v1/" + kind + "/" + url.PathEscape(id) }

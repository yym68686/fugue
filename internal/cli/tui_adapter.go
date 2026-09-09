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
	receiptJournal       string
	cli                  *CLI
	client               *Client
	mu                   sync.Mutex
	principal            authPrincipalContext
	principalAt          time.Time
	plans                map[string]tui.Plan
	uncertain            map[string]tui.Plan
	actionReceipts       bool
	actionCapabilitiesAt time.Time
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
	if request.Target.Kind == "cluster" || request.Target.Kind == "node" || request.Target.Kind == "component" || request.Target.Kind == "component-pod" || request.Target.Kind == "release" {
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
	case "cluster", "node", "component", "component-pod", "release":
		snapshot, err = p.loadTUICluster(client, request)
	case "runtime":
		snapshot, err = p.loadTUIRuntime(client, request)
	case "project":
		snapshot, err = p.loadTUIProject(client, request)
	case "workspace", "":
		snapshot, err = p.loadTUIWorkspace(client, request, principal)
	default:
		snapshot = tui.Snapshot{Target: request.Target, Title: request.Target.Name, Status: "unavailable", Fields: []tui.Field{{Label: "Resource type", Value: request.Target.Kind}, {Label: "Reason", Value: "No detail adapter is available for this resource"}}}
	}
	snapshot.Admin = principal.PlatformAdmin
	if len(snapshot.Actions) > 0 && !p.supportsActionReceipts(client) {
		for i := range snapshot.Actions {
			snapshot.Actions[i].Enabled = false
			snapshot.Actions[i].Reason = "Server upgrade required for atomic action receipts"
		}
	}
	p.mu.Lock()
	_, pending := p.uncertain[request.Target.ID]
	p.mu.Unlock()
	if pending {
		for i := range snapshot.Actions {
			snapshot.Actions[i].Enabled = false
			snapshot.Actions[i].Reason = "Resolve the previous submission receipt first"
		}
		snapshot.Actions = append(snapshot.Actions, tui.Action{ID: "recover", Label: "Check previous submission receipt", Enabled: true})
	}
	// Keep the authentication token and known environment values out of any
	// diagnostic text that an API error or log payload might contain.
	if err != nil {
		return snapshot, fmt.Errorf("%s", redactDiagnosticString(tuiRedactToken(err.Error(), client.token)))
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
func (p *tuiProvider) Watch(ctx context.Context, target tui.Target, cursor string, notify func(tui.Notice)) error {
	client := p.scoped(ctx)
	if target.Kind == "app" || target.Kind == "pod" {
		appID, pod := target.ID, ""
		if target.Kind == "pod" {
			appID, pod = target.ProjectID, target.ID
		}
		query := url.Values{"component": {"app"}, "tail_lines": {"300"}, "follow": {"true"}}
		if pod != "" {
			query.Set("pod", pod)
		}
		relative := "/v1/apps/" + url.PathEscape(appID) + "/runtime-logs/stream?" + query.Encode()
		return client.streamSSEWithOptions(relative, streamSSEOptions{Follow: true, LastEventID: cursor}, func(event sseEvent) error {
			switch event.Event {
			case "log":
				var payload logStreamLogEvent
				if err := json.Unmarshal(event.Data, &payload); err != nil {
					return err
				}
				line := payload.Line
				if client.token != "" {
					line = strings.ReplaceAll(line, client.token, "[redacted]")
				}
				notify(tui.Notice{Section: "logs", Cursor: firstNonEmptyTrimmed(event.ID, payload.Cursor), Logs: []string{redactDiagnosticString(line)}})
			case "warning":
				notify(tui.Notice{Err: fmt.Errorf("log source reported a warning; inspect runtime logs")})
			case "ready", "state":
				notify(tui.Notice{Section: "logs"})
			}
			return nil
		})
	}
	if target.Kind != "workspace" && target.Kind != "project" {
		return nil
	}
	return client.streamSSEWithOptions("/v1/console/gallery/stream?include_live_status=true", streamSSEOptions{Follow: true}, func(event sseEvent) error {
		switch event.Event {
		case "changed", "ready":
			notify(tui.Notice{Section: "overview"})
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
	s.ObservedAt = time.Time{}
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
	if request.Section == "pods" || request.Section == "operations" {
		return p.loadTUIAppResources(client, request, id, request.Section)
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
		if request.Target.Kind == "pod" {
			s.Fields = append(s.Fields, tui.Field{Label: "Stream", Value: "SSE cursor resumes on reconnect"})
		}
		return s, err
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
		return s, err
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
	if app.Spec.Resources != nil {
		r := app.Spec.Resources
		s.MetricLimits = map[string]float64{}
		if r.CPULimitMilliCores > 0 {
			s.MetricLimits["cpu"] = float64(r.CPULimitMilliCores)
		}
		if r.MemoryLimitMebibytes > 0 {
			s.MetricLimits["memory"] = float64(r.MemoryLimitMebibytes) * 1024 * 1024
		}
	}
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
	if request.Section == "" {
		for _, section := range []string{"pods", "operations"} {
			part, err := p.loadTUIAppResources(client, request, id, section)
			s.Sources = append(s.Sources, part.Sources...)
			if err == nil {
				s.Tables = append(s.Tables, part.Tables...)
				s.Events = append(s.Events, part.Events...)
			}
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
	if request.Target.Kind == "pod" {
		s.Title = request.Target.Name
		s.Actions = nil
	}
	return s, nil
}

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
	now := time.Now()
	s := tui.Snapshot{Target: request.Target, Title: runtimeObj.Name, Status: runtimeObj.Status, ObservedAt: now, Fields: []tui.Field{{Label: "Type", Value: runtimeObj.Type}, {Label: "Access", Value: runtimeObj.AccessMode}, {Label: "Node", Value: runtimeObj.ClusterNodeName}, {Label: "Endpoint", Value: runtimeObj.Endpoint}, {Label: "Last heartbeat", Value: formatOptionalTime(runtimeObj.LastHeartbeatAt)}}, Sources: []tui.Source{tuiSource("runtime", nil, now)}}
	if len(runtimeObj.Labels) > 0 {
		keys := make([]string, 0, len(runtimeObj.Labels))
		for k := range runtimeObj.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			s.Fields = append(s.Fields, tui.Field{Label: "Label " + k, Value: runtimeObj.Labels[k]})
		}
	}
	nodes, nodeErr := client.listTUINodes()
	s.Sources = append(s.Sources, tuiSource("runtime metrics", nodeErr, now))
	if nodeErr == nil {
		for _, node := range nodes {
			if node.RuntimeID == runtimeObj.ID || (runtimeObj.ClusterNodeName != "" && node.Name == runtimeObj.ClusterNodeName) {
				appendTUINodeDetails(&s, node)
				appendTUINodeSeries(&s, node)
				break
			}
		}
		s.Title = runtimeObj.Name
	}
	return s, nil
}
func (p *tuiProvider) Plan(ctx context.Context, request tui.ActionRequest) (tui.Plan, error) {
	client := p.scoped(ctx)
	if request.Target.Kind != "app" {
		return tui.Plan{}, fmt.Errorf("this resource has no mutating TUI actions")
	}
	p.mu.Lock()
	pending, uncertain := p.uncertain[request.Target.ID]
	p.mu.Unlock()
	if uncertain {
		if request.Action != "recover" {
			return tui.Plan{}, fmt.Errorf("check the previous submission receipt before creating another plan")
		}
		pending.Request.Action = "recover"
		pending.Confirmation = "check receipt " + request.Target.Name
		pending.Title = "Check submission receipt"
		pending.Effects = []string{"Read-only lookup; does not repeat the write", "Request: " + pending.ID}
		pending.ExpiresAt = time.Now().Add(time.Minute)
		return pending, nil
	}
	if !p.supportsActionReceipts(client) {
		return tui.Plan{}, fmt.Errorf("server does not support atomic action receipts; upgrade the server first")
	}
	principal, err := client.GetAuthContext()
	if err != nil {
		return tui.Plan{}, err
	}
	if request.Action != "restart" && request.Action != "scale" && request.Action != "rollback" {
		return tui.Plan{}, fmt.Errorf("unsupported action")
	}
	scope := "app.deploy"
	if request.Action == "scale" {
		scope = "app.scale"
	}
	allowed := principal.Principal.PlatformAdmin
	for _, value := range principal.Principal.Scopes {
		if value == scope || value == "*" {
			allowed = true
		}
	}
	if !allowed {
		return tui.Plan{}, fmt.Errorf("%s scope required", scope)
	}
	app, err := client.GetApp(request.Target.ID)
	if err != nil {
		return tui.Plan{}, err
	}
	state, err := client.GetAppRuntimeState(app.ID)
	if err != nil {
		return tui.Plan{}, err
	}
	plan := tui.Plan{ID: model.NewID("tui"), Request: request, Title: request.Action + " " + app.Name, Confirmation: request.Action + " " + app.Name, ExpiresAt: time.Now().Add(time.Minute), Precondition: state.DesiredSpecHash}
	plan.Effects = []string{"Receipt request: " + plan.ID, "Target: " + app.Name + " in project " + app.ProjectID, "Uses current committed configuration", "Creates an operation; closing this view does not cancel it"}
	if len(state.PendingOperations) > 0 {
		return tui.Plan{}, fmt.Errorf("an operation is active; wait before creating a plan")
	}
	if request.Action == "restart" && app.Spec.Replicas <= 0 {
		return tui.Plan{}, fmt.Errorf("disabled app cannot be restarted")
	}
	if request.Action == "scale" {
		value, err := strconv.Atoi(request.Argument)
		if err != nil || value < 0 {
			return tui.Plan{}, fmt.Errorf("replicas must be a nonnegative integer")
		}
		plan.Effects = append(plan.Effects, "Replicas: "+strconv.Itoa(app.Spec.Replicas)+" -> "+request.Argument)
	}
	if request.Action == "rollback" {
		inventory, err := client.GetAppImages(app.ID)
		if err != nil {
			return tui.Plan{}, err
		}
		found := false
		for _, version := range inventory.Versions {
			if version.ImageRef == request.Argument && version.RedeploySupported {
				found = true
				plan.ImageDigest = version.Digest
				plan.Effects = append(plan.Effects, "Image digest: "+version.Digest)
				break
			}
		}
		if !found || len(plan.ImageDigest) != 71 || !strings.HasPrefix(plan.ImageDigest, "sha256:") {
			return tui.Plan{}, fmt.Errorf("image with a verified digest is not available in the app image inventory")
		}
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
func (p *tuiProvider) Execute(ctx context.Context, plan tui.Plan) (receipt tui.Receipt, resultErr error) {
	defer func() {
		if resultErr == nil {
			return
		}
		var apiErr *apiServerError
		if errors.As(resultErr, &apiErr) && apiErr.StatusCode >= 400 && apiErr.StatusCode < 500 && plan.Request.Action != "recover" {
			p.mu.Lock()
			delete(p.uncertain, plan.Request.Target.ID)
			_ = p.saveReceiptsLocked()
			p.mu.Unlock()
		}
		resultErr = fmt.Errorf("%s", redactDiagnosticString(tuiRedactToken(resultErr.Error(), p.client.token)))
	}()
	if plan.Request.Action == "recover" {
		p.mu.Lock()
		pending, ok := p.uncertain[plan.Request.Target.ID]
		p.mu.Unlock()
		if !ok {
			return tui.Receipt{}, fmt.Errorf("no unresolved submission")
		}
		client := p.scoped(ctx)
		var response operationResponse
		err := client.doJSON(http.MethodGet, "/v1/apps/"+url.PathEscape(pending.Request.Target.ID)+"/action-requests/"+url.PathEscape(pending.ID), nil, &response)
		if err != nil {
			return tui.Receipt{Unknown: true}, fmt.Errorf("receipt not confirmed; request %s: %w", pending.ID, err)
		}
		p.mu.Lock()
		delete(p.uncertain, pending.Request.Target.ID)
		_ = p.saveReceiptsLocked()
		p.mu.Unlock()
		return tui.Receipt{Operation: tui.Target{Kind: "operation", ID: response.Operation.ID, Name: response.Operation.Type}, Message: "Recovered operation " + response.Operation.ID}, nil
	}
	p.mu.Lock()
	stored, ok := p.plans[plan.ID]
	delete(p.plans, plan.ID)
	p.mu.Unlock()
	if !ok || !stored.ExpiresAt.After(time.Now()) || stored.Request != plan.Request || stored.Precondition != plan.Precondition || stored.ImageDigest != plan.ImageDigest {
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
	p.mu.Lock()
	if p.uncertain == nil {
		p.uncertain = map[string]tui.Plan{}
	}
	p.uncertain[plan.Request.Target.ID] = plan
	if err := p.saveReceiptsLocked(); err != nil {
		delete(p.uncertain, plan.Request.Target.ID)
		p.mu.Unlock()
		return tui.Receipt{}, err
	}
	p.mu.Unlock()
	var op model.Operation
	switch plan.Request.Action {
	case "restart":
		response, err := client.RestartAppForSpec(plan.Request.Target.ID, plan.Precondition, plan.ID)
		if err != nil {
			return tui.Receipt{}, err
		}
		op = response.Operation
	case "scale":
		replicas, err := strconv.Atoi(plan.Request.Argument)
		if err != nil || replicas < 0 {
			return tui.Receipt{}, fmt.Errorf("invalid replica count")
		}
		response, err := client.ScaleAppForSpec(plan.Request.Target.ID, replicas, plan.Precondition, plan.ID)
		if err != nil {
			return tui.Receipt{}, err
		}
		op = response.Operation
	case "rollback":
		response, err := client.RedeployAppImageForSpec(plan.Request.Target.ID, plan.Request.Argument, plan.Precondition, plan.ID, plan.ImageDigest)
		if err != nil {
			return tui.Receipt{}, err
		}
		op = response.Operation
	default:
		return tui.Receipt{}, fmt.Errorf("unsupported action")
	}
	p.mu.Lock()
	delete(p.uncertain, plan.Request.Target.ID)
	_ = p.saveReceiptsLocked()
	p.mu.Unlock()
	return tui.Receipt{Operation: tui.Target{Kind: "operation", ID: op.ID, Name: op.Type}, Message: "Operation submitted: " + op.ID}, nil
}

func (p *tuiProvider) readJSON(client *Client, path string, out any) error {
	return client.doJSON(http.MethodGet, path, nil, out)
}
func tuiResourcePath(kind, id string) string { return "/v1/" + kind + "/" + url.PathEscape(id) }

func (p *tuiProvider) loadTUIAppResources(client *Client, request tui.Request, id, section string) (tui.Snapshot, error) {
	s := tui.Snapshot{Target: request.Target, ObservedAt: time.Now()}
	if section == "pods" {
		pods, podErr := client.GetAppRuntimePods(id, "app")
		s.Sources = append(s.Sources, tuiSource("pods", podErr, s.ObservedAt))
		table := tui.Table{ID: "pods", Title: "Pods", Columns: []string{"Pod", "Phase", "Node", "Ready", "Restarts"}}
		if podErr == nil {
			for _, group := range pods.Groups {
				for _, pod := range group.Pods {
					if request.Target.Kind == "pod" && pod.Name != request.Target.ID {
						continue
					}
					target := tui.Target{Kind: "pod", ID: pod.Name, Name: pod.Name, TenantID: request.Target.TenantID, ProjectID: id}
					var restarts int64
					for _, container := range pod.Containers {
						restarts += int64(container.RestartCount)
					}
					table.Rows = append(table.Rows, tui.Row{ID: pod.Name, Cells: []string{pod.Name, pod.Phase, pod.NodeName, fmt.Sprint(pod.Ready), strconv.FormatInt(restarts, 10)}, Target: &target})
				}
			}
		}
		if podErr == nil {
			s.Tables = append(s.Tables, table)
		}
		return s, podErr
	}
	ops, opErr := client.ListOperationsFiltered(listOperationsOptions{AppID: id, Limit: 50})
	s.Sources = append(s.Sources, tuiSource("operations", opErr, s.ObservedAt))
	if opErr == nil {
		s.Tables = append(s.Tables, operationTable(ops))
		for _, op := range ops {
			s.Events = append(s.Events, tui.Event{ID: op.ID, At: op.UpdatedAt, Severity: op.Status, Message: redactDiagnosticString(firstNonEmptyTrimmed(op.ResultMessage, op.ErrorMessage, op.Type))})
		}
	}
	return s, opErr
}

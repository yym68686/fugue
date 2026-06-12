package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	climonitor "fugue/internal/cli/monitor"
	cliterminal "fugue/internal/cli/terminal"
	"fugue/internal/model"

	xterm "golang.org/x/term"
)

type monitorOptions struct {
	Interval  time.Duration
	Once      bool
	Plain     bool
	AltScreen bool
	Filter    string
	Search    string
	Sort      string
}

func (c *CLI) shouldUseInteractiveMonitor(plain bool) bool {
	if c.wantsJSON() || plain {
		return false
	}
	mode, err := cliterminal.ParseMode(c.root.Interactive)
	if err != nil || mode == cliterminal.ModeNever {
		return false
	}
	if mode == cliterminal.ModeAlways {
		return true
	}
	file, ok := c.stdout.(*os.File)
	return ok && xterm.IsTerminal(int(file.Fd()))
}

func (c *CLI) monitorRenderer() climonitor.Renderer {
	mode, err := cliterminal.ParseMode(c.root.Color)
	if err != nil {
		mode = cliterminal.ModeAuto
	}
	level := cliterminal.DetectColorLevel(mode, c.shouldUseInteractiveMonitor(false), os.LookupEnv)
	return climonitor.NewRenderer(envIntDefault("COLUMNS", 100), cliterminal.Palette{Level: level})
}

func (c *CLI) renderMonitorSnapshot(snapshot climonitor.Snapshot) error {
	_, err := fmt.Fprint(c.stdout, c.monitorRenderer().Render(snapshot))
	return err
}

func monitorControls(opts monitorOptions) climonitor.Controls {
	return climonitor.Controls{
		Filter: strings.TrimSpace(opts.Filter),
		Search: strings.TrimSpace(opts.Search),
		Sort:   strings.TrimSpace(opts.Sort),
	}
}

func buildOperationMonitorSnapshot(op model.Operation, opts monitorOptions) climonitor.Snapshot {
	status := firstNonEmptyTrimmed(op.Status, "unknown")
	summary := []string{
		"operation_id=" + firstNonEmptyTrimmed(op.ID, "-"),
		"type=" + firstNonEmptyTrimmed(op.Type, "-"),
		"status=" + status,
	}
	if op.AppID != "" {
		summary = append(summary, "app_id="+op.AppID)
	}
	if op.ErrorMessage != "" {
		summary = append(summary, "error="+op.ErrorMessage)
	}
	if op.ResultMessage != "" {
		summary = append(summary, "message="+op.ResultMessage)
	}
	rows := [][]string{{
		firstNonEmptyTrimmed(op.ID, "-"),
		firstNonEmptyTrimmed(op.Type, "-"),
		status,
		formatTime(op.CreatedAt),
		formatTime(op.UpdatedAt),
		firstNonEmptyTrimmed(op.ResultMessage, op.ErrorMessage, "-"),
	}}
	return climonitor.Snapshot{
		Title:      "Operation " + firstNonEmptyTrimmed(op.ID, "watch"),
		ObservedAt: time.Now().UTC(),
		Controls:   monitorControls(opts),
		Summary:    summary,
		Sections: []climonitor.Section{{
			Title:   "timeline",
			Headers: []string{"ID", "TYPE", "STATUS", "CREATED", "UPDATED", "MESSAGE"},
			Rows:    rows,
		}},
		ResumeHint: "fugue operation watch " + firstNonEmptyTrimmed(op.ID, "<operation>"),
	}
}

func buildProjectMonitorSnapshot(snapshot any, opts monitorOptions) climonitor.Snapshot {
	out := climonitor.Snapshot{
		Title:      "Projects",
		ObservedAt: time.Now().UTC(),
		Controls:   monitorControls(opts),
		ResumeHint: "fugue project watch",
	}
	switch value := snapshot.(type) {
	case consoleGalleryResponse:
		out.Summary = []string{fmt.Sprintf("projects=%d", len(value.Projects))}
		rows := make([][]string, 0, len(value.Projects))
		for _, project := range value.Projects {
			rows = append(rows, []string{
				firstNonEmptyTrimmed(project.Name, project.ID),
				firstNonEmptyTrimmed(project.Lifecycle.Label, "-"),
				firstNonEmptyTrimmed(project.Lifecycle.SyncMode, "-"),
				formatInt(project.AppCount),
				formatInt(project.ServiceCount),
				formatResourceUsageSummary(&project.ResourceUsageSnapshot),
			})
		}
		out.Sections = []climonitor.Section{{
			Title:   "projects",
			Headers: []string{"PROJECT", "LIFECYCLE", "SYNC", "APPS", "SERVICES", "USAGE"},
			Rows:    rows,
		}}
	case projectOverviewSnapshot:
		projectName := firstNonEmptyTrimmed(value.Detail.ProjectName, value.Detail.ProjectID, "project")
		out.Title = "Project " + projectName
		out.ResumeHint = "fugue project watch " + projectName
		out.Summary = []string{
			"project=" + projectName,
			"apps=" + formatInt(len(value.Detail.Apps)),
			"operations=" + formatInt(len(value.Detail.Operations)),
			"cluster_nodes=" + formatInt(len(value.Detail.ClusterNodes)),
		}
		appRows := make([][]string, 0, len(value.Detail.Apps))
		for _, app := range value.Detail.Apps {
			appRows = append(appRows, []string{
				firstNonEmptyTrimmed(app.Name, app.ID),
				firstNonEmptyTrimmed(app.Status.Phase, "-"),
				fmt.Sprintf("%d/%d", app.Status.CurrentReplicas, maxInt(app.Spec.Replicas, app.Status.CurrentReplicas)),
				firstNonEmptyTrimmed(app.Status.CurrentRuntimeID, app.Spec.RuntimeID, "-"),
				appRouteURL(app),
			})
		}
		operationRows := make([][]string, 0, len(value.Detail.Operations))
		for _, op := range value.Detail.Operations {
			operationRows = append(operationRows, []string{
				firstNonEmptyTrimmed(op.ID, "-"),
				firstNonEmptyTrimmed(op.Type, "-"),
				firstNonEmptyTrimmed(op.Status, "-"),
				firstNonEmptyTrimmed(op.AppID, "-"),
			})
		}
		out.Sections = []climonitor.Section{
			{Title: "apps", Headers: []string{"APP", "PHASE", "REPLICAS", "RUNTIME", "URL"}, Rows: appRows},
			{Title: "operations", Headers: []string{"ID", "TYPE", "STATUS", "APP"}, Rows: operationRows},
		}
	}
	return out
}

func buildClusterTopSnapshot(nodes []model.ClusterNode, runtimes []model.Runtime, controlPlane *model.ControlPlaneStatus, policySummary *model.ClusterNodePolicyStatusSummary, opts monitorOptions) climonitor.Snapshot {
	readyNodes := 0
	for _, node := range nodes {
		if strings.EqualFold(strings.TrimSpace(node.Status), "ready") {
			readyNodes++
		}
	}
	summary := []string{
		fmt.Sprintf("nodes=%d ready=%d", len(nodes), readyNodes),
		fmt.Sprintf("runtimes=%d", len(runtimes)),
	}
	if controlPlane != nil {
		summary = append(summary, "control_plane="+firstNonEmptyTrimmed(controlPlane.Status, "-"))
		if controlPlane.DeployWorkflow != nil {
			summary = append(summary, "deploy_workflow="+firstNonEmptyTrimmed(controlPlane.DeployWorkflow.Workflow, "-"))
		}
	}
	if policySummary != nil {
		summary = append(summary, fmt.Sprintf("node_policy_drifted=%d", policySummary.Drifted))
	}
	nodeRows := make([][]string, 0, len(nodes))
	for _, node := range nodes {
		nodeRows = append(nodeRows, []string{
			firstNonEmptyTrimmed(node.Name, "-"),
			firstNonEmptyTrimmed(node.Status, "-"),
			firstNonEmptyTrimmed(node.Region, node.Zone, "-"),
			firstNonEmptyTrimmed(node.RuntimeID, "-"),
			percentValue(node.CPU),
			percentValue(node.Memory),
			clusterNodePolicyLabel(node.Policy),
		})
	}
	runtimeRows := make([][]string, 0, len(runtimes))
	for _, runtimeObj := range runtimes {
		runtimeRows = append(runtimeRows, []string{
			firstNonEmptyTrimmed(runtimeObj.Name, runtimeObj.ID),
			firstNonEmptyTrimmed(runtimeObj.Status, "-"),
			firstNonEmptyTrimmed(runtimeObj.Type, "-"),
			firstNonEmptyTrimmed(runtimeObj.AccessMode, "-"),
			firstNonEmptyTrimmed(runtimeObj.ClusterNodeName, "-"),
		})
	}
	sections := []climonitor.Section{
		{Title: "cluster nodes", Headers: []string{"NODE", "STATUS", "REGION", "RUNTIME", "CPU", "MEM", "POLICY"}, Rows: nodeRows},
		{Title: "runtime capacity", Headers: []string{"RUNTIME", "STATUS", "TYPE", "ACCESS", "NODE"}, Rows: runtimeRows},
	}
	if controlPlane != nil {
		componentRows := make([][]string, 0, len(controlPlane.Components))
		for _, component := range controlPlane.Components {
			componentRows = append(componentRows, []string{
				firstNonEmptyTrimmed(component.Component, component.DeploymentName),
				firstNonEmptyTrimmed(component.Status, "-"),
				fmt.Sprintf("%d/%d", component.ReadyReplicas, component.DesiredReplicas),
				firstNonEmptyTrimmed(component.ImageTag, component.Image, "-"),
			})
		}
		sections = append(sections, climonitor.Section{Title: "control plane", Headers: []string{"COMPONENT", "STATUS", "READY", "VERSION"}, Rows: componentRows})
	}
	return climonitor.Snapshot{
		Title:      "Admin cluster top",
		ObservedAt: time.Now().UTC(),
		Controls:   monitorControls(opts),
		Summary:    summary,
		Sections:   sections,
		ResumeHint: "fugue admin cluster top",
	}
}

func appRouteURL(app model.App) string {
	if app.Route == nil {
		return "-"
	}
	return firstNonEmptyTrimmed(app.Route.PublicURL, app.Route.Hostname, "-")
}

func percentValue(stats any) string {
	switch value := stats.(type) {
	case *model.ClusterNodeCPUStats:
		if value == nil || value.UsagePercent == nil {
			return "-"
		}
		return fmt.Sprintf("%.0f%%", *value.UsagePercent)
	case *model.ClusterNodeMemoryStats:
		if value == nil || value.UsagePercent == nil {
			return "-"
		}
		return fmt.Sprintf("%.0f%%", *value.UsagePercent)
	default:
		return "-"
	}
}

func clusterNodePolicyLabel(policy *model.ClusterNodePolicy) string {
	if policy == nil {
		return "-"
	}
	parts := []string{}
	if policy.EffectiveAppRuntime {
		parts = append(parts, "app")
	}
	if policy.EffectiveBuilds {
		parts = append(parts, "build")
	}
	if policy.EffectiveSharedPool {
		parts = append(parts, "shared")
	}
	if policy.EffectiveEdge {
		parts = append(parts, "edge")
	}
	if policy.EffectiveDNS {
		parts = append(parts, "dns")
	}
	if role := strings.TrimSpace(policy.EffectiveControlPlaneRole); role != "" && role != "none" {
		parts = append(parts, "cp:"+role)
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ",")
}

package cli

import (
	"fmt"
	"os"
	"strings"

	cliconsole "fugue/internal/cli/console"
	cliterminal "fugue/internal/cli/terminal"
	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newConsoleCommand() *cobra.Command {
	opts := struct {
		Project   string
		Admin     bool
		Mouse     bool
		Plain     bool
		AltScreen bool
		LogLines  int
	}{LogLines: 80}
	cmd := &cobra.Command{
		Use:   "console",
		Short: "Open the preview Fugue terminal console",
		Long: strings.TrimSpace(`
Open a preview, read-only terminal console over the same control-plane API used
by the CLI and Web console. The preview keeps existing commands as the source of
truth and does not replace JSON/script workflows.
`),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			view, err := c.loadConsoleView(client, opts.Project, opts.Admin, opts.Mouse, opts.LogLines)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, view)
			}
			model := cliconsole.NewModel(view)
			if !c.shouldUseInteractiveMonitor(opts.Plain) || opts.Plain {
				_, err := fmt.Fprint(c.stdout, c.consoleRenderer().Render(model))
				return err
			}
			return cliterminal.RunWithSession(cliterminal.SessionOptions{
				Writer:         c.stdout,
				AltScreen:      opts.AltScreen,
				RawMode:        false,
				BracketedPaste: false,
				HideCursor:     opts.AltScreen,
			}, func(*cliterminal.Session) error {
				_, err := fmt.Fprint(c.stdout, c.consoleRenderer().Render(model))
				return err
			})
		},
	}
	cmd.Flags().StringVar(&opts.Project, "project", "", "Open a specific project by name or id")
	cmd.Flags().BoolVar(&opts.Admin, "admin", false, "Include admin overview data")
	cmd.Flags().BoolVar(&opts.Mouse, "mouse", false, "Enable optional mouse affordance labels")
	cmd.Flags().BoolVar(&opts.Plain, "plain", false, "Render one plain console frame and exit")
	cmd.Flags().BoolVar(&opts.AltScreen, "alt-screen", false, "Use alternate screen for the preview frame")
	cmd.Flags().IntVar(&opts.LogLines, "log-lines", opts.LogLines, "Runtime log lines to load for the selected app")
	return cmd
}

func (c *CLI) consoleRenderer() cliconsole.Renderer {
	mode, err := cliterminal.ParseMode(c.root.Color)
	if err != nil {
		mode = cliterminal.ModeAuto
	}
	level := cliterminal.DetectColorLevel(mode, c.shouldUseInteractiveMonitor(false), os.LookupEnv)
	return cliconsole.NewRenderer(envIntDefault("COLUMNS", 100), cliterminal.Palette{Level: level})
}

func (c *CLI) loadConsoleView(client *Client, projectRef string, includeAdmin bool, mouse bool, logLines int) (cliconsole.View, error) {
	view := cliconsole.View{
		State:      cliconsole.State{Kind: cliconsole.StateReady},
		Preview:    true,
		Mouse:      mouse,
		ActivePage: cliconsole.PageProjects,
		Actions: []string{
			"restart action plan required before execution",
			"redeploy action plan required before execution",
			"cancel operation action is not enabled without an OpenAPI endpoint",
		},
	}
	gallery, err := client.GetConsoleGalleryWithLiveStatus(true)
	if err != nil {
		view.State = consoleStateForError(err)
		return view, nil
	}
	view.Summary = append(view.Summary, fmt.Sprintf("projects=%d", len(gallery.Projects)))
	view.Tables = append(view.Tables, consoleProjectsTable(gallery.Projects))

	ref := strings.TrimSpace(projectRef)
	if ref == "" && len(gallery.Projects) > 0 {
		ref = firstNonEmptyTrimmed(gallery.Projects[0].ID, gallery.Projects[0].Name)
	}
	if ref != "" {
		summary, detail, _, err := c.loadConsoleProjectOverview(client, ref, true)
		if err != nil {
			view.State = consoleStateForError(err)
			return view, nil
		}
		projectName := consoleProjectName(summary, detail)
		view.Project = projectName
		view.Summary = append(view.Summary,
			"project="+projectName,
			fmt.Sprintf("apps=%d", len(detail.Apps)),
			fmt.Sprintf("operations=%d", len(detail.Operations)),
		)
		view.Tables = append(view.Tables,
			consoleAppsTable(detail.Apps),
			consoleProjectDetailTable(detail),
			consoleOperationsTable(detail.Operations),
		)
		if len(detail.Apps) > 0 {
			logs, logErr := client.GetRuntimeLogs(detail.Apps[0].ID, runtimeLogsOptions{Component: "app", TailLines: logLines})
			if logErr == nil {
				view.Logs = splitConsoleLogs(logs.Logs)
			} else {
				view.Logs = []string{"runtime logs unavailable: " + logErr.Error()}
			}
		}
	}
	runtimes, runtimeErr := client.ListRuntimes()
	if runtimeErr == nil {
		view.Tables = append(view.Tables, consoleRuntimeTable(runtimes))
	} else {
		view.Tables = append(view.Tables, cliconsole.Table{Title: string(cliconsole.PageRuntime), Headers: []string{"ERROR"}, Rows: []cliconsole.Row{{Cells: []string{runtimeErr.Error()}}}})
	}
	if includeAdmin {
		view.Tables = append(view.Tables, c.loadConsoleAdminTables(client)...)
		view.Actions = append(view.Actions, "control plane release path: GitHub Actions deploy-control-plane.yml")
	}
	return view, nil
}

func consoleProjectsTable(projects []consoleProjectSummary) cliconsole.Table {
	rows := make([]cliconsole.Row, 0, len(projects))
	for _, project := range projects {
		rows = append(rows, cliconsole.Row{Cells: []string{
			firstNonEmptyTrimmed(project.Name, project.ID),
			firstNonEmptyTrimmed(project.Lifecycle.Label, "-"),
			formatInt(project.AppCount),
			formatInt(project.ServiceCount),
		}})
	}
	return cliconsole.Table{Title: string(cliconsole.PageProjects), Headers: []string{"PROJECT", "LIFECYCLE", "APPS", "SERVICES"}, Rows: rows}
}

func consoleAppsTable(apps []model.App) cliconsole.Table {
	rows := make([]cliconsole.Row, 0, len(apps))
	for _, app := range apps {
		rows = append(rows, cliconsole.Row{Cells: []string{
			firstNonEmptyTrimmed(app.Name, app.ID),
			firstNonEmptyTrimmed(app.Status.Phase, "-"),
			fmt.Sprintf("%d/%d", app.Status.CurrentReplicas, maxInt(app.Spec.Replicas, app.Status.CurrentReplicas)),
			firstNonEmptyTrimmed(app.Status.CurrentRuntimeID, app.Spec.RuntimeID, "-"),
			appRouteURL(app),
		}})
	}
	return cliconsole.Table{Title: string(cliconsole.PageApps), Headers: []string{"APP", "PHASE", "REPLICAS", "RUNTIME", "URL"}, Rows: rows}
}

func consoleProjectDetailTable(detail consoleProjectDetailResponse) cliconsole.Table {
	projectName := firstNonEmptyTrimmed(detail.ProjectName, detail.ProjectID)
	rows := []cliconsole.Row{
		{Cells: []string{"project", projectName}},
		{Cells: []string{"project_id", detail.ProjectID}},
		{Cells: []string{"apps", formatInt(len(detail.Apps))}},
		{Cells: []string{"operations", formatInt(len(detail.Operations))}},
		{Cells: []string{"cluster_nodes", formatInt(len(detail.ClusterNodes))}},
	}
	return cliconsole.Table{Title: string(cliconsole.PageDetail), Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}

func consoleOperationsTable(operations []model.Operation) cliconsole.Table {
	rows := make([]cliconsole.Row, 0, len(operations))
	for _, op := range operations {
		rows = append(rows, cliconsole.Row{Cells: []string{
			firstNonEmptyTrimmed(op.ID, "-"),
			firstNonEmptyTrimmed(op.Type, "-"),
			firstNonEmptyTrimmed(op.Status, "-"),
			firstNonEmptyTrimmed(op.AppID, "-"),
		}})
	}
	return cliconsole.Table{Title: string(cliconsole.PageOps), Headers: []string{"ID", "TYPE", "STATUS", "APP"}, Rows: rows}
}

func consoleRuntimeTable(runtimes []model.Runtime) cliconsole.Table {
	rows := make([]cliconsole.Row, 0, len(runtimes))
	for _, runtimeObj := range runtimes {
		rows = append(rows, cliconsole.Row{Cells: []string{
			firstNonEmptyTrimmed(runtimeObj.Name, runtimeObj.ID),
			firstNonEmptyTrimmed(runtimeObj.Status, "-"),
			firstNonEmptyTrimmed(runtimeObj.Type, "-"),
			firstNonEmptyTrimmed(runtimeObj.AccessMode, "-"),
			firstNonEmptyTrimmed(runtimeObj.ClusterNodeName, "-"),
		}})
	}
	return cliconsole.Table{Title: string(cliconsole.PageRuntime), Headers: []string{"RUNTIME", "STATUS", "TYPE", "ACCESS", "NODE"}, Rows: rows}
}

func (c *CLI) loadConsoleAdminTables(client *Client) []cliconsole.Table {
	tables := []cliconsole.Table{}
	if tenants, err := client.ListTenants(); err == nil {
		rows := make([]cliconsole.Row, 0, len(tenants))
		for _, tenant := range tenants {
			rows = append(rows, cliconsole.Row{Cells: []string{firstNonEmptyTrimmed(tenant.Name, tenant.Slug, tenant.ID), tenant.Status, tenant.ID}})
		}
		tables = append(tables, cliconsole.Table{Title: string(cliconsole.PageAdmin), Headers: []string{"TENANT", "STATUS", "ID"}, Rows: rows})
	}
	if nodes, err := client.ListClusterNodes(); err == nil {
		rows := make([]cliconsole.Row, 0, len(nodes))
		for _, node := range nodes {
			rows = append(rows, cliconsole.Row{Cells: []string{firstNonEmptyTrimmed(node.Name, "-"), firstNonEmptyTrimmed(node.Status, "-"), firstNonEmptyTrimmed(node.RuntimeID, "-")}})
		}
		tables = append(tables, cliconsole.Table{Title: string(cliconsole.PageAdmin), Headers: []string{"NODE", "STATUS", "RUNTIME"}, Rows: rows})
	}
	if edge, err := client.ListEdgeNodes(""); err == nil {
		rows := make([]cliconsole.Row, 0, len(edge.Nodes))
		for _, node := range edge.Nodes {
			rows = append(rows, cliconsole.Row{Cells: []string{firstNonEmptyTrimmed(node.ID, "-"), firstNonEmptyTrimmed(node.Status, "-"), fmt.Sprintf("%t", node.Healthy)}})
		}
		tables = append(tables, cliconsole.Table{Title: string(cliconsole.PageAdmin), Headers: []string{"EDGE", "STATUS", "HEALTHY"}, Rows: rows})
	}
	if dns, err := client.ListDNSNodes(""); err == nil {
		rows := make([]cliconsole.Row, 0, len(dns.Nodes))
		for _, node := range dns.Nodes {
			rows = append(rows, cliconsole.Row{Cells: []string{firstNonEmptyTrimmed(node.ID, "-"), firstNonEmptyTrimmed(node.Status, "-"), fmt.Sprintf("%t", node.Healthy)}})
		}
		tables = append(tables, cliconsole.Table{Title: string(cliconsole.PageAdmin), Headers: []string{"DNS", "STATUS", "HEALTHY"}, Rows: rows})
	}
	if controlPlane, err := client.GetControlPlaneStatus(); err == nil {
		rows := []cliconsole.Row{{Cells: []string{"status", controlPlane.Status}}, {Cells: []string{"version", controlPlane.Version}}, {Cells: []string{"release_path", "GitHub Actions deploy-control-plane.yml"}}}
		tables = append(tables, cliconsole.Table{Title: string(cliconsole.PageAdmin), Headers: []string{"FIELD", "VALUE"}, Rows: rows})
	}
	return tables
}

func consoleProjectName(summary *consoleProjectSummary, detail consoleProjectDetailResponse) string {
	if summary != nil {
		return firstNonEmptyTrimmed(summary.Name, summary.ID)
	}
	return firstNonEmptyTrimmed(detail.ProjectName, detail.ProjectID, "project")
}

func splitConsoleLogs(value string) []string {
	value = strings.TrimRight(value, "\n")
	if value == "" {
		return []string{"no runtime logs"}
	}
	return strings.Split(value, "\n")
}

func consoleStateForError(err error) cliconsole.State {
	if err == nil {
		return cliconsole.State{Kind: cliconsole.StateReady}
	}
	message := err.Error()
	kind := cliconsole.StateError
	if isConsolePermissionError(err) {
		kind = cliconsole.StatePermission
	} else if strings.Contains(strings.ToLower(message), "connection") || strings.Contains(strings.ToLower(message), "timeout") {
		kind = cliconsole.StateOffline
	}
	return cliconsole.State{Kind: kind, Message: message}
}

func isConsolePermissionError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "status=401") ||
		strings.Contains(message, "status=403") ||
		strings.Contains(message, "unauthorized") ||
		strings.Contains(message, "forbidden") ||
		strings.Contains(message, "permission")
}

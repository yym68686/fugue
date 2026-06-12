package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	climonitor "fugue/internal/cli/monitor"
	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newProjectOverviewCommand() *cobra.Command {
	opts := struct {
		Live      bool
		Services  bool
		Domains   bool
		Databases bool
	}{Live: true, Services: true, Domains: true, Databases: true}
	cmd := &cobra.Command{
		Use:     "overview [project]",
		Aliases: []string{"status"},
		Short:   "Show project live overview",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			if len(args) == 0 {
				gallery, err := client.GetConsoleGalleryWithLiveStatus(opts.Live)
				if err != nil {
					return err
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, gallery)
				}
				return writeConsoleProjectTable(c.stdout, gallery.Projects)
			}

			summary, detail, status, err := c.loadConsoleProjectOverview(client, args[0], opts.Live)
			if err != nil {
				return err
			}
			extras, err := loadProjectOverviewExtras(client, detail, projectOverviewExtraOptions{
				Services:  opts.Services,
				Domains:   opts.Domains,
				Databases: opts.Databases,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				payload := map[string]any{"project": detail}
				if summary != nil {
					payload["summary"] = summary
				}
				if status != nil {
					payload["status"] = status
				}
				if opts.Services {
					payload["services"] = extras.Services
				}
				if opts.Domains {
					payload["domains"] = extras.Domains
				}
				if opts.Databases {
					payload["databases"] = extras.Databases
				}
				return writeJSON(c.stdout, payload)
			}
			if c.shouldUseRichText() {
				return c.renderRichProjectWorkbench(buildProjectOverviewWorkbenchView(detail, extras.Services), buildProjectStatusDiagnosisEvidenceViews(status))
			}
			if err := renderConsoleProjectOverview(c.stdout, summary, detail, status); err != nil {
				return err
			}
			return renderProjectOverviewExtras(c.stdout, extras)
		},
	}
	cmd.Flags().BoolVar(&opts.Live, "live", opts.Live, "Include live runtime status in project snapshots")
	cmd.Flags().BoolVar(&opts.Services, "with-services", opts.Services, "Include backing service inventory")
	cmd.Flags().BoolVar(&opts.Domains, "with-domains", opts.Domains, "Include app custom domain inventory")
	cmd.Flags().BoolVar(&opts.Databases, "with-db", opts.Databases, "Include app and service database inventory")
	return cmd
}

func (c *CLI) newProjectAppsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "apps <project>",
		Short: "List apps in a project with live context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			_, detail, _, err := c.loadConsoleProjectOverview(client, args[0], true)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"apps": detail.Apps})
			}
			return writeAppTable(c.stdout, detail.Apps)
		},
	}
}

func (c *CLI) newProjectOpsCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ops <project>",
		Aliases: []string{"operations"},
		Short:   "List project operations with app context",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			_, detail, _, err := c.loadConsoleProjectOverview(client, args[0], true)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"operations": detail.Operations})
			}
			return writeOperationTableWithApps(c.stdout, detail.Operations, mapAppNames(detail.Apps))
		},
	}
}

func (c *CLI) newProjectWatchCommand() *cobra.Command {
	opts := struct {
		Interval time.Duration
		Poll     bool
		Live     bool
		Monitor  monitorOptions
	}{Interval: 5 * time.Second, Live: true, Monitor: monitorOptions{Interval: 5 * time.Second, Sort: "PROJECT"}}
	cmd := &cobra.Command{
		Use:   "watch [project]",
		Short: "Watch project overview changes",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			if opts.Monitor.Interval <= 0 {
				opts.Monitor.Interval = opts.Interval
			}
			if !c.wantsJSON() {
				if opts.Monitor.Once || !c.shouldUseInteractiveMonitor(opts.Monitor.Plain) {
					if !opts.Monitor.Once {
						c.progressf("watch_fallback=plain_snapshot reason=non_interactive")
					}
					snapshot, _, err := c.loadProjectWatchSnapshot(ctx, client, args, opts.Live)
					if err != nil {
						return err
					}
					return c.renderMonitorSnapshot(buildProjectMonitorSnapshot(snapshot, opts.Monitor))
				}
				return c.watchProjectMonitor(ctx, client, args, opts.Monitor, opts.Live)
			}
			if opts.Poll {
				return c.watchProjectPolling(ctx, client, args, opts.Interval, opts.Live)
			}
			return c.watchProjectStream(ctx, client, args, opts.Live)
		},
	}
	cmd.Flags().DurationVar(&opts.Interval, "interval", opts.Interval, "Polling interval")
	cmd.Flags().BoolVar(&opts.Poll, "poll", false, "Use polling instead of the default server-sent events stream")
	cmd.Flags().BoolVar(&opts.Live, "live", opts.Live, "Include live runtime status in project snapshots")
	cmd.Flags().BoolVar(&opts.Monitor.Once, "once", false, "Render one monitor snapshot and exit")
	cmd.Flags().BoolVar(&opts.Monitor.Plain, "plain", false, "Force plain monitor output without interactive terminal mode")
	cmd.Flags().BoolVar(&opts.Monitor.AltScreen, "alt-screen", false, "Use the alternate screen for interactive monitor output")
	cmd.Flags().StringVar(&opts.Monitor.Filter, "filter", "", "Filter monitor table rows")
	cmd.Flags().StringVar(&opts.Monitor.Search, "search", "", "Search monitor table rows")
	cmd.Flags().StringVar(&opts.Monitor.Sort, "sort", opts.Monitor.Sort, "Sort monitor table rows by column")
	return cmd
}

type projectOverviewSnapshot struct {
	Summary *consoleProjectSummary       `json:"summary,omitempty"`
	Status  *projectStatusResponse       `json:"status,omitempty"`
	Detail  consoleProjectDetailResponse `json:"detail"`
}

type projectOverviewExtraOptions struct {
	Services  bool
	Domains   bool
	Databases bool
}

type projectOverviewExtras struct {
	Services  []model.BackingService        `json:"services,omitempty"`
	Domains   []model.AppDomain             `json:"domains,omitempty"`
	Databases []projectDatabaseInventoryRow `json:"databases,omitempty"`
}

type projectDatabaseInventoryRow struct {
	AppID       string `json:"app_id,omitempty"`
	AppName     string `json:"app_name,omitempty"`
	ServiceID   string `json:"service_id,omitempty"`
	ServiceName string `json:"service_name,omitempty"`
	Status      string `json:"status,omitempty"`
	Database    string `json:"database,omitempty"`
	User        string `json:"user,omitempty"`
	RuntimeID   string `json:"runtime_id,omitempty"`
	StorageSize string `json:"storage_size,omitempty"`
}

func (c *CLI) watchProjectPolling(ctx context.Context, client *Client, args []string, interval time.Duration, includeLiveStatus bool) error {
	var previousHash [32]byte
	first := true
	for {
		snapshot, hashValue, err := c.loadProjectWatchSnapshot(ctx, client, args, includeLiveStatus)
		if err != nil {
			return err
		}
		if first || hashValue != previousHash {
			if err := c.renderProjectWatchSnapshot(snapshot, !first); err != nil {
				return err
			}
			previousHash = hashValue
			first = false
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
}

func (c *CLI) watchProjectMonitor(ctx context.Context, client *Client, args []string, opts monitorOptions, includeLiveStatus bool) error {
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	session := climonitor.Session{Controls: monitorControls(opts)}
	for {
		rawSnapshot, _, err := c.loadProjectWatchSnapshot(ctx, client, args, includeLiveStatus)
		if err != nil {
			if session.HaveLast {
				if renderErr := c.renderMonitorSnapshot(session.SnapshotWithError(err, time.Now().UTC())); renderErr != nil {
					return renderErr
				}
				if waitErr := waitMonitorInterval(ctx, opts.Interval); waitErr != nil {
					return c.printMonitorSummary(session.Last)
				}
				continue
			}
			return err
		}
		snapshot := buildProjectMonitorSnapshot(rawSnapshot, opts)
		if session.Accept(snapshot) {
			if err := c.renderMonitorSnapshot(snapshot); err != nil {
				return err
			}
		}
		if err := waitMonitorInterval(ctx, opts.Interval); err != nil {
			return c.printMonitorSummary(snapshot)
		}
	}
}

func (c *CLI) watchProjectStream(ctx context.Context, client *Client, args []string, includeLiveStatus bool) error {
	var (
		previousHash [32]byte
		first        = true
	)
	render := func() error {
		snapshot, hashValue, err := c.loadProjectWatchSnapshot(ctx, client, args, includeLiveStatus)
		if err != nil {
			return err
		}
		if !first && hashValue == previousHash {
			return nil
		}
		if err := c.renderProjectWatchSnapshot(snapshot, !first); err != nil {
			return err
		}
		previousHash = hashValue
		first = false
		return nil
	}
	if err := render(); err != nil {
		return err
	}
	for {
		if err := client.StreamConsoleGallery(includeLiveStatus, func(event sseEvent) error {
			switch strings.TrimSpace(event.Event) {
			case "", "heartbeat":
				return nil
			case "error":
				message, err := decodeConsoleStreamError(event.Data)
				if err != nil {
					return err
				}
				return fmt.Errorf("%s", firstNonEmpty(message, "console gallery stream failed"))
			case "ready", "changed":
				return render()
			default:
				return nil
			}
		}); err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(1 * time.Second):
		}
		c.progressf("console stream disconnected; reconnecting")
	}
}

func (c *CLI) renderProjectWatchSnapshot(snapshot any, separate bool) error {
	if separate {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
	}
	if c.wantsJSON() {
		return writeJSON(c.stdout, snapshot)
	}
	if _, err := fmt.Fprintf(c.stdout, "observed_at=%s\n", formatTime(time.Now().UTC())); err != nil {
		return err
	}
	switch value := snapshot.(type) {
	case consoleGalleryResponse:
		return writeConsoleProjectTable(c.stdout, value.Projects)
	case projectOverviewSnapshot:
		return renderConsoleProjectOverview(c.stdout, value.Summary, value.Detail, value.Status)
	default:
		return nil
	}
}

func decodeConsoleStreamError(raw []byte) (string, error) {
	decoded, err := decodeSSEEventData(raw)
	if err != nil {
		return "", err
	}
	if decoded == nil {
		return "", nil
	}
	switch value := decoded.(type) {
	case map[string]any:
		message, _ := value["error"].(string)
		return strings.TrimSpace(message), nil
	default:
		return "", nil
	}
}

func (c *CLI) loadProjectWatchSnapshot(ctx context.Context, client *Client, args []string, includeLiveStatus bool) (any, [32]byte, error) {
	if len(args) == 0 {
		gallery, err := client.GetConsoleGalleryWithLiveStatus(includeLiveStatus)
		if err != nil {
			return nil, [32]byte{}, err
		}
		sum, err := json.Marshal(gallery)
		if err != nil {
			return nil, [32]byte{}, err
		}
		return gallery, sha256.Sum256(sum), nil
	}
	summary, detail, status, err := c.loadConsoleProjectOverview(client, args[0], includeLiveStatus)
	if err != nil {
		return nil, [32]byte{}, err
	}
	snapshot := projectOverviewSnapshot{Summary: summary, Status: status, Detail: detail}
	sum, err := json.Marshal(snapshot)
	if err != nil {
		return nil, [32]byte{}, err
	}
	select {
	case <-ctx.Done():
		return nil, [32]byte{}, ctx.Err()
	default:
	}
	return snapshot, sha256.Sum256(sum), nil
}

func (c *CLI) loadConsoleProjectOverview(client *Client, ref string, includeLiveStatus bool) (*consoleProjectSummary, consoleProjectDetailResponse, *projectStatusResponse, error) {
	project, err := c.resolveNamedProject(client, ref)
	if err != nil {
		return nil, consoleProjectDetailResponse{}, nil, err
	}
	detail, err := client.GetConsoleProjectWithLiveStatus(project.ID, includeLiveStatus)
	if err != nil {
		return nil, consoleProjectDetailResponse{}, nil, err
	}
	status, err := c.loadProjectStatus(client, detail)
	if err != nil {
		return nil, consoleProjectDetailResponse{}, nil, err
	}
	gallery, err := client.GetConsoleGalleryWithLiveStatus(includeLiveStatus)
	if err != nil {
		return nil, detail, status, nil
	}
	for _, summary := range gallery.Projects {
		if strings.EqualFold(strings.TrimSpace(summary.ID), strings.TrimSpace(project.ID)) {
			summaryCopy := summary
			return &summaryCopy, detail, status, nil
		}
	}
	return nil, detail, status, nil
}

func loadProjectOverviewExtras(client *Client, detail consoleProjectDetailResponse, options projectOverviewExtraOptions) (projectOverviewExtras, error) {
	extras := projectOverviewExtras{}
	serviceByID := map[string]model.BackingService{}
	if options.Services || options.Databases {
		services, err := client.ListBackingServices()
		if err != nil {
			return extras, err
		}
		for _, service := range services {
			if strings.TrimSpace(service.ProjectID) != strings.TrimSpace(detail.ProjectID) {
				continue
			}
			serviceByID[strings.TrimSpace(service.ID)] = service
			if options.Services {
				extras.Services = append(extras.Services, service)
			}
		}
	}
	if options.Domains {
		for _, app := range detail.Apps {
			domains, err := client.ListAppDomains(app.ID)
			if err != nil {
				return extras, err
			}
			extras.Domains = append(extras.Domains, domains...)
		}
	}
	if options.Databases {
		extras.Databases = projectDatabaseInventory(detail.Apps, serviceByID)
	}
	return extras, nil
}

func projectDatabaseInventory(apps []model.App, serviceByID map[string]model.BackingService) []projectDatabaseInventoryRow {
	rows := []projectDatabaseInventoryRow{}
	appNameByID := map[string]string{}
	seen := map[string]bool{}
	for _, app := range apps {
		appNameByID[strings.TrimSpace(app.ID)] = app.Name
		if app.Spec.Postgres == nil {
			continue
		}
		postgres := app.Spec.Postgres
		key := "app:" + strings.TrimSpace(app.ID)
		seen[key] = true
		rows = append(rows, projectDatabaseInventoryRow{
			AppID:       app.ID,
			AppName:     app.Name,
			ServiceName: postgres.ServiceName,
			Status:      app.Status.Phase,
			Database:    postgres.Database,
			User:        postgres.User,
			RuntimeID:   firstNonEmpty(postgres.RuntimeID, app.Spec.RuntimeID, app.Status.CurrentRuntimeID),
			StorageSize: postgres.StorageSize,
		})
	}
	for _, service := range serviceByID {
		if !strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) {
			continue
		}
		postgres := service.Spec.Postgres
		row := projectDatabaseInventoryRow{
			ServiceID:   service.ID,
			ServiceName: service.Name,
			AppID:       service.OwnerAppID,
			AppName:     appNameByID[strings.TrimSpace(service.OwnerAppID)],
			Status:      service.Status,
		}
		if postgres != nil {
			row.Database = postgres.Database
			row.User = postgres.User
			row.RuntimeID = postgres.RuntimeID
			row.StorageSize = postgres.StorageSize
		}
		key := "service:" + strings.TrimSpace(service.ID)
		if seen[key] {
			continue
		}
		seen[key] = true
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].AppName != rows[j].AppName {
			return rows[i].AppName < rows[j].AppName
		}
		return rows[i].ServiceName < rows[j].ServiceName
	})
	return rows
}

func renderProjectOverviewExtras(w io.Writer, extras projectOverviewExtras) error {
	if len(extras.Services) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[services]"); err != nil {
			return err
		}
		if err := writeServiceTable(w, extras.Services); err != nil {
			return err
		}
	}
	if len(extras.Domains) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[domains]"); err != nil {
			return err
		}
		if err := writeDomainTable(w, extras.Domains); err != nil {
			return err
		}
	}
	if len(extras.Databases) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[databases]"); err != nil {
			return err
		}
		return writeProjectDatabaseInventoryTable(w, extras.Databases)
	}
	return nil
}

func writeProjectDatabaseInventoryTable(w io.Writer, rows []projectDatabaseInventoryRow) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "APP\tSERVICE\tSTATUS\tDATABASE\tUSER\tRUNTIME\tSTORAGE"); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			firstNonEmpty(row.AppName, row.AppID, "-"),
			firstNonEmpty(row.ServiceName, row.ServiceID, "-"),
			firstNonEmpty(row.Status, "-"),
			firstNonEmpty(row.Database, "-"),
			firstNonEmpty(row.User, "-"),
			firstNonEmpty(row.RuntimeID, "-"),
			firstNonEmpty(row.StorageSize, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeConsoleProjectTable(w io.Writer, projects []consoleProjectSummary) error {
	sorted := append([]consoleProjectSummary(nil), projects...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Name, sorted[j].Name) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PROJECT\tLIFECYCLE\tSYNC\tAPPS\tSERVICES\tUSAGE\tBADGES"); err != nil {
		return err
	}
	for _, project := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%d\t%d\t%s\t%s\n",
			project.Name,
			project.Lifecycle.Label,
			project.Lifecycle.SyncMode,
			project.AppCount,
			project.ServiceCount,
			formatResourceUsageSummary(&project.ResourceUsageSnapshot),
			formatConsoleBadges(project.ServiceBadges),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func renderConsoleProjectOverview(w io.Writer, summary *consoleProjectSummary, detail consoleProjectDetailResponse, status *projectStatusResponse) error {
	projectName := strings.TrimSpace(detail.ProjectName)
	if projectName == "" && detail.Project != nil {
		projectName = strings.TrimSpace(detail.Project.Name)
	}
	if projectName == "" {
		projectName = strings.TrimSpace(detail.ProjectID)
	}
	pairs := []kvPair{
		{Key: "project", Value: projectName},
		{Key: "project_id", Value: detail.ProjectID},
	}
	if summary != nil {
		pairs = append(pairs,
			kvPair{Key: "lifecycle", Value: summary.Lifecycle.Label},
			kvPair{Key: "sync_mode", Value: summary.Lifecycle.SyncMode},
			kvPair{Key: "app_count", Value: fmt.Sprintf("%d", summary.AppCount)},
			kvPair{Key: "service_count", Value: fmt.Sprintf("%d", summary.ServiceCount)},
			kvPair{Key: "resource_usage", Value: formatResourceUsageSummary(&summary.ResourceUsageSnapshot)},
			kvPair{Key: "badges", Value: formatConsoleBadges(summary.ServiceBadges)},
		)
	} else {
		pairs = append(pairs,
			kvPair{Key: "app_count", Value: fmt.Sprintf("%d", len(detail.Apps))},
			kvPair{Key: "operation_count", Value: fmt.Sprintf("%d", len(detail.Operations))},
			kvPair{Key: "cluster_nodes", Value: fmt.Sprintf("%d", len(detail.ClusterNodes))},
		)
	}
	if detail.Project != nil && strings.TrimSpace(detail.Project.Description) != "" {
		pairs = append(pairs, kvPair{Key: "description", Value: detail.Project.Description})
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	if status != nil && (len(status.Services) > 0 || len(status.Deletes) > 0) {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := renderProjectStatus(w, status); err != nil {
			return err
		}
	}
	if len(detail.Apps) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[apps]"); err != nil {
			return err
		}
		if err := writeAppTable(w, detail.Apps); err != nil {
			return err
		}
	}
	if len(detail.Operations) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[operations]"); err != nil {
			return err
		}
		if err := writeOperationTableWithApps(w, detail.Operations, mapAppNames(detail.Apps)); err != nil {
			return err
		}
	}
	if len(detail.ClusterNodes) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[cluster_nodes]"); err != nil {
			return err
		}
		if err := writeClusterNodeTable(w, detail.ClusterNodes); err != nil {
			return err
		}
	}
	return nil
}

func writeOperationTableWithApps(w io.Writer, operations []model.Operation, appNames map[string]string) error {
	sorted := append([]model.Operation(nil), operations...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "OPERATION\tSTATUS\tTYPE\tAPP\tTARGET\tUPDATED"); err != nil {
		return err
	}
	for _, op := range sorted {
		appLabel := strings.TrimSpace(appNames[op.AppID])
		if appLabel == "" {
			appLabel = strings.TrimSpace(op.AppID)
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			op.ID,
			op.Status,
			op.Type,
			appLabel,
			firstNonEmpty(op.TargetRuntimeID, op.AssignedRuntimeID),
			formatTime(op.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func mapAppNames(apps []model.App) map[string]string {
	out := make(map[string]string, len(apps))
	for _, app := range apps {
		if strings.TrimSpace(app.ID) == "" {
			continue
		}
		out[app.ID] = app.Name
	}
	return out
}

func formatConsoleBadges(badges []consoleProjectBadge) string {
	if len(badges) == 0 {
		return ""
	}
	parts := make([]string, 0, len(badges))
	for _, badge := range badges {
		label := strings.TrimSpace(badge.Label)
		if label == "" {
			continue
		}
		if meta := strings.TrimSpace(badge.Meta); meta != "" {
			parts = append(parts, label+" ("+meta+")")
			continue
		}
		parts = append(parts, label)
	}
	return strings.Join(parts, ", ")
}

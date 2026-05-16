package cli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/failover"
	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newProjectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "project",
		Aliases: []string{"projects"},
		Short:   "Inspect and manage projects",
		Long: strings.TrimSpace(`
Project commands normally auto-select the tenant when your key only sees one.

Pass --tenant only when you are acting across multiple visible tenants.
`),
	}
	cmd.AddCommand(
		c.newProjectListCommand(),
		c.newProjectOverviewCommand(),
		c.newProjectWatchCommand(),
		c.newProjectVerifyCommand(),
		c.newProjectAppsCommand(),
		c.newProjectOpsCommand(),
		c.newProjectImagesCommand(),
		c.newProjectMetaCommand(),
		c.newProjectShowCommand(),
		c.newProjectCreateCommand(),
		c.newProjectEditCommand(),
		c.newProjectMoveCommand(),
		c.newProjectRuntimeReservationsCommand(),
		hideCompatCommand(c.newProjectRenameCommand(), "fugue project edit"),
		c.newProjectRemoveCommand(),
		hideCompatCommand(c.newProjectStorageCommand(), "fugue project images usage"),
		hideCompatCommand(c.newProjectUsageCommand(), "fugue project images usage"),
	)
	return cmd
}

type projectMoveCommandOptions struct {
	RuntimeName string
	RuntimeID   string
	Wait        bool
	SkipBlocked bool
	DryRun      bool
}

type projectMoveSkippedApp struct {
	App    model.App `json:"app"`
	Reason string    `json:"reason"`
}

type projectMoveSkippedService struct {
	Service model.BackingService `json:"service"`
	Reason  string               `json:"reason"`
}

type projectMoveResult struct {
	Project         model.Project               `json:"project"`
	TargetRuntimeID string                      `json:"target_runtime_id"`
	DryRun          bool                        `json:"dry_run,omitempty"`
	Apps            []model.App                 `json:"apps,omitempty"`
	Services        []model.BackingService      `json:"services,omitempty"`
	UpdatedServices []model.BackingService      `json:"updated_services,omitempty"`
	Operations      []model.Operation           `json:"operations,omitempty"`
	Skipped         []projectMoveSkippedApp     `json:"skipped,omitempty"`
	SkippedServices []projectMoveSkippedService `json:"skipped_services,omitempty"`
}

func (c *CLI) newProjectMoveCommand() *cobra.Command {
	opts := projectMoveCommandOptions{Wait: true}
	cmd := &cobra.Command{
		Use:     "move <project>",
		Aliases: []string{"migrate"},
		Short:   "Move project apps and backing services to another runtime",
		Long: strings.TrimSpace(`
Move every migratable app and managed backing service in a project to a target
runtime with one command.

The command preflights all project apps and backing services before queueing
operations. By default, any resource that cannot be safely migrated blocks the
whole project move so the project is not left half-moved. Pass --skip-blocked
only when you intentionally want to move eligible resources and leave blocked
resources behind.
`),
		Example: strings.TrimSpace(`
fugue project move marketing --to runtime-b
fugue project move argus --to v2202605354515455529 --skip-blocked
fugue project move argus --to v2202605354515455529 --dry-run
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			if strings.TrimSpace(opts.RuntimeName) == "" && strings.TrimSpace(opts.RuntimeID) == "" {
				return fmt.Errorf("target runtime is required")
			}
			targetRuntimeID, err := resolveRuntimeSelection(client, opts.RuntimeID, opts.RuntimeName)
			if err != nil {
				return err
			}
			if strings.TrimSpace(targetRuntimeID) == "" {
				return fmt.Errorf("target runtime is required")
			}

			apps, err := client.ListAppsWithLiveStatus(false)
			if err != nil {
				return err
			}
			projectApps := filterApps(apps, project.TenantID, project.ID)
			sort.SliceStable(projectApps, func(i, j int) bool {
				return strings.TrimSpace(projectApps[i].Name) < strings.TrimSpace(projectApps[j].Name)
			})
			services, err := client.ListBackingServices()
			if err != nil {
				return err
			}
			projectServices := filterProjectMoveServices(services, project.TenantID, project.ID)
			sort.SliceStable(projectServices, func(i, j int) bool {
				return strings.TrimSpace(projectServices[i].Name) < strings.TrimSpace(projectServices[j].Name)
			})

			candidates, skipped := planProjectMove(projectApps, targetRuntimeID)
			serviceCandidates, skippedServices := planProjectMoveServices(projectServices, targetRuntimeID)
			if (hasBlockedProjectMoveApps(skipped) || hasBlockedProjectMoveServices(skippedServices)) && !opts.SkipBlocked && !opts.DryRun {
				return projectMoveBlockedError(project.Name, skipped, skippedServices)
			}
			if opts.SkipBlocked {
				candidates = removeSkippedProjectMoveApps(candidates, skipped)
				serviceCandidates = removeSkippedProjectMoveServices(serviceCandidates, skippedServices)
			}

			result := projectMoveResult{
				Project:         project,
				TargetRuntimeID: targetRuntimeID,
				DryRun:          opts.DryRun,
				Apps:            candidates,
				Services:        serviceCandidates,
				Skipped:         skipped,
				SkippedServices: skippedServices,
			}
			if opts.DryRun {
				return c.renderProjectMoveResult(result)
			}
			if !opts.Wait && len(serviceCandidates) > 0 && len(candidates) > 0 {
				return fmt.Errorf("project move must wait for backing service switchovers before queueing app moves; rerun without --wait=false or move services and apps separately")
			}

			updatedServices := make([]model.BackingService, 0, len(serviceCandidates))
			operations := make([]model.Operation, 0, len(serviceCandidates)+len(candidates))
			serviceOperations := make([]model.Operation, 0, len(serviceCandidates))
			for _, service := range serviceCandidates {
				response, err := client.MigrateBackingService(service.ID, targetRuntimeID)
				if err != nil {
					return fmt.Errorf("move service %s: %w", formatDisplayName(service.Name, service.ID, c.showIDs()), err)
				}
				updatedServices = append(updatedServices, response.BackingService)
				if response.Operation != nil {
					serviceOperations = append(serviceOperations, *response.Operation)
				}
			}
			if opts.Wait && len(serviceOperations) > 0 {
				finalOps, err := c.waitForOperations(client, serviceOperations)
				if err != nil {
					return err
				}
				operations = append(operations, finalOps...)
			} else {
				operations = append(operations, serviceOperations...)
			}

			appOperations := make([]model.Operation, 0, len(candidates))
			for _, app := range candidates {
				response, err := client.MigrateApp(app.ID, targetRuntimeID)
				if err != nil {
					return fmt.Errorf("move app %s: %w", formatDisplayName(app.Name, app.ID, c.showIDs()), err)
				}
				appOperations = append(appOperations, response.Operation)
			}
			if opts.Wait {
				finalOps, err := c.waitForOperations(client, appOperations)
				if err != nil {
					return err
				}
				appOperations = finalOps
			}
			operations = append(operations, appOperations...)
			result.UpdatedServices = updatedServices
			result.Operations = operations
			return c.renderProjectMoveResult(result)
		},
	}
	cmd.Flags().StringVar(&opts.RuntimeName, "to", "", "Target runtime name")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Target runtime ID")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	cmd.Flags().BoolVar(&opts.SkipBlocked, "skip-blocked", false, "Move eligible apps and leave blocked apps on their current runtime")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "Show the project move plan without queueing operations")
	_ = cmd.Flags().MarkHidden("runtime-id")
	return cmd
}

func planProjectMove(apps []model.App, targetRuntimeID string) ([]model.App, []projectMoveSkippedApp) {
	candidates := make([]model.App, 0, len(apps))
	skipped := make([]projectMoveSkippedApp, 0)
	for _, app := range apps {
		if strings.TrimSpace(app.ID) == "" {
			continue
		}
		if strings.TrimSpace(appEffectiveRuntimeID(app)) == strings.TrimSpace(targetRuntimeID) {
			skipped = append(skipped, projectMoveSkippedApp{App: app, Reason: "already on target runtime"})
			continue
		}
		if blockers := failover.MigrationBlockers(app); len(blockers) > 0 {
			skipped = append(skipped, projectMoveSkippedApp{App: app, Reason: "blocked by " + strings.Join(blockers, ", ")})
			continue
		}
		candidates = append(candidates, app)
	}
	return candidates, skipped
}

func filterProjectMoveServices(services []model.BackingService, tenantID, projectID string) []model.BackingService {
	out := make([]model.BackingService, 0, len(services))
	for _, service := range services {
		if strings.TrimSpace(service.TenantID) != strings.TrimSpace(tenantID) ||
			strings.TrimSpace(service.ProjectID) != strings.TrimSpace(projectID) {
			continue
		}
		if strings.TrimSpace(service.OwnerAppID) != "" {
			continue
		}
		out = append(out, service)
	}
	return out
}

func planProjectMoveServices(services []model.BackingService, targetRuntimeID string) ([]model.BackingService, []projectMoveSkippedService) {
	candidates := make([]model.BackingService, 0, len(services))
	skipped := make([]projectMoveSkippedService, 0)
	for _, service := range services {
		if strings.TrimSpace(service.ID) == "" {
			continue
		}
		runtimeID := backingServiceRuntimeID(service)
		if runtimeID == "" {
			skipped = append(skipped, projectMoveSkippedService{Service: service, Reason: "blocked by unsupported backing service runtime"})
			continue
		}
		if runtimeID == strings.TrimSpace(targetRuntimeID) {
			skipped = append(skipped, projectMoveSkippedService{Service: service, Reason: "already on target runtime"})
			continue
		}
		if blockers := backingServiceMigrationBlockers(service); len(blockers) > 0 {
			skipped = append(skipped, projectMoveSkippedService{Service: service, Reason: "blocked by " + strings.Join(blockers, ", ")})
			continue
		}
		candidates = append(candidates, service)
	}
	return candidates, skipped
}

func removeSkippedProjectMoveApps(apps []model.App, skipped []projectMoveSkippedApp) []model.App {
	blocked := make(map[string]struct{}, len(skipped))
	for _, item := range skipped {
		if strings.HasPrefix(strings.TrimSpace(item.Reason), "blocked by ") {
			blocked[strings.TrimSpace(item.App.ID)] = struct{}{}
		}
	}
	if len(blocked) == 0 {
		return apps
	}
	out := make([]model.App, 0, len(apps))
	for _, app := range apps {
		if _, ok := blocked[strings.TrimSpace(app.ID)]; ok {
			continue
		}
		out = append(out, app)
	}
	return out
}

func removeSkippedProjectMoveServices(services []model.BackingService, skipped []projectMoveSkippedService) []model.BackingService {
	blocked := make(map[string]struct{}, len(skipped))
	for _, item := range skipped {
		if strings.HasPrefix(strings.TrimSpace(item.Reason), "blocked by ") {
			blocked[strings.TrimSpace(item.Service.ID)] = struct{}{}
		}
	}
	if len(blocked) == 0 {
		return services
	}
	out := make([]model.BackingService, 0, len(services))
	for _, service := range services {
		if _, ok := blocked[strings.TrimSpace(service.ID)]; ok {
			continue
		}
		out = append(out, service)
	}
	return out
}

func hasBlockedProjectMoveApps(skipped []projectMoveSkippedApp) bool {
	for _, item := range skipped {
		if strings.HasPrefix(strings.TrimSpace(item.Reason), "blocked by ") {
			return true
		}
	}
	return false
}

func hasBlockedProjectMoveServices(skipped []projectMoveSkippedService) bool {
	for _, item := range skipped {
		if strings.HasPrefix(strings.TrimSpace(item.Reason), "blocked by ") {
			return true
		}
	}
	return false
}

func appEffectiveRuntimeID(app model.App) string {
	if runtimeID := strings.TrimSpace(app.Status.CurrentRuntimeID); runtimeID != "" {
		return runtimeID
	}
	return strings.TrimSpace(app.Spec.RuntimeID)
}

func backingServiceRuntimeID(service model.BackingService) string {
	if service.Spec.Postgres != nil {
		return strings.TrimSpace(service.Spec.Postgres.RuntimeID)
	}
	return ""
}

func backingServiceMigrationBlockers(service model.BackingService) []string {
	if strings.EqualFold(strings.TrimSpace(service.Status), model.BackingServiceStatusDeleted) {
		return []string{"deleted backing service"}
	}
	if !strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) || service.Spec.Postgres == nil {
		return []string{"unsupported backing service type"}
	}
	provisioner := strings.TrimSpace(strings.ToLower(service.Provisioner))
	if provisioner != "" && provisioner != model.BackingServiceProvisionerManaged {
		return []string{"external backing service"}
	}
	return nil
}

func projectMoveBlockedError(projectName string, skipped []projectMoveSkippedApp, skippedServices []projectMoveSkippedService) error {
	blocked := make([]string, 0, len(skipped)+len(skippedServices))
	for _, item := range skipped {
		if !strings.HasPrefix(strings.TrimSpace(item.Reason), "blocked by ") {
			continue
		}
		name := strings.TrimSpace(item.App.Name)
		if name == "" {
			name = strings.TrimSpace(item.App.ID)
		}
		blocked = append(blocked, fmt.Sprintf("%s (%s)", name, item.Reason))
	}
	for _, item := range skippedServices {
		if !strings.HasPrefix(strings.TrimSpace(item.Reason), "blocked by ") {
			continue
		}
		name := strings.TrimSpace(item.Service.Name)
		if name == "" {
			name = strings.TrimSpace(item.Service.ID)
		}
		blocked = append(blocked, fmt.Sprintf("%s (%s)", name, item.Reason))
	}
	if len(blocked) == 0 {
		return nil
	}
	return fmt.Errorf("project %q has resources that cannot be moved safely: %s; rerun with --skip-blocked to move only eligible resources", strings.TrimSpace(projectName), strings.Join(blocked, "; "))
}

func (c *CLI) renderProjectMoveResult(result projectMoveResult) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, result)
	}
	if err := writeKeyValues(c.stdout,
		kvPair{Key: "project", Value: formatDisplayName(result.Project.Name, result.Project.ID, c.showIDs())},
		kvPair{Key: "target_runtime_id", Value: result.TargetRuntimeID},
		kvPair{Key: "dry_run", Value: fmt.Sprintf("%t", result.DryRun)},
		kvPair{Key: "candidate_apps", Value: fmt.Sprintf("%d", len(result.Apps))},
		kvPair{Key: "candidate_services", Value: fmt.Sprintf("%d", len(result.Services))},
		kvPair{Key: "updated_services", Value: fmt.Sprintf("%d", len(result.UpdatedServices))},
		kvPair{Key: "queued_operations", Value: fmt.Sprintf("%d", len(result.Operations))},
		kvPair{Key: "skipped_apps", Value: fmt.Sprintf("%d", len(result.Skipped))},
		kvPair{Key: "skipped_services", Value: fmt.Sprintf("%d", len(result.SkippedServices))},
	); err != nil {
		return err
	}
	if len(result.UpdatedServices) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if err := writeServiceTable(c.stdout, result.UpdatedServices); err != nil {
			return err
		}
	}
	if len(result.Operations) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if err := writeOperationTableWithApps(c.stdout, result.Operations, mapAppNames(result.Apps)); err != nil {
			return err
		}
	}
	if len(result.Skipped) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		for _, item := range result.Skipped {
			if _, err := fmt.Fprintf(c.stdout, "skipped_app=%s reason=%s\n", formatDisplayName(item.App.Name, item.App.ID, c.showIDs()), item.Reason); err != nil {
				return err
			}
		}
	}
	if len(result.SkippedServices) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		for _, item := range result.SkippedServices {
			if _, err := fmt.Fprintf(c.stdout, "skipped_service=%s reason=%s\n", formatDisplayName(item.Service.Name, item.Service.ID, c.showIDs()), item.Reason); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CLI) newProjectShowCommand() *cobra.Command {
	cmd := c.newProjectOverviewCommand()
	cmd.Use = "show [project]"
	cmd.Aliases = []string{"get", "status", "info"}
	cmd.Short = "Compatibility alias for project overview"
	return hideCompatCommand(cmd, "fugue project overview")
}

func (c *CLI) newProjectMetaCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "meta <project>",
		Short: "Show project metadata",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"project": project})
			}
			return c.renderProjectDetail(client, project)
		},
	}
}

func (c *CLI) newProjectRuntimeReservationsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "runtimes",
		Aliases: []string{"runtime-reservations", "dedicated-runtimes", "dedicated-vps", "capacity"},
		Short:   "Manage runtimes reserved for a project",
	}
	cmd.AddCommand(
		c.newProjectRuntimeReservationsListCommand(),
		c.newProjectRuntimeReservationsAddCommand(),
		c.newProjectRuntimeReservationsRemoveCommand(),
	)
	return cmd
}

func (c *CLI) newProjectRuntimeReservationsListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls <project>",
		Aliases: []string{"list"},
		Short:   "List runtimes reserved for a project",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			reservations, err := client.ListProjectRuntimeReservations(project.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"runtime_reservations": reservations})
			}
			runtimes, err := client.ListRuntimes()
			if err != nil {
				return err
			}
			return writeProjectRuntimeReservationTable(c.stdout, reservations, mapRuntimeNames(runtimes), c.showIDs())
		},
	}
}

func (c *CLI) newProjectRuntimeReservationsAddCommand() *cobra.Command {
	opts := struct {
		RuntimeName string
		RuntimeID   string
	}{}
	cmd := &cobra.Command{
		Use:   "add <project>",
		Short: "Reserve a runtime for a project",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			runtimeID, err := resolveRuntimeSelection(client, opts.RuntimeID, opts.RuntimeName)
			if err != nil {
				return err
			}
			if strings.TrimSpace(runtimeID) == "" {
				return fmt.Errorf("runtime is required")
			}
			reservation, err := client.ReserveProjectRuntime(project.ID, runtimeID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"runtime_reservation": reservation})
			}
			return writeProjectRuntimeReservationTable(c.stdout, []model.ProjectRuntimeReservation{reservation}, nil, c.showIDs())
		},
	}
	cmd.Flags().StringVar(&opts.RuntimeName, "runtime", "", "Runtime name to reserve")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Runtime ID to reserve")
	_ = cmd.Flags().MarkHidden("runtime-id")
	return cmd
}

func (c *CLI) newProjectRuntimeReservationsRemoveCommand() *cobra.Command {
	opts := struct {
		RuntimeName string
		RuntimeID   string
	}{}
	cmd := &cobra.Command{
		Use:     "remove <project>",
		Aliases: []string{"delete", "rm"},
		Short:   "Remove a project runtime reservation",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			runtimeID, err := resolveRuntimeSelection(client, opts.RuntimeID, opts.RuntimeName)
			if err != nil {
				return err
			}
			if strings.TrimSpace(runtimeID) == "" {
				return fmt.Errorf("runtime is required")
			}
			reservation, err := client.DeleteProjectRuntimeReservation(project.ID, runtimeID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"deleted": true, "runtime_reservation": reservation})
			}
			return writeProjectRuntimeReservationTable(c.stdout, []model.ProjectRuntimeReservation{reservation}, nil, c.showIDs())
		},
	}
	cmd.Flags().StringVar(&opts.RuntimeName, "runtime", "", "Runtime name to unreserve")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Runtime ID to unreserve")
	_ = cmd.Flags().MarkHidden("runtime-id")
	return cmd
}

func (c *CLI) newProjectListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List visible projects",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
			if err != nil {
				return err
			}
			projects, err := client.ListProjects(tenantID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"projects": projects})
			}
			return writeProjectTableWithContext(c.stdout, projects, c.loadTenantNames(client), c.showIDs())
		},
	}
}

func (c *CLI) newProjectCreateCommand() *cobra.Command {
	opts := struct {
		Description        string
		DefaultRuntimeName string
		DefaultRuntimeID   string
	}{}
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a project",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
			if err != nil {
				return err
			}
			defaultRuntimeID, err := resolveRuntimeSelection(client, opts.DefaultRuntimeID, opts.DefaultRuntimeName)
			if err != nil {
				return err
			}
			project, err := client.CreateProject(tenantID, args[0], opts.Description, defaultRuntimeID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"project": project})
			}
			return c.renderProjectDetail(client, project)
		},
	}
	cmd.Flags().StringVar(&opts.Description, "description", "", "Project description")
	cmd.Flags().StringVar(&opts.DefaultRuntimeName, "default-runtime", "", "Default runtime name for new apps in this project")
	cmd.Flags().StringVar(&opts.DefaultRuntimeID, "default-runtime-id", "", "Default runtime ID for new apps in this project")
	_ = cmd.Flags().MarkHidden("default-runtime-id")
	return cmd
}

func (c *CLI) newProjectRenameCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "rename <project> <new-name>",
		Short: "Compatibility alias for project edit",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			newName := strings.TrimSpace(args[1])
			return c.patchProjectMetadata(args[0], &newName, nil, "", "", false)
		},
	}
}

func (c *CLI) newProjectEditCommand() *cobra.Command {
	opts := struct {
		Name                string
		Description         string
		DefaultRuntimeName  string
		DefaultRuntimeID    string
		ClearDescription    bool
		ClearDefaultRuntime bool
	}{}
	cmd := &cobra.Command{
		Use:   "edit <project> [new-name]",
		Short: "Rename a project or update its description",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 2 && strings.TrimSpace(opts.Name) != "" {
				return fmt.Errorf("new project name must be provided either as an argument or with --name")
			}
			if opts.ClearDescription && flagChanged(cmd, "description") {
				return fmt.Errorf("--description and --clear-description cannot be used together")
			}
			if opts.ClearDefaultRuntime && (flagChanged(cmd, "default-runtime") || flagChanged(cmd, "default-runtime-id")) {
				return fmt.Errorf("--default-runtime, --default-runtime-id, and --clear-default-runtime cannot be used together")
			}
			var name *string
			switch {
			case len(args) == 2:
				value := strings.TrimSpace(args[1])
				if value == "" {
					return fmt.Errorf("new project name is required")
				}
				name = &value
			case flagChanged(cmd, "name"):
				value := strings.TrimSpace(opts.Name)
				if value == "" {
					return fmt.Errorf("new project name is required")
				}
				name = &value
			}

			var description *string
			switch {
			case opts.ClearDescription:
				value := ""
				description = &value
			case flagChanged(cmd, "description"):
				value := opts.Description
				description = &value
			}

			defaultRuntimeChanged := flagChanged(cmd, "default-runtime") || flagChanged(cmd, "default-runtime-id") || opts.ClearDefaultRuntime
			if name == nil && description == nil && !defaultRuntimeChanged {
				return fmt.Errorf("at least one of [new-name], --name, --description, --clear-description, --default-runtime, or --clear-default-runtime is required")
			}
			return c.patchProjectMetadata(args[0], name, description, opts.DefaultRuntimeName, opts.DefaultRuntimeID, opts.ClearDefaultRuntime)
		},
	}
	cmd.Flags().StringVar(&opts.Name, "name", "", "New project name")
	cmd.Flags().StringVar(&opts.Description, "description", "", "Project description")
	cmd.Flags().StringVar(&opts.DefaultRuntimeName, "default-runtime", "", "Default runtime name for new apps in this project")
	cmd.Flags().StringVar(&opts.DefaultRuntimeID, "default-runtime-id", "", "Default runtime ID for new apps in this project")
	cmd.Flags().BoolVar(&opts.ClearDescription, "clear-description", false, "Clear the project description")
	cmd.Flags().BoolVar(&opts.ClearDefaultRuntime, "clear-default-runtime", false, "Clear the project default runtime")
	_ = cmd.Flags().MarkHidden("default-runtime-id")
	return cmd
}

func (c *CLI) newProjectRemoveCommand() *cobra.Command {
	opts := struct {
		Cascade  bool
		Wait     bool
		Interval time.Duration
	}{Cascade: true, Interval: 3 * time.Second}

	cmd := &cobra.Command{
		Use:     "delete <project>",
		Aliases: []string{"rm", "remove"},
		Short:   "Delete a project",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.DeleteProjectDetailed(project.ID, opts.Cascade)
			if err != nil {
				return err
			}
			if opts.Wait && response.DeleteRequested {
				lastDetail, lastStatus, err := c.waitForProjectDelete(client, response.Project, opts.Interval)
				if err != nil {
					return err
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{
						"delete":      response,
						"final_state": "deleted",
						"last_detail": lastDetail,
						"last_status": lastStatus,
					})
				}
				return c.renderProjectDeleteResult(response, true)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return c.renderProjectDeleteResult(response, false)
		},
	}
	cmd.Flags().BoolVar(&opts.Cascade, "cascade", opts.Cascade, "Queue app deletes for the project and remove remaining backing services")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait until the project is fully removed")
	cmd.Flags().DurationVar(&opts.Interval, "interval", opts.Interval, "Polling interval while waiting for project deletion")
	return cmd
}

func (c *CLI) patchProjectMetadata(projectRef string, name, description *string, defaultRuntimeName, defaultRuntimeID string, clearDefaultRuntime bool) error {
	client, err := c.newClient()
	if err != nil {
		return err
	}
	project, err := c.resolveNamedProject(client, projectRef)
	if err != nil {
		return err
	}
	var defaultRuntime *string
	if strings.TrimSpace(defaultRuntimeName) != "" || strings.TrimSpace(defaultRuntimeID) != "" {
		runtimeID, err := resolveRuntimeSelection(client, defaultRuntimeID, defaultRuntimeName)
		if err != nil {
			return err
		}
		defaultRuntime = &runtimeID
	}
	project, err = client.PatchProjectFields(project.ID, name, description, defaultRuntime, clearDefaultRuntime)
	if err != nil {
		return err
	}
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{"project": project})
	}
	return c.renderProjectDetail(client, project)
}

func (c *CLI) newProjectImagesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "images",
		Short: "Inspect project image inventory and usage",
	}
	cmd.AddCommand(c.newProjectImageUsageCommand())
	return cmd
}

func (c *CLI) newProjectImageUsageCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "usage [project]",
		Short: "Show image-usage by project",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			usage, err := client.ListProjectImageUsage()
			if err != nil {
				return err
			}
			projectNames := c.loadProjectNames(client, "")
			if len(args) == 1 {
				project, err := c.resolveNamedProject(client, args[0])
				if err != nil {
					return err
				}
				for _, summary := range usage.Projects {
					if summary.ProjectID != project.ID {
						continue
					}
					if c.wantsJSON() {
						return writeJSON(c.stdout, map[string]any{
							"registry_configured": usage.RegistryConfigured,
							"reclaim_requires_gc": usage.ReclaimRequiresGC,
							"project":             summary,
						})
					}
					projectName := firstNonEmptyTrimmed(projectNames[summary.ProjectID], project.Name, summary.ProjectID)
					if err := writeKeyValues(c.stdout,
						kvPair{Key: "project", Value: formatDisplayName(projectName, summary.ProjectID, c.showIDs())},
						kvPair{Key: "version_count", Value: fmt.Sprintf("%d", summary.VersionCount)},
						kvPair{Key: "reclaimable", Value: formatBytes(summary.ReclaimableSizeBytes)},
					); err != nil {
						return err
					}
					if len(summary.Apps) == 0 {
						return nil
					}
					if _, err := fmt.Fprintln(c.stdout); err != nil {
						return err
					}
					return writeProjectUsageAppsTable(c.stdout, summary.Apps)
				}
				return fmt.Errorf("project %q has no image usage", project.Name)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, usage)
			}
			if err := writeKeyValues(c.stdout,
				kvPair{Key: "registry_configured", Value: fmt.Sprintf("%t", usage.RegistryConfigured)},
				kvPair{Key: "reclaim_requires_gc", Value: fmt.Sprintf("%t", usage.ReclaimRequiresGC)},
			); err != nil {
				return err
			}
			if len(usage.Projects) == 0 {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeProjectUsageTableWithContext(c.stdout, usage.Projects, projectNames, c.showIDs())
		},
	}
	return cmd
}

func (c *CLI) newProjectUsageCommand() *cobra.Command {
	cmd := c.newProjectImageUsageCommand()
	cmd.Use = "usage [project]"
	cmd.Short = "Compatibility alias for project images usage"
	return cmd
}

func (c *CLI) newProjectStorageCommand() *cobra.Command {
	cmd := c.newProjectImageUsageCommand()
	cmd.Use = "storage [project]"
	cmd.Short = "Show project image storage usage"
	cmd.Hidden = false
	cmd.Deprecated = ""
	return cmd
}

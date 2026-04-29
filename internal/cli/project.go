package cli

import (
	"fmt"
	"strings"
	"time"

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
		c.newProjectRuntimeReservationsCommand(),
		hideCompatCommand(c.newProjectRenameCommand(), "fugue project edit"),
		c.newProjectRemoveCommand(),
		hideCompatCommand(c.newProjectStorageCommand(), "fugue project images usage"),
		hideCompatCommand(c.newProjectUsageCommand(), "fugue project images usage"),
	)
	return cmd
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
		Use:     "runtime-reservations",
		Aliases: []string{"dedicated-runtimes", "dedicated-vps"},
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
		Use:   "list <project>",
		Short: "List runtimes reserved for a project",
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

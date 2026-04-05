package cli

import (
	"fmt"
	"io"
	"strings"

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
		c.newProjectAppsCommand(),
		c.newProjectOpsCommand(),
		c.newProjectMetaCommand(),
		c.newProjectShowCommand(),
		c.newProjectCreateCommand(),
		c.newProjectRenameCommand(),
		c.newProjectRemoveCommand(),
		c.newProjectStorageCommand(),
		c.newProjectUsageCommand(),
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
			return renderProject(c.stdout, project)
		},
	}
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
			return writeProjectTable(c.stdout, projects)
		},
	}
}

func (c *CLI) newProjectCreateCommand() *cobra.Command {
	opts := struct {
		Description string
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
			project, err := client.CreateProject(tenantID, args[0], opts.Description)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"project": project})
			}
			return renderProject(c.stdout, project)
		},
	}
	cmd.Flags().StringVar(&opts.Description, "description", "", "Project description")
	return cmd
}

func (c *CLI) newProjectRenameCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "rename <project> <new-name>",
		Short: "Rename a project",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			newName := strings.TrimSpace(args[1])
			project, err = client.PatchProject(project.ID, &newName, nil)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"project": project})
			}
			return renderProject(c.stdout, project)
		},
	}
}

func (c *CLI) newProjectRemoveCommand() *cobra.Command {
	return &cobra.Command{
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
			project, err = client.DeleteProject(project.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"deleted": true,
					"project": project,
				})
			}
			if err := renderProject(c.stdout, project); err != nil {
				return err
			}
			_, err = fmt.Fprintln(c.stdout, "deleted=true")
			return err
		},
	}
}

func (c *CLI) newProjectUsageCommand() *cobra.Command {
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
					if err := writeKeyValues(c.stdout,
						kvPair{Key: "project_id", Value: summary.ProjectID},
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
			return writeProjectUsageTable(c.stdout, usage.Projects)
		},
	}
	return hideCompatCommand(cmd, "fugue project storage")
}

func (c *CLI) newProjectStorageCommand() *cobra.Command {
	cmd := c.newProjectUsageCommand()
	cmd.Use = "storage [project]"
	cmd.Short = "Show project image storage usage"
	cmd.Hidden = false
	cmd.Deprecated = ""
	return cmd
}

func renderProject(w io.Writer, project model.Project) error {
	return writeKeyValues(w,
		kvPair{Key: "project_id", Value: project.ID},
		kvPair{Key: "tenant_id", Value: project.TenantID},
		kvPair{Key: "name", Value: project.Name},
		kvPair{Key: "slug", Value: project.Slug},
		kvPair{Key: "description", Value: project.Description},
	)
}

package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type projectOwnershipMoveOptions struct {
	ProjectName          string
	ProjectID            string
	CreateProject        bool
	IncludeOwnedServices bool
	IncludeBoundServices bool
	OnConflict           string
	DryRun               bool
	Confirm              bool
}

type projectSplitCommandOptions struct {
	Apps                 []string
	CreateProjects       bool
	IncludeOwnedServices bool
	IncludeBoundServices bool
	OnConflict           string
	Confirm              bool
}

func (c *CLI) newAppMoveProjectCommand() *cobra.Command {
	opts := projectOwnershipMoveOptions{IncludeOwnedServices: true, OnConflict: "fail"}
	cmd := &cobra.Command{
		Use:   "move-project <app>",
		Short: "Move an app to another project",
		Long: strings.TrimSpace(`
Move an app's project ownership without changing its runtime.

Use this when you need to split a tenant's default project into separate
project boundaries. App-owned backing services, including app-managed postgres,
move with the app by default so bindings and ownership stay consistent.
`),
		Example: strings.TrimSpace(`
fugue app move-project dataocean --to dataocean --create-project --dry-run
fugue app move-project dataocean --to dataocean --create-project --confirm
fugue app move-project api --to backend --include-bound-services --on-conflict rename --confirm
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			targetProjectID, targetProjectName, err := c.resolveProjectMoveTarget(client, opts.ProjectID, opts.ProjectName, opts.CreateProject)
			if err != nil {
				return err
			}
			if err := requireConfirmedProjectMove(opts.DryRun, opts.Confirm, "app move-project"); err != nil {
				return err
			}
			plan, err := client.MoveAppProject(app.ID, appProjectMoveRequest{
				TargetProjectID:      targetProjectID,
				TargetProjectName:    targetProjectName,
				CreateProject:        opts.CreateProject,
				IncludeOwnedServices: opts.IncludeOwnedServices,
				IncludeBoundServices: opts.IncludeBoundServices,
				OnConflict:           opts.OnConflict,
				DryRun:               opts.DryRun,
			})
			if err != nil {
				return err
			}
			return c.renderProjectMovePlan(plan)
		},
	}
	addProjectOwnershipMoveFlags(cmd, &opts, "Create the target project when --to names a project that does not exist")
	return cmd
}

func (c *CLI) newServiceMoveProjectCommand() *cobra.Command {
	opts := projectOwnershipMoveOptions{OnConflict: "fail"}
	cmd := &cobra.Command{
		Use:   "move-project <service>",
		Short: "Move a backing service to another project",
		Long: strings.TrimSpace(`
Move an independently-managed backing service's project ownership without
changing its runtime.

App-owned services normally move through "fugue app move-project" so the owner
app, service, and binding stay in the same project boundary.
`),
		Example: strings.TrimSpace(`
fugue service move-project shared-db --to analytics --dry-run
fugue service move-project shared-db --to analytics --on-conflict rename --confirm
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			service, err := c.resolveNamedService(client, args[0])
			if err != nil {
				return err
			}
			targetProjectID, targetProjectName, err := c.resolveProjectMoveTarget(client, opts.ProjectID, opts.ProjectName, opts.CreateProject)
			if err != nil {
				return err
			}
			if err := requireConfirmedProjectMove(opts.DryRun, opts.Confirm, "service move-project"); err != nil {
				return err
			}
			plan, err := client.MoveBackingServiceProject(service.ID, serviceProjectMoveRequest{
				TargetProjectID:   targetProjectID,
				TargetProjectName: targetProjectName,
				CreateProject:     opts.CreateProject,
				OnConflict:        opts.OnConflict,
				DryRun:            opts.DryRun,
			})
			if err != nil {
				return err
			}
			return c.renderProjectMovePlan(plan)
		},
	}
	addProjectOwnershipMoveFlags(cmd, &opts, "Create the target project when --to names a project that does not exist")
	_ = cmd.Flags().MarkHidden("include-owned-services")
	_ = cmd.Flags().MarkHidden("include-bound-services")
	return cmd
}

func (c *CLI) newProjectSplitCommand() *cobra.Command {
	opts := projectSplitCommandOptions{IncludeOwnedServices: true, OnConflict: "fail"}
	cmd := &cobra.Command{
		Use:   "split <project>",
		Short: "Split apps out of a project",
		Long: strings.TrimSpace(`
Move selected apps from one source project into target projects in one
project-ownership transaction.

Pass one --app mapping for each app you want to move. App-owned backing
services move with each app by default; independently-created bound services
move only when --include-bound-services is set and every bound app moves to the
same target project.
`),
		Example: strings.TrimSpace(`
fugue project split plan default --app dataocean=dataocean --app cerebr-snapshot-share=cerebr-snapshot-share --create-projects
fugue project split default --app dataocean=dataocean --app cerebr-snapshot-share=cerebr-snapshot-share --create-projects --confirm
fugue project split default --app api=backend --include-bound-services --on-conflict rename --confirm
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireConfirmedProjectMove(false, opts.Confirm, "project split"); err != nil {
				return err
			}
			return c.runProjectSplit(args[0], opts, false)
		},
	}
	addProjectSplitFlags(cmd, &opts)
	cmd.AddCommand(c.newProjectSplitPlanCommand())
	return cmd
}

func (c *CLI) newProjectSplitPlanCommand() *cobra.Command {
	opts := projectSplitCommandOptions{IncludeOwnedServices: true, OnConflict: "fail"}
	cmd := &cobra.Command{
		Use:   "plan <project>",
		Short: "Preview a project split",
		Long: strings.TrimSpace(`
Preview the apps, backing services, binding rewrites, created projects,
warnings, and blockers for a project split without writing changes.
`),
		Example: strings.TrimSpace(`
fugue project split plan default --app dataocean=dataocean --app cerebr-snapshot-share=cerebr-snapshot-share --create-projects
fugue project split plan default --app api=backend --include-bound-services --on-conflict rename
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runProjectSplit(args[0], opts, true)
		},
	}
	addProjectSplitFlags(cmd, &opts)
	_ = cmd.Flags().MarkHidden("confirm")
	return cmd
}

func (c *CLI) runProjectSplit(projectRef string, opts projectSplitCommandOptions, dryRun bool) error {
	client, err := c.newClient()
	if err != nil {
		return err
	}
	project, err := c.resolveNamedProject(client, projectRef)
	if err != nil {
		return err
	}
	targets, err := c.parseProjectSplitTargets(client, opts.Apps, opts.CreateProjects)
	if err != nil {
		return err
	}
	request := projectSplitRequest{
		Targets:              targets,
		CreateProjects:       opts.CreateProjects,
		IncludeOwnedServices: opts.IncludeOwnedServices,
		IncludeBoundServices: opts.IncludeBoundServices,
		OnConflict:           opts.OnConflict,
		DryRun:               dryRun,
	}
	var plan projectMovePlan
	if dryRun {
		plan, err = client.PlanProjectSplit(project.ID, request)
	} else {
		plan, err = client.SplitProject(project.ID, request)
	}
	if err != nil {
		return err
	}
	return c.renderProjectMovePlan(plan)
}

func addProjectOwnershipMoveFlags(cmd *cobra.Command, opts *projectOwnershipMoveOptions, createHelp string) {
	cmd.Flags().StringVar(&opts.ProjectName, "to", "", "Target project name or slug")
	cmd.Flags().StringVar(&opts.ProjectID, "to-project-id", "", "Target project ID")
	cmd.Flags().BoolVar(&opts.CreateProject, "create-project", false, createHelp)
	cmd.Flags().BoolVar(&opts.IncludeOwnedServices, "include-owned-services", opts.IncludeOwnedServices, "Move app-owned backing services with the app")
	cmd.Flags().BoolVar(&opts.IncludeBoundServices, "include-bound-services", false, "Move independently-created bound services when safe")
	cmd.Flags().StringVar(&opts.OnConflict, "on-conflict", opts.OnConflict, "Backing service name conflict policy: fail, rename, or use-existing")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "Show the project ownership move plan without writing changes")
	cmd.Flags().BoolVar(&opts.Confirm, "confirm", false, "Confirm project ownership changes")
	_ = cmd.Flags().MarkHidden("to-project-id")
}

func addProjectSplitFlags(cmd *cobra.Command, opts *projectSplitCommandOptions) {
	cmd.Flags().StringArrayVar(&opts.Apps, "app", nil, "App-to-project mapping in app=project form; repeat for multiple apps")
	cmd.Flags().BoolVar(&opts.CreateProjects, "create-projects", false, "Create target projects that do not exist")
	cmd.Flags().BoolVar(&opts.IncludeOwnedServices, "include-owned-services", opts.IncludeOwnedServices, "Move app-owned backing services with each app")
	cmd.Flags().BoolVar(&opts.IncludeBoundServices, "include-bound-services", false, "Move independently-created bound services when every bound app moves to the same target")
	cmd.Flags().StringVar(&opts.OnConflict, "on-conflict", opts.OnConflict, "Backing service name conflict policy: fail, rename, or use-existing")
	cmd.Flags().BoolVar(&opts.Confirm, "confirm", false, "Confirm project ownership changes")
	_ = cmd.MarkFlagRequired("app")
}

func (c *CLI) resolveProjectMoveTarget(client *Client, projectID, projectName string, create bool) (string, string, error) {
	projectID = strings.TrimSpace(projectID)
	projectName = strings.TrimSpace(projectName)
	if projectID != "" {
		return projectID, "", nil
	}
	if projectName == "" {
		return "", "", fmt.Errorf("target project is required; pass --to or --to-project-id")
	}
	if create {
		return "", projectName, nil
	}
	project, err := c.resolveNamedProject(client, projectName)
	if err != nil {
		return "", "", err
	}
	return project.ID, "", nil
}

func (c *CLI) parseProjectSplitTargets(client *Client, raw []string, createProjects bool) ([]projectSplitTargetRequest, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("at least one --app app=project mapping is required")
	}
	targets := make([]projectSplitTargetRequest, 0, len(raw))
	for _, item := range raw {
		left, right, ok := strings.Cut(strings.TrimSpace(item), "=")
		left = strings.TrimSpace(left)
		right = strings.TrimSpace(right)
		if !ok || left == "" || right == "" {
			return nil, fmt.Errorf("invalid --app mapping %q; expected app=project", item)
		}
		target := projectSplitTargetRequest{}
		if strings.HasPrefix(left, "app_") {
			target.AppID = left
		} else {
			target.AppName = left
		}
		if createProjects {
			target.TargetProjectName = right
		} else {
			project, err := c.resolveNamedProject(client, right)
			if err != nil {
				return nil, err
			}
			target.TargetProjectID = project.ID
		}
		targets = append(targets, target)
	}
	return targets, nil
}

func requireConfirmedProjectMove(dryRun, confirm bool, command string) error {
	if dryRun || confirm {
		return nil
	}
	return fmt.Errorf("%s changes project ownership; rerun with --dry-run to preview or --confirm to apply", command)
}

func (c *CLI) renderProjectMovePlan(plan projectMovePlan) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{"plan": redactProjectMovePlanForOutput(plan)})
	}
	projectNames := mapProjectNamesByID(append(append([]model.Project{plan.SourceProject}, plan.TargetProjects...), plan.CreatedProjects...))
	if err := writeKeyValues(c.stdout,
		kvPair{Key: "dry_run", Value: fmt.Sprintf("%t", plan.DryRun)},
		kvPair{Key: "source_project", Value: formatDisplayName(firstNonEmptyTrimmed(plan.SourceProject.Name, plan.SourceProject.ID), plan.SourceProject.ID, c.showIDs())},
		kvPair{Key: "target_projects", Value: formatProjectMoveProjectList(plan.TargetProjects, c.showIDs())},
		kvPair{Key: "created_projects", Value: formatProjectMoveProjectList(plan.CreatedProjects, c.showIDs())},
		kvPair{Key: "apps", Value: fmt.Sprintf("%d", len(plan.Apps))},
		kvPair{Key: "backing_services", Value: fmt.Sprintf("%d", len(plan.BackingServices))},
		kvPair{Key: "binding_updates", Value: fmt.Sprintf("%d", len(plan.Bindings))},
		kvPair{Key: "warnings", Value: fmt.Sprintf("%d", len(plan.Warnings))},
		kvPair{Key: "blockers", Value: fmt.Sprintf("%d", len(plan.Blockers))},
	); err != nil {
		return err
	}
	if len(plan.Apps) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if err := writeAppTableWithRuntimeNames(c.stdout, plan.Apps, nil, c.showIDs()); err != nil {
			return err
		}
	}
	if len(plan.BackingServices) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if err := writeServiceTableWithContext(c.stdout, plan.BackingServices, projectNames, nil, nil, c.showIDs()); err != nil {
			return err
		}
	}
	if len(plan.Bindings) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if err := writeBindingTable(c.stdout, plan.Bindings, plan.BackingServices); err != nil {
			return err
		}
	}
	for _, warning := range plan.Warnings {
		if _, err := fmt.Fprintf(c.stdout, "warning=%s\n", warning); err != nil {
			return err
		}
	}
	for _, blocker := range plan.Blockers {
		if _, err := fmt.Fprintf(c.stdout, "blocker=%s\n", blocker); err != nil {
			return err
		}
	}
	return nil
}

func formatProjectMoveProjectList(projects []model.Project, showIDs bool) string {
	if len(projects) == 0 {
		return ""
	}
	parts := make([]string, 0, len(projects))
	for _, project := range projects {
		parts = append(parts, formatDisplayName(firstNonEmptyTrimmed(project.Name, project.ID), project.ID, showIDs))
	}
	return strings.Join(parts, ",")
}

func redactProjectMovePlanForOutput(plan projectMovePlan) projectMovePlan {
	out := plan
	out.Apps = redactAppsForOutput(plan.Apps)
	if len(plan.BackingServices) > 0 {
		out.BackingServices = make([]model.BackingService, 0, len(plan.BackingServices))
		for _, service := range plan.BackingServices {
			out.BackingServices = append(out.BackingServices, redactBackingServiceForOutput(service))
		}
	}
	out.Bindings = cloneBindingsForOutput(plan.Bindings, true)
	return out
}

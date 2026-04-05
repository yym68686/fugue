package cli

import "github.com/spf13/cobra"

func (c *CLI) newDeployInspectCommand() *cobra.Command {
	opts := inspectTemplateOptions{}
	cmd := &cobra.Command{
		Use:   "inspect [path-or-repo]",
		Short: "Inspect local source or a GitHub repo before deploy",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target := ""
			if len(args) == 1 {
				target = args[0]
			}
			return c.runInspectTemplateTarget(target, opts, "inspect")
		},
	}
	bindInspectTemplateFlags(cmd, &opts)
	cmd.AddCommand(c.newDeployInspectGitHubCommand())
	return cmd
}

func (c *CLI) newDeployInspectGitHubCommand() *cobra.Command {
	opts := inspectTemplateOptions{}
	cmd := &cobra.Command{
		Use:   "github <repo-or-url>",
		Short: "Inspect a GitHub repository before deploy",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runInspectGitHubTemplate(normalizeGitHubRepoArg(args[0]), opts, "inspect")
		},
	}
	bindInspectTemplateFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newDeployPlanCommand() *cobra.Command {
	opts := inspectTemplateOptions{}
	cmd := &cobra.Command{
		Use:   "plan [path-or-repo]",
		Short: "Preview what Fugue would deploy from local source or GitHub",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target := ""
			if len(args) == 1 {
				target = args[0]
			}
			return c.runInspectTemplateTarget(target, opts, "plan")
		},
	}
	bindInspectTemplateFlags(cmd, &opts)
	cmd.AddCommand(c.newDeployPlanGitHubCommand())
	return cmd
}

func (c *CLI) newDeployPlanGitHubCommand() *cobra.Command {
	opts := inspectTemplateOptions{}
	cmd := &cobra.Command{
		Use:   "github <repo-or-url>",
		Short: "Preview what Fugue would deploy from a GitHub repo",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runInspectGitHubTemplate(normalizeGitHubRepoArg(args[0]), opts, "plan")
		},
	}
	bindInspectTemplateFlags(cmd, &opts)
	return cmd
}

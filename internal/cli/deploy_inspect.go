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

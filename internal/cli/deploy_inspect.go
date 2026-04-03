package cli

import "github.com/spf13/cobra"

func (c *CLI) newDeployInspectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect deploy sources before import",
	}
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
	cmd.Flags().StringVar(&opts.Branch, "branch", "", "Git branch to inspect")
	cmd.Flags().BoolVar(&opts.Private, "private", false, "Treat the repository as private")
	cmd.Flags().StringVar(&opts.RepoToken, "repo-token", "", "GitHub token for private repo access")
	return cmd
}

func (c *CLI) newDeployPlanCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Preview deploy topology",
	}
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
	cmd.Flags().StringVar(&opts.Branch, "branch", "", "Git branch to inspect")
	cmd.Flags().BoolVar(&opts.Private, "private", false, "Treat the repository as private")
	cmd.Flags().StringVar(&opts.RepoToken, "repo-token", "", "GitHub token for private repo access")
	return cmd
}

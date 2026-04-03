package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

type inspectTemplateOptions struct {
	Branch    string
	Private   bool
	RepoToken string
}

func (c *CLI) newTemplateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "template",
		Short: "Inspect deployable Fugue templates",
	}
	cmd.AddCommand(c.newTemplateInspectCommand())
	return cmd
}

func (c *CLI) newTemplateInspectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect a deploy template source",
	}
	cmd.AddCommand(c.newTemplateInspectGitHubCommand())
	return cmd
}

func (c *CLI) newTemplateInspectGitHubCommand() *cobra.Command {
	opts := inspectTemplateOptions{}
	cmd := &cobra.Command{
		Use:   "github <repo-or-url>",
		Short: "Inspect a GitHub repository as a Fugue template",
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

func (c *CLI) runInspectGitHubTemplate(repoURL string, opts inspectTemplateOptions, mode string) error {
	client, err := c.newClient()
	if err != nil {
		return err
	}
	request := inspectGitHubTemplateRequest{
		RepoURL:       repoURL,
		Branch:        strings.TrimSpace(opts.Branch),
		RepoAuthToken: strings.TrimSpace(opts.RepoToken),
	}
	if opts.Private {
		request.RepoVisibility = "private"
	}
	response, err := client.InspectGitHubTemplate(request)
	if err != nil {
		return err
	}
	if c.wantsJSON() {
		return writeJSON(c.stdout, response)
	}
	switch strings.TrimSpace(mode) {
	case "plan":
		return renderTemplatePlan(c.stdout, response)
	default:
		return renderTemplateInspection(c.stdout, response)
	}
}

func renderTemplateInspection(w io.Writer, response inspectGitHubTemplateResponse) error {
	if err := writeKeyValues(w,
		kvPair{Key: "repo_url", Value: response.Repository.RepoURL},
		kvPair{Key: "repo_visibility", Value: response.Repository.RepoVisibility},
		kvPair{Key: "repo_owner", Value: response.Repository.RepoOwner},
		kvPair{Key: "repo_name", Value: response.Repository.RepoName},
		kvPair{Key: "branch", Value: response.Repository.Branch},
		kvPair{Key: "commit_sha", Value: response.Repository.CommitSHA},
		kvPair{Key: "default_app_name", Value: response.Repository.DefaultAppName},
	); err != nil {
		return err
	}
	if response.Template != nil {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeKeyValues(w,
			kvPair{Key: "template_name", Value: response.Template.Name},
			kvPair{Key: "template_slug", Value: response.Template.Slug},
			kvPair{Key: "template_source_mode", Value: response.Template.SourceMode},
			kvPair{Key: "template_default_runtime", Value: response.Template.DefaultRuntime},
		); err != nil {
			return err
		}
	}
	if response.FugueManifest != nil && len(response.FugueManifest.Services) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		return writeTemplateServiceTable(w, response.FugueManifest.Services)
	}
	return nil
}

func renderTemplatePlan(w io.Writer, response inspectGitHubTemplateResponse) error {
	if err := writeKeyValues(w,
		kvPair{Key: "repo", Value: response.Repository.RepoURL},
		kvPair{Key: "default_app_name", Value: response.Repository.DefaultAppName},
	); err != nil {
		return err
	}
	if response.FugueManifest != nil {
		if err := writeKeyValues(w,
			kvPair{Key: "manifest_path", Value: response.FugueManifest.ManifestPath},
			kvPair{Key: "primary_service", Value: response.FugueManifest.PrimaryService},
		); err != nil {
			return err
		}
		if len(response.FugueManifest.Services) > 0 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
			return writeTemplateServiceTable(w, response.FugueManifest.Services)
		}
	}
	return nil
}

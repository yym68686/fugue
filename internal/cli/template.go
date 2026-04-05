package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

type inspectTemplateOptions struct {
	Branch    string
	Private   bool
	RepoToken string
}

type templateInspectionView struct {
	Repository    *inspectGitHubTemplateRepository
	Upload        *inspectUploadTemplateUpload
	FugueManifest *inspectGitHubTemplateManifest
	ComposeStack  *inspectGitHubTemplateComposeStack
	Template      *templateMetadata
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
	opts := inspectTemplateOptions{}
	cmd := &cobra.Command{
		Use:   "inspect [path-or-repo]",
		Short: "Inspect local source or a GitHub repo as a deploy template",
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
	bindInspectTemplateFlags(cmd, &opts)
	return cmd
}

func bindInspectTemplateFlags(cmd *cobra.Command, opts *inspectTemplateOptions) {
	cmd.Flags().StringVar(&opts.Branch, "branch", "", "Git branch to inspect")
	cmd.Flags().BoolVar(&opts.Private, "private", false, "Treat the repository as private")
	cmd.Flags().StringVar(&opts.RepoToken, "repo-token", "", "GitHub token for private repo access")
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
	return renderTemplateView(c.stdout, inspectViewFromGitHub(response), mode)
}

func (c *CLI) runInspectTemplateTarget(target string, opts inspectTemplateOptions, mode string) error {
	kind, value, err := resolveTemplateInspectTarget(target)
	if err != nil {
		return err
	}
	switch kind {
	case "github":
		return c.runInspectGitHubTemplate(value, opts, mode)
	default:
		return c.runInspectUploadTemplate(value, mode)
	}
}

func (c *CLI) runInspectUploadTemplate(pathArg, mode string) error {
	workingDir, err := resolveDeployPath(pathArg, "")
	if err != nil {
		return err
	}
	client, err := c.newClient()
	if err != nil {
		return err
	}

	name := defaultDeployAppName(workingDir, "")
	if name == "" {
		name = "app"
	}
	archiveBytes, archiveName, err := createSourceArchive(workingDir, name)
	if err != nil {
		return err
	}
	response, err := client.InspectUploadTemplate(importUploadRequest{Name: name}, archiveName, archiveBytes)
	if err != nil {
		return err
	}
	if c.wantsJSON() {
		return writeJSON(c.stdout, response)
	}
	return renderTemplateView(c.stdout, inspectViewFromUpload(response), mode)
}

func inspectViewFromGitHub(response inspectGitHubTemplateResponse) templateInspectionView {
	repository := response.Repository
	return templateInspectionView{
		Repository:    &repository,
		FugueManifest: response.FugueManifest,
		ComposeStack:  response.ComposeStack,
		Template:      response.Template,
	}
}

func inspectViewFromUpload(response inspectUploadTemplateResponse) templateInspectionView {
	upload := response.Upload
	return templateInspectionView{
		Upload:        &upload,
		FugueManifest: response.FugueManifest,
		ComposeStack:  response.ComposeStack,
	}
}

func resolveTemplateInspectTarget(target string) (string, string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "local", ".", nil
	}
	if looksLikeGitHubInspectTarget(target) && !localPathExists(target) {
		return "github", normalizeGitHubRepoArg(target), nil
	}
	if strings.HasPrefix(target, "git@github.com:") {
		return "github", normalizeGitHubRepoArg(target), nil
	}
	if strings.Contains(target, "://") && strings.Contains(strings.ToLower(target), "github.com") {
		return "github", normalizeGitHubRepoArg(target), nil
	}
	return "local", target, nil
}

func localPathExists(target string) bool {
	if strings.TrimSpace(target) == "" {
		return false
	}
	_, err := os.Stat(target)
	return err == nil
}

func looksLikeGitHubInspectTarget(target string) bool {
	target = strings.TrimSpace(target)
	switch {
	case target == "":
		return false
	case strings.HasPrefix(target, "git@github.com:"):
		return true
	case strings.Contains(target, "://"):
		return strings.Contains(strings.ToLower(target), "github.com")
	case strings.HasPrefix(target, "."), strings.HasPrefix(target, "/"), strings.HasPrefix(target, "~"):
		return false
	}

	parts := strings.Split(strings.Trim(target, "/"), "/")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" || strings.HasPrefix(part, ".") {
			return false
		}
	}
	return true
}

func renderTemplateInspection(w io.Writer, view templateInspectionView) error {
	return renderTemplateView(w, view, "inspect")
}

func renderTemplatePlan(w io.Writer, view templateInspectionView) error {
	return renderTemplateView(w, view, "plan")
}

func renderTemplateView(w io.Writer, view templateInspectionView, mode string) error {
	pairs := []kvPair{
		{Key: "mode", Value: firstNonEmpty(strings.TrimSpace(mode), "inspect")},
		{Key: "source", Value: view.sourceKind()},
		{Key: "default_app_name", Value: view.defaultAppName()},
		{Key: "topology", Value: view.topologyKind()},
		{Key: "topology_path", Value: view.topologyPath()},
		{Key: "primary_service", Value: view.primaryService()},
		{Key: "service_count", Value: fmt.Sprintf("%d", len(view.services()))},
		{Key: "warning_count", Value: fmt.Sprintf("%d", len(view.warnings()))},
		{Key: "inference_count", Value: fmt.Sprintf("%d", len(view.inferenceReport()))},
	}
	if view.Repository != nil {
		pairs = append(pairs,
			kvPair{Key: "repo_url", Value: view.Repository.RepoURL},
			kvPair{Key: "repo_visibility", Value: view.Repository.RepoVisibility},
			kvPair{Key: "repo_owner", Value: view.Repository.RepoOwner},
			kvPair{Key: "repo_name", Value: view.Repository.RepoName},
			kvPair{Key: "branch", Value: view.Repository.Branch},
			kvPair{Key: "commit_sha", Value: view.Repository.CommitSHA},
			kvPair{Key: "commit_committed_at", Value: view.Repository.CommitCommittedAt},
		)
	}
	if view.Upload != nil {
		pairs = append(pairs,
			kvPair{Key: "archive_filename", Value: view.Upload.ArchiveFilename},
			kvPair{Key: "archive_sha256", Value: view.Upload.ArchiveSHA256},
			kvPair{Key: "archive_size_bytes", Value: fmt.Sprintf("%d", view.Upload.ArchiveSizeBytes)},
			kvPair{Key: "source_kind", Value: view.Upload.SourceKind},
			kvPair{Key: "source_path", Value: view.Upload.SourcePath},
		)
	}
	if view.Template != nil {
		pairs = append(pairs,
			kvPair{Key: "template_name", Value: view.Template.Name},
			kvPair{Key: "template_slug", Value: view.Template.Slug},
			kvPair{Key: "template_description", Value: view.Template.Description},
			kvPair{Key: "template_source_mode", Value: view.Template.SourceMode},
			kvPair{Key: "template_default_runtime", Value: view.Template.DefaultRuntime},
			kvPair{Key: "template_demo_url", Value: view.Template.DemoURL},
			kvPair{Key: "template_docs_url", Value: view.Template.DocsURL},
		)
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}

	if services := view.services(); len(services) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[services]"); err != nil {
			return err
		}
		if err := writeTemplateServiceTable(w, services); err != nil {
			return err
		}
	}
	if view.Template != nil && len(view.Template.Variables) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[template_variables]"); err != nil {
			return err
		}
		if err := writeTemplateVariableTable(w, view.Template.Variables); err != nil {
			return err
		}
	}
	if hasTemplateSeedFiles(view.services()) {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[persistent_storage_seed_files]"); err != nil {
			return err
		}
		if err := writeTemplateSeedFileTable(w, view.services()); err != nil {
			return err
		}
	}
	if warnings := view.warnings(); len(warnings) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[warnings]"); err != nil {
			return err
		}
		for _, warning := range warnings {
			if _, err := fmt.Fprintln(w, warning); err != nil {
				return err
			}
		}
	}
	if report := view.inferenceReport(); len(report) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[inference_report]"); err != nil {
			return err
		}
		if err := writeTemplateInferenceTable(w, report); err != nil {
			return err
		}
	}
	return nil
}

func (view templateInspectionView) sourceKind() string {
	switch {
	case view.Repository != nil:
		return "github"
	case view.Upload != nil:
		return "upload"
	default:
		return ""
	}
}

func (view templateInspectionView) defaultAppName() string {
	switch {
	case view.Repository != nil:
		return strings.TrimSpace(view.Repository.DefaultAppName)
	case view.Upload != nil:
		return strings.TrimSpace(view.Upload.DefaultAppName)
	default:
		return ""
	}
}

func (view templateInspectionView) topologyKind() string {
	switch {
	case view.FugueManifest != nil:
		return "fugue_manifest"
	case view.ComposeStack != nil:
		return "compose_stack"
	default:
		return "none"
	}
}

func (view templateInspectionView) topologyPath() string {
	if view.FugueManifest != nil {
		return strings.TrimSpace(view.FugueManifest.ManifestPath)
	}
	if view.ComposeStack != nil {
		return strings.TrimSpace(view.ComposeStack.ComposePath)
	}
	return ""
}

func (view templateInspectionView) primaryService() string {
	if view.FugueManifest != nil {
		return strings.TrimSpace(view.FugueManifest.PrimaryService)
	}
	if view.ComposeStack != nil {
		return strings.TrimSpace(view.ComposeStack.PrimaryService)
	}
	return ""
}

func (view templateInspectionView) services() []inspectGitHubTemplateManifestService {
	if view.FugueManifest != nil {
		return view.FugueManifest.Services
	}
	if view.ComposeStack != nil {
		return view.ComposeStack.Services
	}
	return nil
}

func (view templateInspectionView) warnings() []string {
	if view.FugueManifest != nil {
		return view.FugueManifest.Warnings
	}
	if view.ComposeStack != nil {
		return view.ComposeStack.Warnings
	}
	return nil
}

func (view templateInspectionView) inferenceReport() []templateTopologyInference {
	if view.FugueManifest != nil {
		return view.FugueManifest.InferenceReport
	}
	if view.ComposeStack != nil {
		return view.ComposeStack.InferenceReport
	}
	return nil
}

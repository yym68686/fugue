package cli

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppSourceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "source",
		Short: "Inspect app source provenance and build inputs",
	}
	cmd.AddCommand(
		c.newAppSourceShowCommand(),
		c.newAppSourceBindGitHubCommand(),
	)
	return cmd
}

func (c *CLI) newAppSourceShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <app>",
		Aliases: []string{"get", "status"},
		Short:   "Show the app's source definition",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"source":        app.Source,
					"origin_source": model.AppOriginSource(app),
					"build_source":  model.AppBuildSource(app),
					"app":           app,
				})
			}
			return writeAppSource(c.stdout, app.Name, model.AppOriginSource(app), model.AppBuildSource(app), app.Spec.ImageMirrorLimit)
		},
	}
}

func (c *CLI) newAppSourceBindGitHubCommand() *cobra.Command {
	opts := struct {
		Branch          string
		Public          bool
		Private         bool
		RepoToken       string
		SourceDir       string
		BuildStrategy   string
		DockerfilePath  string
		BuildContextDir string
		ImageNameSuffix string
		ComposeService  string
	}{}

	cmd := &cobra.Command{
		Use:   "bind-github <app> <repo-or-url>",
		Short: "Rebind an app's durable source ownership to GitHub",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.Public && opts.Private {
				return fmt.Errorf("--public and --private are mutually exclusive")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}

			currentOrigin := model.AppOriginSource(app)
			if currentOrigin == nil {
				currentOrigin = model.AppBuildSource(app)
			}
			nextOrigin := appSourceRepairDefaults(currentOrigin)

			nextOrigin.Type = resolveGitHubRepairSourceType(currentOrigin, opts.Public, opts.Private, opts.RepoToken)
			nextOrigin.RepoURL = normalizeGitHubRepoArg(args[1])
			if trimmed := strings.TrimSpace(opts.Branch); trimmed != "" {
				nextOrigin.RepoBranch = trimmed
			}
			if trimmed := strings.TrimSpace(opts.RepoToken); trimmed != "" {
				nextOrigin.RepoAuthToken = trimmed
			} else if opts.Public {
				nextOrigin.RepoAuthToken = ""
			} else if currentOrigin != nil && model.IsGitHubAppSourceType(currentOrigin.Type) {
				nextOrigin.RepoAuthToken = strings.TrimSpace(currentOrigin.RepoAuthToken)
			} else if nextOrigin.Type == model.AppSourceTypeGitHubPublic {
				nextOrigin.RepoAuthToken = ""
			}
			if trimmed := strings.TrimSpace(opts.SourceDir); trimmed != "" {
				nextOrigin.SourceDir = trimmed
			}
			if trimmed := strings.TrimSpace(opts.BuildStrategy); trimmed != "" {
				nextOrigin.BuildStrategy = trimmed
			}
			if trimmed := strings.TrimSpace(opts.DockerfilePath); trimmed != "" {
				nextOrigin.DockerfilePath = trimmed
			}
			if trimmed := strings.TrimSpace(opts.BuildContextDir); trimmed != "" {
				nextOrigin.BuildContextDir = trimmed
			}
			if trimmed := strings.TrimSpace(opts.ImageNameSuffix); trimmed != "" {
				nextOrigin.ImageNameSuffix = trimmed
			}
			if trimmed := strings.TrimSpace(opts.ComposeService); trimmed != "" {
				nextOrigin.ComposeService = trimmed
			}

			response, err := client.PatchAppOriginSource(app.ID, nextOrigin)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app":             response.App,
					"already_current": response.AlreadyCurrent,
					"origin_source":   model.AppOriginSource(response.App),
					"build_source":    model.AppBuildSource(response.App),
				})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app", Value: response.App.Name},
				kvPair{Key: "already_current", Value: strconv.FormatBool(response.AlreadyCurrent)},
				kvPair{Key: "origin_source_type", Value: sourceField(model.AppOriginSource(response.App), func(source *model.AppSource) string { return source.Type })},
				kvPair{Key: "origin_source_ref", Value: sourceRef(model.AppOriginSource(response.App))},
				kvPair{Key: "build_source_type", Value: sourceField(model.AppBuildSource(response.App), func(source *model.AppSource) string { return source.Type })},
				kvPair{Key: "build_source_ref", Value: sourceRef(model.AppBuildSource(response.App))},
			)
		},
	}
	cmd.Flags().StringVar(&opts.Branch, "branch", "", "Git branch to track for future syncs")
	cmd.Flags().BoolVar(&opts.Public, "public", false, "Treat the rebound repository as public")
	cmd.Flags().BoolVar(&opts.Private, "private", false, "Treat the rebound repository as private")
	cmd.Flags().StringVar(&opts.RepoToken, "repo-token", "", "GitHub token for private repo access")
	cmd.Flags().StringVar(&opts.SourceDir, "source-dir", "", "Override the tracked source directory")
	cmd.Flags().StringVar(&opts.BuildStrategy, "build-strategy", "", "Override the tracked build strategy")
	cmd.Flags().StringVar(&opts.DockerfilePath, "dockerfile", "", "Override the tracked Dockerfile path")
	cmd.Flags().StringVar(&opts.BuildContextDir, "context", "", "Override the tracked Docker build context")
	cmd.Flags().StringVar(&opts.ImageNameSuffix, "image-suffix", "", "Override the tracked managed image suffix")
	cmd.Flags().StringVar(&opts.ComposeService, "compose-service", "", "Override the tracked compose service name")
	return cmd
}

func resolveGitHubRepairSourceType(current *model.AppSource, public, private bool, repoToken string) string {
	if public {
		return model.AppSourceTypeGitHubPublic
	}
	if private || strings.TrimSpace(repoToken) != "" {
		return model.AppSourceTypeGitHubPrivate
	}
	if current != nil && strings.TrimSpace(current.Type) == model.AppSourceTypeGitHubPrivate {
		return model.AppSourceTypeGitHubPrivate
	}
	return model.AppSourceTypeGitHubPublic
}

func appSourceRepairDefaults(current *model.AppSource) *model.AppSource {
	out := &model.AppSource{}
	if current == nil {
		return out
	}
	out.SourceDir = strings.TrimSpace(current.SourceDir)
	out.BuildStrategy = strings.TrimSpace(current.BuildStrategy)
	out.DockerfilePath = strings.TrimSpace(current.DockerfilePath)
	out.BuildContextDir = strings.TrimSpace(current.BuildContextDir)
	out.ImageNameSuffix = strings.TrimSpace(current.ImageNameSuffix)
	out.ComposeService = strings.TrimSpace(current.ComposeService)
	if len(current.ComposeDependsOn) > 0 {
		out.ComposeDependsOn = append([]string(nil), current.ComposeDependsOn...)
	}
	return out
}

func writeAppSource(w io.Writer, appName string, originSource, buildSource *model.AppSource, imageMirrorLimit int) error {
	pairs := []kvPair{
		{Key: "app", Value: strings.TrimSpace(appName)},
		{Key: "release_retain", Value: formatImageMirrorLimit(imageMirrorLimit)},
	}
	if originSource == nil && buildSource == nil {
		pairs = append(pairs, kvPair{Key: "source_type", Value: "unknown"})
		return writeKeyValues(w, pairs...)
	}
	appendSourcePairs := func(prefix string, source *model.AppSource) {
		if source == nil {
			return
		}
		pairs = append(pairs,
			kvPair{Key: prefix + "_type", Value: strings.TrimSpace(source.Type)},
			kvPair{Key: prefix + "_ref", Value: sourceRef(source)},
			kvPair{Key: prefix + "_upload_id", Value: strings.TrimSpace(source.UploadID)},
			kvPair{Key: prefix + "_upload_filename", Value: strings.TrimSpace(source.UploadFilename)},
			kvPair{Key: prefix + "_archive_sha256", Value: strings.TrimSpace(source.ArchiveSHA256)},
			kvPair{Key: prefix + "_archive_size_bytes", Value: formatBytesCount(source.ArchiveSizeBytes)},
			kvPair{Key: prefix + "_repo_branch", Value: strings.TrimSpace(source.RepoBranch)},
			kvPair{Key: prefix + "_commit_sha", Value: strings.TrimSpace(source.CommitSHA)},
			kvPair{Key: prefix + "_commit_committed_at", Value: strings.TrimSpace(source.CommitCommittedAt)},
			kvPair{Key: prefix + "_build_strategy", Value: strings.TrimSpace(source.BuildStrategy)},
			kvPair{Key: prefix + "_source_dir", Value: strings.TrimSpace(source.SourceDir)},
			kvPair{Key: prefix + "_dockerfile_path", Value: strings.TrimSpace(source.DockerfilePath)},
			kvPair{Key: prefix + "_build_context_dir", Value: strings.TrimSpace(source.BuildContextDir)},
			kvPair{Key: prefix + "_compose_service", Value: strings.TrimSpace(source.ComposeService)},
			kvPair{Key: prefix + "_detected_provider", Value: strings.TrimSpace(source.DetectedProvider)},
			kvPair{Key: prefix + "_detected_stack", Value: strings.TrimSpace(source.DetectedStack)},
		)
	}
	appendSourcePairs("origin_source", originSource)
	appendSourcePairs("build_source", buildSource)
	return writeKeyValues(w, pairs...)
}

func formatBytesCount(value int64) string {
	if value <= 0 {
		return ""
	}
	return strconv.FormatInt(value, 10)
}

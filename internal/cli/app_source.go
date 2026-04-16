package cli

import (
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
	cmd.AddCommand(c.newAppSourceShowCommand())
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
				return writeJSON(c.stdout, map[string]any{"source": app.Source, "app": app})
			}
			return writeAppSource(c.stdout, app.Name, app.Source, app.Spec.ImageMirrorLimit)
		},
	}
}

func writeAppSource(w io.Writer, appName string, source *model.AppSource, imageMirrorLimit int) error {
	pairs := []kvPair{
		{Key: "app", Value: strings.TrimSpace(appName)},
		{Key: "release_retain", Value: formatImageMirrorLimit(imageMirrorLimit)},
	}
	if source == nil {
		pairs = append(pairs, kvPair{Key: "source_type", Value: "unknown"})
		return writeKeyValues(w, pairs...)
	}
	pairs = append(pairs,
		kvPair{Key: "source_type", Value: strings.TrimSpace(source.Type)},
		kvPair{Key: "source_ref", Value: sourceRef(source)},
		kvPair{Key: "upload_id", Value: strings.TrimSpace(source.UploadID)},
		kvPair{Key: "upload_filename", Value: strings.TrimSpace(source.UploadFilename)},
		kvPair{Key: "archive_sha256", Value: strings.TrimSpace(source.ArchiveSHA256)},
		kvPair{Key: "archive_size_bytes", Value: formatBytesCount(source.ArchiveSizeBytes)},
		kvPair{Key: "repo_branch", Value: strings.TrimSpace(source.RepoBranch)},
		kvPair{Key: "commit_sha", Value: strings.TrimSpace(source.CommitSHA)},
		kvPair{Key: "commit_committed_at", Value: strings.TrimSpace(source.CommitCommittedAt)},
		kvPair{Key: "build_strategy", Value: strings.TrimSpace(source.BuildStrategy)},
		kvPair{Key: "source_dir", Value: strings.TrimSpace(source.SourceDir)},
		kvPair{Key: "dockerfile_path", Value: strings.TrimSpace(source.DockerfilePath)},
		kvPair{Key: "build_context_dir", Value: strings.TrimSpace(source.BuildContextDir)},
		kvPair{Key: "compose_service", Value: strings.TrimSpace(source.ComposeService)},
		kvPair{Key: "detected_provider", Value: strings.TrimSpace(source.DetectedProvider)},
		kvPair{Key: "detected_stack", Value: strings.TrimSpace(source.DetectedStack)},
	)
	return writeKeyValues(w, pairs...)
}

func formatBytesCount(value int64) string {
	if value <= 0 {
		return ""
	}
	return strconv.FormatInt(value, 10)
}

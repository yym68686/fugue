package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppSyncCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Inspect and trigger app source syncs",
	}
	cmd.AddCommand(
		c.newAppSyncStatusCommand(),
		c.newAppSyncRunCommand(),
		c.newAppSyncResumeCommand(),
	)
	return cmd
}

func (c *CLI) newAppSyncStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "status <app>",
		Aliases: []string{"show", "get"},
		Short:   "Show source sync status for an app",
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
			operations, err := client.ListOperations(app.ID)
			if err != nil {
				return err
			}
			var latest *model.Operation
			if len(operations) > 0 {
				op, err := latestOperation(operations)
				if err != nil {
					return err
				}
				latest = &op
			}
			originSource := model.AppOriginSource(app)
			if c.wantsJSON() {
				payload := map[string]any{
					"app":            app,
					"sync_supported": originSource != nil,
					"origin_source":  originSource,
					"latest":         latest,
				}
				return writeJSON(c.stdout, payload)
			}
			pairs := []kvPair{
				{Key: "app_id", Value: app.ID},
				{Key: "sync_supported", Value: fmt.Sprintf("%t", originSource != nil)},
				{Key: "source_type", Value: sourceTypeForSync(originSource)},
				{Key: "source_ref", Value: sourceRef(originSource)},
				{Key: "repo_branch", Value: sourceField(originSource, func(source *model.AppSource) string { return source.RepoBranch })},
				{Key: "commit_sha", Value: sourceField(originSource, func(source *model.AppSource) string { return source.CommitSHA })},
				{Key: "build_strategy", Value: sourceField(originSource, func(source *model.AppSource) string { return source.BuildStrategy })},
			}
			if app.Status.SourceSync != nil {
				pairs = append(pairs, appSourceSyncStatusPairs(app.Status.SourceSync)...)
			}
			if latest != nil {
				pairs = append(pairs,
					kvPair{Key: "last_operation_id", Value: latest.ID},
					kvPair{Key: "last_operation_type", Value: latest.Type},
					kvPair{Key: "last_operation_status", Value: latest.Status},
					kvPair{Key: "last_operation_updated_at", Value: formatTime(latest.UpdatedAt)},
				)
			}
			return writeKeyValues(c.stdout, pairs...)
		},
	}
}

func (c *CLI) newAppSyncRunCommand() *cobra.Command {
	opts := struct {
		Branch          string
		ImageRef        string
		SourceDir       string
		DockerfilePath  string
		BuildContextDir string
		RepoToken       string
		ClearFiles      bool
		Wait            bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "run <app>",
		Aliases: []string{"rebuild", "now"},
		Short:   "Run a source sync for an app",
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
			if model.AppOriginSource(app) == nil {
				return fmt.Errorf("app does not have source metadata to sync from")
			}
			response, err := client.RebuildApp(app.ID, rebuildPlanRequest{
				Branch:          strings.TrimSpace(opts.Branch),
				ImageRef:        strings.TrimSpace(opts.ImageRef),
				SourceDir:       strings.TrimSpace(opts.SourceDir),
				DockerfilePath:  strings.TrimSpace(opts.DockerfilePath),
				BuildContextDir: strings.TrimSpace(opts.BuildContextDir),
				RepoAuthToken:   strings.TrimSpace(opts.RepoToken),
				ClearFiles:      opts.ClearFiles,
			})
			if err != nil {
				return err
			}
			if err := c.waitForOptionalOperation(client, &response.Operation, opts.Wait); err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app":       redactAppForOutput(app),
					"operation": redactOperationForOutput(response.Operation),
					"build":     response.Build,
				})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "operation_id", Value: response.Operation.ID},
				kvPair{Key: "source_type", Value: response.Build.SourceType},
				kvPair{Key: "build_strategy", Value: response.Build.BuildStrategy},
				kvPair{Key: "image_ref", Value: firstNonEmpty(response.Build.ResolvedImageRef, response.Build.ImageRef)},
			)
		},
	}
	cmd.Flags().StringVar(&opts.Branch, "branch", "", "Override the source branch")
	cmd.Flags().StringVar(&opts.ImageRef, "image", "", "Override the source image reference")
	cmd.Flags().StringVar(&opts.SourceDir, "source-dir", "", "Override the source directory")
	cmd.Flags().StringVar(&opts.DockerfilePath, "dockerfile", "", "Override the Dockerfile path")
	cmd.Flags().StringVar(&opts.BuildContextDir, "context", "", "Override the Docker build context")
	cmd.Flags().StringVar(&opts.RepoToken, "repo-token", "", "Repository auth token for private Git sources")
	cmd.Flags().BoolVar(&opts.ClearFiles, "clear-files", false, "Remove declarative app files before syncing")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) newAppSyncResumeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "resume <app>",
		Short: "Resume automatic source sync for an app",
		Args:  cobra.ExactArgs(1),
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
			originSource := model.AppOriginSource(app)
			if originSource == nil || !model.IsGitHubAppSourceType(originSource.Type) {
				return fmt.Errorf("app does not have a GitHub origin source to resume")
			}
			response, err := client.PatchAppOriginSource(app.ID, originSource)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app": response.App,
				})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: response.App.ID},
				kvPair{Key: "source_ref", Value: sourceRef(model.AppOriginSource(response.App))},
				kvPair{Key: "source_sync_phase", Value: sourceSyncPhase(response.App.Status.SourceSync)},
			)
		},
	}
}

func sourceTypeForSync(source *model.AppSource) string {
	if source == nil {
		return ""
	}
	return strings.TrimSpace(source.Type)
}

func sourceField(source *model.AppSource, fn func(*model.AppSource) string) string {
	if source == nil || fn == nil {
		return ""
	}
	return strings.TrimSpace(fn(source))
}

func appSourceSyncStatusPairs(status *model.AppSourceSyncStatus) []kvPair {
	if status == nil {
		return nil
	}
	return []kvPair{
		{Key: "source_sync_provider", Value: strings.TrimSpace(status.Provider)},
		{Key: "source_sync_phase", Value: strings.TrimSpace(status.Phase)},
		{Key: "source_sync_failures", Value: fmt.Sprintf("%d", status.ConsecutiveFailures)},
		{Key: "source_sync_last_checked_at", Value: formatOptionalTimePtr(status.LastCheckedAt)},
		{Key: "source_sync_last_success_at", Value: formatOptionalTimePtr(status.LastSuccessAt)},
		{Key: "source_sync_last_error_at", Value: formatOptionalTimePtr(status.LastErrorAt)},
		{Key: "source_sync_last_error_code", Value: strings.TrimSpace(status.LastErrorCode)},
		{Key: "source_sync_last_error", Value: strings.TrimSpace(status.LastErrorMessage)},
		{Key: "source_sync_next_check_at", Value: formatOptionalTimePtr(status.NextCheckAt)},
		{Key: "source_sync_suspended_at", Value: formatOptionalTimePtr(status.SuspendedAt)},
		{Key: "source_sync_needs_user_action", Value: fmt.Sprintf("%t", status.NeedsUserAction)},
	}
}

func sourceSyncPhase(status *model.AppSourceSyncStatus) string {
	if status == nil {
		return model.AppSourceSyncPhaseOK
	}
	return strings.TrimSpace(status.Phase)
}

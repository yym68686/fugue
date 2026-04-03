package cli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppReleaseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "release",
		Short: "Inspect and operate app releases",
	}
	cmd.AddCommand(
		c.newAppReleaseListCommand(),
		c.newAppReleaseRebuildCommand(),
		c.newAppReleaseDeployCommand(),
		c.newAppReleaseRollbackCommand(),
		c.newAppReleasePruneCommand(),
	)
	return cmd
}

func (c *CLI) newAppReleaseListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls <app>",
		Aliases: []string{"list"},
		Short:   "List release images for an app",
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
			inventory, err := client.GetAppImages(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, inventory)
			}
			if err := writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: inventory.AppID},
				kvPair{Key: "registry_configured", Value: fmt.Sprintf("%t", inventory.RegistryConfigured)},
				kvPair{Key: "version_count", Value: fmt.Sprintf("%d", inventory.Summary.VersionCount)},
				kvPair{Key: "reclaimable", Value: formatBytes(inventory.Summary.ReclaimableSizeBytes)},
			); err != nil {
				return err
			}
			if len(inventory.Versions) == 0 {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeAppImageTable(c.stdout, inventory.Versions)
		},
	}
}

func (c *CLI) newAppReleaseRebuildCommand() *cobra.Command {
	opts := struct {
		Branch          string
		ImageRef        string
		SourceDir       string
		DockerfilePath  string
		BuildContextDir string
		RepoToken       string
		Wait            bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "rebuild <app>",
		Short: "Rebuild an app from its source definition",
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
			response, err := client.RebuildApp(app.ID, rebuildPlanRequest{
				Branch:          opts.Branch,
				ImageRef:        opts.ImageRef,
				SourceDir:       opts.SourceDir,
				DockerfilePath:  opts.DockerfilePath,
				BuildContextDir: opts.BuildContextDir,
				RepoAuthToken:   opts.RepoToken,
			})
			if err != nil {
				return err
			}
			if err := c.waitForOptionalOperation(client, &response.Operation, opts.Wait); err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app_id":    app.ID,
					"operation": response.Operation,
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
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) newAppReleaseDeployCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "deploy <app>",
		Short: "Deploy the app's current desired spec",
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
			response, err := client.DeployApp(app.ID, nil)
			if err != nil {
				return err
			}
			result := appCommandResult{Operation: &response.Operation}
			if opts.Wait {
				finalApp, err := c.waitForSingleApp(client, app.ID, response.Operation, true)
				if err != nil {
					return err
				}
				result.App = finalApp
			} else {
				result.App = &app
			}
			return c.renderAppCommandResult(result)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) newAppReleaseRollbackCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "rollback <app> [image-ref]",
		Short: "Rollback to a previous release image",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			inventory, err := client.GetAppImages(app.ID)
			if err != nil {
				return err
			}
			imageRef := ""
			if len(args) == 2 {
				imageRef = strings.TrimSpace(args[1])
			} else {
				candidate, err := defaultRollbackImage(inventory.Versions)
				if err != nil {
					return err
				}
				imageRef = candidate.ImageRef
			}
			response, err := client.RedeployAppImage(app.ID, imageRef)
			if err != nil {
				return err
			}
			result := appCommandResult{Operation: &response.Operation}
			if opts.Wait {
				finalApp, err := c.waitForSingleApp(client, app.ID, response.Operation, true)
				if err != nil {
					return err
				}
				result.App = finalApp
			} else {
				result.App = &app
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app":       result.App,
					"operation": result.Operation,
					"image":     response.Image,
				})
			}
			if err := c.renderAppCommandResult(result); err != nil {
				return err
			}
			if response.Image != nil {
				_, err = fmt.Fprintf(c.stdout, "release_image=%s\n", response.Image.ImageRef)
				return err
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) newAppReleasePruneCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "prune <app>",
		Short: "Delete stale release images that can be reclaimed",
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
			inventory, err := client.GetAppImages(app.ID)
			if err != nil {
				return err
			}
			candidates := pruneCandidates(inventory.Versions)
			if c.wantsJSON() {
				results := make([]appImageDeleteResponse, 0, len(candidates))
				for _, version := range candidates {
					result, err := client.DeleteAppImage(app.ID, version.ImageRef)
					if err != nil {
						return err
					}
					results = append(results, result)
				}
				return writeJSON(c.stdout, map[string]any{
					"app_id":  app.ID,
					"deleted": results,
				})
			}
			if len(candidates) == 0 {
				return writeKeyValues(c.stdout, kvPair{Key: "app_id", Value: app.ID}, kvPair{Key: "deleted_images", Value: "0"})
			}
			deleted := 0
			reclaimed := int64(0)
			for _, version := range candidates {
				result, err := client.DeleteAppImage(app.ID, version.ImageRef)
				if err != nil {
					return err
				}
				if result.Deleted || result.AlreadyMissing {
					deleted++
					reclaimed += result.ReclaimedSizeBytes
				}
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "deleted_images", Value: fmt.Sprintf("%d", deleted)},
				kvPair{Key: "reclaimed", Value: formatBytes(reclaimed)},
			)
		},
	}
}

func defaultRollbackImage(versions []appImageVersion) (appImageVersion, error) {
	candidates := make([]appImageVersion, 0, len(versions))
	for _, version := range versions {
		if version.Current || !version.RedeploySupported {
			continue
		}
		candidates = append(candidates, version)
	}
	sort.Slice(candidates, func(i, j int) bool {
		left := timeValue(candidates[i].LastDeployedAt)
		right := timeValue(candidates[j].LastDeployedAt)
		return left.After(right)
	})
	if len(candidates) == 0 {
		return appImageVersion{}, fmt.Errorf("no previous redeployable release is available")
	}
	return candidates[0], nil
}

func pruneCandidates(versions []appImageVersion) []appImageVersion {
	out := make([]appImageVersion, 0, len(versions))
	for _, version := range versions {
		if version.Current {
			continue
		}
		if !version.DeleteSupported && !version.RedeploySupported {
			continue
		}
		if version.DeleteSupported || version.ReclaimableSizeBytes > 0 {
			out = append(out, version)
		}
	}
	return out
}

func timeValue(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return value.UTC()
}

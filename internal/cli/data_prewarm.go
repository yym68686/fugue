package cli

import (
	"context"
	"fmt"
	"fugue/internal/model"
	"github.com/spf13/cobra"
	"net/http"
	"net/url"
	"time"
)

func (c *CLI) newDataPrewarmCommand() *cobra.Command {
	var runtimeRef, version string
	var assets []string
	var wait bool
	var timeout time.Duration
	cmd := &cobra.Command{Use: "prewarm <workspace>", Short: "Verify and cache a snapshot on an owned managed runtime", Long: "Download an immutable snapshot into a runtime cache volume, with SHA256 verification. Requires an S3-compatible backend and an owned managed runtime. Cache expires after 24 hours; this command does not mount data into applications. Use data transfer show/wait/cancel to follow or stop the job.", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if runtimeRef == "" || timeout <= 0 {
			return fmt.Errorf("runtime is required and timeout must be positive")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		workspace, err := client.GetDataWorkspace(args[0])
		if err != nil {
			return err
		}
		runtimeID, err := resolveRuntimeSelection(client, "", runtimeRef)
		if err != nil {
			return err
		}
		var out struct {
			Transfer model.DataTransfer `json:"transfer"`
		}
		err = client.doJSON(http.MethodPost, "/v1/data/workspaces/"+url.PathEscape(workspace.Workspace.ID)+"/prewarm", map[string]any{"runtime_id": runtimeID, "version": version, "assets": assets}, &out)
		if err != nil {
			return err
		}
		t := out.Transfer
		if wait {
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			t, err = waitDataTransfer(ctx, client, t.ID, t)
		}
		report := map[string]any{"schema_version": 1, "transfer": t, "app_mount_changed": false, "next_commands": []string{"fugue data transfer show " + t.ID, "fugue data transfer wait " + t.ID, "fugue data transfer cancel " + t.ID}}
		if err != nil {
			report["error"] = describeCommandError(err)
		}
		if outErr := c.renderResourceResult(report); outErr != nil {
			return outErr
		}
		return err
	}}
	cmd.Flags().StringVar(&runtimeRef, "runtime", "", "Exact target managed runtime name or ID")
	cmd.Example = "fugue data prewarm dataset --version snapshot-v1 --runtime runtime-a"
	_ = cmd.MarkFlagRequired("runtime")
	cmd.Flags().StringVar(&version, "version", "latest", "Snapshot ID or immutable version (latest is frozen at submission)")
	cmd.Flags().StringSliceVar(&assets, "assets", nil, "Only cache these snapshot assets")
	cmd.Flags().BoolVar(&wait, "wait", true, "Wait for verified cache readiness")
	cmd.Flags().DurationVar(&timeout, "timeout", 35*time.Minute, "Local wait deadline; does not cancel the runtime job")
	cmd.AddCommand(c.newDataPrewarmEvictCommand())
	return cmd
}
func waitDataTransfer(ctx context.Context, client *Client, id string, last model.DataTransfer) (model.DataTransfer, error) {
	scoped := *client
	scoped.context = ctx
	for {
		current, err := scoped.GetDataTransferModel(id)
		if err != nil {
			return last, err
		}
		last = current
		switch last.Status {
		case model.DataTransferStatusCompleted:
			return last, nil
		case model.DataTransferStatusFailed:
			return last, withExitCode(fmt.Errorf("data transfer failed: %s", last.ErrorMessage), ExitCodeSystemFault)
		case model.DataTransferStatusCanceled:
			return last, withExitCode(fmt.Errorf("data transfer canceled; cache cleanup may still be pending"), ExitCodeIndeterminate)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, ctx.Err()
		case <-timer.C:
		}
	}
}
func (c *CLI) newDataTransferWaitCommand() *cobra.Command {
	var timeout time.Duration
	cmd := &cobra.Command{Use: "wait <transfer>", Short: "Wait for transfer completion; retain the last state on timeout", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if timeout <= 0 {
			return fmt.Errorf("timeout must be positive")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
		defer cancel()
		t, err := waitDataTransfer(ctx, client, args[0], model.DataTransfer{ID: args[0]})
		result := map[string]any{"schema_version": 1, "transfer": t, "next_commands": []string{"fugue data transfer show " + args[0]}}
		if err != nil {
			result["error"] = describeCommandError(err)
		}
		if outErr := c.renderResourceResult(result); outErr != nil {
			return outErr
		}
		return err
	}}
	cmd.Flags().DurationVar(&timeout, "timeout", 35*time.Minute, "Local wait deadline")
	return cmd
}

func (c *CLI) newDataPrewarmEvictCommand() *cobra.Command {
	var confirm bool
	cmd := &cobra.Command{Use: "evict <transfer>", Short: "Reclaim a runtime cache while preserving the source snapshot", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if !confirm {
			return fmt.Errorf("cache eviction requires --confirm")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		var out map[string]any
		if err = client.doJSON(http.MethodDelete, "/v1/data/transfers/"+url.PathEscape(args[0])+"/cache", nil, &out); err != nil {
			return err
		}
		return c.renderResourceResult(out)
	}}
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Confirm runtime cache eviction")
	cmd.Example = "fugue data prewarm evict data_transfer_123 --confirm"
	return cmd
}

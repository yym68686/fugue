package cli

import (
	"fmt"
	"fugue/internal/model"
	"net/http"
	"net/url"
	"time"

	"github.com/spf13/cobra"
)

func (c *CLI) newDataWorkspaceRetireCommand() *cobra.Command {
	var confirm, wait bool
	var timeout time.Duration
	var opt model.DataWorkspaceRetirementOptions
	cmd := &cobra.Command{Use: "retire <workspace>", Short: "Preview or safely retire a workspace; shared blobs survive and reclamation waits at least seven days", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if wait && !confirm {
			return fmt.Errorf("--wait requires --confirm")
		}
		if timeout <= 0 {
			return fmt.Errorf("--timeout must be positive")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		ws, err := client.GetDataWorkspace(args[0])
		if err != nil {
			return err
		}
		path := "/v1/data/workspaces/" + url.PathEscape(ws.Workspace.ID) + "/retirement"
		var raw map[string]any
		opt.Execute = false
		if err = client.doJSON(http.MethodPost, path, opt, &raw); err != nil {
			return err
		}
		if !confirm {
			return c.renderResourceResult(raw)
		}
		if allowed, _ := raw["allowed"].(bool); !allowed {
			_ = c.renderResourceResult(raw)
			return withExitCode(fmt.Errorf("workspace retirement has blockers; detach applications, finish running transfers, or explicitly authorize the listed cleanup actions"), ExitCodeUserInput)
		}
		opt.Execute = true
		var job model.StorageMaintenanceJob
		if err = client.doJSON(http.MethodPost, path, opt, &job); err != nil {
			return err
		}
		if wait {
			deadline := time.NewTimer(timeout)
			defer deadline.Stop()
			tick := time.NewTicker(5 * time.Second)
			defer tick.Stop()
			for job.Status != "succeeded" && job.Status != "failed" {
				select {
				case <-cmd.Context().Done():
					return cmd.Context().Err()
				case <-deadline.C:
					_ = c.renderResourceResult(job)
					return fmt.Errorf("retirement task %s continues on the server; wait timed out (reclamation normally requires at least seven days)", job.ID)
				case <-tick.C:
					if err = client.doJSON(http.MethodGet, "/v1/data/workspace-retirements/"+url.PathEscape(job.ID), nil, &job); err != nil {
						return err
					}
				}
			}
		}
		if err = c.renderResourceResult(job); err != nil {
			return err
		}
		if job.Status == "failed" {
			return fmt.Errorf("workspace retirement failed; inspect task %s", job.ID)
		}
		return nil
	}}
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Confirm durable retirement after preview; does not bypass reference guards")
	cmd.Flags().BoolVar(&opt.CancelPlannedTransfers, "cancel-planned-transfers", false, "Authorize cancellation of planned transfers; running transfers remain blockers")
	cmd.Flags().BoolVar(&opt.RevokeGrants, "revoke-grants", false, "Authorize revocation of short-lived data grants")
	cmd.Flags().BoolVar(&opt.EvictCaches, "evict-caches", false, "Authorize runtime cache eviction and wait for observed cleanup")
	cmd.Flags().BoolVar(&wait, "wait", false, "Poll the durable task until terminal or timeout; reclamation usually waits seven days")
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "Maximum wait; timeout does not cancel the server task")
	return cmd
}

func (c *CLI) newDataWorkspaceRetirementCommand() *cobra.Command {
	return &cobra.Command{Use: "retirement <job-id>", Short: "Inspect retirement progress, including after workspace metadata deletion", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		var job model.StorageMaintenanceJob
		if err = client.doJSON(http.MethodGet, "/v1/data/workspace-retirements/"+url.PathEscape(args[0]), nil, &job); err != nil {
			return err
		}
		return c.renderResourceResult(job)
	}}
}

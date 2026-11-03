package cli

import (
	"fmt"
	"fugue/internal/model"
	"github.com/spf13/cobra"
	"net/http"
	"net/url"
	"time"
)

func (c *CLI) newAdminStorageJobsCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "jobs", Short: "Queue and inspect durable storage maintenance; preview by default"}
	cmd.AddCommand(&cobra.Command{Use: "ls", Short: "List recent storage maintenance jobs and all pending work", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		var out map[string]any
		if err := client.doJSON(http.MethodGet, "/v1/admin/storage/jobs", nil, &out); err != nil {
			return err
		}
		return c.writeJSON(out)
	}})
	var scope string
	var confirm, wait bool
	var timeout time.Duration
	run := &cobra.Command{Use: "run KIND", Short: "Run retention, longhorn-reconcile, backup-cleanup, data-gc or transfer-expiry", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		switch args[0] {
		case "retention", "longhorn-reconcile", "backup-cleanup", "data-gc", "transfer-expiry":
		default:
			return fmt.Errorf("unknown maintenance kind %q", args[0])
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		var job model.StorageMaintenanceJob
		if err := client.doJSON(http.MethodPost, "/v1/admin/storage/jobs", map[string]any{"kind": args[0], "scope_id": scope, "execute": confirm}, &job); err != nil {
			return err
		}
		if wait {
			return c.waitStorageJob(cmd, job.ID, timeout)
		}
		return c.writeJSON(job)
	}}
	run.Flags().StringVar(&scope, "scope", "", "Policy ID for retention, workspace ID for data-gc")
	run.Flags().BoolVar(&confirm, "confirm", false, "Execute with immediate server-side safety checks; default is a persisted preview")
	run.Flags().BoolVar(&wait, "wait", false, "Wait for the job's terminal result")
	run.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "Maximum time to wait")
	cmd.AddCommand(run)
	var getWait bool
	var getTimeout time.Duration
	get := &cobra.Command{Use: "get JOB_ID", Short: "Inspect a durable maintenance result or wait for completion", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if getWait {
			return c.waitStorageJob(cmd, args[0], getTimeout)
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		var job model.StorageMaintenanceJob
		if err := client.doJSON(http.MethodGet, "/v1/admin/storage/jobs/"+url.PathEscape(args[0]), nil, &job); err != nil {
			return err
		}
		return c.writeJSON(job)
	}}
	get.Flags().BoolVar(&getWait, "wait", false, "Wait for completion or retry exhaustion")
	get.Flags().DurationVar(&getTimeout, "timeout", 10*time.Minute, "Maximum time to wait")
	cmd.AddCommand(get)
	return cmd
}

func (c *CLI) waitStorageJob(cmd *cobra.Command, id string, timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("--timeout must be positive")
	}
	client, err := c.newClient()
	if err != nil {
		return err
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		var job model.StorageMaintenanceJob
		if err := client.doJSON(http.MethodGet, "/v1/admin/storage/jobs/"+url.PathEscape(id), nil, &job); err != nil {
			return err
		}
		if job.Status == "succeeded" || job.Status == "failed" {
			if err := c.writeJSON(job); err != nil {
				return err
			}
			if job.Status == "failed" {
				return fmt.Errorf("storage job failed: %s", job.Error)
			}
			return nil
		}
		select {
		case <-cmd.Context().Done():
			return cmd.Context().Err()
		case <-deadline.C:
			return fmt.Errorf("storage job %s is still %s; inspect it with jobs get", id, job.Status)
		case <-ticker.C:
		}
	}
}

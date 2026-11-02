package cli

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminStorageCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "storage", Short: "Inspect platform R2 buckets, namespace ownership and engine cleanup"}
	for _, name := range []string{"overview", "ls", "reconcile"} {
		name := name
		cmd.AddCommand(&cobra.Command{Use: name, Short: map[string]string{"overview": "Show read-only platform storage inventory and source completeness", "ls": "List provider buckets and configured namespaces", "reconcile": "Audit Longhorn retention and historical physical deletion receipts (read-only)"}[name], Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			var out map[string]any
			if err = client.doJSON(http.MethodGet, "/v1/admin/storage", nil, &out); err != nil {
				return err
			}
			if c.wantsJSON() {
				return c.writeJSON(out)
			}
			fmt.Fprintf(c.stdout, "Storage observation: %v\nProvider bucket inventory complete: %v\nLonghorn reconciliation complete: %v\nMetrics: %v\nBilling: %v\n", out["observed_at"], out["buckets_complete"], out["longhorn_complete"], out["metrics_status"], out["billing_status"])
			keys := []string{"buckets", "namespaces", "longhorn", "warnings"}
			if name == "ls" {
				keys = []string{"buckets", "namespaces", "warnings"}
			}
			if name == "reconcile" {
				keys = []string{"longhorn", "warnings"}
			}
			for _, key := range keys {
				fmt.Fprintf(c.stdout, "\n%s:\n", key)
				if err = c.writeJSON(out[key]); err != nil {
					return err
				}
			}
			return nil
		}})
	}
	cmd.AddCommand(c.newAdminOrphanManifestCommand())
	return cmd
}

func (c *CLI) newAdminOrphanManifestCommand() *cobra.Command {
	var objectKey string
	var confirm bool
	cmd := &cobra.Command{Use: "orphan-manifest", Short: "Reconcile, protect or safely remove one unregistered backup manifest"}
	cleanup := &cobra.Command{
		Use:   "cleanup RUN_ID",
		Short: "Reconcile an orphan manifest, then remove it only if every safety check passes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(objectKey) == "" {
				return fmt.Errorf("--object-key is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			request := map[string]any{"run_id": args[0], "object_key": objectKey, "execute": false}
			var preview map[string]any
			if err := client.doJSON(http.MethodPost, "/v1/admin/storage/orphan-manifests/cleanup", request, &preview); err != nil {
				return err
			}
			cleanup, _ := preview["cleanup"].(map[string]any)
			canDelete, _ := cleanup["can_delete"].(bool)
			if !confirm || !canDelete {
				if c.wantsJSON() {
					if err := c.writeJSON(preview); err != nil {
						return err
					}
				} else {
					c.printOrphanManifestCleanup(preview, confirm)
				}
				if confirm && !canDelete {
					return fmt.Errorf("refusing orphan manifest cleanup: %s", strings.Join(stringList(cleanup["blockers"]), "; "))
				}
				return nil
			}
			request["execute"] = true
			request["expected_etag"] = cleanup["etag"]
			var executed map[string]any
			if err := client.doJSON(http.MethodPost, "/v1/admin/storage/orphan-manifests/cleanup", request, &executed); err != nil {
				return err
			}
			if c.wantsJSON() {
				return c.writeJSON(executed)
			}
			c.printOrphanManifestCleanup(executed, true)
			return nil
		},
	}
	cleanup.Flags().StringVar(&objectKey, "object-key", "", "Exact physical R2 manifest key returned by reconciliation")
	cleanup.Flags().BoolVar(&confirm, "confirm", false, "Delete only after the server-side reconciliation preview passes")
	cmd.AddCommand(cleanup)
	cmd.AddCommand(c.newAdminOrphanManifestAdoptCommand())
	return cmd
}

func (c *CLI) newAdminOrphanManifestAdoptCommand() *cobra.Command {
	var objectKey string
	var confirm bool
	cmd := &cobra.Command{
		Use: "adopt RUN_ID", Short: "Register a completed control-plane Longhorn backup as protected; preview by default",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(objectKey) == "" {
				return fmt.Errorf("--object-key is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			request := map[string]any{"run_id": args[0], "object_key": objectKey, "execute": false}
			var response map[string]any
			if err := client.doJSON(http.MethodPost, "/v1/admin/storage/orphan-manifests/adopt", request, &response); err != nil {
				return err
			}
			adoption, _ := response["adoption"].(map[string]any)
			canAdopt, _ := adoption["can_adopt"].(bool)
			if confirm && canAdopt {
				if adoption["etag"] == nil || adoption["etag"] == "" || adoption["config_etag"] == nil || adoption["config_etag"] == "" {
					return fmt.Errorf("server preview omitted required source ETags")
				}
				request["execute"], request["expected_etag"], request["expected_config_etag"] = true, adoption["etag"], adoption["config_etag"]
				if err := client.doJSON(http.MethodPost, "/v1/admin/storage/orphan-manifests/adopt", request, &response); err != nil {
					return err
				}
				adoption, _ = response["adoption"].(map[string]any)
			}
			if c.wantsJSON() {
				if err := c.writeJSON(response); err != nil {
					return err
				}
			} else {
				fmt.Fprintf(c.stdout, "Run: %v\nRemote state: %v\nCan adopt: %v\nExecuted: %v\nProtected: %v\nRestore verification: %v\n", adoption["run_id"], adoption["remote_state"], adoption["can_adopt"], adoption["executed"], adoption["protected"], adoption["restore_verification"])
				if id, _ := adoption["artifact_id"].(string); id != "" {
					fmt.Fprintf(c.stdout, "Artifact: %s\n", id)
				}
				if blockers := stringList(adoption["blockers"]); len(blockers) > 0 {
					fmt.Fprintf(c.stdout, "Blockers: %s\n", strings.Join(blockers, "; "))
				}
				fmt.Fprintf(c.stdout, "%v\n", adoption["message"])
			}
			if confirm && !canAdopt {
				return fmt.Errorf("refusing orphan manifest adoption: %s", strings.Join(stringList(adoption["blockers"]), "; "))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&objectKey, "object-key", "", "Exact physical R2 manifest key")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Register a protected artifact after rechecking both source ETags; does not certify recovery")
	return cmd
}

func stringList(value any) []string {
	items, _ := value.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}
	return out
}

func (c *CLI) printOrphanManifestCleanup(response map[string]any, confirm bool) {
	cleanup, _ := response["cleanup"].(map[string]any)
	fmt.Fprintf(c.stdout, "Orphan manifest: %v\nObject: %v\nRun status: %v\nRemote state: %v\nCan delete: %v\nExecuted: %v\n", cleanup["run_id"], cleanup["object_key"], cleanup["run_status"], cleanup["remote_state"], cleanup["can_delete"], cleanup["executed"])
	if blockers := stringList(cleanup["blockers"]); len(blockers) > 0 {
		fmt.Fprintf(c.stdout, "Blockers: %s\n", strings.Join(blockers, "; "))
	} else if confirm {
		fmt.Fprintln(c.stdout, "Cleanup completed after the conditional recheck.")
	} else {
		fmt.Fprintln(c.stdout, "Preview passed; use --confirm to execute the conditional cleanup.")
	}
}

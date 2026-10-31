package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"net/http"
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
	return cmd
}

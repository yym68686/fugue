package cli

import (
	"github.com/spf13/cobra"
	"net/http"
)

func (c *CLI) newCompilerInputArchiveBackendCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "archive", Short: "Manage platform-owned compiler input R2 archives without exposing credentials"}
	var execute bool
	for _, action := range []string{"status", "enable"} {
		action := action
		sub := &cobra.Command{Use: action, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			method := http.MethodGet
			var body any
			if action == "enable" {
				method = http.MethodPost
				body = map[string]bool{"execute": execute}
			}
			var out map[string]any
			if err = client.doJSON(method, "/v1/admin/storage/compiler-input-archive", body, &out); err != nil {
				return err
			}
			return c.writeJSON(out)
		}}
		if action == "enable" {
			sub.Short = "Preview provisioning; --execute creates a dedicated protected R2 bucket"
			sub.Flags().BoolVar(&execute, "execute", false, "Provision and enable the dedicated platform archive; does not migrate inputs")
		} else {
			sub.Short = "Inspect the platform archive configuration without secrets"
		}
		cmd.AddCommand(sub)
	}
	return cmd
}

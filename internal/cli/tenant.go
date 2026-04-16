package cli

import (
	"strings"

	"github.com/spf13/cobra"
)

func (c *CLI) newTenantCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "tenant",
		Aliases: []string{"tenants"},
		Short:   "Inspect visible tenants and workspace contexts",
		Long: strings.TrimSpace(`
Use tenant commands to answer "which workspace can this key see?" without
dropping down to raw API requests.

Normal deploy and create flows still auto-select the tenant when your key only
sees one visible tenant.
`),
	}
	cmd.AddCommand(
		c.newTenantListCommand(),
	)
	return cmd
}

func (c *CLI) newTenantListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List visible tenants",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenants, err := client.ListTenants()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"tenants": tenants})
			}
			return writeTenantTable(c.stdout, tenants)
		},
	}
}

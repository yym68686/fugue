package cli

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Platform and tenant administration commands",
		Long: strings.TrimSpace(`
Admin commands are for tenant setup, API keys, runtimes, and billing.

Most end users only need one issued API key and the "deploy" / "app" commands.
Use a bootstrap key or admin API key here only when you are doing setup.
`),
	}
	cmd.AddCommand(
		c.newAdminAccessCommand(),
		hideCompatCommand(c.newAdminRuntimeCommand(), "fugue runtime"),
		c.newAdminClusterCommand(),
		c.newAdminUsersCommand(),
		c.newAdminBillingCommand(),
		c.newAdminTenantCommand(),
	)
	return cmd
}

func (c *CLI) newAdminAccessCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "access",
		Short: "Manage API keys and node keys",
		Long: strings.TrimSpace(`
Access commands are for tenant setup and machine credentials.

Most end users do not need to mint keys here. They usually just export one
issued key as FUGUE_API_KEY and use the regular deploy/app commands.
`),
	}
	cmd.AddCommand(
		c.newAdminAPIKeyCommand(),
		c.newAdminNodeKeyCommand(),
	)
	return cmd
}

func (c *CLI) newAdminAPIKeyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "api-key",
		Short: "Manage API keys",
		Long: strings.TrimSpace(`
Create and rotate tenant API keys.

This is an admin/setup surface. Day-to-day users normally receive a key and
only need:
  export FUGUE_API_KEY=<issued-key>
`),
	}
	cmd.AddCommand(
		c.newAdminAPIKeyListCommand(),
		c.newAdminAPIKeyCreateCommand(),
		c.newAdminAPIKeyUpdateCommand(),
		c.newAdminAPIKeyRotateCommand(),
		c.newAdminAPIKeyDisableCommand(),
		c.newAdminAPIKeyEnableCommand(),
		c.newAdminAPIKeyRemoveCommand(),
	)
	return cmd
}

func (c *CLI) newAdminAPIKeyListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List visible API keys",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			keys, err := client.ListAPIKeys()
			if err != nil {
				return err
			}
			tenantID := c.effectiveTenantID()
			if strings.TrimSpace(tenantID) == "" && strings.TrimSpace(c.effectiveTenantName()) != "" {
				tenantID, err = resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
				if err != nil {
					return err
				}
			}
			filtered := filterAPIKeys(keys, tenantID)
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"api_keys": filtered})
			}
			return writeAPIKeyTable(c.stdout, filtered)
		},
	}
}

func (c *CLI) newAdminAPIKeyCreateCommand() *cobra.Command {
	opts := struct {
		Scopes []string
	}{}
	cmd := &cobra.Command{
		Use:   "create <label>",
		Short: "Create an API key",
		Long: strings.TrimSpace(`
Mint a new tenant API key.

Repeat --scope for each permission to grant. After creation, give the returned
secret to the user or automation and have them export it as FUGUE_API_KEY.
`),
		Args: cobra.ExactArgs(1),
		Example: strings.TrimSpace(`
  fugue admin access api-key create ci --scope app.read --scope app.write
  fugue admin access api-key create deploy-bot --scope project.read --scope app.write
`),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(opts.Scopes) == 0 {
				return fmt.Errorf("at least one --scope is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
			if err != nil {
				return err
			}
			response, err := client.CreateAPIKey(tenantID, args[0], model.NormalizeScopes(opts.Scopes))
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := renderAPIKey(c.stdout, response.APIKey); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "secret=%s\n", response.Secret)
			return err
		},
	}
	cmd.Flags().StringArrayVar(&opts.Scopes, "scope", nil, "Scope to mint on the key (repeatable)")
	return cmd
}

func (c *CLI) newAdminAPIKeyUpdateCommand() *cobra.Command {
	opts := struct {
		Label  string
		Scopes []string
	}{}
	cmd := &cobra.Command{
		Use:   "update <api-key>",
		Short: "Update an API key label and/or scopes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.Label) == "" && len(opts.Scopes) == 0 {
				return fmt.Errorf("at least one of --label or --scope is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			key, err := c.resolveNamedAPIKey(client, args[0])
			if err != nil {
				return err
			}
			request := patchAPIKeyRequest{}
			if strings.TrimSpace(opts.Label) != "" {
				label := strings.TrimSpace(opts.Label)
				request.Label = &label
			}
			if len(opts.Scopes) > 0 {
				scopes := model.NormalizeScopes(opts.Scopes)
				request.Scopes = &scopes
			}
			key, err = client.PatchAPIKey(key.ID, request)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"api_key": key})
			}
			return renderAPIKey(c.stdout, key)
		},
	}
	cmd.Flags().StringVar(&opts.Label, "label", "", "New key label")
	cmd.Flags().StringArrayVar(&opts.Scopes, "scope", nil, "Replacement scope set (repeatable)")
	return cmd
}

func (c *CLI) newAdminAPIKeyRotateCommand() *cobra.Command {
	opts := struct {
		Label  string
		Scopes []string
	}{}
	cmd := &cobra.Command{
		Use:   "rotate <api-key>",
		Short: "Rotate an API key and return a new secret",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			key, err := c.resolveNamedAPIKey(client, args[0])
			if err != nil {
				return err
			}
			var request *rotateAPIKeyRequest
			if strings.TrimSpace(opts.Label) != "" || len(opts.Scopes) > 0 {
				request = &rotateAPIKeyRequest{}
				if strings.TrimSpace(opts.Label) != "" {
					label := strings.TrimSpace(opts.Label)
					request.Label = &label
				}
				if len(opts.Scopes) > 0 {
					scopes := model.NormalizeScopes(opts.Scopes)
					request.Scopes = &scopes
				}
			}
			response, err := client.RotateAPIKey(key.ID, request)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := renderAPIKey(c.stdout, response.APIKey); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "secret=%s\n", response.Secret)
			return err
		},
	}
	cmd.Flags().StringVar(&opts.Label, "label", "", "Replacement key label")
	cmd.Flags().StringArrayVar(&opts.Scopes, "scope", nil, "Replacement scope set (repeatable)")
	return cmd
}

func (c *CLI) newAdminAPIKeyDisableCommand() *cobra.Command {
	return c.newAdminAPIKeyStateCommand("disable")
}

func (c *CLI) newAdminAPIKeyEnableCommand() *cobra.Command {
	return c.newAdminAPIKeyStateCommand("enable")
}

func (c *CLI) newAdminAPIKeyStateCommand(mode string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   mode + " <api-key>",
		Short: strings.Title(mode) + " an API key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			key, err := c.resolveNamedAPIKey(client, args[0])
			if err != nil {
				return err
			}
			switch mode {
			case "disable":
				key, err = client.DisableAPIKey(key.ID)
			default:
				key, err = client.EnableAPIKey(key.ID)
			}
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"api_key": key})
			}
			return renderAPIKey(c.stdout, key)
		},
	}
	return cmd
}

func (c *CLI) newAdminAPIKeyRemoveCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <api-key>",
		Aliases: []string{"rm", "remove"},
		Short:   "Delete an API key",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			key, err := c.resolveNamedAPIKey(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.DeleteAPIKey(key.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := renderAPIKey(c.stdout, response.APIKey); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "deleted=%t\n", response.Deleted)
			return err
		},
	}
}

func (c *CLI) newAdminNodeKeyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node-key",
		Short: "Manage node keys",
	}
	cmd.AddCommand(
		c.newAdminNodeKeyListCommand(),
		c.newAdminNodeKeyCreateCommand(),
		c.newAdminNodeKeyUsageCommand(),
		c.newAdminNodeKeyRevokeCommand(),
	)
	return cmd
}

func (c *CLI) newAdminNodeKeyListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List visible node keys",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			keys, err := client.ListNodeKeys()
			if err != nil {
				return err
			}
			tenantID := c.effectiveTenantID()
			if strings.TrimSpace(tenantID) == "" && strings.TrimSpace(c.effectiveTenantName()) != "" {
				tenantID, err = resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
				if err != nil {
					return err
				}
			}
			filtered := filterNodeKeys(keys, tenantID)
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"node_keys": filtered})
			}
			return writeNodeKeyTable(c.stdout, filtered)
		},
	}
}

func (c *CLI) newAdminNodeKeyCreateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "create <label>",
		Short: "Create a node key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
			if err != nil {
				return err
			}
			response, err := client.CreateNodeKey(tenantID, args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := renderNodeKey(c.stdout, response.NodeKey); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "secret=%s\n", response.Secret)
			return err
		},
	}
}

func (c *CLI) newAdminNodeKeyUsageCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "usage <node-key>",
		Short: "Show where a node key is used",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			key, err := c.resolveNamedNodeKey(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.GetNodeKeyUsages(key.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := renderNodeKey(c.stdout, response.NodeKey); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(c.stdout, "usage_count=%d\n", response.UsageCount); err != nil {
				return err
			}
			if len(response.Runtimes) == 0 {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeRuntimeTable(c.stdout, response.Runtimes)
		},
	}
}

func (c *CLI) newAdminNodeKeyRevokeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "revoke <node-key>",
		Short: "Revoke a node key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			key, err := c.resolveNamedNodeKey(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.RevokeNodeKey(key.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := renderNodeKey(c.stdout, response.NodeKey); err != nil {
				return err
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "deleted_cluster_nodes", Value: strings.Join(response.Cleanup.DeletedClusterNodes, ",")},
				kvPair{Key: "detached_runtime_ids", Value: strings.Join(response.Cleanup.DetachedRuntimeIDs, ",")},
				kvPair{Key: "warnings", Value: strings.Join(response.Cleanup.Warnings, "; ")},
			)
		},
	}
}

func (c *CLI) newAdminRuntimeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "runtime",
		Aliases: []string{"runtimes"},
		Short:   "Manage runtime inventory and sharing",
	}
	cmd.AddCommand(
		c.newAdminRuntimeListCommand(),
		c.newAdminRuntimeShowCommand(),
		c.newAdminRuntimeAccessCommand(),
		c.newAdminRuntimeCreateCommand(),
		c.newAdminRuntimeShareCommand(),
		c.newAdminRuntimeUnshareCommand(),
		c.newAdminRuntimeShareModeCommand(),
		c.newAdminRuntimePoolModeCommand(),
		c.newAdminRuntimeTokenCommand(),
	)
	return cmd
}

func (c *CLI) newAdminRuntimeListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List visible runtimes",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			runtimes, err := client.ListRuntimes()
			if err != nil {
				return err
			}
			tenantID := c.effectiveTenantID()
			if strings.TrimSpace(tenantID) == "" && strings.TrimSpace(c.effectiveTenantName()) != "" {
				tenantID, err = resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
				if err != nil {
					return err
				}
			}
			if strings.TrimSpace(tenantID) != "" {
				filtered := make([]model.Runtime, 0, len(runtimes))
				for _, runtime := range runtimes {
					if runtime.TenantID == tenantID || runtime.TenantID == "" {
						filtered = append(filtered, runtime)
					}
				}
				runtimes = filtered
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"runtimes": runtimes})
			}
			return writeRuntimeTable(c.stdout, runtimes)
		},
	}
}

func (c *CLI) newAdminRuntimeShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <runtime>",
		Aliases: []string{"get", "status", "info"},
		Short:   "Show one runtime",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			runtimeObj, err := c.resolveNamedRuntime(client, args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"runtime": runtimeObj})
			}
			return renderRuntime(c.stdout, runtimeObj)
		},
	}
}

func (c *CLI) newAdminRuntimeAccessCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "access <runtime>",
		Aliases: []string{"sharing"},
		Short:   "Show who can access a runtime",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			runtimeObj, err := c.resolveNamedRuntime(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.GetRuntimeSharing(runtimeObj.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := renderRuntime(c.stdout, response.Runtime); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(c.stdout, "grants=%d\n", len(response.Grants)); err != nil {
				return err
			}
			if len(response.Grants) == 0 {
				return nil
			}
			tenantNames, err := c.visibleTenantNamesByID(client)
			if err != nil {
				c.progressf("warning=tenant inventory unavailable: %v", err)
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeRuntimeAccessGrantTable(c.stdout, response.Grants, tenantNames)
		},
	}
}

func (c *CLI) newAdminRuntimeCreateCommand() *cobra.Command {
	opts := struct {
		Type     string
		Endpoint string
		Labels   []string
	}{Type: model.RuntimeTypeExternalOwned}
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a runtime record",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			var tenantID string
			if !strings.EqualFold(strings.TrimSpace(opts.Type), model.RuntimeTypeManagedShared) {
				tenantID, err = resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
				if err != nil {
					return err
				}
			}
			labels, err := parseEnvAssignments(opts.Labels)
			if err != nil {
				return err
			}
			response, err := client.CreateRuntime(createRuntimeRequest{
				TenantID: tenantID,
				Name:     args[0],
				Type:     strings.TrimSpace(opts.Type),
				Endpoint: strings.TrimSpace(opts.Endpoint),
				Labels:   labels,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := renderRuntime(c.stdout, response.Runtime); err != nil {
				return err
			}
			if strings.TrimSpace(response.RuntimeKey) != "" {
				_, err = fmt.Fprintf(c.stdout, "runtime_key=%s\n", response.RuntimeKey)
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.Type, "type", opts.Type, "Runtime type")
	cmd.Flags().StringVar(&opts.Endpoint, "endpoint", "", "Runtime endpoint")
	cmd.Flags().StringArrayVar(&opts.Labels, "label", nil, "Runtime label as KEY=VALUE (repeatable)")
	return cmd
}

func (c *CLI) newAdminRuntimeShareCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "share <runtime> <tenant>",
		Aliases: []string{"grant"},
		Short:   "Grant another tenant access to a runtime",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			runtimeObj, err := c.resolveNamedRuntime(client, args[0])
			if err != nil {
				return err
			}
			tenant, err := c.resolveNamedTenant(client, args[1])
			if err != nil {
				return err
			}
			grant, err := client.GrantRuntimeAccess(runtimeObj.ID, tenant.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"grant": grant})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "runtime_id", Value: grant.RuntimeID},
				kvPair{Key: "tenant_id", Value: grant.TenantID},
				kvPair{Key: "created_at", Value: formatTime(grant.CreatedAt)},
			)
		},
	}
}

func (c *CLI) newAdminRuntimeUnshareCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "unshare <runtime> <tenant>",
		Aliases: []string{"revoke"},
		Short:   "Revoke tenant access to a runtime",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			runtimeObj, err := c.resolveNamedRuntime(client, args[0])
			if err != nil {
				return err
			}
			tenant, err := c.resolveNamedTenant(client, args[1])
			if err != nil {
				return err
			}
			removed, err := client.RevokeRuntimeAccess(runtimeObj.ID, tenant.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"removed": removed})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "runtime_id", Value: runtimeObj.ID},
				kvPair{Key: "tenant_id", Value: tenant.ID},
				kvPair{Key: "removed", Value: fmt.Sprintf("%t", removed)},
			)
		},
	}
}

func (c *CLI) newAdminRuntimeShareModeCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "share-mode <runtime> <mode>",
		Aliases: []string{"access-mode"},
		Short:   "Set runtime access mode",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			runtimeObj, err := c.resolveNamedRuntime(client, args[0])
			if err != nil {
				return err
			}
			runtimeObj, err = client.SetRuntimeAccessMode(runtimeObj.ID, args[1])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"runtime": runtimeObj})
			}
			return renderRuntime(c.stdout, runtimeObj)
		},
	}
}

func (c *CLI) newAdminRuntimePoolModeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "pool-mode <runtime> <mode>",
		Short: "Set runtime pool mode",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			runtimeObj, err := c.resolveNamedRuntime(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.SetRuntimePoolMode(runtimeObj.ID, args[1])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := renderRuntime(c.stdout, response.Runtime); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "node_reconciled=%t\n", response.NodeReconciled)
			return err
		},
	}
}

func (c *CLI) newAdminRuntimeTokenCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "token",
		Short: "Manage runtime enrollment tokens",
	}
	cmd.AddCommand(
		c.newAdminRuntimeTokenListCommand(),
		c.newAdminRuntimeTokenCreateCommand(),
	)
	return cmd
}

func (c *CLI) newAdminRuntimeTokenListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List runtime enrollment tokens",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
			if err != nil {
				return err
			}
			tokens, err := client.ListEnrollmentTokens(tenantID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"enrollment_tokens": tokens})
			}
			return writeEnrollmentTokenTable(c.stdout, tokens)
		},
	}
}

func (c *CLI) newAdminRuntimeTokenCreateCommand() *cobra.Command {
	opts := struct {
		TTL int
	}{TTL: 3600}
	cmd := &cobra.Command{
		Use:   "create <label>",
		Short: "Create a runtime enrollment token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
			if err != nil {
				return err
			}
			response, err := client.CreateEnrollmentToken(tenantID, args[0], opts.TTL)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeKeyValues(c.stdout,
				kvPair{Key: "token_id", Value: response.EnrollmentToken.ID},
				kvPair{Key: "label", Value: response.EnrollmentToken.Label},
				kvPair{Key: "prefix", Value: response.EnrollmentToken.Prefix},
				kvPair{Key: "expires_at", Value: formatTime(response.EnrollmentToken.ExpiresAt)},
			); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "secret=%s\n", response.Secret)
			return err
		},
	}
	cmd.Flags().IntVar(&opts.TTL, "ttl", opts.TTL, "Token TTL in seconds")
	return cmd
}

func (c *CLI) newAdminClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Inspect cluster infrastructure",
	}
	cmd.AddCommand(
		c.newAdminClusterNodesCommand(),
		c.newAdminClusterStatusCommand(),
		c.newAdminClusterPodsCommand(),
		c.newAdminClusterEventsCommand(),
		c.newAdminClusterLogsCommand(),
		c.newAdminClusterExecCommand(),
		c.newAdminClusterDNSCommand(),
		c.newAdminClusterNetCommand(),
		c.newAdminClusterTLSCommand(),
		c.newAdminClusterWorkloadCommand(),
		c.newAdminClusterRolloutCommand(),
		c.newAdminClusterJoinScriptCommand(),
	)
	return cmd
}

func (c *CLI) newAdminClusterNodesCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "nodes",
		Short: "List cluster nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			nodes, err := client.ListClusterNodes()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"cluster_nodes": nodes})
			}
			return writeClusterNodeTable(c.stdout, nodes)
		},
	}
}

func (c *CLI) newAdminClusterStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show control-plane status",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			status, err := client.GetControlPlaneStatus()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"control_plane": status})
			}
			if err := writeKeyValues(c.stdout,
				kvPair{Key: "namespace", Value: status.Namespace},
				kvPair{Key: "release_instance", Value: status.ReleaseInstance},
				kvPair{Key: "version", Value: status.Version},
				kvPair{Key: "status", Value: status.Status},
				kvPair{Key: "observed_at", Value: formatTime(status.ObservedAt)},
			); err != nil {
				return err
			}
			if status.DeployWorkflow != nil {
				if err := writeKeyValues(c.stdout,
					kvPair{Key: "deploy_workflow_repository", Value: status.DeployWorkflow.Repository},
					kvPair{Key: "deploy_workflow", Value: status.DeployWorkflow.Workflow},
					kvPair{Key: "deploy_workflow_status", Value: status.DeployWorkflow.Status},
					kvPair{Key: "deploy_workflow_conclusion", Value: status.DeployWorkflow.Conclusion},
					kvPair{Key: "deploy_workflow_run_number", Value: formatInt(status.DeployWorkflow.RunNumber)},
					kvPair{Key: "deploy_workflow_head_sha", Value: status.DeployWorkflow.HeadSHA},
					kvPair{Key: "deploy_workflow_head_branch", Value: status.DeployWorkflow.HeadBranch},
					kvPair{Key: "deploy_workflow_url", Value: status.DeployWorkflow.HTMLURL},
					kvPair{Key: "deploy_workflow_error", Value: status.DeployWorkflow.Error},
				); err != nil {
					return err
				}
			}
			if len(status.Components) == 0 {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeControlPlaneComponentTable(c.stdout, status.Components)
		},
	}
}

func (c *CLI) newAdminClusterJoinScriptCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "join-script",
		Short: "Fetch the cluster join install script",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			script, err := client.GetJoinClusterScript()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"script": script})
			}
			_, err = fmt.Fprint(c.stdout, script)
			return err
		},
	}
}

func (c *CLI) newAdminBillingCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "billing",
		Short: "Inspect and update tenant billing",
	}
	cmd.AddCommand(
		c.newAdminBillingShowCommand(),
		c.newAdminBillingCapCommand(),
		c.newAdminBillingTopUpCommand(),
		c.newAdminBillingSetBalanceCommand(),
	)
	return cmd
}

func (c *CLI) newAdminBillingShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show",
		Aliases: []string{"get", "status"},
		Short:   "Show tenant billing status",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
			if err != nil {
				return err
			}
			billing, err := client.GetBilling(tenantID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"billing": billing})
			}
			return renderBillingSummary(c.stdout, billing)
		},
	}
}

func (c *CLI) newAdminBillingCapCommand() *cobra.Command {
	opts := struct {
		CPU     int64
		Memory  int64
		Storage int64
	}{}
	cmd := &cobra.Command{
		Use:   "cap",
		Short: "Update tenant managed-resource cap",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
			if err != nil {
				return err
			}
			billing, err := client.UpdateBilling(tenantID, model.BillingResourceSpec{
				CPUMilliCores:    opts.CPU,
				MemoryMebibytes:  opts.Memory,
				StorageGibibytes: opts.Storage,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"billing": billing})
			}
			return renderBillingSummary(c.stdout, billing)
		},
	}
	cmd.Flags().Int64Var(&opts.CPU, "cpu", 0, "Managed CPU cap in millicores")
	cmd.Flags().Int64Var(&opts.Memory, "memory", 0, "Managed memory cap in MiB")
	cmd.Flags().Int64Var(&opts.Storage, "storage", 0, "Managed storage cap in GiB")
	return cmd
}

func (c *CLI) newAdminBillingTopUpCommand() *cobra.Command {
	opts := struct {
		Note string
	}{}
	cmd := &cobra.Command{
		Use:   "topup <amount>",
		Short: "Top up tenant balance using currency units, e.g. 25 or 25.50",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
			if err != nil {
				return err
			}
			amountCents, err := parseCurrencyAmount(args[0])
			if err != nil {
				return err
			}
			billing, err := client.TopUpBilling(tenantID, amountCents, opts.Note)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"billing": billing})
			}
			return renderBillingSummary(c.stdout, billing)
		},
	}
	cmd.Flags().StringVar(&opts.Note, "note", "", "Audit note for the top-up")
	return cmd
}

func (c *CLI) newAdminBillingSetBalanceCommand() *cobra.Command {
	opts := struct {
		Note string
	}{}
	cmd := &cobra.Command{
		Use:   "set-balance <amount>",
		Short: "Set tenant balance using currency units, e.g. 100 or 100.00",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
			if err != nil {
				return err
			}
			balanceCents, err := parseCurrencyAmount(args[0])
			if err != nil {
				return err
			}
			billing, err := client.SetBillingBalance(tenantID, balanceCents, opts.Note)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"billing": billing})
			}
			return renderBillingSummary(c.stdout, billing)
		},
	}
	cmd.Flags().StringVar(&opts.Note, "note", "", "Audit note for the balance adjustment")
	return cmd
}

func (c *CLI) newAdminTenantCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tenant",
		Short: "Manage tenants",
	}
	cmd.AddCommand(
		c.newAdminTenantListCommand(),
		c.newAdminTenantCreateCommand(),
		c.newAdminTenantRemoveCommand(),
	)
	return cmd
}

func (c *CLI) newAdminTenantListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List tenants",
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

func (c *CLI) newAdminTenantCreateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "create <name>",
		Short: "Create a tenant",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenant, err := client.CreateTenant(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"tenant": tenant})
			}
			return renderTenant(c.stdout, tenant)
		},
	}
}

func (c *CLI) newAdminTenantRemoveCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <tenant>",
		Aliases: []string{"rm", "remove"},
		Short:   "Delete a tenant",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenant, err := c.resolveNamedTenant(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.DeleteTenant(tenant.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := renderTenant(c.stdout, response.Tenant); err != nil {
				return err
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "namespace", Value: response.Cleanup.Namespace},
				kvPair{Key: "namespace_delete_requested", Value: fmt.Sprintf("%t", response.Cleanup.NamespaceDeleteRequested)},
				kvPair{Key: "owned_nodes", Value: fmt.Sprintf("%d", response.Cleanup.OwnedNodes)},
				kvPair{Key: "warnings", Value: strings.Join(response.Cleanup.Warnings, "; ")},
			)
		},
	}
}

func renderAPIKey(w io.Writer, key model.APIKey) error {
	return writeKeyValues(w,
		kvPair{Key: "api_key_id", Value: key.ID},
		kvPair{Key: "tenant_id", Value: key.TenantID},
		kvPair{Key: "label", Value: key.Label},
		kvPair{Key: "prefix", Value: key.Prefix},
		kvPair{Key: "status", Value: key.Status},
		kvPair{Key: "scopes", Value: strings.Join(key.Scopes, ",")},
	)
}

func renderNodeKey(w io.Writer, key model.NodeKey) error {
	return writeKeyValues(w,
		kvPair{Key: "node_key_id", Value: key.ID},
		kvPair{Key: "tenant_id", Value: key.TenantID},
		kvPair{Key: "label", Value: key.Label},
		kvPair{Key: "prefix", Value: key.Prefix},
		kvPair{Key: "status", Value: key.Status},
	)
}

func renderRuntime(w io.Writer, runtimeObj model.Runtime) error {
	return writeKeyValues(w,
		kvPair{Key: "runtime_id", Value: runtimeObj.ID},
		kvPair{Key: "tenant_id", Value: runtimeObj.TenantID},
		kvPair{Key: "name", Value: runtimeObj.Name},
		kvPair{Key: "type", Value: runtimeObj.Type},
		kvPair{Key: "access_mode", Value: runtimeObj.AccessMode},
		kvPair{Key: "pool_mode", Value: runtimeObj.PoolMode},
		kvPair{Key: "status", Value: runtimeObj.Status},
		kvPair{Key: "endpoint", Value: runtimeObj.Endpoint},
	)
}

func renderTenant(w io.Writer, tenant model.Tenant) error {
	return writeKeyValues(w,
		kvPair{Key: "tenant_id", Value: tenant.ID},
		kvPair{Key: "name", Value: tenant.Name},
		kvPair{Key: "slug", Value: tenant.Slug},
		kvPair{Key: "status", Value: tenant.Status},
	)
}

func renderBillingSummary(w io.Writer, billing model.TenantBillingSummary) error {
	if err := writeKeyValues(w,
		kvPair{Key: "tenant_id", Value: billing.TenantID},
		kvPair{Key: "status", Value: billing.Status},
		kvPair{Key: "managed_cap", Value: formatBillingResourceSpec(billing.ManagedCap)},
		kvPair{Key: "managed_committed", Value: formatBillingResourceSpec(billing.ManagedCommitted)},
		kvPair{Key: "managed_available", Value: formatBillingResourceSpec(billing.ManagedAvailable)},
		kvPair{Key: "balance", Value: formatCurrencyMicroCents(billing.BalanceMicroCents, billing.PriceBook.Currency)},
		kvPair{Key: "hourly_rate", Value: formatCurrencyMicroCents(billing.HourlyRateMicroCents, billing.PriceBook.Currency)},
		kvPair{Key: "monthly_estimate", Value: formatCurrencyMicroCents(billing.MonthlyEstimateMicroCents, billing.PriceBook.Currency)},
	); err != nil {
		return err
	}
	if len(billing.Events) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeBillingEventsTable(w, billing.Events, billing.PriceBook.Currency)
}

func parseCurrencyAmount(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("amount is required")
	}
	sign := int64(1)
	if strings.HasPrefix(raw, "-") {
		sign = -1
		raw = strings.TrimPrefix(raw, "-")
	}
	wholeRaw, fracRaw, _ := strings.Cut(raw, ".")
	whole, err := strconv.ParseInt(wholeRaw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount %q", raw)
	}
	if fracRaw == "" {
		return sign * whole * 100, nil
	}
	if len(fracRaw) > 2 {
		return 0, fmt.Errorf("amount %q must have at most 2 decimal places", raw)
	}
	for len(fracRaw) < 2 {
		fracRaw += "0"
	}
	frac, err := strconv.ParseInt(fracRaw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount %q", raw)
	}
	return sign * (whole*100 + frac), nil
}

package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newRuntimeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "runtime",
		Aliases: []string{"runtimes"},
		Short:   "Inspect, attach, and manage runtimes",
	}
	cmd.AddCommand(
		c.newAdminRuntimeListCommand(),
		c.newAdminRuntimeShowCommand(),
		c.newRuntimeAccessCommand(),
		c.newRuntimePoolCommand(),
		c.newAdminRuntimeTokenCommand(),
		c.newRuntimeAttachCommand(),
		c.newRuntimeDoctorCommand(),
	)
	return cmd
}

func (c *CLI) newRuntimeAccessCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "access",
		Short: "Inspect and update runtime access",
	}
	cmd.AddCommand(
		c.newRuntimeAccessShowCommand(),
		c.newRuntimeAccessSetCommand(),
		c.newRuntimeAccessGrantCommand(),
		c.newRuntimeAccessRevokeCommand(),
	)
	return cmd
}

func (c *CLI) newRuntimeAccessShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <runtime>",
		Aliases: []string{"get", "status"},
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

func (c *CLI) newRuntimeAccessSetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "set <runtime> <mode>",
		Short: "Set runtime access mode",
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

func (c *CLI) newRuntimeAccessGrantCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "grant <runtime> <tenant>",
		Short: "Grant another tenant access to a runtime",
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

func (c *CLI) newRuntimeAccessRevokeCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "revoke <runtime> <tenant>",
		Aliases: []string{"unshare"},
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

func (c *CLI) newRuntimePoolCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pool",
		Short: "Inspect and update runtime pool behavior",
	}
	cmd.AddCommand(
		c.newRuntimePoolShowCommand(),
		c.newRuntimePoolSetCommand(),
	)
	return cmd
}

func (c *CLI) newRuntimePoolShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <runtime>",
		Short: "Show current runtime pool mode",
		Args:  cobra.ExactArgs(1),
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

func (c *CLI) newRuntimePoolSetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "set <runtime> <mode>",
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

func (c *CLI) newRuntimeAttachCommand() *cobra.Command {
	opts := struct {
		TTL int
	}{TTL: 3600}
	cmd := &cobra.Command{
		Use:   "attach <label>",
		Short: "Create a runtime enrollment token with next-step instructions",
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
			scriptURL := strings.TrimRight(c.effectiveBaseURL(), "/") + "/install/join-cluster.sh"
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"enrollment_token": response.EnrollmentToken,
					"secret":           response.Secret,
					"install_script":   scriptURL,
				})
			}
			if err := writeKeyValues(c.stdout,
				kvPair{Key: "token_id", Value: response.EnrollmentToken.ID},
				kvPair{Key: "label", Value: response.EnrollmentToken.Label},
				kvPair{Key: "prefix", Value: response.EnrollmentToken.Prefix},
				kvPair{Key: "expires_at", Value: formatTime(response.EnrollmentToken.ExpiresAt)},
				kvPair{Key: "secret", Value: response.Secret},
				kvPair{Key: "install_script", Value: scriptURL},
				kvPair{Key: "export_token", Value: "export FUGUE_ENROLL_TOKEN=<paste-secret-above>"},
				kvPair{Key: "join_command", Value: "curl -fsSL " + scriptURL + " | sudo bash"},
				kvPair{Key: "next_step", Value: "curl -fsSL " + scriptURL + " | sudo FUGUE_ENROLL_TOKEN=<paste-secret-above> bash"},
			); err != nil {
				return err
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&opts.TTL, "ttl", opts.TTL, "Enrollment token TTL in seconds")
	return cmd
}

func (c *CLI) newRuntimeDoctorCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "doctor <runtime>",
		Short: "Inspect runtime health and attachment state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			runtimeObj, err := c.resolveNamedRuntime(client, args[0])
			if err != nil {
				return err
			}
			nodes, nodesErr := client.ListClusterNodes()
			if nodesErr != nil {
				c.progressf("warning=cluster inventory unavailable: %v", nodesErr)
			}
			matchingNodes := make([]model.ClusterNode, 0, 1)
			for _, node := range nodes {
				if strings.EqualFold(strings.TrimSpace(node.RuntimeID), strings.TrimSpace(runtimeObj.ID)) {
					matchingNodes = append(matchingNodes, node)
				}
			}
			warnings := make([]string, 0, 4)
			if !strings.EqualFold(strings.TrimSpace(runtimeObj.Status), "active") {
				warnings = append(warnings, "runtime is not active")
			}
			if strings.TrimSpace(runtimeObj.Endpoint) == "" {
				warnings = append(warnings, "runtime endpoint is empty")
			}
			if runtimeObj.LastSeenAt == nil && runtimeObj.Type != model.RuntimeTypeManagedShared {
				warnings = append(warnings, "runtime has not reported a heartbeat yet")
			}
			if len(matchingNodes) == 0 && runtimeObj.Type != model.RuntimeTypeManagedShared {
				warnings = append(warnings, "no cluster node is currently associated with this runtime")
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"runtime":       runtimeObj,
					"cluster_nodes": matchingNodes,
					"warnings":      warnings,
				})
			}
			if err := renderRuntime(c.stdout, runtimeObj); err != nil {
				return err
			}
			if err := writeKeyValues(c.stdout,
				kvPair{Key: "last_seen_at", Value: formatOptionalTimePtr(runtimeObj.LastSeenAt)},
				kvPair{Key: "cluster_nodes", Value: fmt.Sprintf("%d", len(matchingNodes))},
				kvPair{Key: "warnings", Value: strings.Join(warnings, "; ")},
			); err != nil {
				return err
			}
			if len(matchingNodes) == 0 {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeClusterNodeTable(c.stdout, matchingNodes)
		},
	}
}

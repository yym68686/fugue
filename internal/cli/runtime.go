package cli

import (
	"fmt"
	"strconv"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

const (
	runtimeDoctorManagedSharedBaseID   = "runtime_managed_shared"
	runtimeDoctorManagedSharedIDPrefix = "runtime_managed_shared_loc_"
)

func (c *CLI) newRuntimeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "runtime",
		Aliases: []string{"runtimes"},
		Short:   "Inspect and enroll runtimes",
	}
	cmd.AddCommand(
		c.newRuntimeListCommand(),
		c.newRuntimeShowCommand(),
		c.newRuntimeEnrollCommand(),
		hideCompatCommand(c.newRuntimeAttachCommand(), "fugue runtime enroll create"),
		c.newRuntimeDoctorCommand(),
		hideCompatCommand(c.newRuntimeAccessCommand(), "fugue admin runtime access"),
		hideCompatCommand(c.newRuntimePoolCommand(), "fugue admin runtime pool"),
		hideCompatCommand(c.newRuntimeOfferCommand(), "fugue admin runtime offer"),
		hideCompatCommand(c.newRuntimeDeleteCommand(), "fugue admin runtime delete"),
	)
	return cmd
}

func (c *CLI) newRuntimeListCommand() *cobra.Command {
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
			return writeRuntimeTableWithContext(c.stdout, runtimes, c.loadTenantNames(client), c.showIDs())
		},
	}
}

func (c *CLI) newRuntimeShowCommand() *cobra.Command {
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
			return c.renderRuntimeDetail(client, runtimeObj)
		},
	}
}

func (c *CLI) newRuntimeAccessCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "access [runtime]",
		Aliases: []string{"sharing"},
		Short:   "Inspect and update runtime access",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			return c.runRuntimeAccessShow(args[0])
		},
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
			return c.runRuntimeAccessShow(args[0])
		},
	}
}

func (c *CLI) runRuntimeAccessShow(runtimeRef string) error {
	client, err := c.newClient()
	if err != nil {
		return err
	}
	runtimeObj, err := c.resolveNamedRuntime(client, runtimeRef)
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
	if err := c.renderRuntimeDetail(client, response.Runtime); err != nil {
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
			return c.renderRuntimeDetail(client, runtimeObj)
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
			runtimeLabel := formatDisplayName(runtimeObj.Name, runtimeObj.ID, c.showIDs())
			tenantLabel := formatDisplayName(firstNonEmptyTrimmed(tenant.Name, tenant.Slug, tenant.ID), tenant.ID, c.showIDs())
			return writeKeyValues(c.stdout,
				kvPair{Key: "runtime", Value: runtimeLabel},
				kvPair{Key: "tenant", Value: tenantLabel},
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
			runtimeLabel := formatDisplayName(runtimeObj.Name, runtimeObj.ID, c.showIDs())
			tenantLabel := formatDisplayName(firstNonEmptyTrimmed(tenant.Name, tenant.Slug, tenant.ID), tenant.ID, c.showIDs())
			return writeKeyValues(c.stdout,
				kvPair{Key: "runtime", Value: runtimeLabel},
				kvPair{Key: "tenant", Value: tenantLabel},
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
			return c.renderRuntimeDetail(client, runtimeObj)
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
			if err := c.renderRuntimeDetail(client, response.Runtime); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "node_reconciled=%t\n", response.NodeReconciled)
			return err
		},
	}
}

func (c *CLI) newRuntimeOfferCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "offer",
		Short: "Inspect and update a runtime public offer",
	}
	cmd.AddCommand(
		c.newRuntimeOfferShowCommand(),
		c.newRuntimeOfferSetCommand(),
	)
	return cmd
}

func (c *CLI) newRuntimeOfferShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <runtime>",
		Short: "Show the current public offer for a runtime",
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
				return writeJSON(c.stdout, map[string]any{
					"runtime":      runtimeObj,
					"public_offer": runtimeObj.PublicOffer,
				})
			}
			return c.renderRuntimeOffer(client, runtimeObj)
		},
	}
}

func (c *CLI) newRuntimeOfferSetCommand() *cobra.Command {
	opts := struct {
		CPU         int64
		Memory      int64
		Storage     int64
		MonthlyUSD  string
		Free        bool
		FreeCPU     bool
		FreeMemory  bool
		FreeStorage bool
	}{}
	cmd := &cobra.Command{
		Use:   "set <runtime>",
		Short: "Publish or update a runtime public offer",
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
			monthlyPrice, err := parseRuntimeOfferMonthlyUSD(opts.MonthlyUSD)
			if err != nil {
				return err
			}
			if !opts.Free && !opts.FreeCPU && !opts.FreeMemory && !opts.FreeStorage && monthlyPrice <= 0 {
				return fmt.Errorf("one of --monthly-usd, --free, --free-cpu, --free-memory, or --free-storage is required")
			}
			runtimeObj, err = client.SetRuntimePublicOffer(runtimeObj.ID, setRuntimePublicOfferRequest{
				ReferenceBundle: model.BillingResourceSpec{
					CPUMilliCores:    opts.CPU,
					MemoryMebibytes:  opts.Memory,
					StorageGibibytes: opts.Storage,
				},
				ReferenceMonthlyPriceMicroCents: monthlyPrice,
				Free:                            opts.Free,
				FreeCPU:                         opts.FreeCPU,
				FreeMemory:                      opts.FreeMemory,
				FreeStorage:                     opts.FreeStorage,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"runtime": runtimeObj})
			}
			return c.renderRuntimeOffer(client, runtimeObj)
		},
	}
	cmd.Flags().Int64Var(&opts.CPU, "cpu", 0, "Reference CPU bundle in millicores")
	cmd.Flags().Int64Var(&opts.Memory, "memory", 0, "Reference memory bundle in MiB")
	cmd.Flags().Int64Var(&opts.Storage, "storage", 0, "Reference storage bundle in GiB")
	cmd.Flags().StringVar(&opts.MonthlyUSD, "monthly-usd", "", "Reference monthly price in USD, for example 19.99")
	cmd.Flags().BoolVar(&opts.Free, "free", false, "Mark the full runtime offer as free")
	cmd.Flags().BoolVar(&opts.FreeCPU, "free-cpu", false, "Do not charge for CPU")
	cmd.Flags().BoolVar(&opts.FreeMemory, "free-memory", false, "Do not charge for memory")
	cmd.Flags().BoolVar(&opts.FreeStorage, "free-storage", false, "Do not charge for storage")
	return cmd
}

func (c *CLI) newRuntimeEnrollCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "enroll",
		Short: "Manage runtime enrollment tokens",
	}
	cmd.AddCommand(
		c.newRuntimeEnrollListCommand(),
		c.newRuntimeEnrollCreateCommand(),
	)
	return cmd
}

func (c *CLI) newRuntimeEnrollListCommand() *cobra.Command {
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

func (c *CLI) newRuntimeEnrollCreateCommand() *cobra.Command {
	cmd := c.newRuntimeAttachCommand()
	cmd.Use = "create <label>"
	cmd.Short = "Create a runtime enrollment token with join instructions"
	return cmd
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
			matchingNodes := matchRuntimeDoctorClusterNodes(runtimeObj, nodes)
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
			if err := c.renderRuntimeDetail(client, runtimeObj); err != nil {
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

func matchRuntimeDoctorClusterNodes(runtimeObj model.Runtime, nodes []model.ClusterNode) []model.ClusterNode {
	if len(nodes) == 0 {
		return nil
	}
	runtimeID := strings.TrimSpace(runtimeObj.ID)
	out := make([]model.ClusterNode, 0, len(nodes))
	for _, node := range nodes {
		nodeRuntimeID := strings.TrimSpace(node.RuntimeID)
		if strings.EqualFold(nodeRuntimeID, runtimeID) {
			out = append(out, node)
			continue
		}
		if runtimeObj.Type != model.RuntimeTypeManagedShared {
			continue
		}
		if runtimeID == runtimeDoctorManagedSharedBaseID &&
			(strings.HasPrefix(nodeRuntimeID, runtimeDoctorManagedSharedIDPrefix) || strings.EqualFold(nodeRuntimeID, runtimeDoctorManagedSharedBaseID)) {
			out = append(out, node)
		}
	}
	return out
}

func (c *CLI) newRuntimeDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <runtime>",
		Aliases: []string{"rm", "remove"},
		Short:   "Delete a runtime",
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
			response, err := client.DeleteRuntime(runtimeObj.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := c.renderRuntimeDetail(client, response.Runtime); err != nil {
				return err
			}
			return writeKeyValues(c.stdout, kvPair{Key: "deleted", Value: fmt.Sprintf("%t", response.Deleted)})
		},
	}
}

func (c *CLI) renderRuntimeOffer(client *Client, runtimeObj model.Runtime) error {
	tenantName := firstNonEmptyTrimmed(c.loadTenantNames(client)[runtimeObj.TenantID], runtimeObj.TenantID)
	pairs := []kvPair{
		{Key: "runtime", Value: formatDisplayName(runtimeObj.Name, runtimeObj.ID, c.showIDs())},
		{Key: "tenant", Value: formatDisplayName(tenantName, runtimeObj.TenantID, c.showIDs())},
	}
	if runtimeObj.PublicOffer == nil {
		pairs = append(pairs, kvPair{Key: "published", Value: "false"})
		return writeKeyValues(c.stdout, pairs...)
	}
	offer := runtimeObj.PublicOffer
	pairs = append(pairs,
		kvPair{Key: "published", Value: "true"},
		kvPair{Key: "reference_bundle", Value: formatBillingResourceSpec(offer.ReferenceBundle)},
		kvPair{Key: "reference_monthly_price", Value: formatCurrencyMicroCents(offer.ReferenceMonthlyPriceMicroCents, offer.PriceBook.Currency)},
		kvPair{Key: "free", Value: fmt.Sprintf("%t", offer.Free)},
		kvPair{Key: "free_cpu", Value: fmt.Sprintf("%t", offer.FreeCPU)},
		kvPair{Key: "free_memory", Value: fmt.Sprintf("%t", offer.FreeMemory)},
		kvPair{Key: "free_storage", Value: fmt.Sprintf("%t", offer.FreeStorage)},
		kvPair{Key: "updated_at", Value: formatTime(offer.UpdatedAt)},
	)
	return writeKeyValues(c.stdout, pairs...)
}

func parseRuntimeOfferMonthlyUSD(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("parse --monthly-usd: %w", err)
	}
	if value < 0 {
		return 0, fmt.Errorf("--monthly-usd must be greater than or equal to 0")
	}
	return int64(value * 1_000_000), nil
}

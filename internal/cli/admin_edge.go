package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminEdgeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "edge",
		Short: "Manage edge data-plane canary policy",
	}
	cmd.AddCommand(c.newAdminEdgeRoutePolicyCommand())
	return cmd
}

func (c *CLI) newAdminEdgeRoutePolicyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "route-policy",
		Short: "Manage hostname opt-in for regional edge routing",
		Long: strings.TrimSpace(`
Edge route policies are a platform-admin safety gate. Hostnames stay on Route A
by default and enter regional edge only when explicitly set to edge_canary or
edge_enabled for one edge group.
`),
	}
	cmd.AddCommand(
		c.newAdminEdgeRoutePolicyListCommand(),
		c.newAdminEdgeRoutePolicySetCommand(),
		c.newAdminEdgeRoutePolicyDeleteCommand(),
	)
	return cmd
}

func (c *CLI) newAdminEdgeRoutePolicyListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List configured edge route policies",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			policies, err := client.ListEdgeRoutePolicies()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policies": policies})
			}
			return writeEdgeRoutePolicyTable(c.stdout, policies)
		},
	}
}

func (c *CLI) newAdminEdgeRoutePolicySetCommand() *cobra.Command {
	opts := struct {
		EdgeGroupID string
		RoutePolicy string
	}{}
	cmd := &cobra.Command{
		Use:   "set <hostname>",
		Short: "Opt a hostname into one regional edge group",
		Args:  cobra.ExactArgs(1),
		Example: strings.TrimSpace(`
  fugue admin edge route-policy set app-canary.fugue.pro --edge-group edge-group-country-us --policy edge_canary
  fugue admin edge route-policy set app-canary.fugue.pro --edge-group edge-group-country-us --policy edge_enabled
`),
		RunE: func(cmd *cobra.Command, args []string) error {
			policy := model.NormalizeEdgeRoutePolicy(opts.RoutePolicy)
			if policy == "" || policy == model.EdgeRoutePolicyRouteAOnly {
				return fmt.Errorf("--policy must be edge_canary or edge_enabled")
			}
			if strings.TrimSpace(opts.EdgeGroupID) == "" {
				return fmt.Errorf("--edge-group is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			updated, err := client.PutEdgeRoutePolicy(args[0], opts.EdgeGroupID, policy)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policy": updated})
			}
			return writeEdgeRoutePolicy(c.stdout, updated)
		},
	}
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Edge group allowed to serve this hostname")
	cmd.Flags().StringVar(&opts.RoutePolicy, "policy", model.EdgeRoutePolicyCanary, "Policy to apply: edge_canary or edge_enabled")
	return cmd
}

func (c *CLI) newAdminEdgeRoutePolicyDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <hostname>",
		Aliases: []string{"rm", "remove", "disable"},
		Short:   "Remove a hostname edge opt-in policy",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.DeleteEdgeRoutePolicy(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeEdgeRoutePolicy(c.stdout, response.Policy); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "deleted=%t\n", response.Deleted)
			return err
		},
	}
}

func writeEdgeRoutePolicy(w io.Writer, policy model.EdgeRoutePolicy) error {
	return writeKeyValues(w,
		kvPair{Key: "hostname", Value: strings.TrimSpace(policy.Hostname)},
		kvPair{Key: "app", Value: strings.TrimSpace(policy.AppID)},
		kvPair{Key: "tenant", Value: strings.TrimSpace(policy.TenantID)},
		kvPair{Key: "edge_group", Value: strings.TrimSpace(policy.EdgeGroupID)},
		kvPair{Key: "route_policy", Value: strings.TrimSpace(policy.RoutePolicy)},
		kvPair{Key: "enabled", Value: fmt.Sprintf("%t", policy.Enabled)},
		kvPair{Key: "updated", Value: formatTime(policy.UpdatedAt)},
	)
}

func writeEdgeRoutePolicyTable(w io.Writer, policies []model.EdgeRoutePolicy) error {
	sorted := append([]model.EdgeRoutePolicy(nil), policies...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Hostname < sorted[j].Hostname
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "HOSTNAME\tPOLICY\tEDGE_GROUP\tAPP\tENABLED\tUPDATED"); err != nil {
		return err
	}
	for _, policy := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%t\t%s\n",
			strings.TrimSpace(policy.Hostname),
			strings.TrimSpace(policy.RoutePolicy),
			strings.TrimSpace(policy.EdgeGroupID),
			strings.TrimSpace(policy.AppID),
			policy.Enabled,
			formatTime(policy.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

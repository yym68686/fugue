package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminEdgeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "edge",
		Short: "Manage edge data-plane canary policy",
	}
	cmd.AddCommand(c.newAdminEdgeRoutePolicyCommand())
	cmd.AddCommand(c.newAdminEdgeNodesCommand())
	return cmd
}

func (c *CLI) newAdminEdgeNodesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "nodes",
		Aliases: []string{"node"},
		Short:   "Inspect registered edge nodes",
	}
	cmd.AddCommand(
		c.newAdminEdgeNodesListCommand(),
		c.newAdminEdgeNodesGetCommand(),
		c.newAdminEdgeNodesTokenCommand(),
	)
	return cmd
}

func (c *CLI) newAdminEdgeNodesListCommand() *cobra.Command {
	opts := struct {
		EdgeGroupID string
	}{}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List registered edge nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.ListEdgeNodes(opts.EdgeGroupID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeEdgeGroupTable(c.stdout, response.Groups); err != nil {
				return err
			}
			if len(response.Nodes) == 0 {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeEdgeNodeTable(c.stdout, response.Nodes)
		},
	}
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Only show nodes in this edge group")
	return cmd
}

func (c *CLI) newAdminEdgeNodesGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "get <edge-id>",
		Aliases: []string{"show"},
		Short:   "Show one registered edge node",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.GetEdgeNode(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeEdgeNode(c.stdout, response.Node); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeEdgeGroup(c.stdout, response.Group)
		},
	}
}

func (c *CLI) newAdminEdgeNodesTokenCommand() *cobra.Command {
	opts := createEdgeNodeTokenRequest{}
	cmd := &cobra.Command{
		Use:   "token <edge-id>",
		Short: "Create or rotate an edge-scoped token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.EdgeGroupID) == "" {
				return fmt.Errorf("--edge-group is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.CreateEdgeNodeToken(args[0], opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeEdgeNode(c.stdout, response.Node); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "token=%s\n", response.Token)
			return err
		},
	}
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Edge group this node token is allowed to serve")
	cmd.Flags().StringVar(&opts.Region, "region", "", "Region metadata for the edge node")
	cmd.Flags().StringVar(&opts.Country, "country", "", "Country metadata for the edge node")
	cmd.Flags().StringVar(&opts.PublicHostname, "public-hostname", "", "Public hostname for the edge node")
	cmd.Flags().StringVar(&opts.PublicIPv4, "public-ipv4", "", "Public IPv4 for the edge node")
	cmd.Flags().StringVar(&opts.PublicIPv6, "public-ipv6", "", "Public IPv6 for the edge node")
	cmd.Flags().StringVar(&opts.MeshIP, "mesh-ip", "", "Private mesh IP for the edge node")
	cmd.Flags().BoolVar(&opts.Draining, "draining", false, "Create the node in draining mode")
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

func writeEdgeNode(w io.Writer, node model.EdgeNode) error {
	return writeKeyValues(w,
		kvPair{Key: "edge_id", Value: strings.TrimSpace(node.ID)},
		kvPair{Key: "edge_group", Value: strings.TrimSpace(node.EdgeGroupID)},
		kvPair{Key: "status", Value: strings.TrimSpace(node.Status)},
		kvPair{Key: "healthy", Value: fmt.Sprintf("%t", node.Healthy)},
		kvPair{Key: "draining", Value: fmt.Sprintf("%t", node.Draining)},
		kvPair{Key: "region", Value: strings.TrimSpace(node.Region)},
		kvPair{Key: "country", Value: strings.TrimSpace(node.Country)},
		kvPair{Key: "public_ipv4", Value: strings.TrimSpace(node.PublicIPv4)},
		kvPair{Key: "public_ipv6", Value: strings.TrimSpace(node.PublicIPv6)},
		kvPair{Key: "mesh_ip", Value: strings.TrimSpace(node.MeshIP)},
		kvPair{Key: "route_bundle", Value: strings.TrimSpace(node.RouteBundleVersion)},
		kvPair{Key: "dns_bundle", Value: strings.TrimSpace(node.DNSBundleVersion)},
		kvPair{Key: "caddy_routes", Value: fmt.Sprintf("%d", node.CaddyRouteCount)},
		kvPair{Key: "caddy_applied", Value: strings.TrimSpace(node.CaddyAppliedVersion)},
		kvPair{Key: "cache_status", Value: strings.TrimSpace(node.CacheStatus)},
		kvPair{Key: "last_seen", Value: formatOptionalEdgeTime(node.LastSeenAt)},
		kvPair{Key: "last_heartbeat", Value: formatOptionalEdgeTime(node.LastHeartbeatAt)},
		kvPair{Key: "updated", Value: formatTime(node.UpdatedAt)},
	)
}

func writeEdgeGroup(w io.Writer, group model.EdgeGroup) error {
	return writeKeyValues(w,
		kvPair{Key: "edge_group", Value: strings.TrimSpace(group.ID)},
		kvPair{Key: "status", Value: strings.TrimSpace(group.Status)},
		kvPair{Key: "region", Value: strings.TrimSpace(group.Region)},
		kvPair{Key: "country", Value: strings.TrimSpace(group.Country)},
		kvPair{Key: "nodes", Value: fmt.Sprintf("%d", group.NodeCount)},
		kvPair{Key: "healthy_nodes", Value: fmt.Sprintf("%d", group.HealthyNodeCount)},
		kvPair{Key: "has_healthy_nodes", Value: fmt.Sprintf("%t", group.HasHealthyNodes)},
		kvPair{Key: "last_seen", Value: formatOptionalEdgeTime(group.LastSeenAt)},
	)
}

func writeEdgeNodeTable(w io.Writer, nodes []model.EdgeNode) error {
	sorted := append([]model.EdgeNode(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].EdgeGroupID != sorted[j].EdgeGroupID {
			return sorted[i].EdgeGroupID < sorted[j].EdgeGroupID
		}
		return sorted[i].ID < sorted[j].ID
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "EDGE_ID\tGROUP\tSTATUS\tHEALTHY\tDRAINING\tROUTE_BUNDLE\tDNS_BUNDLE\tCADDY_ROUTES\tLAST_SEEN"); err != nil {
		return err
	}
	for _, node := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%t\t%t\t%s\t%s\t%d\t%s\n",
			strings.TrimSpace(node.ID),
			strings.TrimSpace(node.EdgeGroupID),
			strings.TrimSpace(node.Status),
			node.Healthy,
			node.Draining,
			strings.TrimSpace(node.RouteBundleVersion),
			strings.TrimSpace(node.DNSBundleVersion),
			node.CaddyRouteCount,
			formatOptionalEdgeTime(node.LastSeenAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeEdgeGroupTable(w io.Writer, groups []model.EdgeGroup) error {
	sorted := append([]model.EdgeGroup(nil), groups...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ID < sorted[j].ID
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "GROUP\tSTATUS\tNODES\tHEALTHY\tHAS_HEALTHY\tREGION\tCOUNTRY\tLAST_SEEN"); err != nil {
		return err
	}
	for _, group := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%d\t%d\t%t\t%s\t%s\t%s\n",
			strings.TrimSpace(group.ID),
			strings.TrimSpace(group.Status),
			group.NodeCount,
			group.HealthyNodeCount,
			group.HasHealthyNodes,
			strings.TrimSpace(group.Region),
			strings.TrimSpace(group.Country),
			formatOptionalEdgeTime(group.LastSeenAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatOptionalEdgeTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return ""
	}
	return formatTime(*value)
}

package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminDNSCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dns",
		Short: "Inspect fugue-dns inventory and delegation readiness",
	}
	cmd.AddCommand(c.newAdminDNSNodesCommand())
	cmd.AddCommand(c.newAdminDNSACMECommand())
	cmd.AddCommand(c.newAdminDNSStatusCommand())
	cmd.AddCommand(c.newAdminDNSDelegationCommand())
	return cmd
}

func (c *CLI) newAdminDNSNodesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "nodes",
		Aliases: []string{"node"},
		Short:   "Inspect registered DNS nodes",
	}
	cmd.AddCommand(
		c.newAdminDNSNodesListCommand(),
		c.newAdminDNSNodesGetCommand(),
	)
	return cmd
}

func (c *CLI) newAdminDNSNodesListCommand() *cobra.Command {
	opts := struct {
		EdgeGroupID string
	}{}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List registered DNS nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.ListDNSNodes(opts.EdgeGroupID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeDNSNodeTable(c.stdout, response.Nodes)
		},
	}
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Only show DNS nodes in this edge group")
	return cmd
}

func (c *CLI) newAdminDNSNodesGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "get <dns-node-id>",
		Aliases: []string{"show"},
		Short:   "Show one registered DNS node",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.GetDNSNode(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeDNSNode(c.stdout, response.Node)
		},
	}
}

func (c *CLI) newAdminDNSStatusCommand() *cobra.Command {
	opts := dnsDelegationPreflightOptions{}
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Run read-only DNS delegation preflight",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.DNSDelegationPreflight(opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeDNSDelegationPreflight(c.stdout, response)
		},
	}
	cmd.Flags().StringVar(&opts.Zone, "zone", "dns.fugue.pro", "Delegated DNS zone to check")
	cmd.Flags().StringVar(&opts.ProbeName, "probe-name", "d-test.dns.fugue.pro", "A record each DNS node must answer")
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Only check DNS nodes in this edge group")
	cmd.Flags().IntVar(&opts.MinHealthyNodes, "min-healthy-nodes", 2, "Minimum healthy DNS nodes required")
	return cmd
}

func writeDNSNodeTable(w io.Writer, nodes []model.DNSNode) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ID\tGROUP\tSTATUS\tHEALTHY\tZONE\tBUNDLE\tRECORDS\tCACHE\tUDP\tTCP\tQUERIES\tERRORS\tLAST_SEEN"); err != nil {
		return err
	}
	for _, node := range nodes {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%t\t%s\t%s\t%d\t%s\t%t\t%t\t%d\t%d\t%s\n",
			node.ID,
			node.EdgeGroupID,
			node.Status,
			node.Healthy,
			node.Zone,
			firstNonEmpty(node.DNSBundleVersion, "-"),
			node.RecordCount,
			firstNonEmpty(node.CacheStatus, "-"),
			node.UDPListen,
			node.TCPListen,
			node.QueryCount,
			node.QueryErrorCount,
			formatOptionalTime(node.LastSeenAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeDNSNode(w io.Writer, node model.DNSNode) error {
	return writeKeyValues(w,
		kvPair{Key: "id", Value: node.ID},
		kvPair{Key: "edge_group", Value: node.EdgeGroupID},
		kvPair{Key: "zone", Value: node.Zone},
		kvPair{Key: "status", Value: node.Status},
		kvPair{Key: "healthy", Value: fmt.Sprintf("%t", node.Healthy)},
		kvPair{Key: "public_hostname", Value: firstNonEmpty(node.PublicHostname, "-")},
		kvPair{Key: "public_ipv4", Value: firstNonEmpty(node.PublicIPv4, "-")},
		kvPair{Key: "public_ipv6", Value: firstNonEmpty(node.PublicIPv6, "-")},
		kvPair{Key: "mesh_ip", Value: firstNonEmpty(node.MeshIP, "-")},
		kvPair{Key: "dns_bundle_version", Value: firstNonEmpty(node.DNSBundleVersion, "-")},
		kvPair{Key: "record_count", Value: fmt.Sprintf("%d", node.RecordCount)},
		kvPair{Key: "cache_status", Value: firstNonEmpty(node.CacheStatus, "-")},
		kvPair{Key: "cache_write_errors", Value: fmt.Sprintf("%d", node.CacheWriteErrors)},
		kvPair{Key: "cache_load_errors", Value: fmt.Sprintf("%d", node.CacheLoadErrors)},
		kvPair{Key: "bundle_sync_errors", Value: fmt.Sprintf("%d", node.BundleSyncErrors)},
		kvPair{Key: "query_count", Value: fmt.Sprintf("%d", node.QueryCount)},
		kvPair{Key: "query_error_count", Value: fmt.Sprintf("%d", node.QueryErrorCount)},
		kvPair{Key: "listen_addr", Value: firstNonEmpty(node.ListenAddr, "-")},
		kvPair{Key: "udp_addr", Value: firstNonEmpty(node.UDPAddr, "-")},
		kvPair{Key: "tcp_addr", Value: firstNonEmpty(node.TCPAddr, "-")},
		kvPair{Key: "udp_listen", Value: fmt.Sprintf("%t", node.UDPListen)},
		kvPair{Key: "tcp_listen", Value: fmt.Sprintf("%t", node.TCPListen)},
		kvPair{Key: "last_error", Value: firstNonEmpty(node.LastError, "-")},
		kvPair{Key: "last_seen", Value: formatOptionalTime(node.LastSeenAt)},
		kvPair{Key: "last_heartbeat", Value: formatOptionalTime(node.LastHeartbeatAt)},
		kvPair{Key: "created", Value: formatTime(node.CreatedAt)},
		kvPair{Key: "updated", Value: formatTime(node.UpdatedAt)},
	)
}

func writeDNSDelegationPreflight(w io.Writer, response model.DNSDelegationPreflightResponse) error {
	if err := writeKeyValues(w,
		kvPair{Key: "pass", Value: fmt.Sprintf("%t", response.Pass)},
		kvPair{Key: "zone", Value: response.Zone},
		kvPair{Key: "probe_name", Value: response.ProbeName},
		kvPair{Key: "healthy_nodes", Value: fmt.Sprintf("%d/%d", response.HealthyNodeCount, response.MinHealthyNodes)},
		kvPair{Key: "dns_bundle_version", Value: firstNonEmpty(response.DNSBundleVersion, "-")},
		kvPair{Key: "generated_at", Value: formatTime(response.GeneratedAt)},
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if err := writeDNSPreflightCheckTable(w, response.Checks); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if err := writeDNSPreflightNodeTable(w, response.Nodes); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeDNSDelegationPlan(w, response.DelegationPlan)
}

func writeDNSPreflightCheckTable(w io.Writer, checks []model.DNSDelegationPreflightCheck) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "CHECK\tPASS\tMESSAGE"); err != nil {
		return err
	}
	for _, check := range checks {
		if _, err := fmt.Fprintf(tw, "%s\t%t\t%s\n", check.Name, check.Pass, firstNonEmpty(check.Message, "-")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeDNSPreflightNodeTable(w io.Writer, nodes []model.DNSDelegationNodeCheck) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NODE\tGROUP\tPASS\tREADY\tDISK_PRESSURE\tUDP53\tTCP53\tPROBE\tBUNDLE\tPUBLIC_IP\tMESSAGE"); err != nil {
		return err
	}
	for _, node := range nodes {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%t\t%t\t%t\t%t\t%t\t%t\t%s\t%s\t%s\n",
			node.DNSNodeID,
			node.EdgeGroupID,
			node.Pass,
			node.NodeReady,
			node.NodeDiskPressure,
			node.UDP53Reachable,
			node.TCP53Reachable,
			node.ProbePass,
			firstNonEmpty(node.DNSBundleVersion, "-"),
			firstNonEmpty(node.PublicIP, "-"),
			firstNonEmpty(node.Message, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeDNSDelegationPlan(w io.Writer, plan model.DNSDelegationPlan) error {
	if err := writeDNSDelegationRecords(w, "planned A records", plan.PlannedARecords); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if err := writeDNSDelegationRecords(w, "planned NS records", plan.PlannedNSRecords); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if err := writeDNSDelegationRecords(w, "rollback delete records", plan.RollbackDeleteRecords); err != nil {
		return err
	}
	if len(plan.CurrentParentNS) > 0 || len(plan.Notes) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}
	if len(plan.CurrentParentNS) > 0 {
		if _, err := fmt.Fprintf(w, "current_parent_ns=%s\n", strings.Join(plan.CurrentParentNS, ",")); err != nil {
			return err
		}
	}
	if len(plan.Notes) > 0 {
		_, err := fmt.Fprintf(w, "notes=%s\n", strings.Join(plan.Notes, " | "))
		return err
	}
	return nil
}

func writeDNSDelegationRecords(w io.Writer, title string, records []model.DNSDelegationRecord) error {
	if _, err := fmt.Fprintf(w, "%s\n", title); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tTYPE\tVALUES\tTTL\tCOMMENT"); err != nil {
		return err
	}
	for _, record := range records {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n",
			record.Name,
			record.Type,
			strings.Join(record.Values, ","),
			record.TTL,
			firstNonEmpty(record.Comment, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

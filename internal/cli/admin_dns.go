package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	miekgdns "github.com/miekg/dns"
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
	cmd.AddCommand(c.newAdminDNSAnswerCheckCommand())
	cmd.AddCommand(c.newAdminDNSDelegationCommand())
	cmd.AddCommand(c.newAdminDNSFullZoneCommand())
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
	defaultZone := defaultDNSDelegationZone()
	opts := dnsDelegationPreflightOptions{Zone: defaultZone}
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
	cmd.Flags().StringVar(&opts.Zone, "zone", defaultZone, "Delegated DNS zone to check")
	cmd.Flags().StringVar(&opts.ProbeName, "probe-name", "", "A record each DNS node must answer; defaults to d-test.<zone>")
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Only check DNS nodes in this edge group")
	cmd.Flags().IntVar(&opts.MinHealthyNodes, "min-healthy-nodes", 2, "Minimum healthy DNS nodes required")
	return cmd
}

func (c *CLI) newAdminDNSAnswerCheckCommand() *cobra.Command {
	opts := struct {
		Hostname  string
		QueryName string
		ClientIP  string
		Explain   bool
	}{}
	cmd := &cobra.Command{
		Use:   "answer-check <hostname>",
		Short: "Check whether DNS answers point at route-ready edges",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Hostname = strings.TrimSpace(args[0])
			client, err := c.newClient()
			if err != nil {
				return err
			}
			if strings.TrimSpace(opts.ClientIP) != "" && net.ParseIP(strings.TrimSpace(opts.ClientIP)) == nil {
				return fmt.Errorf("--client-ip must be an IP address")
			}
			report, err := c.checkDNSAnswersWithQueryName(client, opts.Hostname, opts.QueryName, opts.ClientIP)
			if err != nil {
				return err
			}
			if opts.Explain {
				quality, err := client.GetEdgeQualityRank(opts.Hostname, "", "", "", "", "global", "30m", "")
				if err != nil {
					return err
				}
				report.QualityRank = &quality
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, report)
			}
			return writeDNSAnswerCheck(c.stdout, report)
		},
	}
	cmd.Flags().StringVar(&opts.QueryName, "query-name", "", "Authoritative Fugue name to query while preserving <hostname> as TLS SNI and HTTP Host")
	cmd.Flags().StringVar(&opts.ClientIP, "client-ip", "", "EDNS client subnet IP to use when probing authoritative answers")
	cmd.Flags().BoolVar(&opts.Explain, "explain", false, "Include scoped edge quality ranking explanation")
	return cmd
}

func (c *CLI) newAdminDNSFullZoneCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "full-zone",
		Short: "Inspect full-zone delegation and protected-record readiness",
	}
	opts := struct {
		Zone            string
		DNSSECStatus    string
		MinHealthyNodes int
	}{Zone: defaultDNSDelegationZone(), DNSSECStatus: "disabled", MinHealthyNodes: 2}
	cmd.AddCommand(&cobra.Command{
		Use:   "preflight",
		Short: "Run full-zone DNS preflight",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.DNSFullZonePreflight(opts.Zone, opts.DNSSECStatus, opts.MinHealthyNodes)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"preflight": response})
			}
			return writeDNSFullZonePreflight(c.stdout, response)
		},
	})
	preflight := cmd.Commands()[0]
	preflight.Flags().StringVar(&opts.Zone, "zone", opts.Zone, "DNS zone to validate")
	preflight.Flags().StringVar(&opts.DNSSECStatus, "dnssec-status", opts.DNSSECStatus, "DNSSEC state: disabled, enabling, enabled, drift")
	preflight.Flags().IntVar(&opts.MinHealthyNodes, "min-healthy-nodes", opts.MinHealthyNodes, "Minimum healthy DNS nodes required")
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

func writeDNSFullZonePreflight(w io.Writer, response model.DNSFullZonePreflightResponse) error {
	if err := writeKeyValues(w,
		kvPair{Key: "pass", Value: fmt.Sprintf("%t", response.Pass)},
		kvPair{Key: "zone", Value: response.Zone},
		kvPair{Key: "dnssec_status", Value: response.DNSSECStatus},
		kvPair{Key: "generated_at", Value: formatTime(response.GeneratedAt)},
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeDNSPreflightCheckTable(w, response.Checks)
}

type dnsAnswerCheckReport struct {
	Hostname                string                         `json:"hostname"`
	QueryName               string                         `json:"query_name,omitempty"`
	ClientIP                string                         `json:"client_ip,omitempty"`
	PolicyReason            string                         `json:"policy_reason,omitempty"`
	GeneratedAt             time.Time                      `json:"generated_at"`
	Pass                    bool                           `json:"pass"`
	AuthoritativeConsistent bool                           `json:"authoritative_consistent"`
	AuthoritativeAnswerSets []dnsAuthoritativeAnswerSet    `json:"authoritative_answer_sets"`
	FailureReasons          []string                       `json:"failure_reasons,omitempty"`
	RouteExplain            model.RouteExplainResponse     `json:"route_explain"`
	QualityRank             *model.EdgeQualityRankResponse `json:"quality_rank,omitempty"`
	RouteReadyEdgeGroups    []string                       `json:"route_ready_edge_groups,omitempty"`
	RouteGenerations        map[string]string              `json:"route_generations,omitempty"`
	HostProbes              []dnsAnswerCheckHostProbe      `json:"host_probes,omitempty"`
	Nodes                   []dnsAnswerCheckNode           `json:"nodes"`
}

type dnsAnswerCheckNode struct {
	DNSNodeID        string   `json:"dns_node_id"`
	EdgeGroupID      string   `json:"edge_group_id,omitempty"`
	Zone             string   `json:"zone"`
	PublicIP         string   `json:"public_ip,omitempty"`
	Status           string   `json:"status"`
	Healthy          bool     `json:"healthy"`
	QueryOK          bool     `json:"query_ok"`
	TLSStatus        string   `json:"tls_status,omitempty"`
	Answers          []string `json:"answers"`
	EdgeGroups       []string `json:"edge_groups,omitempty"`
	RouteGenerations []string `json:"route_generations,omitempty"`
	RouteReady       bool     `json:"route_ready"`
	HostReady        bool     `json:"host_ready"`
	Pass             bool     `json:"pass"`
	Message          string   `json:"message,omitempty"`
}

type dnsAuthoritativeAnswerSet struct {
	Answers      []string `json:"answers"`
	DNSNodeIDs   []string `json:"dns_node_ids"`
	EdgeGroupIDs []string `json:"edge_group_ids,omitempty"`
}

type dnsAnswerCheckHostProbe struct {
	Hostname      string `json:"hostname"`
	IP            string `json:"ip"`
	TLSReady      bool   `json:"tls_ready"`
	StatusCode    int    `json:"status_code,omitempty"`
	RouteNotFound bool   `json:"route_not_found"`
	Pass          bool   `json:"pass"`
	Message       string `json:"message,omitempty"`
}

func (c *CLI) checkDNSAnswers(client *Client, hostname string) (dnsAnswerCheckReport, error) {
	return c.checkDNSAnswersWithClientIP(client, hostname, "")
}

func (c *CLI) checkDNSAnswersWithClientIP(client *Client, hostname, clientIP string) (dnsAnswerCheckReport, error) {
	return c.checkDNSAnswersWithQueryName(client, hostname, "", clientIP)
}

func (c *CLI) checkDNSAnswersWithQueryName(client *Client, hostname, explicitQueryName, clientIP string) (dnsAnswerCheckReport, error) {
	explain, err := client.ExplainRoute(hostname)
	if err != nil {
		return dnsAnswerCheckReport{}, err
	}
	routeReady := routeReadyEdgeGroups(explain)
	dnsTargetOnly := len(routeReady) == 0 && strings.EqualFold(strings.TrimSpace(explain.ServingMode), "unrouted")
	dnsNodes, err := client.ListDNSNodes("")
	if err != nil {
		return dnsAnswerCheckReport{}, err
	}
	edgeNodes, err := client.ListEdgeNodes("")
	if err != nil {
		return dnsAnswerCheckReport{}, err
	}
	edgeGroupsByIP := edgeGroupsByIPFromEdgeNodes(edgeNodes.Nodes)
	edgeNodesByIP := edgeNodesByIPFromEdgeNodes(edgeNodes.Nodes)
	queryName := normalizeDNSHostname(explicitQueryName)
	if queryName == "" {
		queryName = dnsAnswerCheckQueryHostname(hostname, dnsNodes.Nodes)
	}

	nodes := make([]dnsAnswerCheckNode, 0, len(dnsNodes.Nodes))
	pass := len(routeReady) > 0 || dnsTargetOnly
	routeGenerations := map[string]string{}
	for _, edgeNode := range edgeNodes.Nodes {
		groupID := strings.TrimSpace(edgeNode.EdgeGroupID)
		generation := firstNonEmpty(strings.TrimSpace(edgeNode.RouteBundleVersion), strings.TrimSpace(edgeNode.ServingGeneration))
		if groupID != "" && generation != "" && routeGenerations[groupID] == "" {
			routeGenerations[groupID] = generation
		}
	}
	for _, node := range dnsNodes.Nodes {
		if !dnsNodeServesHostname(node, queryName) {
			continue
		}
		nodeReport := dnsAnswerCheckNode{
			DNSNodeID:   strings.TrimSpace(node.ID),
			EdgeGroupID: strings.TrimSpace(node.EdgeGroupID),
			Zone:        strings.TrimSpace(node.Zone),
			PublicIP:    firstNonEmpty(strings.TrimSpace(node.PublicIPv4), strings.TrimSpace(node.PublicIPv6)),
			Status:      strings.TrimSpace(node.Status),
			Healthy:     node.Healthy,
			Answers:     []string{},
		}
		answers, warnings, err := queryDNSNodeAnswers(queryName, node, clientIP)
		if err != nil {
			nodeReport.Pass = false
			nodeReport.Message = err.Error()
			pass = false
			nodes = append(nodes, nodeReport)
			continue
		}
		nodeReport.QueryOK = true
		nodeReport.Answers = answers
		if len(warnings) > 0 {
			nodeReport.Message = appendMessage(nodeReport.Message, strings.Join(warnings, "; "))
		}
		seenGroups := map[string]struct{}{}
		nodePass := true
		for _, answer := range answers {
			groups := edgeGroupsByIP[strings.TrimSpace(answer)]
			edgeNode, hasNode := edgeNodesByIP[strings.TrimSpace(answer)]
			for _, groupID := range groups {
				if groupID != "" {
					seenGroups[groupID] = struct{}{}
				}
			}
			if len(groups) == 0 {
				nodePass = false
				pass = false
				nodeReport.Message = appendMessage(nodeReport.Message, fmt.Sprintf("answer %s is absent from current edge inventory", answer))
				continue
			}
			edgeReady := dnsAnswerEdgeReady(groups, routeReady, dnsTargetOnly)
			if !edgeReady {
				nodePass = false
				pass = false
				nodeReport.Message = appendMessage(nodeReport.Message, fmt.Sprintf("answer %s is mapped to edge groups %s but none are route-ready", answer, strings.Join(groups, ", ")))
			}
			if hasNode {
				generation := firstNonEmpty(strings.TrimSpace(edgeNode.RouteBundleVersion), strings.TrimSpace(edgeNode.ServingGeneration))
				if generation != "" && !stringSliceContains(nodeReport.RouteGenerations, generation) {
					nodeReport.RouteGenerations = append(nodeReport.RouteGenerations, generation)
				}
				if nodeReport.TLSStatus == "" {
					nodeReport.TLSStatus = firstNonEmpty(strings.TrimSpace(edgeNode.TLSStatus), "-")
				}
				if !edgeNodeTLSReady(edgeNode) {
					nodePass = false
					pass = false
					nodeReport.Message = appendMessage(nodeReport.Message, fmt.Sprintf("answer %s is mapped to edge node %s with tls_status=%s", answer, edgeNode.ID, firstNonEmpty(strings.TrimSpace(edgeNode.TLSStatus), "unknown")))
				}
			}
		}
		if len(seenGroups) > 0 {
			nodeReport.EdgeGroups = sortedStringSetKeys(seenGroups)
		}
		if dnsTargetOnly && nodeReport.Message == "" {
			nodeReport.Message = "dns target hostname is not an HTTP route; validated answers against healthy edge inventory"
		}
		if len(nodeReport.Answers) == 0 {
			nodePass = false
			if nodeReport.Message == "" {
				nodeReport.Message = "no A/AAAA answers"
			}
			pass = false
		}
		nodeReport.RouteReady = nodePass
		nodeReport.Pass = nodePass
		nodes = append(nodes, nodeReport)
	}
	if len(nodes) == 0 {
		pass = false
	}
	consistent, answerSets, consensusReasons := summarizeAuthoritativeAnswerSets(nodes)
	if !consistent {
		pass = false
	}
	hostProbes := probeDNSAnswerHosts(hostname, nodes)
	hostProbesByIP := make(map[string]dnsAnswerCheckHostProbe, len(hostProbes))
	for _, probe := range hostProbes {
		hostProbesByIP[probe.IP] = probe
		if !probe.Pass {
			pass = false
		}
	}
	for index := range nodes {
		hostReady := len(nodes[index].Answers) > 0
		for _, answer := range nodes[index].Answers {
			probe, ok := hostProbesByIP[answer]
			if !ok || !probe.Pass {
				hostReady = false
				message := "host probe missing"
				if ok {
					message = firstNonEmpty(probe.Message, "host probe failed")
				}
				nodes[index].Message = appendMessage(nodes[index].Message, fmt.Sprintf("answer %s: %s", answer, message))
			}
		}
		nodes[index].HostReady = hostReady
		nodes[index].Pass = nodes[index].Pass && hostReady
		if !nodes[index].Pass {
			pass = false
		}
	}
	failureReasons := append([]string{}, consensusReasons...)
	for _, probe := range hostProbes {
		if !probe.Pass {
			failureReasons = append(failureReasons, fmt.Sprintf("real Host probe failed for %s via %s: %s", hostname, probe.IP, firstNonEmpty(probe.Message, "unknown error")))
		}
	}
	if len(nodes) == 0 {
		failureReasons = append(failureReasons, fmt.Sprintf("no registered authoritative DNS node serves %s", queryName))
	}
	for _, node := range nodes {
		if !node.Pass {
			failureReasons = append(failureReasons, fmt.Sprintf("DNS node %s (%s) failed: %s", node.DNSNodeID, firstNonEmpty(node.EdgeGroupID, "unknown-group"), firstNonEmpty(node.Message, "unknown error")))
		}
	}
	return dnsAnswerCheckReport{
		Hostname:                hostname,
		QueryName:               queryName,
		ClientIP:                strings.TrimSpace(clientIP),
		PolicyReason:            strings.Join(explain.Reasons, "; "),
		GeneratedAt:             time.Now().UTC(),
		Pass:                    pass,
		AuthoritativeConsistent: consistent,
		AuthoritativeAnswerSets: answerSets,
		FailureReasons:          uniqueStringsPreserveOrder(failureReasons),
		RouteExplain:            explain,
		RouteReadyEdgeGroups:    sortedBoolSetKeys(routeReady),
		RouteGenerations:        routeGenerations,
		HostProbes:              hostProbes,
		Nodes:                   nodes,
	}, nil
}

func writeDNSAnswerCheck(w io.Writer, report dnsAnswerCheckReport) error {
	if err := writeKeyValues(w,
		kvPair{Key: "hostname", Value: report.Hostname},
		kvPair{Key: "query_name", Value: firstNonEmpty(report.QueryName, "-")},
		kvPair{Key: "client_ip", Value: firstNonEmpty(report.ClientIP, "-")},
		kvPair{Key: "policy_reason", Value: firstNonEmpty(report.PolicyReason, "-")},
		kvPair{Key: "pass", Value: fmt.Sprintf("%t", report.Pass)},
		kvPair{Key: "authoritative_consistent", Value: fmt.Sprintf("%t", report.AuthoritativeConsistent)},
		kvPair{Key: "failure_reasons", Value: strings.Join(report.FailureReasons, " | ")},
		kvPair{Key: "route_ready_edge_groups", Value: strings.Join(report.RouteReadyEdgeGroups, ", ")},
		kvPair{Key: "generated_at", Value: formatTime(report.GeneratedAt)},
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if err := writeRouteExplain(w, report.RouteExplain); err != nil {
		return err
	}
	if len(report.Nodes) == 0 {
		if report.QualityRank != nil {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
			return writeEdgeQualityRank(w, *report.QualityRank)
		}
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if err := writeDNSAuthoritativeAnswerSetTable(w, report.AuthoritativeAnswerSets); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if err := writeDNSAnswerCheckHostProbeTable(w, report.HostProbes); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if err := writeDNSAnswerCheckTable(w, report.Nodes); err != nil {
		return err
	}
	if report.QualityRank == nil {
		return nil
	}
	if _, err := fmt.Fprintln(w, "\nQuality rank:"); err != nil {
		return err
	}
	return writeEdgeQualityRank(w, *report.QualityRank)
}

func writeDNSAnswerCheckTable(w io.Writer, nodes []dnsAnswerCheckNode) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "DNS_NODE\tDNS_GROUP\tZONE\tSTATUS\tHEALTHY\tTLS\tANSWERS\tEDGE_GROUPS\tROUTE_GENERATIONS\tROUTE_READY\tHOST_READY\tPASS\tMESSAGE"); err != nil {
		return err
	}
	for _, node := range nodes {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%t\t%s\t%s\t%s\t%s\t%t\t%t\t%t\t%s\n",
			node.DNSNodeID,
			node.EdgeGroupID,
			node.Zone,
			node.Status,
			node.Healthy,
			firstNonEmpty(node.TLSStatus, "-"),
			strings.Join(node.Answers, ", "),
			strings.Join(node.EdgeGroups, ", "),
			strings.Join(node.RouteGenerations, ", "),
			node.RouteReady,
			node.HostReady,
			node.Pass,
			firstNonEmpty(node.Message, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeDNSAuthoritativeAnswerSetTable(w io.Writer, sets []dnsAuthoritativeAnswerSet) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "AUTHORITATIVE_ANSWERS\tDNS_NODES\tDNS_GROUPS"); err != nil {
		return err
	}
	for _, set := range sets {
		answers := strings.Join(set.Answers, ", ")
		if answers == "" {
			answers = "<empty>"
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\n", answers, strings.Join(set.DNSNodeIDs, ", "), strings.Join(set.EdgeGroupIDs, ", ")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeDNSAnswerCheckHostProbeTable(w io.Writer, probes []dnsAnswerCheckHostProbe) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "HOST\tIP\tTLS_READY\tSTATUS\tROUTE_NOT_FOUND\tPASS\tMESSAGE"); err != nil {
		return err
	}
	for _, probe := range probes {
		status := "-"
		if probe.StatusCode > 0 {
			status = strconv.Itoa(probe.StatusCode)
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%t\t%s\t%t\t%t\t%s\n", probe.Hostname, probe.IP, probe.TLSReady, status, probe.RouteNotFound, probe.Pass, firstNonEmpty(probe.Message, "-")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func summarizeAuthoritativeAnswerSets(nodes []dnsAnswerCheckNode) (bool, []dnsAuthoritativeAnswerSet, []string) {
	type answerSetAccumulator struct {
		answers []string
		nodes   map[string]struct{}
		groups  map[string]struct{}
	}
	setsByKey := map[string]*answerSetAccumulator{}
	failedNodes := []string{}
	queriedNodes := 0
	for _, node := range nodes {
		if !node.QueryOK {
			failedNodes = append(failedNodes, firstNonEmpty(node.DNSNodeID, node.PublicIP, "unknown"))
			continue
		}
		queriedNodes++
		answers := append([]string(nil), node.Answers...)
		sort.Strings(answers)
		key := strings.Join(answers, "\x00")
		set := setsByKey[key]
		if set == nil {
			set = &answerSetAccumulator{answers: answers, nodes: map[string]struct{}{}, groups: map[string]struct{}{}}
			setsByKey[key] = set
		}
		if node.DNSNodeID != "" {
			set.nodes[node.DNSNodeID] = struct{}{}
		}
		if node.EdgeGroupID != "" {
			set.groups[node.EdgeGroupID] = struct{}{}
		}
	}
	keys := make([]string, 0, len(setsByKey))
	for key := range setsByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	sets := make([]dnsAuthoritativeAnswerSet, 0, len(keys))
	for _, key := range keys {
		set := setsByKey[key]
		sets = append(sets, dnsAuthoritativeAnswerSet{
			Answers:      append([]string{}, set.answers...),
			DNSNodeIDs:   sortedStringSetKeys(set.nodes),
			EdgeGroupIDs: sortedStringSetKeys(set.groups),
		})
	}
	reasons := []string{}
	if len(nodes) < 2 {
		reasons = append(reasons, fmt.Sprintf("authoritative consensus requires at least 2 nodes; found %d", len(nodes)))
	}
	if len(failedNodes) > 0 {
		sort.Strings(failedNodes)
		reasons = append(reasons, "authoritative query failed on nodes: "+strings.Join(failedNodes, ", "))
	}
	if len(sets) > 1 {
		parts := make([]string, 0, len(sets))
		for _, set := range sets {
			answers := strings.Join(set.Answers, ",")
			if answers == "" {
				answers = "<empty>"
			}
			parts = append(parts, fmt.Sprintf("nodes=%s groups=%s answers=%s", strings.Join(set.DNSNodeIDs, ","), strings.Join(set.EdgeGroupIDs, ","), answers))
		}
		reasons = append(reasons, "authoritative answer split: "+strings.Join(parts, " | "))
	}
	consistent := len(nodes) >= 2 && queriedNodes == len(nodes) && len(sets) == 1
	return consistent, sets, reasons
}

func probeDNSAnswerHosts(hostname string, nodes []dnsAnswerCheckNode) []dnsAnswerCheckHostProbe {
	hostname = normalizeDNSHostname(hostname)
	answerSet := map[string]struct{}{}
	for _, node := range nodes {
		for _, answer := range node.Answers {
			if parsed := net.ParseIP(strings.TrimSpace(answer)); parsed != nil {
				answerSet[parsed.String()] = struct{}{}
			}
		}
	}
	answers := sortedStringSetKeys(answerSet)
	probes := make([]dnsAnswerCheckHostProbe, 0, len(answers))
	for _, answer := range answers {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		probe := probeDNSAnswerHost(ctx, hostname, answer)
		cancel()
		probes = append(probes, probe)
	}
	return probes
}

func probeDNSAnswerHost(ctx context.Context, hostname, answerIP string) dnsAnswerCheckHostProbe {
	probe := dnsAnswerCheckHostProbe{Hostname: normalizeDNSHostname(hostname), IP: strings.TrimSpace(answerIP)}
	parsedIP := net.ParseIP(probe.IP)
	if probe.Hostname == "" || parsedIP == nil {
		probe.Message = "hostname and answer IP are required"
		return probe
	}
	probe.IP = parsedIP.String()
	dialer := net.Dialer{}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(dialCtx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(dialCtx, "tcp", net.JoinHostPort(probe.IP, "443"))
		},
		TLSClientConfig: &tls.Config{ServerName: probe.Hostname, MinVersion: tls.VersionTLS12},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	requestURL := (&url.URL{Scheme: "https", Host: probe.Hostname, Path: "/"}).String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		probe.Message = "build Host probe: " + err.Error()
		return probe
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", "fugue-dns-answer-check/1")
	resp, err := client.Do(req)
	if err != nil {
		probe.Message = err.Error()
		return probe
	}
	defer resp.Body.Close()
	probe.StatusCode = resp.StatusCode
	probe.TLSReady = resp.TLS != nil && resp.TLS.HandshakeComplete
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		probe.Message = "read Host probe response: " + err.Error()
		return probe
	}
	probe.RouteNotFound = bytes.Contains(bytes.ToLower(body), []byte("edge route not found"))
	probe.Pass = probe.TLSReady && !probe.RouteNotFound
	switch {
	case !probe.TLSReady:
		probe.Message = "TLS handshake did not complete"
	case probe.RouteNotFound:
		probe.Message = "response body contains edge route not found"
	default:
		probe.Message = fmt.Sprintf("HTTP %d with valid TLS/SNI/Host routing", probe.StatusCode)
	}
	return probe
}

func queryDNSNodeAnswers(hostname string, node model.DNSNode, clientIP string) ([]string, []string, error) {
	address := ""
	if ip := strings.TrimSpace(node.PublicIPv4); ip != "" {
		address = net.JoinHostPort(ip, "53")
	} else if ip := strings.TrimSpace(node.PublicIPv6); ip != "" {
		address = net.JoinHostPort(ip, "53")
	}
	if address == "" {
		return nil, nil, fmt.Errorf("dns node has no public IP")
	}
	answers := []string{}
	warnings := []string{}
	successfulQueries := 0
	if udpAnswers, err := queryAuthoritativeDNSRecord(hostname, address, "udp", miekgdns.TypeA, clientIP); err == nil {
		successfulQueries++
		answers = append(answers, udpAnswers...)
	} else {
		warnings = append(warnings, fmt.Sprintf("udp A query failed: %v", err))
	}
	if tcpAnswers, err := queryAuthoritativeDNSRecord(hostname, address, "tcp", miekgdns.TypeA, clientIP); err == nil {
		successfulQueries++
		answers = append(answers, tcpAnswers...)
	} else {
		warnings = append(warnings, fmt.Sprintf("tcp A query failed: %v", err))
	}
	if udpAAAA, err := queryAuthoritativeDNSRecord(hostname, address, "udp", miekgdns.TypeAAAA, clientIP); err == nil {
		successfulQueries++
		answers = append(answers, udpAAAA...)
	} else {
		warnings = append(warnings, fmt.Sprintf("udp AAAA query failed: %v", err))
	}
	if tcpAAAA, err := queryAuthoritativeDNSRecord(hostname, address, "tcp", miekgdns.TypeAAAA, clientIP); err == nil {
		successfulQueries++
		answers = append(answers, tcpAAAA...)
	} else {
		warnings = append(warnings, fmt.Sprintf("tcp AAAA query failed: %v", err))
	}
	answers = uniqueStringsPreserveOrder(answers)
	if successfulQueries == 0 {
		return nil, warnings, fmt.Errorf("%s", strings.Join(warnings, "; "))
	}
	if len(answers) == 0 {
		warnings = append(warnings, "no A/AAAA answers")
	}
	return answers, warnings, nil
}

func queryAuthoritativeDNSRecord(hostname, address, network string, qtype uint16, clientIP string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	msg := new(miekgdns.Msg)
	msg.SetQuestion(miekgdns.Fqdn(hostname), qtype)
	if subnet := dnsClientSubnetOption(clientIP); subnet != nil {
		opt := msg.IsEdns0()
		if opt == nil {
			opt = &miekgdns.OPT{Hdr: miekgdns.RR_Header{Name: ".", Rrtype: miekgdns.TypeOPT}}
			msg.Extra = append(msg.Extra, opt)
		}
		opt.Option = append(opt.Option, subnet)
	}
	client := &miekgdns.Client{Net: network, Timeout: 3 * time.Second}
	resp, _, err := client.ExchangeContext(ctx, msg, address)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("empty DNS response")
	}
	if resp.Rcode != miekgdns.RcodeSuccess {
		return nil, fmt.Errorf("rcode=%s", miekgdns.RcodeToString[resp.Rcode])
	}
	answers := []string{}
	for _, answer := range resp.Answer {
		switch rr := answer.(type) {
		case *miekgdns.A:
			if rr.A != nil {
				answers = append(answers, rr.A.String())
			}
		case *miekgdns.AAAA:
			if rr.AAAA != nil {
				answers = append(answers, rr.AAAA.String())
			}
		}
	}
	return uniqueStringsPreserveOrder(answers), nil
}

func dnsClientSubnetOption(clientIP string) *miekgdns.EDNS0_SUBNET {
	ip := net.ParseIP(strings.TrimSpace(clientIP))
	if ip == nil {
		return nil
	}
	if v4 := ip.To4(); v4 != nil {
		return &miekgdns.EDNS0_SUBNET{
			Code:          miekgdns.EDNS0SUBNET,
			Family:        1,
			SourceNetmask: 24,
			Address:       v4,
		}
	}
	return &miekgdns.EDNS0_SUBNET{
		Code:          miekgdns.EDNS0SUBNET,
		Family:        2,
		SourceNetmask: 56,
		Address:       ip,
	}
}

func dnsAnswerCheckQueryHostname(hostname string, nodes []model.DNSNode) string {
	candidates := []string{hostname}
	candidates = append(candidates, lookupDNSCNAMECandidates(hostname)...)
	return dnsAnswerCheckQueryHostnameFromCandidates(hostname, nodes, candidates)
}

func lookupDNSCNAMECandidates(hostname string) []string {
	hostname = strings.TrimSpace(hostname)
	out := []string{}
	add := func(value string) {
		value = normalizeDNSHostname(value)
		if value == "" || stringSliceContains(out, value) {
			return
		}
		out = append(out, value)
	}
	if cname, err := net.LookupCNAME(hostname); err == nil {
		add(cname)
	}
	addresses := dnsRecursiveResolverAddresses()
	for _, address := range addresses {
		for _, network := range []string{"udp", "tcp"} {
			cnames, err := queryRecursiveDNSCNAME(hostname, address, network)
			if err != nil {
				continue
			}
			for _, cname := range cnames {
				add(cname)
			}
			if len(out) > 0 {
				return out
			}
		}
	}
	return out
}

func dnsRecursiveResolverAddresses() []string {
	seen := map[string]struct{}{}
	out := []string{}
	add := func(address string) {
		address = strings.TrimSpace(address)
		if address == "" {
			return
		}
		if _, ok := seen[address]; ok {
			return
		}
		seen[address] = struct{}{}
		out = append(out, address)
	}
	if config, err := miekgdns.ClientConfigFromFile("/etc/resolv.conf"); err == nil && config != nil {
		port := firstNonEmpty(strings.TrimSpace(config.Port), "53")
		for _, server := range config.Servers {
			if strings.TrimSpace(server) == "" {
				continue
			}
			add(net.JoinHostPort(server, port))
		}
	}
	add("1.1.1.1:53")
	add("8.8.8.8:53")
	add("9.9.9.9:53")
	return out
}

func queryRecursiveDNSCNAME(hostname, address, network string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg := new(miekgdns.Msg)
	msg.SetQuestion(miekgdns.Fqdn(hostname), miekgdns.TypeCNAME)
	client := &miekgdns.Client{Net: network, Timeout: 2 * time.Second}
	resp, _, err := client.ExchangeContext(ctx, msg, address)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("empty DNS response")
	}
	if resp.Rcode != miekgdns.RcodeSuccess {
		return nil, fmt.Errorf("rcode=%s", miekgdns.RcodeToString[resp.Rcode])
	}
	out := []string{}
	for _, answer := range resp.Answer {
		if rr, ok := answer.(*miekgdns.CNAME); ok {
			out = append(out, rr.Target)
		}
	}
	return uniqueStringsPreserveOrder(out), nil
}

func dnsAnswerCheckQueryHostnameFromCandidates(hostname string, nodes []model.DNSNode, candidates []string) string {
	fallback := normalizeDNSHostname(hostname)
	for _, candidate := range candidates {
		candidate = normalizeDNSHostname(candidate)
		if candidate == "" {
			continue
		}
		for _, node := range nodes {
			if dnsNodeServesHostname(node, candidate) {
				return candidate
			}
		}
	}
	return fallback
}

func routeReadyEdgeGroups(explain model.RouteExplainResponse) map[string]bool {
	out := map[string]bool{}
	add := func(route model.EdgeRouteBinding) {
		if strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive) &&
			model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) &&
			strings.TrimSpace(route.EdgeGroupID) != "" &&
			strings.TrimSpace(route.UpstreamURL) != "" {
			out[strings.TrimSpace(route.EdgeGroupID)] = true
		}
	}
	for _, route := range explain.Routes {
		add(route)
	}
	if len(out) == 0 && explain.Route != nil {
		add(*explain.Route)
	}
	return out
}

func dnsAnswerEdgeReady(groups []string, routeReady map[string]bool, dnsTargetOnly bool) bool {
	if len(groups) == 0 {
		return false
	}
	if dnsTargetOnly {
		return true
	}
	for _, groupID := range groups {
		if routeReady[strings.TrimSpace(groupID)] {
			return true
		}
	}
	return false
}

func edgeGroupsByIPFromEdgeNodes(nodes []model.EdgeNode) map[string][]string {
	out := map[string][]string{}
	for _, node := range nodes {
		groupID := strings.TrimSpace(node.EdgeGroupID)
		if groupID == "" {
			continue
		}
		for _, raw := range []string{node.PublicIPv4, node.PublicIPv6} {
			ip := strings.TrimSpace(raw)
			if ip == "" {
				continue
			}
			if !stringSliceContains(out[ip], groupID) {
				out[ip] = append(out[ip], groupID)
			}
		}
	}
	for ip := range out {
		sort.Strings(out[ip])
	}
	return out
}

func edgeNodesByIPFromEdgeNodes(nodes []model.EdgeNode) map[string]model.EdgeNode {
	out := map[string]model.EdgeNode{}
	for _, node := range nodes {
		for _, raw := range []string{node.PublicIPv4, node.PublicIPv6} {
			ip := strings.TrimSpace(raw)
			if ip == "" {
				continue
			}
			if _, ok := out[ip]; !ok {
				out[ip] = node
			}
		}
	}
	return out
}

func edgeNodeTLSReady(node model.EdgeNode) bool {
	switch model.NormalizeEdgeTLSStatus(node.TLSStatus) {
	case model.EdgeTLSStatusReady:
		return true
	case model.EdgeTLSStatusPending, model.EdgeTLSStatusError:
		return false
	default:
		if node.CaddyRouteCount <= 0 {
			return false
		}
		if strings.Contains(strings.ToLower(strings.TrimSpace(node.CaddyLastError)), "error") {
			return false
		}
		if strings.Contains(strings.ToLower(strings.TrimSpace(node.CacheStatus)), "error") {
			return false
		}
		return true
	}
}

func dnsNodeServesHostname(node model.DNSNode, hostname string) bool {
	host := normalizeDNSHostname(hostname)
	zone := normalizeDNSHostname(node.Zone)
	return host != "" && zone != "" && (host == zone || strings.HasSuffix(host, "."+zone))
}

func appendMessage(current, addition string) string {
	current = strings.TrimSpace(current)
	addition = strings.TrimSpace(addition)
	if addition == "" {
		return current
	}
	if current == "" {
		return addition
	}
	return current + "; " + addition
}

func sortedBoolSetKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key, ok := range values {
		if ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func sortedStringSetKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func uniqueStringsPreserveOrder(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizeDNSHostname(raw string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(raw)), ".")
}

func stringSliceContains(values []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
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

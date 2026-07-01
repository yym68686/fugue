package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type adminCockpitPayload struct {
	Tenants           []model.Tenant                        `json:"tenants,omitempty"`
	Projects          []consoleProjectSummary               `json:"projects,omitempty"`
	Runtimes          []model.Runtime                       `json:"runtimes,omitempty"`
	ClusterNodes      []model.ClusterNode                   `json:"cluster_nodes,omitempty"`
	NodePolicySummary *model.ClusterNodePolicyStatusSummary `json:"node_policy_summary,omitempty"`
	NodePolicies      []model.ClusterNodePolicyStatus       `json:"node_policies,omitempty"`
	EdgeNodes         []model.EdgeNode                      `json:"edge_nodes,omitempty"`
	DNSNodes          []model.DNSNode                       `json:"dns_nodes,omitempty"`
	ControlPlane      *model.ControlPlaneStatus             `json:"control_plane,omitempty"`
	Backup            *adminBackupStatusResponse            `json:"backup,omitempty"`
	Users             *adminUsersPageSnapshot               `json:"users,omitempty"`
	RouteTrace        []adminCockpitRouteTraceRow           `json:"route_trace,omitempty"`
	Warnings          []string                              `json:"warnings,omitempty"`
	ReleasePath       string                                `json:"release_path"`
}

type adminCockpitRouteTraceRow struct {
	App     string `json:"app"`
	Route   string `json:"route"`
	Runtime string `json:"runtime"`
	Node    string `json:"node"`
	Edge    string `json:"edge"`
	DNS     string `json:"dns"`
	Status  string `json:"status"`
}

func (c *CLI) newAdminCockpitCommand() *cobra.Command {
	opts := struct {
		Cookie    string
		WithUsers bool
		Redacted  bool
	}{Redacted: true}
	cmd := &cobra.Command{
		Use:   "cockpit",
		Short: "Show a read-only admin cockpit across users, cluster, runtime, edge, and DNS",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			payload := c.loadAdminCockpit(client)
			if opts.WithUsers {
				webClient, err := c.newWebClient(opts.Cookie)
				if err != nil {
					payload.Warnings = append(payload.Warnings, "users unavailable: "+err.Error())
				} else if users, err := fetchAdminUsersSnapshot(webClient, "/api/fugue/admin/pages/users/enrich"); err != nil {
					payload.Warnings = append(payload.Warnings, "users unavailable: "+err.Error())
				} else {
					payload.Users = &users
				}
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, payload)
			}
			return c.renderAdminCockpit(payload, opts.Redacted)
		},
	}
	cmd.Flags().StringVar(&opts.Cookie, "cookie", "", "Optional web session cookie header value for user snapshots")
	cmd.Flags().BoolVar(&opts.WithUsers, "with-users", false, "Include fugue-web admin users snapshot")
	cmd.Flags().BoolVar(&opts.Redacted, "redacted", opts.Redacted, "Redact user emails in the rendered diagnostic summary")
	return cmd
}

func (c *CLI) loadAdminCockpit(client *Client) adminCockpitPayload {
	payload := adminCockpitPayload{ReleasePath: "GitHub Actions deploy-control-plane.yml"}
	if tenants, err := client.ListTenants(); err == nil {
		payload.Tenants = tenants
	} else {
		payload.Warnings = append(payload.Warnings, "tenant resolve unavailable: "+err.Error())
	}
	if gallery, err := client.GetConsoleGalleryWithLiveStatus(true); err == nil {
		payload.Projects = gallery.Projects
	} else {
		payload.Warnings = append(payload.Warnings, "project gallery unavailable: "+err.Error())
	}
	if runtimes, err := client.ListRuntimes(); err == nil {
		payload.Runtimes = runtimes
	} else {
		payload.Warnings = append(payload.Warnings, "runtime capacity unavailable: "+err.Error())
	}
	if nodes, err := client.ListClusterNodes(); err == nil {
		payload.ClusterNodes = nodes
	} else {
		payload.Warnings = append(payload.Warnings, "cluster nodes unavailable: "+err.Error())
	}
	if summary, policies, err := client.GetClusterNodePolicyStatus(); err == nil {
		payload.NodePolicySummary = &summary
		payload.NodePolicies = policies
	} else {
		payload.Warnings = append(payload.Warnings, "node policy unavailable: "+err.Error())
	}
	if edge, err := client.ListEdgeNodes(""); err == nil {
		payload.EdgeNodes = edge.Nodes
	} else {
		payload.Warnings = append(payload.Warnings, "edge health unavailable: "+err.Error())
	}
	if dns, err := client.ListDNSNodes(""); err == nil {
		payload.DNSNodes = dns.Nodes
	} else {
		payload.Warnings = append(payload.Warnings, "DNS health unavailable: "+err.Error())
	}
	if controlPlane, err := client.GetControlPlaneStatus(); err == nil {
		payload.ControlPlane = &controlPlane
	} else {
		payload.Warnings = append(payload.Warnings, "control plane unavailable: "+err.Error())
	}
	if backup, err := client.GetAdminBackupStatus(); err == nil {
		payload.Backup = &backup
	} else {
		payload.Warnings = append(payload.Warnings, "backup status unavailable: "+err.Error())
	}
	payload.RouteTrace = buildAdminRouteTrace(payload)
	return payload
}

func (c *CLI) renderAdminCockpit(payload adminCockpitPayload, redacted bool) error {
	renderer := c.richRenderer()
	body := []string{
		fmt.Sprintf("tenants=%d projects=%d runtimes=%d nodes=%d edge=%d dns=%d", len(payload.Tenants), len(payload.Projects), len(payload.Runtimes), len(payload.ClusterNodes), len(payload.EdgeNodes), len(payload.DNSNodes)),
		"release_path=" + payload.ReleasePath,
	}
	if payload.ControlPlane != nil {
		body = append(body, "control_plane="+firstNonEmptyTrimmed(payload.ControlPlane.Status, "-"))
	}
	if payload.Backup != nil {
		body = append(body, fmt.Sprintf("backup=%s billable_backup_bytes=%d", summarizeAdminBackupPosture(payload.Backup.Posture), payload.Backup.Usage.BillableBytes))
	}
	if payload.NodePolicySummary != nil {
		body = append(body, fmt.Sprintf("node_policy_drifted=%d blocked_by_health=%d filesystem_pressure=%d", payload.NodePolicySummary.Drifted, payload.NodePolicySummary.BlockedByHealth, payload.NodePolicySummary.FilesystemPressure))
	}
	if payload.Users != nil {
		body = append(body, fmt.Sprintf("users=%d admins=%d blocked=%d", payload.Users.Summary.UserCount, payload.Users.Summary.AdminCount, payload.Users.Summary.BlockedCount))
	}
	if len(payload.RouteTrace) > 0 {
		rows := make([][]string, 0, len(payload.RouteTrace))
		for _, trace := range payload.RouteTrace {
			rows = append(rows, []string{trace.App, trace.Route, trace.Runtime, trace.Node, trace.Edge, trace.DNS, trace.Status})
		}
		body = append(body, "", renderer.TableWithTitle("route drilldown", []string{"APP", "ROUTE", "RUNTIME", "NODE", "EDGE", "DNS", "STATUS"}, rows))
	}
	if len(payload.ClusterNodes) > 0 {
		rows := make([][]string, 0, len(payload.ClusterNodes))
		for _, node := range payload.ClusterNodes {
			rows = append(rows, []string{node.Name, firstNonEmptyTrimmed(node.Status, "-"), firstNonEmptyTrimmed(node.RuntimeID, "-"), firstNonEmptyTrimmed(node.Region, node.Zone, "-")})
		}
		body = append(body, "", renderer.TableWithTitle("cluster nodes", []string{"NODE", "STATUS", "RUNTIME", "REGION"}, rows))
	} else {
		body = append(body, "cluster_nodes=empty")
	}
	if payload.Backup != nil && len(payload.Backup.Posture) > 0 {
		rows := make([][]string, 0, len(payload.Backup.Posture))
		for _, posture := range payload.Backup.Posture {
			rows = append(rows, []string{
				posture.Target.Type,
				firstNonEmptyTrimmed(posture.Status, "-"),
				formatBackupTime(posture.LastSuccessfulAt),
				fmt.Sprintf("%d", posture.BillableBytes),
				firstNonEmptyTrimmed(posture.Message, "-"),
			})
		}
		body = append(body, "", renderer.TableWithTitle("backup posture", []string{"TARGET", "STATUS", "LAST SUCCESS", "BILLABLE BYTES", "MESSAGE"}, rows))
	}
	if payload.Users != nil && len(payload.Users.Users) > 0 {
		rows := make([][]string, 0, len(payload.Users.Users))
		for _, user := range payload.Users.Users {
			email := user.Email
			if redacted {
				email = redactEmail(email)
			}
			tenantID := "-"
			if user.Workspace != nil {
				tenantID = user.Workspace.TenantID
			}
			rows = append(rows, []string{email, user.Status, fmt.Sprintf("%t", user.IsAdmin), tenantID})
		}
		body = append(body, "", renderer.TableWithTitle("users", []string{"USER", "STATUS", "ADMIN", "TENANT"}, rows))
	}
	if len(payload.Warnings) > 0 {
		body = append(body, "", "warnings")
		body = append(body, payload.Warnings...)
	}
	body = append(body, "", "admin_writes=disabled_until_action_plan")
	_, err := fmt.Fprint(c.stdout, renderer.Panel("Admin cockpit", strings.Join(body, "\n")))
	return err
}

func summarizeAdminBackupPosture(posture []model.BackupPosture) string {
	if len(posture) == 0 {
		return "unknown"
	}
	counts := map[string]int{}
	for _, item := range posture {
		status := firstNonEmptyTrimmed(item.Status, "unknown")
		counts[status]++
	}
	if counts["blocked"] > 0 || counts["failed"] > 0 {
		return fmt.Sprintf("attention(%d)", counts["blocked"]+counts["failed"])
	}
	if counts["ready"] == len(posture) {
		return "ready"
	}
	if counts["disabled"] == len(posture) {
		return "disabled"
	}
	return "mixed"
}

func buildAdminRouteTrace(payload adminCockpitPayload) []adminCockpitRouteTraceRow {
	runtimeByID := map[string]model.Runtime{}
	for _, runtimeObj := range payload.Runtimes {
		runtimeByID[strings.TrimSpace(runtimeObj.ID)] = runtimeObj
	}
	nodeByRuntime := map[string]string{}
	for _, node := range payload.ClusterNodes {
		if node.RuntimeID != "" {
			nodeByRuntime[strings.TrimSpace(node.RuntimeID)] = strings.TrimSpace(node.Name)
		}
	}
	edgeID := "-"
	if len(payload.EdgeNodes) > 0 {
		edgeID = firstNonEmptyTrimmed(payload.EdgeNodes[0].ID, payload.EdgeNodes[0].PublicHostname, "-")
	}
	dnsID := "-"
	if len(payload.DNSNodes) > 0 {
		dnsID = firstNonEmptyTrimmed(payload.DNSNodes[0].ID, payload.DNSNodes[0].Zone, "-")
	}
	rows := []adminCockpitRouteTraceRow{}
	for _, project := range payload.Projects {
		_ = project
	}
	// Project gallery does not include individual app routes, so the detailed
	// trace is populated by console project views when available in future.
	for _, runtimeObj := range payload.Runtimes {
		node := firstNonEmptyTrimmed(runtimeObj.ClusterNodeName, nodeByRuntime[runtimeObj.ID], "-")
		rows = append(rows, adminCockpitRouteTraceRow{
			App:     "-",
			Route:   "-",
			Runtime: firstNonEmptyTrimmed(runtimeObj.Name, runtimeObj.ID),
			Node:    node,
			Edge:    edgeID,
			DNS:     dnsID,
			Status:  firstNonEmptyTrimmed(runtimeObj.Status, "-"),
		})
	}
	return rows
}

func redactEmail(email string) string {
	email = strings.TrimSpace(email)
	at := strings.Index(email, "@")
	if at <= 1 {
		return "<redacted>"
	}
	return email[:1] + "***" + email[at:]
}

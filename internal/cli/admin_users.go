package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

type adminUsersPageSnapshot struct {
	EnrichmentState string            `json:"enrichmentState"`
	Errors          []string          `json:"errors"`
	Summary         adminUsersSummary `json:"summary"`
	Users           []adminUserView   `json:"users"`
}

type adminUsersSummary struct {
	AdminCount   int `json:"adminCount"`
	BlockedCount int `json:"blockedCount"`
	DeletedCount int `json:"deletedCount"`
	UserCount    int `json:"userCount"`
}

type adminUserView struct {
	Billing           adminUserBillingView    `json:"billing"`
	CanBlock          bool                    `json:"canBlock"`
	CanDelete         bool                    `json:"canDelete"`
	CanDemoteAdmin    bool                    `json:"canDemoteAdmin"`
	CanPromoteToAdmin bool                    `json:"canPromoteToAdmin"`
	CanUnblock        bool                    `json:"canUnblock"`
	Email             string                  `json:"email"`
	IsAdmin           bool                    `json:"isAdmin"`
	LastLoginExact    string                  `json:"lastLoginExact"`
	LastLoginLabel    string                  `json:"lastLoginLabel"`
	Name              string                  `json:"name"`
	Provider          string                  `json:"provider"`
	ServiceCount      int                     `json:"serviceCount"`
	Status            string                  `json:"status"`
	StatusTone        string                  `json:"statusTone"`
	Usage             adminUserUsageView      `json:"usage"`
	Verified          bool                    `json:"verified"`
	Workspace         *adminUserWorkspaceView `json:"workspace"`
}

type adminUserBillingView struct {
	BalanceLabel         string `json:"balanceLabel"`
	LimitLabel           string `json:"limitLabel"`
	LoadError            string `json:"loadError"`
	Loading              bool   `json:"loading"`
	MonthlyEstimateLabel string `json:"monthlyEstimateLabel"`
	StatusLabel          string `json:"statusLabel"`
	StatusReason         string `json:"statusReason"`
	TenantID             string `json:"tenantId"`
}

type adminUserUsageView struct {
	CPULabel          string `json:"cpuLabel"`
	DiskLabel         string `json:"diskLabel"`
	ImageLabel        string `json:"imageLabel"`
	Loading           bool   `json:"loading"`
	MemoryLabel       string `json:"memoryLabel"`
	ServiceCount      int    `json:"serviceCount"`
	ServiceCountLabel string `json:"serviceCountLabel"`
}

type adminUserWorkspaceView struct {
	AdminKeyLabel      string `json:"adminKeyLabel"`
	DefaultProjectID   string `json:"defaultProjectId"`
	DefaultProjectName string `json:"defaultProjectName"`
	FirstAppID         string `json:"firstAppId"`
	TenantID           string `json:"tenantId"`
	TenantName         string `json:"tenantName"`
}

type adminUsersUsageSnapshot struct {
	Users []adminUserUsageSnapshotEntry `json:"users"`
}

type adminUserUsageSnapshotEntry struct {
	Email        string             `json:"email"`
	ServiceCount int                `json:"serviceCount"`
	Usage        adminUserUsageView `json:"usage"`
}

func (c *CLI) newAdminUsersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "users",
		Short: "Inspect product-layer admin user snapshots from fugue-web",
	}
	cmd.AddCommand(
		c.newAdminUsersListCommand(),
		c.newAdminUsersShowCommand(),
		c.newAdminUsersResolveCommand(),
		c.newAdminUsersEnrichCommand(),
		c.newAdminUsersUsageCommand(),
	)
	return cmd
}

func (c *CLI) newAdminUsersListCommand() *cobra.Command {
	opts := struct {
		Cookie string
	}{}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List admin users from the product-layer snapshot",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newWebClient(opts.Cookie)
			if err != nil {
				return err
			}
			snapshot, err := fetchAdminUsersSnapshot(client, "/api/fugue/admin/pages/users")
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, snapshot)
			}
			return renderAdminUsersSnapshot(c.stdout, snapshot)
		},
	}
	cmd.Flags().StringVar(&opts.Cookie, "cookie", "", "Optional web session cookie header value")
	return cmd
}

func (c *CLI) newAdminUsersShowCommand() *cobra.Command {
	opts := struct {
		Cookie string
	}{}
	cmd := &cobra.Command{
		Use:   "show <email>",
		Short: "Show one admin user from the enriched product-layer snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newWebClient(opts.Cookie)
			if err != nil {
				return err
			}
			snapshot, err := fetchAdminUsersSnapshot(client, "/api/fugue/admin/pages/users/enrich")
			if err != nil {
				return err
			}
			user, ok := findAdminUser(snapshot.Users, args[0])
			if !ok {
				return fmt.Errorf("admin user %q not found", args[0])
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"enrichmentState": snapshot.EnrichmentState,
					"errors":          snapshot.Errors,
					"user":            user,
				})
			}
			return renderAdminUser(c.stdout, snapshot.Errors, user)
		},
	}
	cmd.Flags().StringVar(&opts.Cookie, "cookie", "", "Optional web session cookie header value")
	return cmd
}

func (c *CLI) newAdminUsersEnrichCommand() *cobra.Command {
	opts := struct {
		Cookie string
	}{}
	cmd := &cobra.Command{
		Use:   "enrich",
		Short: "Load the enriched admin users snapshot with billing and usage",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newWebClient(opts.Cookie)
			if err != nil {
				return err
			}
			snapshot, err := fetchAdminUsersSnapshot(client, "/api/fugue/admin/pages/users/enrich")
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, snapshot)
			}
			return renderAdminUsersSnapshot(c.stdout, snapshot)
		},
	}
	cmd.Flags().StringVar(&opts.Cookie, "cookie", "", "Optional web session cookie header value")
	return cmd
}

func (c *CLI) newAdminUsersResolveCommand() *cobra.Command {
	opts := struct {
		Cookie string
	}{}
	cmd := &cobra.Command{
		Use:   "resolve <email>",
		Short: "Resolve one email to the concrete workspace and tenant snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newWebClient(opts.Cookie)
			if err != nil {
				return err
			}
			snapshot, err := fetchAdminUsersSnapshot(client, "/api/fugue/admin/pages/users/enrich")
			if err != nil {
				return err
			}
			user, ok := findAdminUser(snapshot.Users, args[0])
			if !ok {
				return fmt.Errorf("admin user %q not found", args[0])
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"enrichmentState": snapshot.EnrichmentState,
					"errors":          snapshot.Errors,
					"user":            user,
				})
			}
			return renderAdminUserResolve(c.stdout, snapshot.Errors, user)
		},
	}
	cmd.Flags().StringVar(&opts.Cookie, "cookie", "", "Optional web session cookie header value")
	return cmd
}

func (c *CLI) newAdminUsersUsageCommand() *cobra.Command {
	opts := struct {
		Cookie string
	}{}
	cmd := &cobra.Command{
		Use:   "usage",
		Short: "Load the admin users usage-only snapshot",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newWebClient(opts.Cookie)
			if err != nil {
				return err
			}
			snapshot, err := fetchAdminUsersUsageSnapshot(client)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, snapshot)
			}
			return renderAdminUsersUsageSnapshot(c.stdout, snapshot)
		},
	}
	cmd.Flags().StringVar(&opts.Cookie, "cookie", "", "Optional web session cookie header value")
	return cmd
}

func fetchAdminUsersSnapshot(client *Client, target string) (adminUsersPageSnapshot, error) {
	response, err := client.DoRequest(http.MethodGet, target, nil, nil, "")
	if err != nil {
		return adminUsersPageSnapshot{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return adminUsersPageSnapshot{}, fmt.Errorf("request failed: status=%d body=%s", response.StatusCode, strings.TrimSpace(string(response.Payload)))
	}
	var snapshot adminUsersPageSnapshot
	if err := json.Unmarshal(response.Payload, &snapshot); err != nil {
		return adminUsersPageSnapshot{}, fmt.Errorf("decode admin users snapshot: %w", err)
	}
	return snapshot, nil
}

func fetchAdminUsersUsageSnapshot(client *Client) (adminUsersUsageSnapshot, error) {
	response, err := client.DoRequest(http.MethodGet, "/api/fugue/admin/pages/users?include_usage=1", nil, nil, "")
	if err != nil {
		return adminUsersUsageSnapshot{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return adminUsersUsageSnapshot{}, fmt.Errorf("request failed: status=%d body=%s", response.StatusCode, strings.TrimSpace(string(response.Payload)))
	}
	var snapshot adminUsersUsageSnapshot
	if err := json.Unmarshal(response.Payload, &snapshot); err != nil {
		return adminUsersUsageSnapshot{}, fmt.Errorf("decode admin users usage snapshot: %w", err)
	}
	return snapshot, nil
}

func findAdminUser(users []adminUserView, email string) (adminUserView, bool) {
	email = strings.TrimSpace(strings.ToLower(email))
	for _, user := range users {
		if strings.EqualFold(strings.TrimSpace(user.Email), email) {
			return user, true
		}
	}
	return adminUserView{}, false
}

func renderAdminUsersSnapshot(w io.Writer, snapshot adminUsersPageSnapshot) error {
	pairs := []kvPair{
		{Key: "users", Value: fmt.Sprintf("%d", len(snapshot.Users))},
		{Key: "enrichment_state", Value: firstNonEmpty(snapshot.EnrichmentState, "pending")},
		{Key: "admins", Value: fmt.Sprintf("%d", snapshot.Summary.AdminCount)},
		{Key: "blocked", Value: fmt.Sprintf("%d", snapshot.Summary.BlockedCount)},
		{Key: "deleted", Value: fmt.Sprintf("%d", snapshot.Summary.DeletedCount)},
	}
	if len(snapshot.Errors) > 0 {
		pairs = append(pairs, kvPair{Key: "errors", Value: strings.Join(snapshot.Errors, " | ")})
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeAdminUsersTable(w, snapshot.Users)
}

func renderAdminUser(w io.Writer, errors []string, user adminUserView) error {
	pairs := []kvPair{
		{Key: "email", Value: user.Email},
		{Key: "name", Value: user.Name},
		{Key: "status", Value: user.Status},
		{Key: "provider", Value: user.Provider},
		{Key: "admin", Value: fmt.Sprintf("%t", user.IsAdmin)},
		{Key: "verified", Value: fmt.Sprintf("%t", user.Verified)},
		{Key: "last_login", Value: firstNonEmpty(user.LastLoginLabel, user.LastLoginExact)},
		{Key: "service_count", Value: fmt.Sprintf("%d", user.ServiceCount)},
		{Key: "usage", Value: summarizeAdminUserUsage(user.Usage)},
		{Key: "billing_limit", Value: firstNonEmpty(user.Billing.LimitLabel, "unavailable")},
		{Key: "billing_balance", Value: firstNonEmpty(user.Billing.BalanceLabel, "unavailable")},
		{Key: "billing_monthly_estimate", Value: firstNonEmpty(user.Billing.MonthlyEstimateLabel, "unavailable")},
		{Key: "billing_status", Value: firstNonEmpty(user.Billing.StatusLabel, "unavailable")},
		{Key: "tenant_id", Value: firstNonEmpty(user.Billing.TenantID, workspaceTenantID(user.Workspace))},
		{Key: "workspace", Value: workspaceTenantName(user.Workspace)},
		{Key: "default_project", Value: workspaceDefaultProject(user.Workspace)},
		{Key: "first_app_id", Value: workspaceFirstAppID(user.Workspace)},
	}
	if user.Billing.StatusReason != "" {
		pairs = append(pairs, kvPair{Key: "billing_reason", Value: user.Billing.StatusReason})
	}
	if user.Billing.LoadError != "" {
		pairs = append(pairs, kvPair{Key: "billing_error", Value: user.Billing.LoadError})
	}
	if len(errors) > 0 {
		pairs = append(pairs, kvPair{Key: "snapshot_errors", Value: strings.Join(errors, " | ")})
	}
	return writeKeyValues(w, pairs...)
}

func renderAdminUserResolve(w io.Writer, errors []string, user adminUserView) error {
	pairs := []kvPair{
		{Key: "email", Value: user.Email},
		{Key: "tenant_id", Value: firstNonEmpty(user.Billing.TenantID, workspaceTenantID(user.Workspace))},
		{Key: "tenant_name", Value: workspaceTenantName(user.Workspace)},
		{Key: "default_project", Value: workspaceDefaultProject(user.Workspace)},
		{Key: "first_app_id", Value: workspaceFirstAppID(user.Workspace)},
		{Key: "workspace_admin_key", Value: workspaceAdminKeyLabel(user.Workspace)},
	}
	if len(errors) > 0 {
		pairs = append(pairs, kvPair{Key: "snapshot_errors", Value: strings.Join(errors, " | ")})
	}
	return writeKeyValues(w, pairs...)
}

func renderAdminUsersUsageSnapshot(w io.Writer, snapshot adminUsersUsageSnapshot) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "EMAIL\tSERVICES\tCPU\tMEMORY\tIMAGE\tDISK"); err != nil {
		return err
	}
	entries := append([]adminUserUsageSnapshotEntry(nil), snapshot.Users...)
	sort.Slice(entries, func(i, j int) bool {
		return strings.Compare(strings.ToLower(entries[i].Email), strings.ToLower(entries[j].Email)) < 0
	})
	for _, entry := range entries {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%d\t%s\t%s\t%s\t%s\n",
			entry.Email,
			entry.ServiceCount,
			entry.Usage.CPULabel,
			entry.Usage.MemoryLabel,
			entry.Usage.ImageLabel,
			entry.Usage.DiskLabel,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeAdminUsersTable(w io.Writer, users []adminUserView) error {
	sorted := append([]adminUserView(nil), users...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(strings.ToLower(sorted[i].Email), strings.ToLower(sorted[j].Email)) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "EMAIL\tNAME\tSTATUS\tADMIN\tVERIFIED\tPROVIDER\tSERVICES\tLAST LOGIN"); err != nil {
		return err
	}
	for _, user := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%t\t%t\t%s\t%d\t%s\n",
			user.Email,
			firstNonEmpty(user.Name, user.Email),
			user.Status,
			user.IsAdmin,
			user.Verified,
			user.Provider,
			user.ServiceCount,
			firstNonEmpty(user.LastLoginLabel, user.LastLoginExact),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func summarizeAdminUserUsage(usage adminUserUsageView) string {
	if usage.Loading {
		return "loading"
	}
	parts := []string{}
	for _, value := range []string{
		firstNonEmpty(usage.ServiceCountLabel, fmt.Sprintf("%d services", usage.ServiceCount)),
		usage.CPULabel,
		usage.MemoryLabel,
		usage.ImageLabel,
		usage.DiskLabel,
	} {
		value = strings.TrimSpace(value)
		if value != "" {
			parts = append(parts, value)
		}
	}
	return strings.Join(parts, " / ")
}

func workspaceTenantID(workspace *adminUserWorkspaceView) string {
	if workspace == nil {
		return ""
	}
	return strings.TrimSpace(workspace.TenantID)
}

func workspaceTenantName(workspace *adminUserWorkspaceView) string {
	if workspace == nil {
		return ""
	}
	return strings.TrimSpace(workspace.TenantName)
}

func workspaceDefaultProject(workspace *adminUserWorkspaceView) string {
	if workspace == nil {
		return ""
	}
	if name := strings.TrimSpace(workspace.DefaultProjectName); name != "" {
		return name
	}
	return strings.TrimSpace(workspace.DefaultProjectID)
}

func workspaceFirstAppID(workspace *adminUserWorkspaceView) string {
	if workspace == nil {
		return ""
	}
	return strings.TrimSpace(workspace.FirstAppID)
}

func workspaceAdminKeyLabel(workspace *adminUserWorkspaceView) string {
	if workspace == nil {
		return ""
	}
	return strings.TrimSpace(workspace.AdminKeyLabel)
}

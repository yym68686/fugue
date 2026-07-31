package cli

import (
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"fugue/internal/backupschedule"
	"fugue/internal/backupusage"
	"fugue/internal/model"

	"github.com/spf13/cobra"
)

const backupListServerPageLimit = 500

func cloneBackupListQuery(values url.Values) url.Values {
	clone := make(url.Values, len(values)+2)
	for key, entries := range values {
		clone[key] = append([]string(nil), entries...)
	}
	return clone
}

func collectBackupList[T any](
	values url.Values,
	totalLimit int,
	all bool,
	kind string,
	fetch func(url.Values) ([]T, *backupListPageInfo, error),
	itemID func(T) string,
) ([]T, error) {
	if !all && totalLimit <= 0 {
		return nil, fmt.Errorf("--limit must be greater than zero")
	}

	query := cloneBackupListQuery(values)
	query.Del("cursor")
	items := make([]T, 0)
	seenItems := make(map[string]struct{})
	seenCursors := make(map[string]struct{})
	cursor := ""
	for {
		pageLimit := backupListServerPageLimit
		if !all {
			remaining := totalLimit - len(items)
			if remaining <= 0 {
				return items, nil
			}
			if remaining < pageLimit {
				pageLimit = remaining
			}
		}
		query.Set("limit", strconv.Itoa(pageLimit))
		if cursor == "" {
			query.Del("cursor")
		} else {
			query.Set("cursor", cursor)
		}

		pageItems, pageInfo, err := fetch(query)
		if err != nil {
			return nil, err
		}
		if len(pageItems) > pageLimit {
			return nil, fmt.Errorf("backup %s list returned %d items for page limit %d", kind, len(pageItems), pageLimit)
		}
		for _, item := range pageItems {
			id := strings.TrimSpace(itemID(item))
			if id == "" {
				return nil, fmt.Errorf("backup %s list returned an item without an id", kind)
			}
			if _, duplicate := seenItems[id]; duplicate {
				return nil, fmt.Errorf("backup %s pagination repeated item %s", kind, id)
			}
			seenItems[id] = struct{}{}
			items = append(items, item)
		}
		if !all && len(items) >= totalLimit {
			return items[:totalLimit], nil
		}

		if pageInfo == nil {
			if len(pageItems) == pageLimit {
				return nil, fmt.Errorf("server omitted backup %s pagination metadata; cannot safely retrieve the remaining results", kind)
			}
			return items, nil
		}
		if pageInfo.Limit != pageLimit {
			return nil, fmt.Errorf("backup %s pagination reported page limit %d, requested %d", kind, pageInfo.Limit, pageLimit)
		}
		if !pageInfo.HasNextPage {
			return items, nil
		}
		nextCursor := strings.TrimSpace(pageInfo.NextCursor)
		if nextCursor == "" || len(pageItems) == 0 {
			return nil, fmt.Errorf("backup %s pagination reported another page without a usable cursor", kind)
		}
		if _, repeated := seenCursors[nextCursor]; repeated || nextCursor == cursor {
			return nil, fmt.Errorf("backup %s pagination repeated a cursor", kind)
		}
		seenCursors[nextCursor] = struct{}{}
		cursor = nextCursor
	}
}

func (c *CLI) newBackupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Manage backup backends, policies, runs, artifacts, and restore plans",
	}
	cmd.AddCommand(
		c.newBackupBackendCommand(),
		c.newBackupPolicyCommand(),
		c.newBackupRunCommand(),
		c.newBackupArtifactCommand(),
		c.newBackupRestoreCommand(),
		c.newBackupUsageCommand(),
	)
	return cmd
}

func (c *CLI) newAdminBackupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Manage platform backup posture",
	}
	cmd.AddCommand(
		c.newBackupBackendCommand(),
		c.newAdminBackupStatusCommand(),
		c.newAdminBackupEnableCommand(),
		c.newAdminBackupRunCommand(),
		c.newAdminBackupListCommand(),
		c.newAdminBackupShowCommand(),
	)
	return cmd
}

func (c *CLI) newBackupBackendCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backend",
		Short: "Manage backup object storage backends",
	}
	cmd.AddCommand(
		c.newBackupBackendListCommand(),
		c.newBackupBackendCreateCommand(),
		c.newBackupBackendShowCommand(),
		c.newBackupBackendDeleteCommand(),
		c.newBackupBackendTestCommand(),
		c.newBackupBackendRotateCommand(),
	)
	return cmd
}

func (c *CLI) newBackupBackendListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List backup backends",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			backends, err := client.ListBackupBackends()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"backends": backends})
			}
			return renderBackupBackends(c.stdout, backends, c.showIDs())
		},
	}
}

func (c *CLI) newBackupBackendCreateCommand() *cobra.Command {
	opts := backupBackendCreateOptions{Provider: model.DataBackendProviderCloudflareR2, Region: "auto"}
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a backup backend",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			req := backupBackendRequestMap(args[0], opts)
			backend, err := client.CreateBackupBackend(req)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"backend": backend})
			}
			return renderBackupBackend(c.stdout, backend, c.showIDs())
		},
	}
	addBackupBackendFlags(cmd, &opts, true)
	return cmd
}

func (c *CLI) newBackupBackendRotateCommand() *cobra.Command {
	opts := backupBackendCreateOptions{}
	cmd := &cobra.Command{
		Use:   "rotate <backend>",
		Short: "Rotate backup backend credentials",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			req := backupBackendRequestMap(args[0], opts)
			req["rotate_only"] = true
			backend, err := client.CreateBackupBackend(req)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"backend": backend})
			}
			return renderBackupBackend(c.stdout, backend, c.showIDs())
		},
	}
	addBackupBackendFlags(cmd, &opts, false)
	return cmd
}

func (c *CLI) newBackupBackendShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <backend>",
		Aliases: []string{"get"},
		Short:   "Show a backup backend",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			backend, err := client.GetBackupBackend(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"backend": backend})
			}
			return renderBackupBackend(c.stdout, backend, true)
		},
	}
}

func (c *CLI) newBackupBackendDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <backend>",
		Aliases: []string{"rm", "remove"},
		Short:   "Delete a backup backend",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			backend, err := client.DeleteBackupBackend(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"backend": backend})
			}
			_, _ = fmt.Fprintf(c.stdout, "deleted backup backend %s\n", backend.Name)
			return nil
		},
	}
}

func (c *CLI) newBackupBackendTestCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "test <backend>",
		Short: "Probe backup backend write/read/list/head/delete behavior",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.TestBackupBackend(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			_, _ = fmt.Fprintf(c.stdout, "status=%s backend=%s message=%s\n", resp.Status, resp.Backend.Name, resp.Message)
			return nil
		},
	}
}

type backupBackendCreateOptions struct {
	Provider        string
	Bucket          string
	Region          string
	Endpoint        string
	AccountID       string
	BaseURL         string
	Prefix          string
	AccessKeyID     string
	SecretAccessKey string
	Token           string
	FugueManaged    bool
	Billable        bool
}

func addBackupBackendFlags(cmd *cobra.Command, opts *backupBackendCreateOptions, includeStorage bool) {
	if includeStorage {
		cmd.Flags().StringVar(&opts.Provider, "provider", opts.Provider, "Provider: cloudflare-r2, backblaze-b2, s3, minio")
		cmd.Flags().StringVar(&opts.Bucket, "bucket", "", "Object storage bucket")
		cmd.Flags().StringVar(&opts.Region, "region", opts.Region, "Object storage region")
		cmd.Flags().StringVar(&opts.Endpoint, "endpoint", "", "S3-compatible endpoint")
		cmd.Flags().StringVar(&opts.AccountID, "r2-account-id", "", "Cloudflare R2 account id; fills endpoint when --endpoint is omitted")
		cmd.Flags().StringVar(&opts.BaseURL, "base-url", "", "Optional public object base URL")
		cmd.Flags().StringVar(&opts.Prefix, "prefix", "", "Object key prefix")
		cmd.Flags().BoolVar(&opts.FugueManaged, "fugue-managed", false, "Mark backend as Fugue-managed")
		cmd.Flags().BoolVar(&opts.Billable, "billable", false, "Bill stored bytes through Fugue")
	}
	cmd.Flags().StringVar(&opts.AccessKeyID, "access-key-id", "", "Object storage access key id")
	cmd.Flags().StringVar(&opts.SecretAccessKey, "secret-access-key", "", "Object storage secret access key")
	cmd.Flags().StringVar(&opts.Token, "session-token", "", "Optional object storage session token")
}

func backupBackendRequestMap(name string, opts backupBackendCreateOptions) map[string]any {
	endpoint := strings.TrimSpace(opts.Endpoint)
	if endpoint == "" && strings.EqualFold(opts.Provider, model.DataBackendProviderCloudflareR2) && strings.TrimSpace(opts.AccountID) != "" {
		endpoint = "https://" + strings.TrimSpace(opts.AccountID) + ".r2.cloudflarestorage.com"
	}
	req := map[string]any{
		"name":     strings.TrimSpace(name),
		"provider": strings.TrimSpace(opts.Provider),
		"bucket":   strings.TrimSpace(opts.Bucket),
		"region":   strings.TrimSpace(opts.Region),
		"endpoint": endpoint,
		"base_url": strings.TrimSpace(opts.BaseURL),
		"prefix":   strings.TrimSpace(opts.Prefix),
		"credentials": model.DataBackendCredentials{
			AccessKeyID:     strings.TrimSpace(opts.AccessKeyID),
			SecretAccessKey: strings.TrimSpace(opts.SecretAccessKey),
			Token:           strings.TrimSpace(opts.Token),
		},
	}
	if opts.FugueManaged {
		req["fugue_managed"] = true
	}
	if opts.Billable {
		req["billable"] = true
	}
	return req
}

func (c *CLI) newBackupPolicyCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "policy", Short: "Manage backup policies"}
	cmd.AddCommand(
		c.newBackupPolicyListCommand(),
		c.newBackupPolicyShowCommand(),
		c.newBackupPolicyEnableCommand(),
		c.newBackupPolicyDisableCommand(),
	)
	return cmd
}

func (c *CLI) newBackupPolicyListCommand() *cobra.Command {
	opts := struct {
		TargetType      string
		AppID           string
		ProjectID       string
		IncludeDisabled bool
	}{}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List backup policies",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			values := url.Values{}
			addNonEmptyQuery(values, "target_type", opts.TargetType)
			addNonEmptyQuery(values, "app_id", opts.AppID)
			addNonEmptyQuery(values, "project_id", opts.ProjectID)
			if opts.IncludeDisabled {
				values.Set("include_disabled", "true")
			}
			policies, err := client.ListBackupPolicies(values)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policies": policies})
			}
			return renderBackupPolicies(c.stdout, policies, c.showIDs())
		},
	}
	cmd.Flags().StringVar(&opts.TargetType, "target", "", "Target type filter")
	cmd.Flags().StringVar(&opts.AppID, "app-id", "", "App id filter")
	cmd.Flags().StringVar(&opts.ProjectID, "project-id", "", "Project id filter")
	cmd.Flags().BoolVar(&opts.IncludeDisabled, "include-disabled", false, "Include disabled policies")
	return cmd
}

func (c *CLI) newBackupPolicyShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <policy>",
		Short: "Show a backup policy",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			policy, err := client.GetBackupPolicy(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policy": policy})
			}
			return renderBackupPolicy(c.stdout, policy, true)
		},
	}
}

func (c *CLI) newBackupPolicyEnableCommand() *cobra.Command {
	opts := backupPolicyOptions{TargetType: model.BackupTargetControlPlaneDatabase, Schedule: model.BackupDefaultSchedule, RetainCount: model.BackupDefaultRetainCount}
	cmd := &cobra.Command{
		Use:   "enable",
		Short: "Enable or update a backup policy",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			req, err := backupPolicyRequestMap(opts, true)
			if err != nil {
				return err
			}
			policy, err := client.UpsertBackupPolicy(req)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policy": policy})
			}
			return renderBackupPolicy(c.stdout, policy, c.showIDs())
		},
	}
	addBackupPolicyFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newBackupPolicyDisableCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "disable <policy>",
		Short: "Disable a backup policy",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			policy, err := client.PatchBackupPolicy(args[0], map[string]any{"enabled": false})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policy": policy})
			}
			return renderBackupPolicy(c.stdout, policy, c.showIDs())
		},
	}
}

type backupPolicyOptions struct {
	Name        string
	TargetType  string
	BackendID   string
	Schedule    string
	RetainCount int
	Version     string
	AppID       string
	ProjectID   string
	WorkspaceID string
}

func addBackupPolicyFlags(cmd *cobra.Command, opts *backupPolicyOptions) {
	cmd.Flags().StringVar(&opts.Name, "name", "", "Policy name")
	cmd.Flags().StringVar(&opts.TargetType, "target", opts.TargetType, "Target: control-plane-db, app-database, persistent-storage, data-workspace, registry")
	cmd.Flags().StringVar(&opts.BackendID, "backend", "", "Backup backend id/name")
	cmd.Flags().StringVar(&opts.Schedule, "schedule", opts.Schedule, "Numeric five-field UTC cron schedule (or @hourly)")
	cmd.Flags().IntVar(&opts.RetainCount, "retain-count", opts.RetainCount, "Number of successful artifacts to retain")
	cmd.Flags().IntVar(&opts.RetainCount, "retain", opts.RetainCount, "Alias for --retain-count")
	cmd.Flags().StringVar(&opts.Version, "version", "", "Version label for manual/user backups")
	cmd.Flags().StringVar(&opts.AppID, "app-id", "", "App id target")
	cmd.Flags().StringVar(&opts.ProjectID, "project-id", "", "Project id target")
	cmd.Flags().StringVar(&opts.WorkspaceID, "workspace-id", "", "Data workspace id target")
}

func backupPolicyRequestMap(opts backupPolicyOptions, enabled bool) (map[string]any, error) {
	if err := backupschedule.Validate(opts.Schedule); err != nil {
		return nil, fmt.Errorf("invalid backup schedule: %w", err)
	}
	target := map[string]any{
		"type":         strings.TrimSpace(opts.TargetType),
		"project_id":   strings.TrimSpace(opts.ProjectID),
		"app_id":       strings.TrimSpace(opts.AppID),
		"workspace_id": strings.TrimSpace(opts.WorkspaceID),
	}
	req := map[string]any{
		"name":         strings.TrimSpace(opts.Name),
		"target":       target,
		"backend_id":   strings.TrimSpace(opts.BackendID),
		"enabled":      enabled,
		"schedule":     strings.TrimSpace(opts.Schedule),
		"retain_count": opts.RetainCount,
		"version":      strings.TrimSpace(opts.Version),
	}
	return req, nil
}

func (c *CLI) newBackupRunCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "run", Short: "Create and inspect backup runs"}
	cmd.AddCommand(
		c.newBackupRunListCommand(),
		c.newBackupRunStartCommand(),
		c.newBackupRunShowCommand(),
	)
	return cmd
}

func (c *CLI) newBackupRunListCommand() *cobra.Command {
	opts := struct {
		PolicyID   string
		TargetType string
		Status     string
		Limit      int
		All        bool
	}{Limit: 20}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List backup runs",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.All && cmd.Flags().Changed("limit") {
				return fmt.Errorf("--all and --limit cannot be used together")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			values := url.Values{}
			addNonEmptyQuery(values, "policy_id", opts.PolicyID)
			addNonEmptyQuery(values, "target_type", opts.TargetType)
			addNonEmptyQuery(values, "status", opts.Status)
			runs, err := collectBackupList(values, opts.Limit, opts.All, "run", func(query url.Values) ([]model.BackupRun, *backupListPageInfo, error) {
				page, listErr := client.ListBackupRunsPage(query)
				return page.Runs, page.PageInfo, listErr
			}, func(run model.BackupRun) string {
				return run.ID
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"runs": runs})
			}
			return renderBackupRuns(c.stdout, runs, c.showIDs())
		},
	}
	cmd.Flags().StringVar(&opts.PolicyID, "policy", "", "Policy id filter")
	cmd.Flags().StringVar(&opts.TargetType, "target", "", "Target type filter")
	cmd.Flags().StringVar(&opts.Status, "status", "", "Status filter")
	cmd.Flags().IntVar(&opts.Limit, "limit", opts.Limit, "Maximum runs")
	cmd.Flags().BoolVar(&opts.All, "all", false, "List all runs")
	return cmd
}

func (c *CLI) newBackupRunStartCommand() *cobra.Command {
	opts := backupRunOptions{TargetType: model.BackupTargetControlPlaneDatabase}
	cmd := &cobra.Command{
		Use:     "start",
		Aliases: []string{"create", "run"},
		Short:   "Start a backup run",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := c.createBackupRunAndMaybeWait(client, backupRunRequestMap(opts), opts.Wait, client.CreateBackupRun)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			return renderBackupRun(c.stdout, resp.Run, c.showIDs())
		},
	}
	addBackupRunFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newBackupRunShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <run>",
		Short: "Show a backup run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := client.GetBackupRun(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			if err := renderBackupRun(c.stdout, resp.Run, true); err != nil {
				return err
			}
			return renderBackupArtifacts(c.stdout, resp.Artifacts, c.showIDs())
		},
	}
}

type backupRunOptions struct {
	PolicyID   string
	TargetType string
	BackendID  string
	Version    string
	Wait       bool
}

func addBackupRunFlags(cmd *cobra.Command, opts *backupRunOptions) {
	cmd.Flags().StringVar(&opts.PolicyID, "policy", "", "Policy id/name")
	cmd.Flags().StringVar(&opts.TargetType, "target", opts.TargetType, "Target type")
	cmd.Flags().StringVar(&opts.BackendID, "backend", "", "Backend id/name override")
	cmd.Flags().StringVar(&opts.Version, "version", "", "Version label")
	cmd.Flags().BoolVar(&opts.Wait, "wait", false, "Wait for the run to finish")
}

func backupRunRequestMap(opts backupRunOptions) map[string]any {
	req := map[string]any{
		"policy_id":  strings.TrimSpace(opts.PolicyID),
		"backend_id": strings.TrimSpace(opts.BackendID),
		"version":    strings.TrimSpace(opts.Version),
		"wait":       opts.Wait,
	}
	if strings.TrimSpace(opts.PolicyID) == "" {
		req["target"] = map[string]any{"type": strings.TrimSpace(opts.TargetType)}
	}
	return req
}

func (c *CLI) createBackupRunAndMaybeWait(client *Client, req map[string]any, wait bool, create func(map[string]any) (backupRunEnvelope, error)) (backupRunEnvelope, error) {
	if wait {
		req = cloneBackupRunRequest(req)
		req["wait"] = false
	}
	resp, err := create(req)
	if err != nil || !wait {
		return resp, err
	}
	return c.waitForBackupRun(client, resp.Run.ID)
}

func cloneBackupRunRequest(req map[string]any) map[string]any {
	clone := make(map[string]any, len(req))
	for key, value := range req {
		clone[key] = value
	}
	return clone
}

func (c *CLI) waitForBackupRun(client *Client, runID string) (backupRunEnvelope, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return backupRunEnvelope{}, fmt.Errorf("backup run id is empty")
	}
	deadline := time.Now().Add(45 * time.Minute)
	for {
		resp, err := client.GetBackupRun(runID)
		if err != nil {
			return resp, err
		}
		switch resp.Run.Status {
		case model.BackupRunStatusSucceeded, model.BackupRunStatusFailed, model.BackupRunStatusCanceled, model.BackupRunStatusBlocked:
			return resp, nil
		}
		if time.Now().After(deadline) {
			return resp, fmt.Errorf("timed out waiting for backup run %s to finish", runID)
		}
		time.Sleep(2 * time.Second)
	}
}

func (c *CLI) newBackupArtifactCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "artifact", Short: "Inspect backup artifacts"}
	cmd.AddCommand(
		c.newBackupArtifactListCommand(),
		c.newBackupArtifactShowCommand(),
		c.newBackupArtifactDeleteCommand(),
	)
	return cmd
}

func (c *CLI) newBackupArtifactListCommand() *cobra.Command {
	opts := struct {
		PolicyID       string
		RunID          string
		TargetType     string
		IncludeDeleted bool
		Limit          int
		All            bool
	}{Limit: 20}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List backup artifacts",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.All && cmd.Flags().Changed("limit") {
				return fmt.Errorf("--all and --limit cannot be used together")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			values := url.Values{}
			addNonEmptyQuery(values, "policy_id", opts.PolicyID)
			addNonEmptyQuery(values, "run_id", opts.RunID)
			addNonEmptyQuery(values, "target_type", opts.TargetType)
			if opts.IncludeDeleted {
				values.Set("include_deleted", "true")
			}
			artifacts, err := collectBackupList(values, opts.Limit, opts.All, "artifact", func(query url.Values) ([]model.BackupArtifact, *backupListPageInfo, error) {
				page, listErr := client.ListBackupArtifactsPage(query)
				return page.Artifacts, page.PageInfo, listErr
			}, func(artifact model.BackupArtifact) string {
				return artifact.ID
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"artifacts": artifacts})
			}
			return renderBackupArtifacts(c.stdout, artifacts, c.showIDs())
		},
	}
	cmd.Flags().StringVar(&opts.PolicyID, "policy", "", "Policy id filter")
	cmd.Flags().StringVar(&opts.RunID, "run", "", "Run id filter")
	cmd.Flags().StringVar(&opts.TargetType, "target", "", "Target type filter")
	cmd.Flags().BoolVar(&opts.IncludeDeleted, "include-deleted", false, "Include deleted artifacts")
	cmd.Flags().IntVar(&opts.Limit, "limit", opts.Limit, "Maximum artifacts")
	cmd.Flags().BoolVar(&opts.All, "all", false, "List all artifacts")
	return cmd
}

func (c *CLI) newBackupArtifactShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <artifact>",
		Short: "Show a backup artifact",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			artifact, err := client.GetBackupArtifact(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"artifact": artifact})
			}
			return renderBackupArtifact(c.stdout, artifact, true)
		},
	}
}

func (c *CLI) newBackupArtifactDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <artifact>",
		Short: "Mark a backup artifact deleted",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			artifact, err := client.DeleteBackupArtifact(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"artifact": artifact})
			}
			_, _ = fmt.Fprintf(c.stdout, "deleted backup artifact %s\n", artifact.ID)
			return nil
		},
	}
}

func (c *CLI) newBackupRestoreCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "restore", Short: "Create and inspect backup restore plans"}
	cmd.AddCommand(
		c.newBackupRestorePlanCommand(),
		c.newBackupRestoreRunCommand(),
		c.newBackupRestoreListCommand(),
		c.newBackupRestoreOfflineControlPlaneCommand(),
	)
	return cmd
}

func (c *CLI) newBackupRestorePlanCommand() *cobra.Command {
	mode := model.BackupRestoreModePlanOnly
	cmd := &cobra.Command{
		Use:   "plan <artifact>",
		Short: "Create a restore plan from a backup artifact",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			plan, err := client.CreateBackupRestorePlan(map[string]any{"artifact_id": args[0], "mode": mode})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"plan": plan})
			}
			return renderBackupRestorePlan(c.stdout, plan, true)
		},
	}
	cmd.Flags().StringVar(&mode, "mode", mode, "Restore mode: plan-only, clone, replace, offline-control-plane")
	return cmd
}

func (c *CLI) newBackupRestoreRunCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "run <plan>",
		Short: "Create a restore run from a plan",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			run, err := client.CreateBackupRestoreRun(map[string]any{"plan_id": args[0]})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"run": run})
			}
			return renderBackupRestoreRun(c.stdout, run, true)
		},
	}
}

func (c *CLI) newBackupRestoreListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List restore plans",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			plans, err := client.ListBackupRestorePlans()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"plans": plans})
			}
			return renderBackupRestorePlans(c.stdout, plans, c.showIDs())
		},
	}
}

func (c *CLI) newBackupRestoreOfflineControlPlaneCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "control-plane <artifact>",
		Short: "Create an offline control-plane restore plan",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			plan, err := client.CreateBackupRestorePlan(map[string]any{"artifact_id": args[0], "mode": model.BackupRestoreModeOfflineControlPlane})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"plan": plan})
			}
			if err := renderBackupRestorePlan(c.stdout, plan, true); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(c.stdout, "offline restore requires stopping control-plane writers, downloading the artifact, running pg_restore, then verifying store invariants")
			return nil
		},
	}
}

func (c *CLI) newBackupUsageCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "usage",
		Short: "Show billable and physical backup storage usage",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			usage, err := client.GetBackupUsage()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"usage": usage})
			}
			return renderBackupUsage(c.stdout, usage)
		},
	}
}

func (c *CLI) newAdminBackupStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show platform backup status",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			status, err := client.GetAdminBackupStatus()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, status)
			}
			return renderAdminBackupStatus(c.stdout, status, c.showIDs())
		},
	}
}

func (c *CLI) newAdminBackupEnableCommand() *cobra.Command {
	opts := backupPolicyOptions{TargetType: model.BackupTargetControlPlaneDatabase, Schedule: model.BackupDefaultSchedule, RetainCount: model.BackupDefaultRetainCount}
	cmd := &cobra.Command{
		Use:   "enable",
		Short: "Enable or update the default control-plane backup policy",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			req, err := backupPolicyRequestMap(opts, true)
			if err != nil {
				return err
			}
			policy, err := client.UpsertBackupPolicy(req)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policy": policy})
			}
			return renderBackupPolicy(c.stdout, policy, c.showIDs())
		},
	}
	addBackupPolicyFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newAdminBackupRunCommand() *cobra.Command {
	opts := backupRunOptions{TargetType: model.BackupTargetControlPlaneDatabase}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Start a platform backup run",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			resp, err := c.createBackupRunAndMaybeWait(client, backupRunRequestMap(opts), opts.Wait, client.CreateBackupRun)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			return renderBackupRun(c.stdout, resp.Run, c.showIDs())
		},
	}
	addBackupRunFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newAdminBackupListCommand() *cobra.Command {
	return c.newBackupRunListCommand()
}

func (c *CLI) newAdminBackupShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <id>",
		Short: "Show a backup run, policy, or artifact",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			if run, err := client.GetBackupRun(args[0]); err == nil && run.Run.ID != "" {
				if c.wantsJSON() {
					return writeJSON(c.stdout, run)
				}
				return renderBackupRun(c.stdout, run.Run, true)
			}
			if policy, err := client.GetBackupPolicy(args[0]); err == nil && policy.ID != "" {
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"policy": policy})
				}
				return renderBackupPolicy(c.stdout, policy, true)
			}
			artifact, err := client.GetBackupArtifact(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"artifact": artifact})
			}
			return renderBackupArtifact(c.stdout, artifact, true)
		},
	}
}

func (c *CLI) newAppBackupCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "backup", Short: "Manage app backups"}
	cmd.AddCommand(
		c.newAppBackupStatusCommand(),
		c.newAppBackupEnableCommand(),
		c.newAppBackupRunCommand(),
		c.newAppBackupListCommand(),
		c.newAppBackupShowCommand(),
	)
	return cmd
}

func (c *CLI) newAppBackupStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status <app>",
		Short: "Show app backup status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			status, err := client.GetAppBackupStatus(app.ID)
			if err != nil {
				return err
			}
			if c.shouldRedact() {
				// Keep the CLI safe even when it talks to an older server that
				// returned raw structured or diagnostic backup-status fields.
				status = sanitizeAppBackupStatusForOutput(status)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, status)
			}
			return renderAppBackupStatus(c.stdout, status, c.showIDs())
		},
	}
}

func (c *CLI) newAppBackupEnableCommand() *cobra.Command {
	opts := backupPolicyOptions{TargetType: model.BackupTargetAppDatabase, Schedule: model.BackupDefaultSchedule, RetainCount: model.BackupDefaultRetainCount}
	cmd := &cobra.Command{
		Use:   "enable <app>",
		Short: "Enable an app backup policy",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			req, err := backupPolicyRequestMap(opts, true)
			if err != nil {
				return err
			}
			policy, err := client.CreateAppBackupPolicy(app.ID, req)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policy": policy})
			}
			return renderBackupPolicy(c.stdout, policy, c.showIDs())
		},
	}
	addBackupPolicyFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newAppBackupRunCommand() *cobra.Command {
	opts := backupRunOptions{TargetType: model.BackupTargetAppDatabase}
	cmd := &cobra.Command{
		Use:   "run <app>",
		Short: "Start an app backup run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			resp, err := c.createBackupRunAndMaybeWait(client, backupRunRequestMap(opts), opts.Wait, func(req map[string]any) (backupRunEnvelope, error) {
				return client.CreateAppBackupRun(app.ID, req)
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, resp)
			}
			return renderBackupRun(c.stdout, resp.Run, c.showIDs())
		},
	}
	addBackupRunFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newAppBackupListCommand() *cobra.Command {
	opts := struct {
		Limit int
		All   bool
	}{Limit: 100}
	cmd := &cobra.Command{
		Use:   "ls <app>",
		Short: "List backup runs for an app",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.All && cmd.Flags().Changed("limit") {
				return fmt.Errorf("--all and --limit cannot be used together")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			values := url.Values{}
			values.Set("app_id", app.ID)
			runs, err := collectBackupList(values, opts.Limit, opts.All, "run", func(query url.Values) ([]model.BackupRun, *backupListPageInfo, error) {
				page, listErr := client.ListBackupRunsPage(query)
				return page.Runs, page.PageInfo, listErr
			}, func(run model.BackupRun) string {
				return run.ID
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"runs": runs})
			}
			return renderBackupRuns(c.stdout, runs, c.showIDs())
		},
	}
	cmd.Flags().IntVar(&opts.Limit, "limit", opts.Limit, "Maximum runs")
	cmd.Flags().BoolVar(&opts.All, "all", false, "List all runs")
	return cmd
}

func (c *CLI) newAppBackupShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <app> <backup-run-or-artifact>",
		Short: "Show an app backup run or artifact",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			if run, err := client.GetBackupRun(args[1]); err == nil && run.Run.ID != "" {
				if run.Run.AppID != app.ID && run.Run.Target.AppID != app.ID {
					return fmt.Errorf("backup run %s does not belong to app %s", run.Run.ID, app.Name)
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, run)
				}
				return renderBackupRun(c.stdout, run.Run, true)
			}
			artifact, err := client.GetBackupArtifact(args[1])
			if err != nil {
				return err
			}
			if artifact.AppID != app.ID && artifact.Target.AppID != app.ID {
				return fmt.Errorf("backup artifact %s does not belong to app %s", artifact.ID, app.Name)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"artifact": artifact})
			}
			return renderBackupArtifact(c.stdout, artifact, true)
		},
	}
}

func (c *CLI) newAppRestoreCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "restore", Short: "Plan, verify, and record app backup restores"}
	cmd.AddCommand(
		c.newAppRestorePlanCommand(),
		c.newAppRestoreVerifyCommand(),
		c.newAppRestoreRunCommand(),
	)
	return cmd
}

func (c *CLI) newAppRestorePlanCommand() *cobra.Command {
	opts := struct {
		ArtifactID string
		Mode       string
		Clone      bool
		Replace    bool
	}{Mode: model.BackupRestoreModePlanOnly}
	cmd := &cobra.Command{
		Use:   "plan <app>",
		Short: "Create an app restore plan from a backup artifact",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			artifact, err := client.GetBackupArtifact(opts.ArtifactID)
			if err != nil {
				return err
			}
			if !backupArtifactBelongsToApp(artifact, app.ID) {
				return fmt.Errorf("backup artifact %s does not belong to app %s", artifact.ID, app.Name)
			}
			mode := appRestoreMode(opts.Mode, opts.Clone, opts.Replace)
			plan, err := client.CreateBackupRestorePlan(map[string]any{
				"artifact_id": artifact.ID,
				"mode":        mode,
				"target":      artifact.Target,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"plan": plan})
			}
			return renderBackupRestorePlan(c.stdout, plan, true)
		},
	}
	cmd.Flags().StringVar(&opts.ArtifactID, "from", "", "Backup artifact id")
	cmd.Flags().StringVar(&opts.Mode, "mode", opts.Mode, "Restore mode: plan-only, clone, replace")
	cmd.Flags().BoolVar(&opts.Clone, "clone", false, "Plan a clone restore")
	cmd.Flags().BoolVar(&opts.Replace, "replace", false, "Plan a destructive replace restore")
	_ = cmd.MarkFlagRequired("from")
	return cmd
}

func (c *CLI) newAppRestoreVerifyCommand() *cobra.Command {
	opts := struct {
		ArtifactID string
		PlanID     string
	}{}
	cmd := &cobra.Command{
		Use:   "verify <app>",
		Short: "Verify an app restore plan or source artifact belongs to the app",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			switch {
			case strings.TrimSpace(opts.ArtifactID) != "":
				artifact, err := client.GetBackupArtifact(opts.ArtifactID)
				if err != nil {
					return err
				}
				if !backupArtifactBelongsToApp(artifact, app.ID) {
					return fmt.Errorf("backup artifact %s does not belong to app %s", artifact.ID, app.Name)
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"artifact": artifact, "verified": true})
				}
				_, _ = fmt.Fprintf(c.stdout, "verified artifact=%s app=%s target=%s checksum=%s\n", artifact.ID, app.Name, artifact.Target.Type, blankDash(artifact.SHA256))
				return nil
			case strings.TrimSpace(opts.PlanID) != "":
				plan, err := findBackupRestorePlan(client, opts.PlanID)
				if err != nil {
					return err
				}
				if plan.AppID != app.ID && plan.Target.AppID != app.ID {
					return fmt.Errorf("restore plan %s does not belong to app %s", plan.ID, app.Name)
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"plan": plan, "verified": true})
				}
				_, _ = fmt.Fprintf(c.stdout, "verified restore_plan=%s app=%s mode=%s status=%s\n", plan.ID, app.Name, plan.Mode, plan.Status)
				return nil
			default:
				return fmt.Errorf("one of --from or --plan is required")
			}
		},
	}
	cmd.Flags().StringVar(&opts.ArtifactID, "from", "", "Backup artifact id")
	cmd.Flags().StringVar(&opts.PlanID, "plan", "", "Restore plan id")
	return cmd
}

func (c *CLI) newAppRestoreRunCommand() *cobra.Command {
	opts := struct {
		PlanID  string
		Mode    string
		Clone   bool
		Replace bool
		Confirm bool
	}{Mode: ""}
	cmd := &cobra.Command{
		Use:   "run <app>",
		Short: "Record an app restore run from a restore plan",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			plan, err := findBackupRestorePlan(client, opts.PlanID)
			if err != nil {
				return err
			}
			if plan.AppID != app.ID && plan.Target.AppID != app.ID {
				return fmt.Errorf("restore plan %s does not belong to app %s", plan.ID, app.Name)
			}
			mode := appRestoreMode(firstNonEmptyTrimmed(opts.Mode, plan.Mode), opts.Clone, opts.Replace)
			if mode == model.BackupRestoreModeReplace && !opts.Confirm {
				return fmt.Errorf("--confirm-destructive-restore is required for replace mode")
			}
			run, err := client.CreateBackupRestoreRun(map[string]any{"plan_id": plan.ID, "mode": mode})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"run": run})
			}
			return renderBackupRestoreRun(c.stdout, run, true)
		},
	}
	cmd.Flags().StringVar(&opts.PlanID, "plan", "", "Restore plan id")
	cmd.Flags().StringVar(&opts.Mode, "mode", "", "Restore mode override: clone or replace")
	cmd.Flags().BoolVar(&opts.Clone, "clone", false, "Run as clone restore")
	cmd.Flags().BoolVar(&opts.Replace, "replace", false, "Run as destructive replace restore")
	cmd.Flags().BoolVar(&opts.Confirm, "confirm-destructive-restore", false, "Confirm destructive replacement")
	_ = cmd.MarkFlagRequired("plan")
	return cmd
}

func appRestoreMode(raw string, clone bool, replace bool) string {
	switch {
	case replace:
		return model.BackupRestoreModeReplace
	case clone:
		return model.BackupRestoreModeClone
	default:
		return model.NormalizeBackupRestoreMode(raw)
	}
}

func backupArtifactBelongsToApp(artifact model.BackupArtifact, appID string) bool {
	return artifact.AppID == appID || artifact.Target.AppID == appID
}

func findBackupRestorePlan(client *Client, id string) (model.BackupRestorePlan, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.BackupRestorePlan{}, fmt.Errorf("restore plan id is required")
	}
	plans, err := client.ListBackupRestorePlans()
	if err != nil {
		return model.BackupRestorePlan{}, err
	}
	for _, plan := range plans {
		if plan.ID == id {
			return plan, nil
		}
	}
	return model.BackupRestorePlan{}, fmt.Errorf("restore plan %s not found", id)
}

func addNonEmptyQuery(values url.Values, key, value string) {
	if strings.TrimSpace(value) != "" {
		values.Set(key, strings.TrimSpace(value))
	}
}

func renderBackupBackends(w io.Writer, backends []model.BackupBackend, showIDs bool) error {
	if len(backends) == 0 {
		_, _ = fmt.Fprintln(w, "No backup backends.")
		return nil
	}
	for _, backend := range backends {
		name := backend.Name
		if showIDs {
			name += " (" + backend.ID + ")"
		}
		_, _ = fmt.Fprintf(w, "%s\tprovider=%s\tbucket=%s\tstatus=%s\tbillable=%t\n", name, backend.Provider, blankDash(backend.Bucket), backend.Status, backend.Billable)
	}
	return nil
}

func renderBackupBackend(w io.Writer, backend model.BackupBackend, showID bool) error {
	_, _ = fmt.Fprintf(w, "backend: %s\n", backend.Name)
	if showID {
		_, _ = fmt.Fprintf(w, "id: %s\n", backend.ID)
	}
	_, _ = fmt.Fprintf(w, "provider: %s\nbucket: %s\nregion: %s\nendpoint: %s\nprefix: %s\nstatus: %s\nbillable: %t\n",
		backend.Provider, blankDash(backend.Bucket), blankDash(backend.Region), blankDash(backend.Endpoint), blankDash(backend.Prefix), backend.Status, backend.Billable)
	if backend.LastTestResult != "" {
		_, _ = fmt.Fprintf(w, "last_test: %s %s\n", backend.LastTestResult, blankDash(backend.ErrorMessage))
	}
	return nil
}

func renderBackupPolicies(w io.Writer, policies []model.BackupPolicy, showIDs bool) error {
	if len(policies) == 0 {
		_, _ = fmt.Fprintln(w, "No backup policies.")
		return nil
	}
	for _, policy := range policies {
		name := policy.Name
		if showIDs {
			name += " (" + policy.ID + ")"
		}
		_, _ = fmt.Fprintf(w, "%s\ttarget=%s\tstatus=%s\tschedule=%s\tretain=%d\tbackend=%s\n", name, policy.Target.Type, policy.Status, blankDash(policy.Schedule), policy.RetainCount, blankDash(policy.BackendID))
	}
	return nil
}

func renderBackupPolicy(w io.Writer, policy model.BackupPolicy, showID bool) error {
	_, _ = fmt.Fprintf(w, "policy: %s\n", policy.Name)
	if showID {
		_, _ = fmt.Fprintf(w, "id: %s\n", policy.ID)
	}
	_, _ = fmt.Fprintf(w, "target: %s\nstatus: %s\nschedule: %s\nretain_count: %d\nbackend: %s\nnext_run_at: %s\nlast_success: %s\n",
		policy.Target.Type, policy.Status, blankDash(policy.Schedule), policy.RetainCount, blankDash(policy.BackendID), formatBackupTime(policy.NextRunAt), formatBackupTime(policy.LastSuccessfulAt))
	if policy.DisabledReason != "" {
		_, _ = fmt.Fprintf(w, "disabled_reason: %s\n", policy.DisabledReason)
	}
	return nil
}

func renderBackupRuns(w io.Writer, runs []model.BackupRun, showIDs bool) error {
	if len(runs) == 0 {
		_, _ = fmt.Fprintln(w, "No backup runs.")
		return nil
	}
	for _, run := range runs {
		id := run.ID
		if !showIDs && len(id) > 18 {
			id = id[:18]
		}
		_, _ = fmt.Fprintf(w, "%s\ttarget=%s\tstatus=%s\ttrigger=%s\tbytes=%d\tcreated=%s\n", id, run.Target.Type, run.Status, run.Trigger, run.BytesWritten, run.CreatedAt.Format(time.RFC3339))
	}
	return nil
}

func renderBackupRun(w io.Writer, run model.BackupRun, showID bool) error {
	_, _ = fmt.Fprintf(w, "run: %s\n", run.ID)
	_, _ = fmt.Fprintf(w, "target: %s\nstatus: %s\ntrigger: %s\nbytes_written: %d\nartifacts: %d\ncreated_at: %s\nfinished_at: %s\n",
		run.Target.Type, run.Status, run.Trigger, run.BytesWritten, run.ArtifactCount, run.CreatedAt.Format(time.RFC3339), formatBackupTime(run.FinishedAt))
	if run.ErrorMessage != "" {
		_, _ = fmt.Fprintf(w, "error: %s %s\n", run.ErrorCode, run.ErrorMessage)
	}
	return nil
}

func renderBackupArtifacts(w io.Writer, artifacts []model.BackupArtifact, showIDs bool) error {
	if len(artifacts) == 0 {
		_, _ = fmt.Fprintln(w, "No backup artifacts.")
		return nil
	}
	for _, artifact := range artifacts {
		id := artifact.ID
		if !showIDs && len(id) > 18 {
			id = id[:18]
		}
		_, _ = fmt.Fprintf(w, "%s\tkind=%s\tstatus=%s\tbytes=%d\tversion=%s\tcreated=%s\n", id, artifact.Kind, artifact.Status, artifact.SizeBytes, blankDash(artifact.Version), artifact.CreatedAt.Format(time.RFC3339))
	}
	return nil
}

func renderBackupArtifact(w io.Writer, artifact model.BackupArtifact, showID bool) error {
	_, _ = fmt.Fprintf(w, "artifact: %s\n", artifact.ID)
	_, _ = fmt.Fprintf(w, "kind: %s\nstatus: %s\nversion: %s\nbytes: %d\nsha256: %s\nobject_key: %s\nmanifest_key: %s\nbillable: %t\n",
		artifact.Kind, artifact.Status, blankDash(artifact.Version), artifact.SizeBytes, blankDash(artifact.SHA256), blankDash(artifact.ObjectKey), blankDash(artifact.ManifestObjectKey), artifact.Billable)
	return nil
}

func renderBackupRestorePlans(w io.Writer, plans []model.BackupRestorePlan, showIDs bool) error {
	if len(plans) == 0 {
		_, _ = fmt.Fprintln(w, "No restore plans.")
		return nil
	}
	for _, plan := range plans {
		id := plan.ID
		if !showIDs && len(id) > 18 {
			id = id[:18]
		}
		_, _ = fmt.Fprintf(w, "%s\tartifact=%s\tmode=%s\tstatus=%s\tcreated=%s\n", id, plan.ArtifactID, plan.Mode, plan.Status, plan.CreatedAt.Format(time.RFC3339))
	}
	return nil
}

func renderBackupRestorePlan(w io.Writer, plan model.BackupRestorePlan, showID bool) error {
	_, _ = fmt.Fprintf(w, "restore_plan: %s\nartifact: %s\nmode: %s\nstatus: %s\n", plan.ID, plan.ArtifactID, plan.Mode, plan.Status)
	for _, phase := range plan.Phases {
		_, _ = fmt.Fprintf(w, "- %s: %s %s\n", phase.Name, phase.Status, phase.Message)
	}
	return nil
}

func renderBackupRestoreRun(w io.Writer, run model.BackupRestoreRun, showID bool) error {
	_, _ = fmt.Fprintf(w, "restore_run: %s\nplan: %s\nmode: %s\nstatus: %s\n", run.ID, run.PlanID, run.Mode, run.Status)
	return nil
}

func renderBackupUsage(w io.Writer, usage backupusage.Usage) error {
	_, _ = fmt.Fprintf(w, "billable_bytes: %d\nprovider: %s\nmarkup_percent: %d\neffective_multiplier: %.2f\nprice_code: %s\n",
		usage.BillableBytes, usage.Provider, usage.MarkupPercent, usage.EffectiveMultiplier, blankDash(usage.CloudflareR2PriceCode))
	physicalBytes := "unavailable"
	if usage.PhysicalBytes != nil {
		physicalBytes = strconv.FormatInt(*usage.PhysicalBytes, 10)
	}
	physicalObjects := "unavailable"
	if usage.PhysicalObjectCount != nil {
		physicalObjects = strconv.Itoa(*usage.PhysicalObjectCount)
	}
	_, _ = fmt.Fprintf(w, "physical_bytes: %s\nphysical_object_count: %s\n", physicalBytes, physicalObjects)
	if reconciliation := usage.Reconciliation; reconciliation != nil {
		_, _ = fmt.Fprintf(w, "reconciliation_status: %s\nreconciliation_backends: %d/%d\nreferenced_bytes: %d\npending_deletion_objects: %d\nunreferenced_bytes: %d\nprovisional_objects: %d\norphaned_bytes: %d\norphaned_objects: %d\nmissing_active_objects: %d\noverdue_deletion_objects: %d\nlingering_deleted_objects: %d\nduplicate_references: %d\ninvalid_references: %d\nsize_mismatches: %d\nunresolved_backends: %d\nreconciled_at: %s\n",
			reconciliation.Status,
			reconciliation.MeasuredBackendCount,
			reconciliation.BackendCount,
			reconciliation.ReferencedBytes,
			reconciliation.PendingDeletionObjectCount,
			reconciliation.UnreferencedBytes,
			reconciliation.ProvisionalObjectCount,
			reconciliation.OrphanedBytes,
			reconciliation.OrphanedObjectCount,
			reconciliation.MissingActiveObjectCount,
			reconciliation.OverdueDeletionObjectCount,
			reconciliation.LingeringDeletedObjectCount,
			reconciliation.DuplicateReferenceCount,
			reconciliation.InvalidReferenceCount,
			reconciliation.SizeMismatchCount,
			reconciliation.UnresolvedBackendCount,
			reconciliation.ObservedAt.UTC().Format(time.RFC3339),
		)
		if strings.TrimSpace(reconciliation.Message) != "" {
			_, _ = fmt.Fprintf(w, "reconciliation_message: %s\n", reconciliation.Message)
		}
	}
	return nil
}

func renderAdminBackupStatus(w io.Writer, status adminBackupStatusResponse, showIDs bool) error {
	if len(status.Posture) > 0 {
		_, _ = fmt.Fprintln(w, "posture:")
		for _, posture := range status.Posture {
			_, _ = fmt.Fprintf(w, "- target=%s status=%s last_success=%s billable_bytes=%d %s\n", posture.Target.Type, posture.Status, formatBackupTime(posture.LastSuccessfulAt), posture.BillableBytes, posture.Message)
		}
	}
	if err := renderBackupPolicies(w, status.Policies, showIDs); err != nil {
		return err
	}
	if err := renderBackupRuns(w, status.Runs, showIDs); err != nil {
		return err
	}
	return renderBackupUsage(w, status.Usage)
}

func renderAppBackupStatus(w io.Writer, status appBackupStatusResponse, showIDs bool) error {
	_, _ = fmt.Fprintf(w, "app: %s\n", status.App.Name)
	for _, posture := range status.Posture {
		_, _ = fmt.Fprintf(w, "- target=%s status=%s last_success=%s %s\n", posture.Target.Type, posture.Status, formatBackupTime(posture.LastSuccessfulAt), posture.Message)
	}
	return renderBackupPolicies(w, status.Policies, showIDs)
}

func formatBackupTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}

func blankDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return strings.TrimSpace(value)
}

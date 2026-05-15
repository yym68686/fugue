package cli

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

const appDatabaseRestoreInventorySQL = `select current_database() as database, current_user as "user", (select count(*) from information_schema.tables where table_schema not in ('pg_catalog','information_schema'))::bigint as user_table_count`

type appDatabaseRestorePlanOptions struct {
	SourceNode       string
	SourcePGData     string
	ExpectedSystemID string
	ExpectedDatabase string
	TableMinRows     []string
	Timeout          time.Duration
}

type appDatabaseRestoreVerifyOptions struct {
	ExpectedDatabase string
	TableMinRows     []string
	Timeout          time.Duration
}

type appDatabaseRestorePlan struct {
	AppID             string                               `json:"app_id"`
	AppName           string                               `json:"app_name"`
	ServiceName       string                               `json:"service_name,omitempty"`
	Database          string                               `json:"database,omitempty"`
	User              string                               `json:"user,omitempty"`
	RuntimeID         string                               `json:"runtime_id,omitempty"`
	FailoverRuntimeID string                               `json:"failover_runtime_id,omitempty"`
	SourceNode        string                               `json:"source_node"`
	SourcePGData      string                               `json:"source_pgdata"`
	ExpectedSystemID  string                               `json:"expected_system_id"`
	RestoreMode       string                               `json:"restore_mode"`
	CurrentProbe      *appDatabaseRestoreCurrentProbe      `json:"current_probe,omitempty"`
	Checks            []appDatabaseRestoreCheck            `json:"checks"`
	PostRestoreChecks []appDatabaseRestoreTableRequirement `json:"post_restore_checks,omitempty"`
	Actions           []string                             `json:"actions"`
	Warnings          []string                             `json:"warnings,omitempty"`
	Status            *model.ManagedPostgresStatus         `json:"status,omitempty"`
}

type appDatabaseRestoreCurrentProbe struct {
	Database       string `json:"database,omitempty"`
	User           string `json:"user,omitempty"`
	UserTableCount int64  `json:"user_table_count,omitempty"`
	Reachable      bool   `json:"reachable"`
	Message        string `json:"message,omitempty"`
}

type appDatabaseRestoreCheck struct {
	Name    string `json:"name"`
	Pass    bool   `json:"pass"`
	Message string `json:"message,omitempty"`
}

type appDatabaseRestoreTableRequirement struct {
	Table   string `json:"table"`
	MinRows int64  `json:"min_rows"`
}

type appDatabaseRestoreVerifyResult struct {
	AppID   string                    `json:"app_id"`
	AppName string                    `json:"app_name"`
	Checks  []appDatabaseRestoreCheck `json:"checks"`
}

func (c *CLI) newAppDatabaseRestoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Plan and verify app managed Postgres restores",
		Long: strings.TrimSpace(`
Use this entrypoint before touching Kubernetes storage during a managed
Postgres recovery. The plan command is intentionally non-mutating: it records
the old PGDATA source, expected PostgreSQL system identifier, and the exact
post-restore checks that must pass. The verify command runs read-only SQL
checks against the app's effective database after a restore.
`),
	}
	cmd.AddCommand(c.newAppDatabaseRestorePlanCommand(), c.newAppDatabaseRestoreVerifyCommand())
	return cmd
}

func (c *CLI) newAppDatabaseRestorePlanCommand() *cobra.Command {
	opts := appDatabaseRestorePlanOptions{Timeout: 10 * time.Second}
	cmd := &cobra.Command{
		Use:   "plan <app>",
		Short: "Build a guarded restore plan for an app managed Postgres database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			requirements, err := parseAppDatabaseTableRequirements(opts.TableMinRows)
			if err != nil {
				return err
			}
			opts.SourceNode = strings.TrimSpace(opts.SourceNode)
			opts.SourcePGData = strings.TrimSpace(opts.SourcePGData)
			opts.ExpectedSystemID = strings.TrimSpace(opts.ExpectedSystemID)
			opts.ExpectedDatabase = strings.TrimSpace(opts.ExpectedDatabase)
			if opts.SourcePGData == "" {
				return fmt.Errorf("--source-pgdata is required")
			}
			if opts.ExpectedSystemID == "" {
				return fmt.Errorf("--expected-system-id is required; inspect the source PGDATA with pg_controldata before planning a restore")
			}

			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}
			database := ownedManagedPostgresSpec(app)
			if database == nil {
				return fmt.Errorf("managed postgres is not configured for this app")
			}

			statusResponse, err := client.GetAppDatabaseStatus(app.ID)
			if err != nil {
				return err
			}
			plan := buildAppDatabaseRestorePlan(app, database, statusResponse.Status, opts, requirements)
			probe, probeErr := probeCurrentAppDatabaseForRestore(client, app.ID, opts.Timeout)
			if probeErr != nil {
				plan.CurrentProbe = &appDatabaseRestoreCurrentProbe{
					Reachable: false,
					Message:   probeErr.Error(),
				}
				plan.Warnings = append(plan.Warnings, "current database probe failed: "+probeErr.Error())
			} else {
				plan.CurrentProbe = &probe
				plan.Checks = append(plan.Checks, appDatabaseRestoreCheck{
					Name:    "current_database_reachable",
					Pass:    true,
					Message: fmt.Sprintf("current database %q is reachable as %q", probe.Database, probe.User),
				})
			}
			return c.renderAppDatabaseRestorePlan(plan)
		},
	}
	cmd.Flags().StringVar(&opts.SourceNode, "source-node", "", "Kubernetes node that holds the old PGDATA directory")
	cmd.Flags().StringVar(&opts.SourcePGData, "source-pgdata", "", "Absolute source PGDATA path to restore from")
	cmd.Flags().StringVar(&opts.ExpectedSystemID, "expected-system-id", "", "PostgreSQL system identifier read from source PGDATA")
	cmd.Flags().StringVar(&opts.ExpectedDatabase, "expected-database", "", "Expected database name after restore; defaults to app managed Postgres database")
	cmd.Flags().StringArrayVar(&opts.TableMinRows, "table-min-rows", nil, "Post-restore row-count check in table=min_rows form; may be repeated")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Current database probe timeout")
	return cmd
}

func (c *CLI) newAppDatabaseRestoreVerifyCommand() *cobra.Command {
	opts := appDatabaseRestoreVerifyOptions{Timeout: 10 * time.Second}
	cmd := &cobra.Command{
		Use:   "verify <app>",
		Short: "Verify an app managed Postgres database after restore",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			requirements, err := parseAppDatabaseTableRequirements(opts.TableMinRows)
			if err != nil {
				return err
			}
			opts.ExpectedDatabase = strings.TrimSpace(opts.ExpectedDatabase)

			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}
			if ownedManagedPostgresSpec(app) == nil {
				return fmt.Errorf("managed postgres is not configured for this app")
			}
			result := appDatabaseRestoreVerifyResult{AppID: app.ID, AppName: app.Name}
			if opts.ExpectedDatabase != "" {
				response, err := client.QueryAppDatabase(app.ID, `select current_database() as database`, 1, opts.Timeout)
				if err != nil {
					result.Checks = append(result.Checks, appDatabaseRestoreCheck{Name: "database_name", Pass: false, Message: err.Error()})
				} else {
					got := ""
					if len(response.Rows) > 0 {
						got = restoreCellString(response.Rows[0], "database")
					}
					result.Checks = append(result.Checks, appDatabaseRestoreCheck{
						Name:    "database_name",
						Pass:    strings.EqualFold(got, opts.ExpectedDatabase),
						Message: fmt.Sprintf("expected %q, got %q", opts.ExpectedDatabase, got),
					})
				}
			}
			for _, requirement := range requirements {
				check := c.runAppDatabaseRestoreTableCheck(client, app.ID, requirement, opts.Timeout)
				result.Checks = append(result.Checks, check)
			}
			if len(result.Checks) == 0 {
				return fmt.Errorf("nothing to verify; pass --expected-database or at least one --table-min-rows check")
			}
			if err := c.renderAppDatabaseRestoreVerifyResult(result); err != nil {
				return err
			}
			if !appDatabaseRestoreChecksPass(result.Checks) {
				return fmt.Errorf("database restore verification failed")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.ExpectedDatabase, "expected-database", "", "Expected database name after restore")
	cmd.Flags().StringArrayVar(&opts.TableMinRows, "table-min-rows", nil, "Row-count check in table=min_rows form; may be repeated")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Verification query timeout")
	return cmd
}

func buildAppDatabaseRestorePlan(app model.App, database *model.AppPostgresSpec, status model.ManagedPostgresStatus, opts appDatabaseRestorePlanOptions, requirements []appDatabaseRestoreTableRequirement) appDatabaseRestorePlan {
	expectedDatabase := strings.TrimSpace(opts.ExpectedDatabase)
	if expectedDatabase == "" && database != nil {
		expectedDatabase = strings.TrimSpace(database.Database)
	}
	plan := appDatabaseRestorePlan{
		AppID:             app.ID,
		AppName:           app.Name,
		Database:          expectedDatabase,
		SourceNode:        strings.TrimSpace(opts.SourceNode),
		SourcePGData:      strings.TrimSpace(opts.SourcePGData),
		ExpectedSystemID:  strings.TrimSpace(opts.ExpectedSystemID),
		RestoreMode:       "plan_only",
		PostRestoreChecks: requirements,
		Status:            &status,
		Actions: []string{
			"Freeze app writes before restore and keep the app scaled down until verification passes.",
			"Inspect source PGDATA read-only and confirm PG_VERSION plus pg_controldata system identifier match this plan.",
			"Restore the actual source PGDATA as the target pgdata directory; do not leave the old data under pgdata_TIMESTAMP next to a fresh pgdata directory.",
			"Bring CNPG back only after the target PVC points at the verified data directory.",
			"Run fugue app db restore verify with every post-restore check in this plan before unfreezing users.",
		},
	}
	if database != nil {
		plan.ServiceName = strings.TrimSpace(database.ServiceName)
		plan.User = strings.TrimSpace(database.User)
		plan.RuntimeID = strings.TrimSpace(database.RuntimeID)
		plan.FailoverRuntimeID = strings.TrimSpace(database.FailoverTargetRuntimeID)
	}
	plan.Checks = append(plan.Checks,
		appDatabaseRestoreCheck{Name: "managed_postgres_configured", Pass: database != nil, Message: "app has an app-owned managed Postgres configuration"},
		appDatabaseRestoreCheck{Name: "source_pgdata_explicit", Pass: strings.TrimSpace(opts.SourcePGData) != "", Message: strings.TrimSpace(opts.SourcePGData)},
		appDatabaseRestoreCheck{Name: "expected_system_id_recorded", Pass: strings.TrimSpace(opts.ExpectedSystemID) != "", Message: strings.TrimSpace(opts.ExpectedSystemID)},
	)
	if plan.SourceNode == "" {
		plan.Warnings = append(plan.Warnings, "source node was not recorded; include --source-node when the PGDATA path is node-local")
	}
	if len(requirements) == 0 {
		plan.Warnings = append(plan.Warnings, "no table row-count checks were recorded; add --table-min-rows for user-critical tables")
	}
	return plan
}

func probeCurrentAppDatabaseForRestore(client *Client, appID string, timeout time.Duration) (appDatabaseRestoreCurrentProbe, error) {
	response, err := client.QueryAppDatabase(appID, appDatabaseRestoreInventorySQL, 1, timeout)
	if err != nil {
		return appDatabaseRestoreCurrentProbe{}, err
	}
	probe := appDatabaseRestoreCurrentProbe{Reachable: true}
	if len(response.Rows) > 0 {
		row := response.Rows[0]
		probe.Database = restoreCellString(row, "database")
		probe.User = restoreCellString(row, "user")
		if value, ok := restoreCellInt64(row, "user_table_count"); ok {
			probe.UserTableCount = value
		}
	}
	return probe, nil
}

func (c *CLI) runAppDatabaseRestoreTableCheck(client *Client, appID string, requirement appDatabaseRestoreTableRequirement, timeout time.Duration) appDatabaseRestoreCheck {
	tableSQL, err := quotePostgresQualifiedIdentifier(requirement.Table)
	if err != nil {
		return appDatabaseRestoreCheck{Name: "table_min_rows:" + requirement.Table, Pass: false, Message: err.Error()}
	}
	response, err := client.QueryAppDatabase(appID, fmt.Sprintf("select count(*)::bigint as row_count from %s", tableSQL), 1, timeout)
	if err != nil {
		return appDatabaseRestoreCheck{Name: "table_min_rows:" + requirement.Table, Pass: false, Message: err.Error()}
	}
	var count int64
	if len(response.Rows) > 0 {
		count, _ = restoreCellInt64(response.Rows[0], "row_count")
	}
	return appDatabaseRestoreCheck{
		Name:    "table_min_rows:" + requirement.Table,
		Pass:    count >= requirement.MinRows,
		Message: fmt.Sprintf("expected at least %d rows, got %d", requirement.MinRows, count),
	}
}

func parseAppDatabaseTableRequirements(values []string) ([]appDatabaseRestoreTableRequirement, error) {
	requirements := make([]appDatabaseRestoreTableRequirement, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		parts := strings.SplitN(value, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("table row check %q must use table=min_rows", value)
		}
		table := strings.TrimSpace(parts[0])
		minRows, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || minRows < 0 {
			return nil, fmt.Errorf("table row check %q has invalid min_rows", value)
		}
		if _, err := quotePostgresQualifiedIdentifier(table); err != nil {
			return nil, err
		}
		requirements = append(requirements, appDatabaseRestoreTableRequirement{Table: table, MinRows: minRows})
	}
	return requirements, nil
}

func quotePostgresQualifiedIdentifier(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("table name is required")
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("table name %q must be table or schema.table", value)
	}
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if !isSimplePostgresIdentifier(part) {
			return "", fmt.Errorf("table name %q must use simple PostgreSQL identifiers", value)
		}
		quoted = append(quoted, `"`+part+`"`)
	}
	return strings.Join(quoted, "."), nil
}

func isSimplePostgresIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if i == 0 {
			if r == '_' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') {
				continue
			}
			return false
		}
		if r == '_' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || ('0' <= r && r <= '9') {
			continue
		}
		return false
	}
	return true
}

func restoreCellString(row map[string]any, key string) string {
	if row == nil {
		return ""
	}
	value, ok := row[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func restoreCellInt64(row map[string]any, key string) (int64, bool) {
	if row == nil {
		return 0, false
	}
	value, ok := row[key]
	if !ok || value == nil {
		return 0, false
	}
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case float64:
		return int64(typed), true
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		parsed, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(typed)), 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	}
}

func appDatabaseRestoreChecksPass(checks []appDatabaseRestoreCheck) bool {
	if len(checks) == 0 {
		return false
	}
	for _, check := range checks {
		if !check.Pass {
			return false
		}
	}
	return true
}

func (c *CLI) renderAppDatabaseRestorePlan(plan appDatabaseRestorePlan) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, plan)
	}
	pairs := []kvPair{
		{Key: "app_id", Value: plan.AppID},
		{Key: "app_name", Value: plan.AppName},
		{Key: "restore_mode", Value: plan.RestoreMode},
		{Key: "source_node", Value: plan.SourceNode},
		{Key: "source_pgdata", Value: plan.SourcePGData},
		{Key: "expected_system_id", Value: plan.ExpectedSystemID},
		{Key: "service_name", Value: plan.ServiceName},
		{Key: "database", Value: plan.Database},
		{Key: "runtime_id", Value: plan.RuntimeID},
	}
	if plan.CurrentProbe != nil {
		pairs = append(pairs,
			kvPair{Key: "current_database_reachable", Value: fmt.Sprintf("%t", plan.CurrentProbe.Reachable)},
			kvPair{Key: "current_database", Value: plan.CurrentProbe.Database},
			kvPair{Key: "current_user_table_count", Value: fmt.Sprintf("%d", plan.CurrentProbe.UserTableCount)},
		)
	}
	if err := writeKeyValues(c.stdout, pairs...); err != nil {
		return err
	}
	return renderAppDatabaseRestoreSections(c.stdout, plan.Checks, plan.PostRestoreChecks, plan.Actions, plan.Warnings)
}

func (c *CLI) renderAppDatabaseRestoreVerifyResult(result appDatabaseRestoreVerifyResult) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, result)
	}
	if err := writeKeyValues(c.stdout,
		kvPair{Key: "app_id", Value: result.AppID},
		kvPair{Key: "app_name", Value: result.AppName},
		kvPair{Key: "checks_passed", Value: fmt.Sprintf("%t", appDatabaseRestoreChecksPass(result.Checks))},
	); err != nil {
		return err
	}
	for _, check := range result.Checks {
		state := "fail"
		if check.Pass {
			state = "pass"
		}
		if _, err := fmt.Fprintf(c.stdout, "check=%s status=%s message=%q\n", check.Name, state, check.Message); err != nil {
			return err
		}
	}
	return nil
}

func renderAppDatabaseRestoreSections(w io.Writer, checks []appDatabaseRestoreCheck, requirements []appDatabaseRestoreTableRequirement, actions []string, warnings []string) error {
	for _, check := range checks {
		state := "fail"
		if check.Pass {
			state = "pass"
		}
		if _, err := fmt.Fprintf(w, "check=%s status=%s message=%q\n", check.Name, state, check.Message); err != nil {
			return err
		}
	}
	for _, requirement := range requirements {
		if _, err := fmt.Fprintf(w, "post_restore_check=table_min_rows table=%s min_rows=%d\n", requirement.Table, requirement.MinRows); err != nil {
			return err
		}
	}
	for _, action := range actions {
		if _, err := fmt.Fprintf(w, "action=%q\n", action); err != nil {
			return err
		}
	}
	for _, warning := range warnings {
		if _, err := fmt.Fprintf(w, "warning=%q\n", warning); err != nil {
			return err
		}
	}
	return nil
}

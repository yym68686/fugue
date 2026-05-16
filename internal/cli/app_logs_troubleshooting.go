package cli

import (
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appLogQueryOptions struct {
	SQL        string
	File       string
	Table      string
	Columns    []string
	TimeColumn string
	Since      string
	Until      string
	Match      []string
	Contains   []string
	SortColumn string
	SortOrder  string
	Limit      int
	Timeout    time.Duration
}

var sqlIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (c *CLI) newAppLogsQueryCommand() *cobra.Command {
	opts := appLogQueryOptions{
		TimeColumn: "created_at",
		SortOrder:  "desc",
		Limit:      200,
		Timeout:    10 * time.Second,
	}
	cmd := &cobra.Command{
		Use:     "table <app>",
		Aliases: []string{"query"},
		Short:   "Query business log tables with time-window filters",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			statement, err := buildAppLogQueryStatement(opts, time.Now().UTC())
			if err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			result, err := client.QueryAppDatabase(app.ID, statement, opts.Limit, opts.Timeout)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return renderAppDatabaseQueryResult(c.stdout, result)
		},
	}
	cmd.Flags().StringVar(&opts.SQL, "sql", "", "Run an explicit SQL query instead of the structured log-table builder")
	cmd.Flags().StringVar(&opts.File, "file", "", "Read SQL from a local path or '-' for stdin")
	cmd.Flags().StringVar(&opts.Table, "table", "", "Business log table name, for example gateway_request_logs")
	cmd.Flags().StringArrayVar(&opts.Columns, "column", nil, "Column to select (repeatable, defaults to *)")
	cmd.Flags().StringVar(&opts.TimeColumn, "time-column", opts.TimeColumn, "Timestamp column used by --since/--until and default sorting")
	cmd.Flags().StringVar(&opts.Since, "since", "", "Lower time bound as RFC3339 or relative duration like 1h")
	cmd.Flags().StringVar(&opts.Until, "until", "", "Upper time bound as RFC3339 or relative duration like 15m")
	cmd.Flags().StringArrayVar(&opts.Match, "match", nil, "Exact match in column=value form (repeatable)")
	cmd.Flags().StringArrayVar(&opts.Contains, "contains", nil, "Substring match in column=value form (repeatable)")
	cmd.Flags().StringVar(&opts.SortColumn, "sort-column", "", "Column used for ORDER BY; defaults to --time-column")
	cmd.Flags().StringVar(&opts.SortOrder, "sort-order", opts.SortOrder, "Sort order: asc or desc")
	cmd.Flags().IntVar(&opts.Limit, "limit", opts.Limit, "Maximum rows to return")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Query timeout")
	return cmd
}

func (c *CLI) newAppLogsPodsCommand() *cobra.Command {
	opts := struct {
		Component string
	}{Component: "app"}
	cmd := &cobra.Command{
		Use:   "pods <app>",
		Short: "Show current and recent runtime pod groups for an app",
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
			inventory, err := client.GetAppRuntimePods(app.ID, opts.Component)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, inventory)
			}
			for _, warning := range inventory.Warnings {
				c.progressf("warning=%s", warning)
			}
			return renderAppRuntimePodInventory(c.stdout, inventory)
		},
	}
	cmd.Flags().StringVar(&opts.Component, "component", opts.Component, "Runtime component: app or postgres")
	return cmd
}

func buildAppLogQueryStatement(opts appLogQueryOptions, now time.Time) (string, error) {
	if statement, err := loadSQLStatement(opts.SQL, opts.File); err == nil && strings.TrimSpace(statement) != "" {
		if hasStructuredAppLogQueryOptions(opts) {
			return "", fmt.Errorf("structured log-query flags cannot be combined with --sql or --file")
		}
		return statement, nil
	} else if err != nil && !isMissingSQLSelection(err) {
		return "", err
	}

	tableName := strings.TrimSpace(opts.Table)
	if tableName == "" {
		return "", fmt.Errorf("--table is required when --sql or --file is not used")
	}
	if opts.Limit <= 0 {
		return "", fmt.Errorf("--limit must be greater than zero")
	}

	selectClause, err := buildStructuredSelectClause(opts.Columns)
	if err != nil {
		return "", err
	}
	timeColumn := strings.TrimSpace(opts.TimeColumn)
	if timeColumn == "" {
		timeColumn = "created_at"
	}

	predicates := make([]string, 0, len(opts.Match)+len(opts.Contains)+2)
	if since, ok, err := parseDiagnosticTimeWindowValue(opts.Since, now); err != nil {
		return "", err
	} else if ok {
		column, err := quoteSQLIdentifierPath(timeColumn)
		if err != nil {
			return "", err
		}
		predicates = append(predicates, fmt.Sprintf("%s >= %s::timestamptz", column, sqlStringLiteral(since.Format(time.RFC3339Nano))))
	}
	if until, ok, err := parseDiagnosticTimeWindowValue(opts.Until, now); err != nil {
		return "", err
	} else if ok {
		column, err := quoteSQLIdentifierPath(timeColumn)
		if err != nil {
			return "", err
		}
		predicates = append(predicates, fmt.Sprintf("%s <= %s::timestamptz", column, sqlStringLiteral(until.Format(time.RFC3339Nano))))
	}

	matchPredicates, err := buildStructuredLogPredicates(opts.Match, false)
	if err != nil {
		return "", err
	}
	predicates = append(predicates, matchPredicates...)
	containsPredicates, err := buildStructuredLogPredicates(opts.Contains, true)
	if err != nil {
		return "", err
	}
	predicates = append(predicates, containsPredicates...)

	tableIdentifier, err := quoteSQLIdentifierPath(tableName)
	if err != nil {
		return "", err
	}
	sortColumn := strings.TrimSpace(opts.SortColumn)
	if sortColumn == "" {
		sortColumn = timeColumn
	}
	sortIdentifier, err := quoteSQLIdentifierPath(sortColumn)
	if err != nil {
		return "", err
	}
	sortOrder := strings.ToUpper(strings.TrimSpace(opts.SortOrder))
	if sortOrder == "" {
		sortOrder = "DESC"
	}
	if sortOrder != "ASC" && sortOrder != "DESC" {
		return "", fmt.Errorf("--sort-order must be asc or desc")
	}

	statement := fmt.Sprintf("select %s from %s", selectClause, tableIdentifier)
	if len(predicates) > 0 {
		statement += " where " + strings.Join(predicates, " and ")
	}
	statement += fmt.Sprintf(" order by %s %s limit %d", sortIdentifier, sortOrder, opts.Limit)
	return statement, nil
}

func hasStructuredAppLogQueryOptions(opts appLogQueryOptions) bool {
	return strings.TrimSpace(opts.Table) != "" ||
		len(opts.Columns) > 0 ||
		(strings.TrimSpace(opts.TimeColumn) != "" && !strings.EqualFold(strings.TrimSpace(opts.TimeColumn), "created_at")) ||
		strings.TrimSpace(opts.Since) != "" ||
		strings.TrimSpace(opts.Until) != "" ||
		len(opts.Match) > 0 ||
		len(opts.Contains) > 0 ||
		strings.TrimSpace(opts.SortColumn) != "" ||
		(strings.TrimSpace(opts.SortOrder) != "" && !strings.EqualFold(strings.TrimSpace(opts.SortOrder), "desc")) ||
		opts.Limit != 200
}

func isMissingSQLSelection(err error) bool {
	return err != nil && strings.Contains(err.Error(), "one of --sql or --file is required")
}

func buildStructuredSelectClause(columns []string) (string, error) {
	if len(columns) == 0 {
		return "*", nil
	}
	selectors := make([]string, 0, len(columns))
	for _, column := range columns {
		column = strings.TrimSpace(column)
		if column == "" {
			continue
		}
		quoted, err := quoteSQLIdentifierPath(column)
		if err != nil {
			return "", err
		}
		selectors = append(selectors, quoted)
	}
	if len(selectors) == 0 {
		return "*", nil
	}
	return strings.Join(selectors, ", "), nil
}

func buildStructuredLogPredicates(filters []string, contains bool) ([]string, error) {
	predicates := make([]string, 0, len(filters))
	for _, raw := range filters {
		column, value, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("filter %q must use column=value", raw)
		}
		column = strings.TrimSpace(column)
		value = strings.TrimSpace(value)
		if column == "" {
			return nil, fmt.Errorf("filter %q has an empty column", raw)
		}
		quotedColumn, err := quoteSQLIdentifierPath(column)
		if err != nil {
			return nil, err
		}
		if contains {
			predicates = append(predicates, fmt.Sprintf("%s::text ILIKE %s", quotedColumn, sqlStringLiteral("%"+value+"%")))
			continue
		}
		predicates = append(predicates, fmt.Sprintf("%s = %s", quotedColumn, sqlStringLiteral(value)))
	}
	return predicates, nil
}

func parseDiagnosticTimeWindowValue(raw string, now time.Time) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	if value, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return value.UTC(), true, nil
	}
	if value, err := time.Parse(time.RFC3339, raw); err == nil {
		return value.UTC(), true, nil
	}
	if duration, err := time.ParseDuration(raw); err == nil {
		if duration > 0 {
			duration = -duration
		}
		return now.UTC().Add(duration), true, nil
	}
	return time.Time{}, false, fmt.Errorf("time window %q must be RFC3339 or a relative duration like 1h", raw)
}

func quoteSQLIdentifierPath(raw string) (string, error) {
	parts := strings.Split(strings.TrimSpace(raw), ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if !sqlIdentifierPattern.MatchString(part) {
			return "", fmt.Errorf("identifier %q must match %s", raw, sqlIdentifierPattern.String())
		}
		quoted = append(quoted, `"`+part+`"`)
	}
	return strings.Join(quoted, "."), nil
}

func sqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func renderAppRuntimePodInventory(w io.Writer, inventory model.AppRuntimePodInventory) error {
	groupCount := len(inventory.Groups)
	livePodCount := 0
	for _, group := range inventory.Groups {
		livePodCount += len(group.Pods)
	}
	if err := writeKeyValues(w,
		kvPair{Key: "component", Value: inventory.Component},
		kvPair{Key: "namespace", Value: inventory.Namespace},
		kvPair{Key: "selector", Value: inventory.Selector},
		kvPair{Key: "container", Value: inventory.Container},
		kvPair{Key: "group_count", Value: formatInt(groupCount)},
		kvPair{Key: "live_pods", Value: formatInt(livePodCount)},
	); err != nil {
		return err
	}
	if groupCount == 0 {
		return nil
	}
	for _, group := range inventory.Groups {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		pairs := []kvPair{
			{Key: "owner", Value: strings.TrimSpace(group.OwnerKind) + "/" + strings.TrimSpace(group.OwnerName)},
			{Key: "revision", Value: strings.TrimSpace(group.Revision)},
			{Key: "created_at", Value: formatOptionalTime(group.CreatedAt)},
			{Key: "desired_replicas", Value: formatOptionalInt32(group.DesiredReplicas)},
			{Key: "current_replicas", Value: formatOptionalInt32(group.CurrentReplicas)},
			{Key: "ready_replicas", Value: formatOptionalInt32(group.ReadyReplicas)},
			{Key: "available_replicas", Value: formatOptionalInt32(group.AvailableReplicas)},
			{Key: "containers", Value: formatRuntimePodGroupContainers(group.Containers)},
			{Key: "pods", Value: formatInt(len(group.Pods))},
		}
		if group.Parent != nil {
			pairs = append([]kvPair{
				{Key: "owner", Value: strings.TrimSpace(group.OwnerKind) + "/" + strings.TrimSpace(group.OwnerName)},
				{Key: "parent", Value: clusterPodOwnerLabel(group.Parent)},
			}, pairs[1:]...)
		}
		if err := writeKeyValues(w, pairs...); err != nil {
			return err
		}
		if len(group.Pods) == 0 {
			continue
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeClusterPodTable(w, group.Pods); err != nil {
			return err
		}
	}
	return nil
}

func formatRuntimePodGroupContainers(containers []model.ClusterWorkloadContainer) string {
	if len(containers) == 0 {
		return ""
	}
	sorted := append([]model.ClusterWorkloadContainer(nil), containers...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Name < sorted[j].Name
	})
	parts := make([]string, 0, len(sorted))
	for _, container := range sorted {
		label := strings.TrimSpace(container.Name)
		image := strings.TrimSpace(container.Image)
		if image != "" {
			label += "=" + image
		}
		parts = append(parts, label)
	}
	return strings.Join(parts, ", ")
}

func formatOptionalTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return ""
	}
	return formatTime(value.UTC())
}

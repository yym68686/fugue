package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppDatabaseTablesCommand() *cobra.Command {
	opts := struct {
		MaxRows int
		Timeout time.Duration
	}{
		MaxRows: 250,
		Timeout: 10 * time.Second,
	}
	cmd := &cobra.Command{
		Use:   "tables <app> [match]",
		Short: "List visible database tables and views",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			statement := appDatabaseTablesSQL("")
			if len(args) > 1 {
				statement = appDatabaseTablesSQL(args[1])
			}
			response, err := c.queryNamedAppDatabase(args[0], statement, opts.MaxRows, opts.Timeout)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderAppDatabaseSimpleRows(c.stdout, response, []string{"table_schema", "table_name", "table_type"})
		},
	}
	cmd.Flags().IntVar(&opts.MaxRows, "max-rows", opts.MaxRows, "Maximum tables to return")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Query timeout")
	return cmd
}

func (c *CLI) newAppDatabaseSchemaCommand() *cobra.Command {
	opts := struct {
		MaxRows int
		Timeout time.Duration
	}{
		MaxRows: 500,
		Timeout: 10 * time.Second,
	}
	cmd := &cobra.Command{
		Use:   "schema <app> [table]",
		Short: "Show database columns for all tables or one table",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			statement := appDatabaseSchemaSQL("")
			if len(args) > 1 {
				statement = appDatabaseSchemaSQL(args[1])
			}
			response, err := c.queryNamedAppDatabase(args[0], statement, opts.MaxRows, opts.Timeout)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderAppDatabaseSimpleRows(c.stdout, response, []string{"table_schema", "table_name", "column_name", "data_type", "is_nullable", "column_default"})
		},
	}
	cmd.Flags().IntVar(&opts.MaxRows, "max-rows", opts.MaxRows, "Maximum columns to return")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Query timeout")
	return cmd
}

func (c *CLI) newAppDatabaseSearchCommand() *cobra.Command {
	opts := struct {
		Email       string
		Value       string
		ColumnMatch string
		MaxColumns  int
		MaxRows     int
		Timeout     time.Duration
	}{
		ColumnMatch: "email",
		MaxColumns:  80,
		MaxRows:     50,
		Timeout:     10 * time.Second,
	}
	cmd := &cobra.Command{
		Use:   "search <app>",
		Short: "Search likely text columns for an email or exact value",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			value := strings.TrimSpace(opts.Value)
			if value == "" {
				value = strings.TrimSpace(opts.Email)
			}
			if value == "" {
				return fmt.Errorf("one of --email or --value is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			result, err := searchAppDatabase(client, app.ID, value, opts.ColumnMatch, opts.MaxColumns, opts.MaxRows, opts.Timeout)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return renderAppDatabaseSearchResult(c.stdout, result)
		},
	}
	cmd.Flags().StringVar(&opts.Email, "email", "", "Email address to search for")
	cmd.Flags().StringVar(&opts.Value, "value", "", "Exact value to search for")
	cmd.Flags().StringVar(&opts.ColumnMatch, "column-match", opts.ColumnMatch, "Only inspect text columns whose name contains this value")
	cmd.Flags().IntVar(&opts.MaxColumns, "max-columns", opts.MaxColumns, "Maximum candidate columns to inspect")
	cmd.Flags().IntVar(&opts.MaxRows, "max-rows", opts.MaxRows, "Maximum matching rows to return")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Per-query timeout")
	return cmd
}

func (c *CLI) queryNamedAppDatabase(appRef, statement string, maxRows int, timeout time.Duration) (appDatabaseQueryResponse, error) {
	client, err := c.newClient()
	if err != nil {
		return appDatabaseQueryResponse{}, err
	}
	app, err := c.resolveNamedApp(client, appRef)
	if err != nil {
		return appDatabaseQueryResponse{}, err
	}
	return client.QueryAppDatabase(app.ID, statement, maxRows, timeout)
}

func appDatabaseTablesSQL(match string) string {
	where := "table_schema not in ('pg_catalog', 'information_schema') and table_type in ('BASE TABLE', 'VIEW', 'FOREIGN TABLE')"
	if match = strings.TrimSpace(match); match != "" {
		where += " and (table_schema ilike '%' || " + sqlLiteral(match) + " || '%' or table_name ilike '%' || " + sqlLiteral(match) + " || '%')"
	}
	return "select table_schema, table_name, table_type from information_schema.tables where " + where + " order by table_schema, table_name"
}

func appDatabaseSchemaSQL(tableRef string) string {
	where := "table_schema not in ('pg_catalog', 'information_schema')"
	if tableRef = strings.TrimSpace(tableRef); tableRef != "" {
		schemaName, tableName := splitSQLTableRef(tableRef)
		if schemaName != "" {
			where += " and table_schema = " + sqlLiteral(schemaName)
		}
		where += " and table_name = " + sqlLiteral(tableName)
	}
	return "select table_schema, table_name, column_name, data_type, is_nullable, column_default from information_schema.columns where " + where + " order by table_schema, table_name, ordinal_position"
}

type appDatabaseSearchResult struct {
	Value            string                   `json:"value"`
	ColumnMatch      string                   `json:"column_match"`
	CandidateColumns []appDatabaseColumnRef   `json:"candidate_columns"`
	Matches          []appDatabaseSearchMatch `json:"matches"`
	Truncated        bool                     `json:"truncated,omitempty"`
}

type appDatabaseColumnRef struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Column string `json:"column"`
}

type appDatabaseSearchMatch struct {
	Schema string         `json:"schema"`
	Table  string         `json:"table"`
	Column string         `json:"column"`
	Row    map[string]any `json:"row"`
}

func searchAppDatabase(client *Client, appID, value, columnMatch string, maxColumns, maxRows int, timeout time.Duration) (appDatabaseSearchResult, error) {
	if maxColumns <= 0 {
		maxColumns = 80
	}
	if maxRows <= 0 {
		maxRows = 50
	}
	columnMatch = strings.TrimSpace(columnMatch)
	if columnMatch == "" {
		columnMatch = "email"
	}
	candidates, err := queryAppDatabaseCandidateColumns(client, appID, columnMatch, maxColumns, timeout)
	if err != nil {
		return appDatabaseSearchResult{}, err
	}
	result := appDatabaseSearchResult{
		Value:            value,
		ColumnMatch:      columnMatch,
		CandidateColumns: candidates,
	}
	remaining := maxRows
	for _, candidate := range candidates {
		if remaining <= 0 {
			result.Truncated = true
			break
		}
		statement, err := appDatabaseExactSearchSQL(candidate, value)
		if err != nil {
			return appDatabaseSearchResult{}, err
		}
		response, err := client.QueryAppDatabase(appID, statement, remaining, timeout)
		if err != nil {
			return appDatabaseSearchResult{}, err
		}
		for _, row := range response.Rows {
			match := appDatabaseSearchMatch{
				Schema: candidate.Schema,
				Table:  candidate.Table,
				Column: candidate.Column,
				Row:    map[string]any{},
			}
			if raw, ok := row["row"].(map[string]any); ok {
				match.Row = raw
			} else {
				match.Row = row
			}
			result.Matches = append(result.Matches, match)
			remaining--
			if remaining <= 0 {
				result.Truncated = true
				break
			}
		}
	}
	return result, nil
}

func queryAppDatabaseCandidateColumns(client *Client, appID, columnMatch string, maxColumns int, timeout time.Duration) ([]appDatabaseColumnRef, error) {
	statement := "select table_schema, table_name, column_name from information_schema.columns where table_schema not in ('pg_catalog', 'information_schema') and (data_type in ('text', 'character varying', 'character') or udt_name = 'citext') and column_name ilike '%' || " + sqlLiteral(columnMatch) + " || '%' order by table_schema, table_name, ordinal_position"
	response, err := client.QueryAppDatabase(appID, statement, maxColumns, timeout)
	if err != nil {
		return nil, err
	}
	out := make([]appDatabaseColumnRef, 0, len(response.Rows))
	for _, row := range response.Rows {
		out = append(out, appDatabaseColumnRef{
			Schema: fmt.Sprint(row["table_schema"]),
			Table:  fmt.Sprint(row["table_name"]),
			Column: fmt.Sprint(row["column_name"]),
		})
	}
	return out, nil
}

func appDatabaseExactSearchSQL(column appDatabaseColumnRef, value string) (string, error) {
	schemaName, err := sqlIdentifier(column.Schema)
	if err != nil {
		return "", err
	}
	tableName, err := sqlIdentifier(column.Table)
	if err != nil {
		return "", err
	}
	columnName, err := sqlIdentifier(column.Column)
	if err != nil {
		return "", err
	}
	return "select to_jsonb(t) as row from " + schemaName + "." + tableName + " as t where lower(" + columnName + "::text) = lower(" + sqlLiteral(value) + ")", nil
}

func renderAppDatabaseSimpleRows(w io.Writer, result appDatabaseQueryResponse, columns []string) error {
	if len(result.Rows) == 0 {
		return writeKeyValues(w,
			kvPair{Key: "database", Value: result.Database},
			kvPair{Key: "row_count", Value: "0"},
		)
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	headers := make([]string, 0, len(columns))
	for _, column := range columns {
		headers = append(headers, strings.ToUpper(column))
	}
	if _, err := fmt.Fprintln(tw, strings.Join(headers, "\t")); err != nil {
		return err
	}
	for _, row := range result.Rows {
		values := make([]string, 0, len(columns))
		for _, column := range columns {
			values = append(values, formatInlineTableValue(formatDatabaseCell(row[column])))
		}
		if _, err := fmt.Fprintln(tw, strings.Join(values, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func renderAppDatabaseSearchResult(w io.Writer, result appDatabaseSearchResult) error {
	if err := writeKeyValues(w,
		kvPair{Key: "value", Value: result.Value},
		kvPair{Key: "column_match", Value: result.ColumnMatch},
		kvPair{Key: "candidate_columns", Value: fmt.Sprintf("%d", len(result.CandidateColumns))},
		kvPair{Key: "matches", Value: fmt.Sprintf("%d", len(result.Matches))},
	); err != nil {
		return err
	}
	if result.Truncated {
		if err := writeKeyValues(w, kvPair{Key: "truncated", Value: "true"}); err != nil {
			return err
		}
	}
	if len(result.Matches) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TABLE\tCOLUMN\tROW"); err != nil {
		return err
	}
	for _, match := range result.Matches {
		if _, err := fmt.Fprintf(
			tw,
			"%s.%s\t%s\t%s\n",
			match.Schema,
			match.Table,
			match.Column,
			formatInlineTableValue(fmt.Sprintf("%v", match.Row)),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func splitSQLTableRef(ref string) (string, string) {
	ref = strings.TrimSpace(ref)
	if left, right, ok := strings.Cut(ref, "."); ok {
		return strings.TrimSpace(left), strings.TrimSpace(right)
	}
	return "", ref
}

func sqlLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func sqlIdentifier(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("sql identifier is empty")
	}
	if strings.ContainsRune(value, 0) {
		return "", fmt.Errorf("sql identifier contains a NUL byte")
	}
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`, nil
}

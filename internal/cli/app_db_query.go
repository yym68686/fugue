package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppDatabaseQueryCommand() *cobra.Command {
	opts := struct {
		SQL     string
		File    string
		MaxRows int
		Timeout time.Duration
	}{
		MaxRows: 100,
		Timeout: 10 * time.Second,
	}
	cmd := &cobra.Command{
		Use:   "query <app>",
		Short: "Run a read-only SQL query against the app effective Postgres connection",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			statement, err := loadSQLStatement(opts.SQL, opts.File)
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
			response, err := client.QueryAppDatabase(app.ID, statement, opts.MaxRows, opts.Timeout)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderAppDatabaseQueryResult(c.stdout, response)
		},
	}
	cmd.Flags().StringVar(&opts.SQL, "sql", "", "Inline SQL statement to run")
	cmd.Flags().StringVar(&opts.File, "file", "", "Read SQL from a local path or '-' for stdin")
	cmd.Flags().IntVar(&opts.MaxRows, "max-rows", opts.MaxRows, "Maximum rows to return")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Query timeout")
	return cmd
}

func loadSQLStatement(statement, file string) (string, error) {
	hasStatement := strings.TrimSpace(statement) != ""
	hasFile := strings.TrimSpace(file) != ""
	switch {
	case hasStatement && hasFile:
		return "", fmt.Errorf("--sql and --file cannot be used together")
	case hasStatement:
		return strings.TrimSpace(statement), nil
	case hasFile:
		file = strings.TrimSpace(file)
		var data []byte
		var err error
		if file == "-" {
			data, err = io.ReadAll(os.Stdin)
		} else {
			data, err = os.ReadFile(file)
		}
		if err != nil {
			return "", fmt.Errorf("read %s: %w", file, err)
		}
		statement = strings.TrimSpace(string(data))
		if statement == "" {
			return "", fmt.Errorf("sql statement is empty")
		}
		return statement, nil
	default:
		return "", fmt.Errorf("one of --sql or --file is required")
	}
}

func renderAppDatabaseQueryResult(w io.Writer, result appDatabaseQueryResponse) error {
	if err := writeKeyValues(w,
		kvPair{Key: "database", Value: result.Database},
		kvPair{Key: "host", Value: result.Host},
		kvPair{Key: "user", Value: result.User},
		kvPair{Key: "row_count", Value: fmt.Sprintf("%d", result.RowCount)},
		kvPair{Key: "max_rows", Value: fmt.Sprintf("%d", result.MaxRows)},
		kvPair{Key: "read_only", Value: fmt.Sprintf("%t", result.ReadOnly)},
		kvPair{Key: "duration_ms", Value: fmt.Sprintf("%d", result.DurationMS)},
	); err != nil {
		return err
	}
	if result.Truncated {
		if err := writeKeyValues(w, kvPair{Key: "truncated", Value: "true"}); err != nil {
			return err
		}
	}
	if len(result.Columns) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	header := make([]string, 0, len(result.Columns))
	for _, column := range result.Columns {
		header = append(header, column.Name)
	}
	if _, err := fmt.Fprintln(tw, strings.Join(header, "\t")); err != nil {
		return err
	}
	for _, row := range result.Rows {
		values := make([]string, 0, len(result.Columns))
		for _, column := range result.Columns {
			values = append(values, formatInlineTableValue(formatDatabaseCell(row[column.Name])))
		}
		if _, err := fmt.Fprintln(tw, strings.Join(values, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatDatabaseCell(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprintf("%v", typed)
	}
}

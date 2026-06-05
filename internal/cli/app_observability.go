package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

const defaultAppObservabilityLimit = 200

func (c *CLI) newAppMetricsCommand() *cobra.Command {
	opts := appObservabilityMetricsOptions{}
	cmd := &cobra.Command{
		Use:   "metrics <app>",
		Short: "Show app observability metric summaries",
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
			response, err := client.GetAppObservabilityMetricsSummary(app.ID, opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderAppObservabilityMetricsSummary(c.stdout, response)
		},
	}
	addAppObservabilityWindowFlags(cmd, &opts.appObservabilityWindowOptions)
	return cmd
}

func (c *CLI) newAppRequestsCommand() *cobra.Command {
	opts := appObservabilityRequestsOptions{Limit: defaultAppObservabilityLimit}
	cmd := &cobra.Command{
		Use:   "requests <app>",
		Short: "List app observability request summaries",
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
			response, err := client.ListAppObservabilityRequests(app.ID, opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderAppObservabilityRequests(c.stdout, response)
		},
	}
	addAppObservabilityWindowFlags(cmd, &opts.appObservabilityWindowOptions)
	cmd.Flags().IntVar(&opts.Limit, "limit", opts.Limit, "Maximum request summaries to return")
	cmd.Flags().StringVar(&opts.TraceID, "trace", "", "Limit request summaries to one trace identifier")
	cmd.Flags().StringVar(&opts.StatusClass, "status-class", "", "Limit request summaries to one status class")
	cmd.Flags().BoolVar(&opts.Slow, "slow", false, "Only show slow request summaries")
	cmd.Flags().BoolVar(&opts.Errors, "errors", false, "Only show error request summaries")
	return cmd
}

func (c *CLI) newAppTracesCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "traces <app> <trace_id>",
		Short: "Show one app observability trace",
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
			response, err := client.GetAppObservabilityTrace(app.ID, args[1])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderAppObservabilityTrace(c.stdout, response)
		},
	}
}

func addAppObservabilityWindowFlags(cmd *cobra.Command, opts *appObservabilityWindowOptions) {
	cmd.Flags().StringVar(&opts.Since, "since", "", "Lower time bound as RFC3339 or relative duration like 1h")
	cmd.Flags().StringVar(&opts.Until, "until", "", "Upper time bound as RFC3339 timestamp")
}

func renderAppObservabilityMetricsSummary(w io.Writer, response appObservabilityMetricsSummaryResponse) error {
	if err := renderAppObservabilityHeader(w, response.Source, response.Window); err != nil {
		return err
	}
	if err := writeKeyValues(w, kvPair{Key: "metrics", Value: formatInt(len(response.Metrics))}); err != nil {
		return err
	}
	if len(response.Metrics) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeGenericMapTable(w, response.Metrics)
}

func renderAppObservabilityLogs(w io.Writer, response appObservabilityLogsQueryResponse) error {
	if err := renderAppObservabilityHeader(w, response.Source, response.Window); err != nil {
		return err
	}
	if err := writeKeyValues(w, kvPair{Key: "logs", Value: formatInt(len(response.Logs))}); err != nil {
		return err
	}
	if len(response.Logs) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeGenericMapTable(w, response.Logs)
}

func renderAppObservabilityRequests(w io.Writer, response appObservabilityRequestsResponse) error {
	if err := renderAppObservabilityHeader(w, response.Source, response.Window); err != nil {
		return err
	}
	if err := writeKeyValues(w, kvPair{Key: "requests", Value: formatInt(len(response.Requests))}); err != nil {
		return err
	}
	if len(response.Requests) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeGenericMapTable(w, response.Requests)
}

func renderAppObservabilityTrace(w io.Writer, response appObservabilityTraceResponse) error {
	if err := renderAppObservabilityHeader(w, response.Source, appObservabilityWindow{}); err != nil {
		return err
	}
	if err := writeKeyValues(w,
		kvPair{Key: "trace_id", Value: response.TraceID},
		kvPair{Key: "spans", Value: formatInt(len(response.Spans))},
	); err != nil {
		return err
	}
	if len(response.Spans) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeGenericMapTable(w, response.Spans)
}

func renderAppObservabilityDiagnosis(w io.Writer, response appObservabilityDiagnosisResponse) error {
	if err := renderAppObservabilityHeader(w, response.Source, response.Window); err != nil {
		return err
	}
	if err := writeKeyValues(w,
		kvPair{Key: "bottleneck", Value: strings.TrimSpace(response.Diagnosis.Bottleneck)},
		kvPair{Key: "confidence", Value: fmt.Sprintf("%.2f", response.Diagnosis.Confidence)},
	); err != nil {
		return err
	}
	for _, evidence := range response.Diagnosis.Evidence {
		if _, err := fmt.Fprintf(w, "evidence=%s\n", strings.TrimSpace(evidence)); err != nil {
			return err
		}
	}
	for _, action := range response.Diagnosis.NextActions {
		if _, err := fmt.Fprintf(w, "next_action=%s\n", strings.TrimSpace(action)); err != nil {
			return err
		}
	}
	return nil
}

func renderAppObservabilityHeader(w io.Writer, source appObservabilitySourceStatus, window appObservabilityWindow) error {
	pairs := []kvPair{
		{Key: "observability_status", Value: strings.TrimSpace(source.Status)},
		{Key: "available", Value: fmt.Sprintf("%t", source.Available)},
		{Key: "mode", Value: strings.TrimSpace(source.Mode)},
		{Key: "retention", Value: strings.TrimSpace(source.Retention)},
		{Key: "active_exporters", Value: strings.Join(source.ActiveExporters, ",")},
		{Key: "reason", Value: strings.TrimSpace(source.Reason)},
	}
	if strings.TrimSpace(source.Freshness) != "" {
		pairs = append(pairs, kvPair{Key: "freshness", Value: strings.TrimSpace(source.Freshness)})
	}
	if strings.TrimSpace(window.Since) != "" || strings.TrimSpace(window.Until) != "" {
		pairs = append(pairs,
			kvPair{Key: "since", Value: strings.TrimSpace(window.Since)},
			kvPair{Key: "until", Value: strings.TrimSpace(window.Until)},
		)
	}
	return writeKeyValues(w, pairs...)
}

func writeGenericMapTable(w io.Writer, rows []map[string]any) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ROW\tDATA"); err != nil {
		return err
	}
	for index, row := range rows {
		if _, err := fmt.Fprintf(tw, "%d\t%v\n", index+1, row); err != nil {
			return err
		}
	}
	return tw.Flush()
}

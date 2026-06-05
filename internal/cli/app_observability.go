package cli

import (
	"encoding/json"
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
			if opts.Follow {
				return c.streamAppObservabilityRequests(client, app.ID, opts)
			}
			response, err := client.ListAppObservabilityRequests(app.ID, opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderAppObservabilityRequests(c.stdout, response, appObservabilityRequestFields(opts.Fields))
		},
	}
	addAppObservabilityWindowFlags(cmd, &opts.appObservabilityWindowOptions)
	cmd.Flags().IntVar(&opts.Limit, "limit", opts.Limit, "Maximum request summaries to return")
	cmd.Flags().StringVar(&opts.TraceID, "trace", "", "Limit request summaries to one trace identifier")
	cmd.Flags().StringVar(&opts.StatusClass, "status-class", "", "Limit request summaries to one status class")
	cmd.Flags().BoolVar(&opts.Slow, "slow", false, "Only show slow request summaries")
	cmd.Flags().BoolVar(&opts.Errors, "errors", false, "Only show error request summaries")
	cmd.Flags().BoolVar(&opts.Follow, "follow", false, "Follow request summaries from the observability stream")
	cmd.Flags().StringVar(&opts.Fields, "fields", "", "Comma-separated fields to print, for example timestamp,status,duration,summary.stage")
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

func renderAppObservabilityRequests(w io.Writer, response appObservabilityRequestsResponse, fields []string) error {
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
	if len(fields) > 0 {
		return writeAppObservabilityRequestFieldTable(w, response.Requests, fields)
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

type appObservabilityRequestStreamReadyEvent struct {
	Cursor string                       `json:"cursor"`
	Source appObservabilitySourceStatus `json:"source"`
	Window appObservabilityWindow       `json:"window"`
	Follow bool                         `json:"follow"`
}

type appObservabilityRequestStreamRequestEvent struct {
	Cursor  string         `json:"cursor"`
	Request map[string]any `json:"request"`
}

type appObservabilityRequestStreamWarningEvent struct {
	Cursor  string `json:"cursor"`
	Message string `json:"message"`
}

type appObservabilityRequestStreamEndEvent struct {
	Cursor string `json:"cursor"`
	Reason string `json:"reason"`
}

func (c *CLI) streamAppObservabilityRequests(client *Client, appID string, opts appObservabilityRequestsOptions) error {
	if c.wantsJSON() {
		return client.StreamAppObservabilityRequests(appID, opts, func(event sseEvent) error {
			return c.writeStreamJSON(event)
		})
	}
	fields := appObservabilityRequestFields(opts.Fields)
	out := newRuntimeFollowTextOutput(c.stdout, c.progressf)
	headerWritten := false
	err := client.StreamAppObservabilityRequests(appID, opts, func(event sseEvent) error {
		switch event.Event {
		case "ready":
			var payload appObservabilityRequestStreamReadyEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			c.progressf("observability_status=%s available=%t reason=%s", strings.TrimSpace(payload.Source.Status), payload.Source.Available, strings.TrimSpace(payload.Source.Reason))
		case "request":
			var payload appObservabilityRequestStreamRequestEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			if !headerWritten {
				if err := out.enqueue(strings.Join(appObservabilityRequestFieldHeaders(fields), "\t")); err != nil {
					return err
				}
				headerWritten = true
			}
			return out.enqueue(appObservabilityRequestFieldLine(payload.Request, fields))
		case "warning":
			var payload appObservabilityRequestStreamWarningEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			c.progressf("warning=%s", strings.TrimSpace(payload.Message))
		case "end":
			var payload appObservabilityRequestStreamEndEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			if reason := strings.TrimSpace(payload.Reason); reason != "" {
				c.progressf("end=%s", reason)
			}
		}
		return nil
	})
	if closeErr := out.close(); err == nil {
		err = closeErr
	}
	return err
}

func appObservabilityRequestFields(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	fields := make([]string, 0, len(parts))
	for _, part := range parts {
		field := normalizeAppObservabilityRequestField(part)
		if field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}

func normalizeAppObservabilityRequestField(raw string) string {
	field := strings.TrimSpace(raw)
	switch strings.ToLower(field) {
	case "":
		return ""
	case "status":
		return "status_code"
	case "duration":
		return "duration_ms"
	case "ttft", "ttfb":
		return "ttft_ms"
	default:
		return field
	}
}

func writeAppObservabilityRequestFieldTable(w io.Writer, rows []map[string]any, fields []string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, strings.Join(appObservabilityRequestFieldHeaders(fields), "\t")); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintln(tw, appObservabilityRequestFieldLine(row, fields)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func appObservabilityRequestFieldHeaders(fields []string) []string {
	if len(fields) == 0 {
		fields = []string{"timestamp", "status_code", "duration_ms", "ttft_ms", "route"}
	}
	headers := make([]string, 0, len(fields))
	for _, field := range fields {
		headers = append(headers, strings.ToUpper(strings.ReplaceAll(field, ".", "_")))
	}
	return headers
}

func appObservabilityRequestFieldLine(row map[string]any, fields []string) string {
	if len(fields) == 0 {
		fields = []string{"timestamp", "status_code", "duration_ms", "ttft_ms", "route"}
	}
	values := make([]string, 0, len(fields))
	for _, field := range fields {
		values = append(values, appObservabilityRequestFieldString(row, field))
	}
	return strings.Join(values, "\t")
}

func appObservabilityRequestFieldString(row map[string]any, field string) string {
	value, ok := appObservabilityRequestFieldValue(row, field)
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return fmt.Sprintf("%.4g", typed)
	case float32:
		return fmt.Sprintf("%.4g", typed)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(typed)
	default:
		encoded, err := json.Marshal(typed)
		if err == nil {
			return string(encoded)
		}
		return fmt.Sprint(typed)
	}
}

func appObservabilityRequestFieldValue(row map[string]any, field string) (any, bool) {
	parts := strings.Split(strings.TrimSpace(field), ".")
	if len(parts) == 0 || parts[0] == "" {
		return nil, false
	}
	var current any = row
	for _, part := range parts {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

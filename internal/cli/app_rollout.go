package cli

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type appRolloutTimelineCommandOptions struct {
	Around string
	Window time.Duration
}

func (c *CLI) newAppRolloutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rollout",
		Short: "Inspect app rollout history and live state",
	}
	cmd.AddCommand(c.newAppRolloutTimelineCommand())
	return cmd
}

func (c *CLI) newAppRolloutTimelineCommand() *cobra.Command {
	opts := appRolloutTimelineCommandOptions{Window: 10 * time.Minute}
	cmd := &cobra.Command{
		Use:   "timeline <app>",
		Short: "Summarize rollout operations, Kubernetes state, endpoint samples, and 5xx around a request or time",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.Around) == "" {
				return fmt.Errorf("--around is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.GetAppRolloutTimeline(app.ID, appRolloutTimelineOptions{
				Around: opts.Around,
				Window: opts.Window,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderAppRolloutTimeline(c.stdout, response)
		},
	}
	cmd.Flags().StringVar(&opts.Around, "around", "", "Request id, trace id, or RFC3339 timestamp to center the timeline")
	cmd.Flags().DurationVar(&opts.Window, "window", opts.Window, "Duration on each side of --around")
	return cmd
}

func renderAppRolloutTimeline(w io.Writer, response appRolloutTimelineResponse) error {
	if err := renderAppObservabilityHeader(w, response.Source, response.Window); err != nil {
		return err
	}
	if err := writeKeyValues(w,
		kvPair{Key: "around", Value: strings.TrimSpace(fmt.Sprint(response.Around["value"]))},
		kvPair{Key: "around_kind", Value: strings.TrimSpace(fmt.Sprint(response.Around["kind"]))},
		kvPair{Key: "around_time", Value: strings.TrimSpace(fmt.Sprint(response.Around["time"]))},
		kvPair{Key: "operations", Value: formatInt(len(response.Operations))},
		kvPair{Key: "events", Value: formatInt(len(response.Events))},
		kvPair{Key: "requests_5xx", Value: formatInt(len(response.Requests5xx))},
	); err != nil {
		return err
	}
	if endpoints := mapField(response.Kubernetes, "endpoints"); len(endpoints) > 0 {
		if err := writeKeyValues(w,
			kvPair{Key: "service_name", Value: strings.TrimSpace(fmt.Sprint(endpoints["service_name"]))},
			kvPair{Key: "ready_endpoints", Value: strings.TrimSpace(fmt.Sprint(endpoints["ready_endpoints"]))},
			kvPair{Key: "total_endpoints", Value: strings.TrimSpace(fmt.Sprint(endpoints["total_endpoints"]))},
		); err != nil {
			return err
		}
	}
	for _, warning := range response.Warnings {
		if _, err := fmt.Fprintf(w, "warning=%s\n", strings.TrimSpace(warning)); err != nil {
			return err
		}
	}
	if err := renderTimelineSection(w, "operations", response.Operations); err != nil {
		return err
	}
	if err := renderTimelineSection(w, "rollout_events", response.Events); err != nil {
		return err
	}
	if err := renderTimelineSection(w, "requests_5xx", response.Requests5xx); err != nil {
		return err
	}
	if deployment := mapField(response.Kubernetes, "deployment"); len(deployment) > 0 {
		if _, err := fmt.Fprintln(w, "\ndeployment"); err != nil {
			return err
		}
		if err := writeKeyValues(w,
			kvPair{Key: "name", Value: strings.TrimSpace(fmt.Sprint(deployment["name"]))},
			kvPair{Key: "strategy", Value: strings.TrimSpace(fmt.Sprint(deployment["strategy"]))},
			kvPair{Key: "ready_replicas", Value: strings.TrimSpace(fmt.Sprint(deployment["ready_replicas"]))},
			kvPair{Key: "available_replicas", Value: strings.TrimSpace(fmt.Sprint(deployment["available_replicas"]))},
			kvPair{Key: "unavailable_replicas", Value: strings.TrimSpace(fmt.Sprint(deployment["unavailable_replicas"]))},
		); err != nil {
			return err
		}
	}
	if err := renderTimelineSection(w, "replica_sets", mapSliceField(response.Kubernetes, "replica_sets")); err != nil {
		return err
	}
	if err := renderTimelineSection(w, "pods", mapSliceField(response.Kubernetes, "pods")); err != nil {
		return err
	}
	return renderTimelineSection(w, "kubernetes_events", mapSliceField(response.Kubernetes, "events"))
}

func renderTimelineSection(w io.Writer, title string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(w, "\n%s\n", title); err != nil {
		return err
	}
	return writeGenericMapTable(w, rows)
}

func mapField(values map[string]any, key string) map[string]any {
	if len(values) == 0 {
		return nil
	}
	if direct, ok := values[key].(map[string]any); ok {
		return direct
	}
	if raw, ok := values[key].(map[string]interface{}); ok {
		out := make(map[string]any, len(raw))
		for key, value := range raw {
			out[key] = value
		}
		return out
	}
	return nil
}

func mapSliceField(values map[string]any, key string) []map[string]any {
	if len(values) == 0 {
		return nil
	}
	if direct, ok := values[key].([]map[string]any); ok {
		return direct
	}
	rawItems, ok := values[key].([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(rawItems))
	for _, raw := range rawItems {
		if item, ok := raw.(map[string]any); ok {
			out = append(out, item)
		}
	}
	return out
}

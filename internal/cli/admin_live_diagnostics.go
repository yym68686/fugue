package cli

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/livediagnostics"

	"github.com/spf13/cobra"
)

type platformDiagnosticCommandOptions struct {
	direct          bool
	kubeconfig      string
	kubeContext     string
	controlNS       string
	releaseInstance string
}

func (c *CLI) newAdminDiagnosticsCommand() *cobra.Command {
	opts := platformDiagnosticCommandOptions{controlNS: "fugue-system", releaseInstance: "fugue"}
	cmd := &cobra.Command{
		Use:   "diagnostics",
		Short: "Run bounded diagnostics against Fugue platform targets",
		Long: strings.TrimSpace(`
Run temporary CPU or memory probes against trusted Fugue components and
allowlisted host processes. --direct-kubernetes is a break-glass control path
that uses the current kubeconfig when the control-plane API is unavailable.
`),
	}
	flags := cmd.PersistentFlags()
	flags.BoolVar(&opts.direct, "direct-kubernetes", false, "Use Kubernetes directly instead of the Fugue API")
	flags.StringVar(&opts.kubeconfig, "kubeconfig", "", "Kubeconfig path for direct Kubernetes mode")
	flags.StringVar(&opts.kubeContext, "kube-context", "", "Kubeconfig context for direct Kubernetes mode")
	flags.StringVar(&opts.controlNS, "control-namespace", opts.controlNS, "Fugue control-plane namespace")
	flags.StringVar(&opts.releaseInstance, "release-instance", opts.releaseInstance, "Fugue Helm release instance label")
	cmd.AddCommand(
		c.newAdminDiagnosticsStartCommand(&opts),
		c.newAdminDiagnosticsListCommand(&opts),
		c.newAdminDiagnosticsShowCommand(&opts),
		c.newAdminDiagnosticsReportCommand(&opts),
		c.newAdminDiagnosticsCancelCommand(&opts),
	)
	return cmd
}

func (c *CLI) newAdminDiagnosticsStartCommand(opts *platformDiagnosticCommandOptions) *cobra.Command {
	request := platformDiagnosticStartRequest{
		Target: platformDiagnosticTargetRequest{Type: livediagnostics.TargetPlatformComponent},
		Kind:   livediagnostics.ProbeCPUProfile, DurationSeconds: 60, FrequencyHz: 19, SampleIntervalMilliseconds: 1000,
	}
	wait := false
	waitTimeout := 8 * time.Minute
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a platform live diagnostic session",
		RunE: func(cmd *cobra.Command, _ []string) error {
			probe := livediagnostics.StartRequest{
				Kind: request.Kind, DurationSeconds: request.DurationSeconds, FrequencyHz: request.FrequencyHz,
				SampleIntervalMilliseconds: request.SampleIntervalMilliseconds,
			}
			if err := probe.Normalize(); err != nil {
				return err
			}
			request.Kind, request.DurationSeconds, request.FrequencyHz = probe.Kind, probe.DurationSeconds, probe.FrequencyHz
			request.SampleIntervalMilliseconds = probe.SampleIntervalMilliseconds
			if opts.direct {
				direct, err := newDirectPlatformDiagnosticClient(*opts)
				if err != nil {
					return err
				}
				response, err := direct.Start(cmd.Context(), request)
				if err != nil {
					return err
				}
				return c.finishPlatformDiagnosticStart(cmd, response, wait, waitTimeout, direct.Get, direct.Report)
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.StartPlatformDiagnosticSession(request)
			if err != nil {
				return err
			}
			return c.finishPlatformDiagnosticStart(cmd, response, wait, waitTimeout,
				func(_ context.Context, id string) (platformDiagnosticSessionResponse, error) {
					return client.GetPlatformDiagnosticSession(id)
				},
				func(_ context.Context, id string) (platformDiagnosticReportResponse, error) {
					return client.GetPlatformDiagnosticReport(id)
				},
			)
		},
	}
	cmd.Flags().StringVar((*string)(&request.Target.Type), "target-type", string(request.Target.Type), "Target type (platform_component or node_process)")
	cmd.Flags().StringVar(&request.Target.Component, "component", "", "Fugue component label, such as api or controller")
	cmd.Flags().StringVar(&request.Target.Namespace, "target-namespace", "", "Target component namespace")
	cmd.Flags().StringVar(&request.Target.Pod, "pod", "", "Exact ready target Pod")
	cmd.Flags().StringVar(&request.Target.Container, "container", "", "Exact target container")
	cmd.Flags().StringVar(&request.Target.Node, "node", "", "Ready node for a node_process target")
	cmd.Flags().StringVar(&request.Target.ProcessName, "process", "", "Allowlisted host process name")
	cmd.Flags().StringVar((*string)(&request.Kind), "kind", string(request.Kind), "Probe kind (cpu-profile, memory-profile, or process-snapshot)")
	cmd.Flags().IntVar(&request.DurationSeconds, "duration", request.DurationSeconds, "Diagnostic duration in seconds")
	cmd.Flags().IntVar(&request.FrequencyHz, "frequency", request.FrequencyHz, "CPU sampling frequency in Hz")
	cmd.Flags().IntVar(&request.SampleIntervalMilliseconds, "sample-interval-ms", request.SampleIntervalMilliseconds, "Memory sampling interval in milliseconds")
	cmd.Flags().BoolVar(&wait, "wait", false, "Wait for completion and print the report")
	cmd.Flags().DurationVar(&waitTimeout, "wait-timeout", waitTimeout, "Maximum time to wait for the report")
	return cmd
}

type platformDiagnosticGetFunc func(context.Context, string) (platformDiagnosticSessionResponse, error)
type platformDiagnosticReportFunc func(context.Context, string) (platformDiagnosticReportResponse, error)

func (c *CLI) finishPlatformDiagnosticStart(cmd *cobra.Command, response platformDiagnosticSessionResponse, wait bool, waitTimeout time.Duration, get platformDiagnosticGetFunc, report platformDiagnosticReportFunc) error {
	if !wait {
		return c.renderPlatformDiagnosticSession(response.Session)
	}
	deadline := time.Now().Add(waitTimeout)
	for response.Session.Status == "queued" || response.Session.Status == "running" {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for diagnostic session %s", response.Session.ID)
		}
		select {
		case <-cmd.Context().Done():
			return cmd.Context().Err()
		case <-time.After(2 * time.Second):
		}
		var err error
		response, err = get(cmd.Context(), response.Session.ID)
		if err != nil {
			return err
		}
	}
	result, err := report(cmd.Context(), response.Session.ID)
	if err != nil {
		return err
	}
	if c.wantsJSON() {
		if err := writeJSON(c.stdout, result); err != nil {
			return err
		}
	} else if err := renderPlatformDiagnosticReport(c.stdout, result); err != nil {
		return err
	}
	if response.Session.Status != "succeeded" {
		return fmt.Errorf("diagnostic session %s failed: %s", response.Session.ID, response.Session.FailureReason)
	}
	return nil
}

func (c *CLI) newAdminDiagnosticsListCommand(opts *platformDiagnosticCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List recent platform diagnostic sessions",
		RunE: func(cmd *cobra.Command, _ []string) error {
			var response platformDiagnosticSessionListResponse
			var err error
			if opts.direct {
				direct, directErr := newDirectPlatformDiagnosticClient(*opts)
				if directErr != nil {
					return directErr
				}
				response, err = direct.List(cmd.Context())
			} else {
				client, clientErr := c.newClient()
				if clientErr != nil {
					return clientErr
				}
				response, err = client.ListPlatformDiagnosticSessions()
			}
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderPlatformDiagnosticSessions(c.stdout, response.Sessions)
		},
	}
}

func (c *CLI) newAdminDiagnosticsShowCommand(opts *platformDiagnosticCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "show <session-id>",
		Short: "Show platform diagnostic session status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var response platformDiagnosticSessionResponse
			var err error
			if opts.direct {
				direct, directErr := newDirectPlatformDiagnosticClient(*opts)
				if directErr != nil {
					return directErr
				}
				response, err = direct.Get(cmd.Context(), args[0])
			} else {
				client, clientErr := c.newClient()
				if clientErr != nil {
					return clientErr
				}
				response, err = client.GetPlatformDiagnosticSession(args[0])
			}
			if err != nil {
				return err
			}
			return c.renderPlatformDiagnosticSession(response.Session)
		},
	}
}

func (c *CLI) newAdminDiagnosticsReportCommand(opts *platformDiagnosticCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "report <session-id>",
		Short: "Read a completed platform diagnostic report",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var response platformDiagnosticReportResponse
			var err error
			if opts.direct {
				direct, directErr := newDirectPlatformDiagnosticClient(*opts)
				if directErr != nil {
					return directErr
				}
				response, err = direct.Report(cmd.Context(), args[0])
			} else {
				client, clientErr := c.newClient()
				if clientErr != nil {
					return clientErr
				}
				response, err = client.GetPlatformDiagnosticReport(args[0])
			}
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderPlatformDiagnosticReport(c.stdout, response)
		},
	}
}

func (c *CLI) newAdminDiagnosticsCancelCommand(opts *platformDiagnosticCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <session-id>",
		Short: "Cancel and remove a platform diagnostic session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var response platformDiagnosticSessionCancelResponse
			var err error
			if opts.direct {
				direct, directErr := newDirectPlatformDiagnosticClient(*opts)
				if directErr != nil {
					return directErr
				}
				response, err = direct.Cancel(cmd.Context(), args[0])
			} else {
				client, clientErr := c.newClient()
				if clientErr != nil {
					return clientErr
				}
				response, err = client.CancelPlatformDiagnosticSession(args[0])
			}
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "diagnostic_session", Value: response.Session.ID},
				kvPair{Key: "canceled", Value: strconv.FormatBool(response.Canceled)},
			)
		},
	}
}

func (c *CLI) renderPlatformDiagnosticSession(session platformDiagnosticSession) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, platformDiagnosticSessionResponse{Session: session})
	}
	return writeKeyValues(c.stdout,
		kvPair{Key: "diagnostic_session", Value: session.ID},
		kvPair{Key: "status", Value: session.Status},
		kvPair{Key: "kind", Value: string(session.Kind)},
		kvPair{Key: "target", Value: formatPlatformDiagnosticTarget(session.Target)},
		kvPair{Key: "node", Value: session.Target.Node},
		kvPair{Key: "control_path", Value: session.ControlPath},
		kvPair{Key: "duration_seconds", Value: formatInt(session.DurationSeconds)},
		kvPair{Key: "sample_interval_ms", Value: formatInt(session.SampleIntervalMilliseconds)},
		kvPair{Key: "failure_reason", Value: session.FailureReason},
	)
}

func renderPlatformDiagnosticSessions(w io.Writer, sessions []platformDiagnosticSession) error {
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].CreatedAt.After(sessions[j].CreatedAt) })
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SESSION\tSTATUS\tKIND\tTARGET\tNODE\tCONTROL"); err != nil {
		return err
	}
	for _, session := range sessions {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", session.ID, session.Status, session.Kind, formatPlatformDiagnosticTarget(session.Target), session.Target.Node, session.ControlPath); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatPlatformDiagnosticTarget(target livediagnostics.Target) string {
	if target.Type == livediagnostics.TargetNodeProcess {
		return target.Node + "/" + target.ProcessName
	}
	return target.Namespace + "/" + target.Pod + "/" + target.Container
}

func renderPlatformDiagnosticReport(w io.Writer, response platformDiagnosticReportResponse) error {
	report := response.Report
	if err := writeKeyValues(w,
		kvPair{Key: "diagnostic_session", Value: response.Session.ID},
		kvPair{Key: "status", Value: response.Session.Status},
		kvPair{Key: "schema", Value: stringValue(report["schema"])},
		kvPair{Key: "peak_process_rss_bytes", Value: fmt.Sprint(report["peak_process_rss_bytes"])},
		kvPair{Key: "peak_cgroup_memory_bytes", Value: fmt.Sprint(report["peak_cgroup_memory_bytes"])},
		kvPair{Key: "oom_events_delta", Value: fmt.Sprint(report["oom_events_delta"])},
		kvPair{Key: "oom_kills_delta", Value: fmt.Sprint(report["oom_kills_delta"])},
		kvPair{Key: "go_runtime_profile_available", Value: fmt.Sprint(report["go_runtime_profile_available"])},
	); err != nil {
		return err
	}
	for _, field := range []struct{ key, title string }{
		{"go_inuse_space_top", "go_live_heap_top"},
		{"go_alloc_space_delta_top", "go_allocation_delta_top"},
	} {
		value := strings.TrimSpace(fmt.Sprint(report[field.key]))
		if value == "" || value == "<nil>" {
			continue
		}
		if _, err := fmt.Fprintf(w, "\n%s\n%s\n", field.title, value); err != nil {
			return err
		}
	}
	if functions, ok := report["leaf_functions"].([]any); ok {
		if _, err := fmt.Fprintln(w, "\nleaf_functions"); err != nil {
			return err
		}
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "SAMPLES\tFUNCTION")
		for _, item := range functions {
			fields, _ := item.(map[string]any)
			_, _ = fmt.Fprintf(tw, "%v\t%s\n", fields["samples"], strings.TrimSpace(fmt.Sprint(fields["function"])))
		}
		return tw.Flush()
	}
	return nil
}

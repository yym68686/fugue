package cli

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppDiagnosticsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "diagnostics",
		Aliases: []string{"probe"},
		Short:   "Run bounded live diagnostic sessions",
	}
	cmd.AddCommand(
		c.newAppDiagnosticsStartCommand(),
		c.newAppDiagnosticsListCommand(),
		c.newAppDiagnosticsShowCommand(),
		c.newAppDiagnosticsReportCommand(),
		c.newAppDiagnosticsCancelCommand(),
	)
	return cmd
}

func (c *CLI) newAppDiagnosticsStartCommand() *cobra.Command {
	request := appDiagnosticSessionStartRequest{Kind: "cpu-profile", DurationSeconds: 60, FrequencyHz: 19}
	wait := false
	waitTimeout := 3 * time.Minute
	cmd := &cobra.Command{
		Use:   "start <app>",
		Short: "Start a temporary digest-addressed diagnostic probe",
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
			response, err := client.StartAppDiagnosticSession(app.ID, request)
			if err != nil {
				return err
			}
			if !wait {
				return c.renderDiagnosticSession(response.Session)
			}
			deadline := time.Now().Add(waitTimeout)
			for response.Session.Status == "queued" || response.Session.Status == "running" {
				if time.Now().After(deadline) {
					return fmt.Errorf("timed out waiting for diagnostic session %s", response.Session.ID)
				}
				time.Sleep(2 * time.Second)
				response, err = client.GetAppDiagnosticSession(app.ID, response.Session.ID)
				if err != nil {
					return err
				}
			}
			report, err := client.GetAppDiagnosticReport(app.ID, response.Session.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				if err := writeJSON(c.stdout, report); err != nil {
					return err
				}
			} else if err := renderDiagnosticReport(c.stdout, report); err != nil {
				return err
			}
			if response.Session.Status != "succeeded" {
				return fmt.Errorf("diagnostic session %s failed: %s", response.Session.ID, response.Session.FailureReason)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&request.Kind, "kind", request.Kind, "Probe kind (cpu-profile)")
	cmd.Flags().IntVar(&request.DurationSeconds, "duration", request.DurationSeconds, "Profile duration in seconds (5-120)")
	cmd.Flags().IntVar(&request.FrequencyHz, "frequency", request.FrequencyHz, "Sampling frequency in Hz (1-99)")
	cmd.Flags().StringVar(&request.Pod, "pod", "", "Target a specific ready app pod")
	cmd.Flags().StringVar(&request.Container, "container", "", "Target a specific app container")
	cmd.Flags().BoolVar(&wait, "wait", false, "Wait for completion and print the report")
	cmd.Flags().DurationVar(&waitTimeout, "wait-timeout", waitTimeout, "Maximum time to wait for the report")
	return cmd
}

func (c *CLI) newAppDiagnosticsListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list <app>",
		Short: "List recent diagnostic sessions",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, app, err := c.diagnosticClientAndApp(args[0])
			if err != nil {
				return err
			}
			response, err := client.ListAppDiagnosticSessions(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderDiagnosticSessions(c.stdout, response.Sessions)
		},
	}
}

func (c *CLI) newAppDiagnosticsShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <app> <session-id>",
		Short: "Show diagnostic session status",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, app, err := c.diagnosticClientAndApp(args[0])
			if err != nil {
				return err
			}
			response, err := client.GetAppDiagnosticSession(app.ID, args[1])
			if err != nil {
				return err
			}
			return c.renderDiagnosticSession(response.Session)
		},
	}
}

func (c *CLI) newAppDiagnosticsReportCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "report <app> <session-id>",
		Short: "Read a completed diagnostic report",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, app, err := c.diagnosticClientAndApp(args[0])
			if err != nil {
				return err
			}
			response, err := client.GetAppDiagnosticReport(app.ID, args[1])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderDiagnosticReport(c.stdout, response)
		},
	}
}

func (c *CLI) newAppDiagnosticsCancelCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <app> <session-id>",
		Short: "Cancel and remove a diagnostic session",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, app, err := c.diagnosticClientAndApp(args[0])
			if err != nil {
				return err
			}
			response, err := client.CancelAppDiagnosticSession(app.ID, args[1])
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

func (c *CLI) diagnosticClientAndApp(reference string) (*Client, model.App, error) {
	client, err := c.newClient()
	if err != nil {
		return nil, model.App{}, err
	}
	app, err := c.resolveNamedApp(client, reference)
	return client, app, err
}

func (c *CLI) renderDiagnosticSession(session appDiagnosticSession) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, appDiagnosticSessionResponse{Session: session})
	}
	return writeKeyValues(c.stdout,
		kvPair{Key: "diagnostic_session", Value: session.ID},
		kvPair{Key: "status", Value: session.Status},
		kvPair{Key: "kind", Value: session.Kind},
		kvPair{Key: "target", Value: session.TargetPod + "/" + session.TargetContainer},
		kvPair{Key: "node", Value: session.TargetNode},
		kvPair{Key: "duration_seconds", Value: formatInt(session.DurationSeconds)},
		kvPair{Key: "frequency_hz", Value: formatInt(session.FrequencyHz)},
		kvPair{Key: "failure_reason", Value: session.FailureReason},
	)
}

func renderDiagnosticSessions(w io.Writer, sessions []appDiagnosticSession) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SESSION\tSTATUS\tKIND\tTARGET\tDURATION\tFREQUENCY"); err != nil {
		return err
	}
	for _, session := range sessions {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s/%s\t%ds\t%dHz\n", session.ID, session.Status, session.Kind, session.TargetPod, session.TargetContainer, session.DurationSeconds, session.FrequencyHz); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func renderDiagnosticReport(w io.Writer, response appDiagnosticReportResponse) error {
	if err := writeKeyValues(w,
		kvPair{Key: "diagnostic_session", Value: response.Session.ID},
		kvPair{Key: "status", Value: response.Session.Status},
		kvPair{Key: "schema", Value: stringValue(response.Report["schema"])},
		kvPair{Key: "samples", Value: fmt.Sprint(response.Report["samples"])},
		kvPair{Key: "user_samples", Value: fmt.Sprint(response.Report["user_samples"])},
		kvPair{Key: "kernel_samples", Value: fmt.Sprint(response.Report["kernel_samples"])},
		kvPair{Key: "lost_samples", Value: fmt.Sprint(response.Report["lost_samples"])},
	); err != nil {
		return err
	}
	functions, ok := response.Report["leaf_functions"].([]any)
	if !ok {
		return errors.New("diagnostic report does not contain leaf_functions")
	}
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

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

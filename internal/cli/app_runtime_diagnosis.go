package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppDiagnosisCommand() *cobra.Command {
	opts := struct {
		Component string
		Window    string
	}{Component: "app"}
	cmd := &cobra.Command{
		Use:   "diagnose <app>",
		Short: "Explain the most likely runtime or observability root cause for an app",
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
			if strings.TrimSpace(opts.Window) != "" {
				if cmd.Flags().Changed("component") {
					return fmt.Errorf("--component cannot be combined with --window")
				}
				diagnosis, err := client.GetAppObservabilityDiagnosis(app.ID, appObservabilityDiagnosisOptions{
					appObservabilityWindowOptions: appObservabilityWindowOptions{Since: opts.Window},
				})
				if err != nil {
					return err
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, diagnosis)
				}
				return renderAppObservabilityDiagnosis(c.stdout, diagnosis)
			}
			diagnosis, err := client.GetAppDiagnosis(app.ID, opts.Component)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"diagnosis": diagnosis})
			}
			return renderAppDiagnosis(c.stdout, diagnosis)
		},
	}
	cmd.Flags().StringVar(&opts.Component, "component", opts.Component, "Runtime component: app or postgres")
	cmd.Flags().StringVar(&opts.Window, "window", "", "Use Fugue Observability diagnosis for a relative window like 5m")
	return cmd
}

func renderAppDiagnosis(w io.Writer, diagnosis appDiagnosis) error {
	pairs := []kvPair{
		{Key: "category", Value: strings.TrimSpace(diagnosis.Category)},
		{Key: "summary", Value: strings.TrimSpace(diagnosis.Summary)},
		{Key: "hint", Value: strings.TrimSpace(diagnosis.Hint)},
		{Key: "component", Value: strings.TrimSpace(diagnosis.Component)},
		{Key: "namespace", Value: strings.TrimSpace(diagnosis.Namespace)},
		{Key: "selector", Value: strings.TrimSpace(diagnosis.Selector)},
		{Key: "implicated_node", Value: strings.TrimSpace(diagnosis.ImplicatedNode)},
		{Key: "implicated_pod", Value: strings.TrimSpace(diagnosis.ImplicatedPod)},
		{Key: "live_pods", Value: formatInt(diagnosis.LivePods)},
		{Key: "ready_pods", Value: formatInt(diagnosis.ReadyPods)},
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	for _, evidence := range diagnosis.Evidence {
		if _, err := fmt.Fprintf(w, "evidence=%s\n", evidence); err != nil {
			return err
		}
	}
	for _, warning := range diagnosis.Warnings {
		if _, err := fmt.Fprintf(w, "warning=%s\n", warning); err != nil {
			return err
		}
	}
	if len(diagnosis.Events) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "events"); err != nil {
		return err
	}
	return writeClusterEventTable(w, diagnosis.Events)
}

func appDiagnosisToOverviewDiagnosis(diagnosis *appDiagnosis) *appOverviewDiagnosis {
	if diagnosis == nil {
		return nil
	}
	evidence := append([]string(nil), diagnosis.Evidence...)
	for _, warning := range diagnosis.Warnings {
		evidence = appendUniqueString(evidence, warning)
	}
	return &appOverviewDiagnosis{
		Category: strings.TrimSpace(diagnosis.Category),
		Summary:  strings.TrimSpace(diagnosis.Summary),
		Hint:     strings.TrimSpace(diagnosis.Hint),
		Evidence: evidence,
	}
}

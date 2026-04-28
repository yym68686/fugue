package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appResourcesOptions struct {
	WindowHours int
	MinSamples  int
	Wait        bool
	Mode        string
}

func (c *CLI) newAppResourcesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resources",
		Short: "Inspect and apply resource right-sizing recommendations",
	}
	cmd.AddCommand(
		c.newAppResourcesRecommendCommand(),
		c.newAppResourcesApplyCommand(),
		c.newAppResourcesAutoCommand(),
	)
	return cmd
}

func (c *CLI) newAppResourcesRecommendCommand() *cobra.Command {
	opts := appResourcesOptions{WindowHours: 168, MinSamples: 12}
	cmd := &cobra.Command{
		Use:   "recommend <app>",
		Short: "Show resource request recommendations",
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
			response, err := client.GetAppResourceRecommendation(app.ID, opts.WindowHours, opts.MinSamples)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeResourceRecommendationTable(c.stdout, response.Recommendation)
		},
	}
	bindAppResourcesWindowFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newAppResourcesApplyCommand() *cobra.Command {
	opts := appResourcesOptions{WindowHours: 168, MinSamples: 12}
	cmd := &cobra.Command{
		Use:   "apply <app>",
		Short: "Queue a deploy operation with recommended resource requests",
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
			response, err := client.ApplyAppResourceRecommendation(app.ID, opts.WindowHours, opts.MinSamples)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeResourceRecommendationTable(c.stdout, response.Recommendation); err != nil {
				return err
			}
			if response.Operation == nil {
				_, err = fmt.Fprintln(c.stdout, "already_current=true")
				return err
			}
			result := appCommandResult{Operation: response.Operation}
			if opts.Wait {
				if finalApp, err := c.waitForSingleApp(client, app.ID, *response.Operation, true); err != nil {
					return err
				} else if finalApp != nil {
					result.App = finalApp
				}
			}
			return c.renderAppCommandResult(result)
		},
	}
	bindAppResourcesWindowFlags(cmd, &opts)
	cmd.Flags().BoolVar(&opts.Wait, "wait", false, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newAppResourcesAutoCommand() *cobra.Command {
	opts := appResourcesOptions{WindowHours: 168, MinSamples: 12, Mode: model.AppRightSizingModeAuto}
	cmd := &cobra.Command{
		Use:   "auto <app>",
		Short: "Configure automatic right-sizing for an app",
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
			mode := model.NormalizeAppRightSizingMode(opts.Mode)
			if mode == "" {
				return fmt.Errorf("mode must be disabled, recommend, or auto")
			}
			response, err := client.PatchAppRightSizing(app.ID, &model.AppRightSizingSpec{
				Mode:        mode,
				WindowHours: opts.WindowHours,
				MinSamples:  opts.MinSamples,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			result := appCommandResult{App: &response.App, Operation: response.Operation}
			return c.renderAppCommandResult(result)
		},
	}
	cmd.Flags().StringVar(&opts.Mode, "mode", opts.Mode, "Right-sizing mode: disabled, recommend, or auto")
	bindAppResourcesWindowFlags(cmd, &opts)
	return cmd
}

func bindAppResourcesWindowFlags(cmd *cobra.Command, opts *appResourcesOptions) {
	cmd.Flags().IntVar(&opts.WindowHours, "window-hours", opts.WindowHours, "Historical usage window in hours, capped at 168")
	cmd.Flags().IntVar(&opts.MinSamples, "min-samples", opts.MinSamples, "Minimum samples required before recommending changes")
}

func writeResourceRecommendationTable(w io.Writer, recommendation model.AppRightSizingRecommendation) error {
	rows := []model.ResourceRightSizingRecommendation{recommendation.App}
	rows = append(rows, recommendation.BackingServices...)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TARGET\tCLASS\tSAMPLES\tCURRENT\tRECOMMENDED\tREADY\tREASON"); err != nil {
		return err
	}
	for _, row := range rows {
		target := strings.TrimSpace(row.TargetName)
		if target == "" {
			target = row.TargetID
		}
		if row.ServiceType != "" {
			target += " (" + row.ServiceType + ")"
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%d\t%s\t%s\t%t\t%s\n",
			target,
			row.WorkloadClass,
			row.SampleCount,
			formatResourceSpec(row.Current),
			formatResourceSpec(row.Recommended),
			row.Ready,
			row.Reason,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatResourceSpec(spec *model.ResourceSpec) string {
	if spec == nil {
		return ""
	}
	parts := make([]string, 0, 4)
	if spec.CPUMilliCores > 0 {
		parts = append(parts, fmt.Sprintf("%dm CPU req", spec.CPUMilliCores))
	}
	if spec.CPULimitMilliCores > 0 {
		parts = append(parts, fmt.Sprintf("%dm CPU limit", spec.CPULimitMilliCores))
	}
	if spec.MemoryMebibytes > 0 {
		parts = append(parts, fmt.Sprintf("%dMi RAM req", spec.MemoryMebibytes))
	}
	if spec.MemoryLimitMebibytes > 0 {
		parts = append(parts, fmt.Sprintf("%dMi RAM limit", spec.MemoryLimitMebibytes))
	}
	return strings.Join(parts, " / ")
}

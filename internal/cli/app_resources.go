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
		Short: "Inspect and manage app resources and right-sizing",
	}
	cmd.AddCommand(
		c.newAppResourcesShowCommand(),
		c.newAppResourcesSetCommand(),
		c.newAppResourcesClearCommand(),
		c.newAppResourcesRecommendCommand(),
		c.newAppResourcesApplyCommand(),
		c.newAppResourcesAutoCommand(),
	)
	return cmd
}

func (c *CLI) newAppResourcesShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <app>",
		Aliases: []string{"get", "status"},
		Short:   "Show explicit app resources and right-sizing policy",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}
			return c.renderAppResourcesState(app, nil, false)
		},
	}
}

func (c *CLI) newAppResourcesSetCommand() *cobra.Command {
	opts := struct {
		CPU      int64
		Memory   int64
		CPULimit int64
		MemLimit int64
		Wait     bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "set <app>",
		Short: "Set explicit app resource requests and limits",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !flagChanged(cmd, "cpu-millicores") &&
				!flagChanged(cmd, "memory-mebibytes") &&
				!flagChanged(cmd, "cpu-limit-millicores") &&
				!flagChanged(cmd, "memory-limit-mebibytes") {
				return fmt.Errorf("at least one resource flag must be provided")
			}
			if opts.CPU < 0 || opts.Memory < 0 || opts.CPULimit < 0 || opts.MemLimit < 0 {
				return fmt.Errorf("resource values must be greater than or equal to zero")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, alreadyCurrent, err := deployUpdatedAppSpec(client, app.ID, func(spec *model.AppSpec) error {
				resources := model.ResourceSpec{}
				if spec.Resources != nil {
					resources = *spec.Resources
				}
				if flagChanged(cmd, "cpu-millicores") {
					resources.CPUMilliCores = opts.CPU
				}
				if flagChanged(cmd, "memory-mebibytes") {
					resources.MemoryMebibytes = opts.Memory
				}
				if flagChanged(cmd, "cpu-limit-millicores") {
					resources.CPULimitMilliCores = opts.CPULimit
				}
				if flagChanged(cmd, "memory-limit-mebibytes") {
					resources.MemoryLimitMebibytes = opts.MemLimit
				}
				if resources.CPUMilliCores == 0 &&
					resources.MemoryMebibytes == 0 &&
					resources.CPULimitMilliCores == 0 &&
					resources.MemoryLimitMebibytes == 0 {
					spec.Resources = nil
				} else {
					spec.Resources = &resources
				}
				return nil
			})
			if err != nil {
				return err
			}
			response, err = c.waitForAppSpecMutation(client, app.ID, response, opts.Wait)
			if err != nil {
				return err
			}
			return c.renderAppResourcesState(response.App, response.Operation, alreadyCurrent)
		},
	}
	cmd.Flags().Int64Var(&opts.CPU, "cpu-millicores", 0, "CPU request in millicores")
	cmd.Flags().Int64Var(&opts.Memory, "memory-mebibytes", 0, "Memory request in MiB")
	cmd.Flags().Int64Var(&opts.CPULimit, "cpu-limit-millicores", 0, "CPU limit in millicores")
	cmd.Flags().Int64Var(&opts.MemLimit, "memory-limit-mebibytes", 0, "Memory limit in MiB")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newAppResourcesClearCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "clear <app>",
		Aliases: []string{"unset", "delete", "remove"},
		Short:   "Clear explicit app resource requests and limits",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, alreadyCurrent, err := deployUpdatedAppSpec(client, app.ID, func(spec *model.AppSpec) error {
				spec.Resources = nil
				return nil
			})
			if err != nil {
				return err
			}
			response, err = c.waitForAppSpecMutation(client, app.ID, response, opts.Wait)
			if err != nil {
				return err
			}
			return c.renderAppResourcesState(response.App, response.Operation, alreadyCurrent)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
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

func (c *CLI) renderAppResourcesState(app model.App, operation *model.Operation, alreadyCurrent bool) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{
			"app":             app,
			"resources":       app.Spec.Resources,
			"right_sizing":    app.Spec.RightSizing,
			"operation":       operation,
			"already_current": alreadyCurrent,
		})
	}
	pairs := []kvPair{
		{Key: "app", Value: formatDisplayName(app.Name, app.ID, c.showIDs())},
		{Key: "resources", Value: firstNonEmpty(formatResourceSpec(app.Spec.Resources), "-")},
	}
	if app.Spec.RightSizing != nil {
		pairs = append(pairs,
			kvPair{Key: "right_sizing_mode", Value: app.Spec.RightSizing.Mode},
			kvPair{Key: "right_sizing_window_hours", Value: fmt.Sprintf("%d", app.Spec.RightSizing.WindowHours)},
			kvPair{Key: "right_sizing_min_samples", Value: fmt.Sprintf("%d", app.Spec.RightSizing.MinSamples)},
		)
	} else {
		pairs = append(pairs, kvPair{Key: "right_sizing_mode", Value: model.AppRightSizingModeDisabled})
	}
	if operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: operation.ID})
	}
	if alreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	return writeKeyValues(c.stdout, pairs...)
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

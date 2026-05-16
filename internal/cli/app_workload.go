package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppWorkloadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workload",
		Short: "Inspect and set app workload semantics",
		Long: strings.TrimSpace(`
Workload class is a reusable scheduling and right-sizing hint.

Use service for normal web/API apps, batch for offline work, demo for low-risk
demo workloads, and critical for latency-sensitive or high-availability apps.
`),
	}
	cmd.AddCommand(
		c.newAppWorkloadShowCommand(),
		c.newAppWorkloadSetCommand(),
		c.newAppWorkloadClearCommand(),
	)
	return cmd
}

func (c *CLI) newAppWorkloadShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <app>",
		Aliases: []string{"get", "status"},
		Short:   "Show the app workload class",
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
			return c.renderAppWorkloadState(app, nil, false)
		},
	}
}

func (c *CLI) newAppWorkloadSetCommand() *cobra.Command {
	opts := struct {
		Class string
		Wait  bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "set <app>",
		Short: "Set the app workload class",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			class := model.NormalizeWorkloadClass(opts.Class)
			if class == "" {
				return fmt.Errorf("--class must be critical, service, demo, or batch")
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
				spec.WorkloadClass = class
				return nil
			})
			if err != nil {
				return err
			}
			response, err = c.waitForAppSpecMutation(client, app.ID, response, opts.Wait)
			if err != nil {
				return err
			}
			return c.renderAppWorkloadState(response.App, response.Operation, alreadyCurrent)
		},
	}
	cmd.Flags().StringVar(&opts.Class, "class", "", "Workload class: critical, service, demo, or batch")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newAppWorkloadClearCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "clear <app>",
		Aliases: []string{"unset", "delete", "remove"},
		Short:   "Clear the explicit workload class",
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
				spec.WorkloadClass = ""
				return nil
			})
			if err != nil {
				return err
			}
			response, err = c.waitForAppSpecMutation(client, app.ID, response, opts.Wait)
			if err != nil {
				return err
			}
			return c.renderAppWorkloadState(response.App, response.Operation, alreadyCurrent)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) renderAppWorkloadState(app model.App, operation *model.Operation, alreadyCurrent bool) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{
			"app":                      app,
			"workload_class":           strings.TrimSpace(app.Spec.WorkloadClass),
			"effective_workload_class": model.EffectiveWorkloadClass(app.Spec),
			"operation":                operation,
			"already_current":          alreadyCurrent,
		})
	}
	pairs := []kvPair{
		{Key: "app", Value: formatDisplayName(app.Name, app.ID, c.showIDs())},
		{Key: "workload_class", Value: firstNonEmpty(strings.TrimSpace(app.Spec.WorkloadClass), "-")},
		{Key: "effective_workload_class", Value: model.EffectiveWorkloadClass(app.Spec)},
	}
	if operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: operation.ID})
	}
	if alreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	return writeKeyValues(c.stdout, pairs...)
}

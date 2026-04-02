package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appLogsCommandOptions struct {
	Build       bool
	OperationID string
	Component   string
	Pod         string
	TailLines   int
	Previous    bool
}

type appScaleCommandOptions struct {
	Replicas int
	Wait     bool
}

type appRemoveCommandOptions struct {
	Wait bool
}

type appMoveCommandOptions struct {
	RuntimeName string
	RuntimeID   string
	Wait        bool
}

type appCommandResult struct {
	App             *model.App       `json:"app,omitempty"`
	Operation       *model.Operation `json:"operation,omitempty"`
	RestartToken    string           `json:"restart_token,omitempty"`
	Deleted         bool             `json:"deleted,omitempty"`
	AlreadyDeleting bool             `json:"already_deleting,omitempty"`
}

func (c *CLI) newAppCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "app",
		Short: "Inspect and mutate apps",
	}
	cmd.AddCommand(
		c.newAppListCommand(),
		c.newAppStatusCommand(),
		c.newAppFailoverCommand(),
		c.newAppLogsCommand(),
		c.newAppRestartCommand(),
		c.newAppScaleCommand(),
		c.newAppRemoveCommand(),
		c.newAppMoveCommand(),
	)
	return cmd
}

func (c *CLI) newAppListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List apps",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, projectID, err := c.resolveFilterSelections(client)
			if err != nil {
				return err
			}
			apps, err := client.ListApps()
			if err != nil {
				return err
			}
			filtered := filterApps(apps, tenantID, projectID)
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"apps": filtered})
			}
			return writeAppTable(c.stdout, filtered)
		},
	}
}

func (c *CLI) newAppStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "status <app>",
		Aliases: []string{"info"},
		Short:   "Show app status",
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
			finalApp, err := client.GetApp(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"app": finalApp})
			}
			return writeAppStatus(c.stdout, finalApp)
		},
	}
}

func (c *CLI) newAppLogsCommand() *cobra.Command {
	opts := appLogsCommandOptions{
		Component: "app",
		TailLines: 200,
	}
	cmd := &cobra.Command{
		Use:   "logs <app>",
		Short: "Read runtime or build logs",
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
			if opts.Build {
				logs, err := client.GetBuildLogs(app.ID, opts.OperationID, opts.TailLines)
				if err != nil {
					return err
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, logs)
				}
				text := strings.TrimSpace(logs.Logs)
				if text == "" {
					text = strings.TrimSpace(logs.Summary)
				}
				if text == "" {
					text = strings.TrimSpace(logs.ResultMessage)
				}
				if text == "" {
					text = "no build logs available"
				}
				_, err = fmt.Fprintln(c.stdout, text)
				return err
			}

			logs, err := client.GetRuntimeLogs(app.ID, runtimeLogsOptions{
				Component: opts.Component,
				Pod:       opts.Pod,
				TailLines: opts.TailLines,
				Previous:  opts.Previous,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, logs)
			}
			if len(logs.Warnings) > 0 {
				for _, warning := range logs.Warnings {
					c.progressf("warning=%s", warning)
				}
			}
			_, err = fmt.Fprintln(c.stdout, strings.TrimRight(logs.Logs, "\n"))
			return err
		},
	}
	cmd.Flags().BoolVar(&opts.Build, "build", false, "Read build logs instead of runtime logs")
	cmd.Flags().StringVar(&opts.OperationID, "operation", "", "Specific build operation ID")
	cmd.Flags().StringVar(&opts.Component, "component", opts.Component, "Runtime component: app or postgres")
	cmd.Flags().StringVar(&opts.Pod, "pod", "", "Specific pod name")
	cmd.Flags().IntVar(&opts.TailLines, "tail", opts.TailLines, "Number of log lines to read")
	cmd.Flags().BoolVar(&opts.Previous, "previous", false, "Read the previous container logs")
	return cmd
}

func (c *CLI) newAppRestartCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "restart <app>",
		Short: "Restart an app",
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
			response, err := client.RestartApp(app.ID)
			if err != nil {
				return err
			}
			finalResult := appCommandResult{
				Operation:    &response.Operation,
				RestartToken: response.RestartToken,
			}
			finalApp, err := c.waitForSingleApp(client, app.ID, response.Operation, opts.Wait)
			if err != nil {
				return err
			}
			finalResult.App = finalApp
			return c.renderAppCommandResult(finalResult)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) newAppScaleCommand() *cobra.Command {
	opts := appScaleCommandOptions{Wait: true}
	cmd := &cobra.Command{
		Use:   "scale <app>",
		Short: "Scale an app",
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
			response, err := client.ScaleApp(app.ID, opts.Replicas)
			if err != nil {
				return err
			}
			result := appCommandResult{Operation: &response.Operation}
			if opts.Wait {
				finalApp, err := c.waitForSingleApp(client, app.ID, response.Operation, true)
				if err != nil {
					return err
				}
				result.App = finalApp
			} else {
				result.App = &app
			}
			return c.renderAppCommandResult(result)
		},
	}
	cmd.Flags().IntVar(&opts.Replicas, "replicas", 0, "Desired replica count")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	_ = cmd.MarkFlagRequired("replicas")
	return cmd
}

func (c *CLI) newAppRemoveCommand() *cobra.Command {
	opts := appRemoveCommandOptions{Wait: true}
	cmd := &cobra.Command{
		Use:     "remove <app>",
		Aliases: []string{"rm", "delete"},
		Short:   "Delete an app",
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
			response, err := client.DeleteApp(app.ID)
			if err != nil {
				return err
			}

			result := appCommandResult{
				App:             &app,
				Deleted:         false,
				AlreadyDeleting: response.AlreadyDeleting,
			}
			if response.Operation != nil {
				result.Operation = response.Operation
				if opts.Wait {
					if _, err := c.waitForOperations(client, []model.Operation{*response.Operation}); err != nil {
						return err
					}
					result.Deleted = true
				}
			}
			return c.renderAppCommandResult(result)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) newAppMoveCommand() *cobra.Command {
	opts := appMoveCommandOptions{Wait: true}
	cmd := &cobra.Command{
		Use:     "move <app>",
		Aliases: []string{"migrate"},
		Short:   "Move an app to another runtime",
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
			if strings.TrimSpace(opts.RuntimeName) == "" && strings.TrimSpace(opts.RuntimeID) == "" {
				return fmt.Errorf("target runtime is required")
			}
			runtimeID, err := resolveRuntimeSelection(client, opts.RuntimeID, opts.RuntimeName)
			if err != nil {
				return err
			}
			if strings.TrimSpace(runtimeID) == "" {
				return fmt.Errorf("target runtime is required")
			}
			response, err := client.MigrateApp(app.ID, runtimeID)
			if err != nil {
				return err
			}
			result := appCommandResult{Operation: &response.Operation}
			if opts.Wait {
				finalApp, err := c.waitForSingleApp(client, app.ID, response.Operation, true)
				if err != nil {
					return err
				}
				result.App = finalApp
			} else {
				result.App = &app
			}
			return c.renderAppCommandResult(result)
		},
	}
	cmd.Flags().StringVar(&opts.RuntimeName, "to", "", "Target runtime name")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Target runtime ID")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	_ = cmd.Flags().MarkHidden("runtime-id")
	return cmd
}

func filterApps(apps []model.App, tenantID, projectID string) []model.App {
	out := make([]model.App, 0, len(apps))
	for _, app := range apps {
		if strings.TrimSpace(tenantID) != "" && app.TenantID != tenantID {
			continue
		}
		if strings.TrimSpace(projectID) != "" && app.ProjectID != projectID {
			continue
		}
		out = append(out, app)
	}
	return out
}

func (c *CLI) resolveNamedApp(client *Client, appRef string) (model.App, error) {
	tenantID, projectID, err := c.resolveFilterSelections(client)
	if err != nil {
		return model.App{}, err
	}
	return resolveAppReference(client, appRef, projectID, tenantID)
}

func (c *CLI) waitForSingleApp(client *Client, appID string, op model.Operation, wait bool) (*model.App, error) {
	if !wait {
		app, err := client.GetApp(appID)
		if err != nil {
			return nil, err
		}
		return &app, nil
	}
	finalOps, err := c.waitForOperations(client, []model.Operation{op})
	if err != nil {
		return nil, err
	}
	if len(finalOps) > 0 {
		op = finalOps[0]
	}
	app, err := client.GetApp(appID)
	if err != nil {
		return nil, err
	}
	return &app, nil
}

func (c *CLI) renderAppCommandResult(result appCommandResult) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, result)
	}
	pairs := make([]kvPair, 0, 6)
	if result.App != nil {
		pairs = append(pairs, kvPair{Key: "app_id", Value: result.App.ID})
		pairs = append(pairs, kvPair{Key: "phase", Value: strings.TrimSpace(result.App.Status.Phase)})
		runtimeID := strings.TrimSpace(result.App.Status.CurrentRuntimeID)
		if runtimeID == "" {
			runtimeID = strings.TrimSpace(result.App.Spec.RuntimeID)
		}
		pairs = append(pairs, kvPair{Key: "runtime_id", Value: runtimeID})
		if result.App.Route != nil && strings.TrimSpace(result.App.Route.PublicURL) != "" {
			pairs = append(pairs, kvPair{Key: "url", Value: result.App.Route.PublicURL})
		}
	}
	if result.Operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: result.Operation.ID})
	}
	if result.RestartToken != "" {
		pairs = append(pairs, kvPair{Key: "restart_token", Value: result.RestartToken})
	}
	if result.Deleted {
		pairs = append(pairs, kvPair{Key: "deleted", Value: "true"})
	}
	if result.AlreadyDeleting {
		pairs = append(pairs, kvPair{Key: "already_deleting", Value: "true"})
	}
	return writeKeyValues(c.stdout, pairs...)
}

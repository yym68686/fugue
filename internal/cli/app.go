package cli

import (
	"encoding/json"
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appLogsCommandOptions struct {
	Build       bool
	Follow      bool
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

type appStartCommandOptions struct {
	Replicas int
	Wait     bool
}

type appCommandResult struct {
	App             *model.App       `json:"app,omitempty"`
	Operation       *model.Operation `json:"operation,omitempty"`
	RestartToken    string           `json:"restart_token,omitempty"`
	Deleted         bool             `json:"deleted,omitempty"`
	AlreadyDisabled bool             `json:"already_disabled,omitempty"`
	AlreadyDeleting bool             `json:"already_deleting,omitempty"`
}

func (c *CLI) newAppCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "app",
		Aliases: []string{
			"apps",
		},
		Short: "Inspect, operate, and troubleshoot apps",
		Long: strings.TrimSpace(`
Use app names for normal day-to-day operations.

You usually do not need --tenant or --project unless the same app name appears
in more than one visible project or tenant.
`),
	}
	cmd.AddCommand(
		c.newAppCreateCommand(),
		c.newAppListCommand(),
		c.newAppStatusCommand(),
		c.newAppOverviewCommand(),
		c.newAppWatchCommand(),
		c.newAppDiagnosisCommand(),
		c.newAppSourceCommand(),
		hideCompatCommand(c.newAppSyncCommand(), "fugue app source show or fugue app rebuild"),
		c.newAppLogsCommand(),
		c.newAppRequestCommand(),
		c.newEnvCommand(),
		c.newFilesCommand(),
		c.newFilesystemCommand(),
		hideCompatCommand(c.newWorkspaceCommand(), "fugue app fs"),
		c.newAppCommandCommand(),
		c.newAppStorageCommand(),
		hideCompatCommand(c.newAppRouteCommand(), "fugue app domain primary"),
		c.newDomainCommand(),
		c.newAppServiceCommand(),
		hideCompatCommand(c.newAppBindingCompatCommand(), "fugue app service"),
		c.newAppDatabaseCommand(),
		c.newAppRedeployCommand(),
		c.newAppRebuildShortcutCommand(),
		c.newAppRollbackShortcutCommand(),
		c.newAppReleaseCommand(),
		hideCompatCommand(c.newAppDeployShortcutCommand(), "fugue app redeploy"),
		hideCompatCommand(c.newAppContinuityCommand(), "fugue app failover"),
		c.newAppFailoverCommand(),
		c.newAppRestartCommand(),
		c.newAppScaleCommand(),
		c.newAppStartCommand(),
		c.newAppStopCommand(),
		c.newAppRemoveCommand(),
		c.newAppMoveCommand(),
	)
	return cmd
}

func (c *CLI) newAppListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
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
			runtimes, err := client.ListRuntimes()
			if err != nil {
				c.progressf("warning=runtime inventory unavailable: %v", err)
			}
			return writeAppTableWithRuntimeNames(c.stdout, filtered, mapRuntimeNames(runtimes), c.showIDs())
		},
	}
}

func (c *CLI) newAppStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "status <app>",
		Aliases: []string{"show", "get", "info"},
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
			return c.renderAppStatus(client, finalApp)
		},
	}
}

func (c *CLI) newAppLogsCommand() *cobra.Command {
	opts := appLogsCommandOptions{
		Component: "app",
		TailLines: 200,
	}
	cmd := &cobra.Command{
		Use:   "logs [app]",
		Short: "Read runtime and build logs",
		Long: strings.TrimSpace(`
Read app runtime logs or build logs.

Use "app logs runtime" and "app logs build" for explicit semantics. The bare
"app logs <app>" form remains as a runtime-log shortcut.
`),
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			if opts.Build {
				return c.renderBuildLogs(client, app.ID, opts)
			}
			return c.renderRuntimeLogs(client, app.ID, runtimeLogsOptions{
				Component: opts.Component,
				Pod:       opts.Pod,
				TailLines: opts.TailLines,
				Previous:  opts.Previous,
			}, opts.Follow)
		},
	}
	cmd.Flags().BoolVar(&opts.Build, "build", false, "Compatibility flag for build logs; prefer 'app logs build'")
	cmd.Flags().BoolVar(&opts.Follow, "follow", false, "Follow logs in real time")
	cmd.Flags().StringVar(&opts.OperationID, "operation", "", "Specific build operation ID")
	cmd.Flags().StringVar(&opts.Component, "component", opts.Component, "Runtime component: app or postgres")
	cmd.Flags().StringVar(&opts.Pod, "pod", "", "Specific pod name")
	cmd.Flags().IntVar(&opts.TailLines, "tail", opts.TailLines, "Number of log lines to read")
	cmd.Flags().BoolVar(&opts.Previous, "previous", false, "Read the previous container logs")
	cmd.AddCommand(
		c.newAppRuntimeLogsCommand(),
		c.newAppBuildLogsCommand(),
		c.newAppLogsQueryCommand(),
		c.newAppLogsPodsCommand(),
	)
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
	opts := struct {
		appRemoveCommandOptions
		Force bool
	}{
		appRemoveCommandOptions: appRemoveCommandOptions{Wait: true},
	}
	cmd := &cobra.Command{
		Use:     "delete <app>",
		Aliases: []string{"rm", "remove"},
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
			var response appDeleteResponse
			if opts.Force {
				response, err = client.DeleteAppForce(app.ID)
			} else {
				response, err = client.DeleteApp(app.ID)
			}
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
	cmd.Flags().BoolVar(&opts.Force, "force", false, "Abort in-flight work before deleting and purge the app when possible")
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

func (c *CLI) newAppStartCommand() *cobra.Command {
	opts := appStartCommandOptions{
		Replicas: 1,
		Wait:     true,
	}
	cmd := &cobra.Command{
		Use:   "start <app>",
		Short: "Start a stopped app",
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
			replicas := opts.Replicas
			if replicas <= 0 {
				replicas = 1
			}
			response, err := client.ScaleApp(app.ID, replicas)
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
	cmd.Flags().IntVar(&opts.Replicas, "replicas", opts.Replicas, "Replica count to start with")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) newAppStopCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "stop <app>",
		Short: "Stop an app without deleting it",
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
			response, err := client.DisableApp(app.ID)
			if err != nil {
				return err
			}
			result := appCommandResult{
				App:             &app,
				Operation:       response.Operation,
				AlreadyDisabled: response.AlreadyDisabled,
			}
			if response.Operation != nil && opts.Wait {
				finalApp, err := c.waitForSingleApp(client, app.ID, *response.Operation, true)
				if err != nil {
					return err
				}
				result.App = finalApp
			}
			return c.renderAppCommandResult(result)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) newAppRuntimeLogsCommand() *cobra.Command {
	opts := appLogsCommandOptions{
		Component: "app",
		TailLines: 200,
	}
	cmd := &cobra.Command{
		Use:   "runtime <app>",
		Short: "Read runtime logs",
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
			return c.renderRuntimeLogs(client, app.ID, runtimeLogsOptions{
				Component: opts.Component,
				Pod:       opts.Pod,
				TailLines: opts.TailLines,
				Previous:  opts.Previous,
			}, opts.Follow)
		},
	}
	cmd.Flags().BoolVar(&opts.Follow, "follow", false, "Follow logs in real time")
	cmd.Flags().StringVar(&opts.Component, "component", opts.Component, "Runtime component: app or postgres")
	cmd.Flags().StringVar(&opts.Pod, "pod", "", "Specific pod name")
	cmd.Flags().IntVar(&opts.TailLines, "tail", opts.TailLines, "Number of log lines to read")
	cmd.Flags().BoolVar(&opts.Previous, "previous", false, "Read the previous container logs")
	return cmd
}

func (c *CLI) newAppBuildLogsCommand() *cobra.Command {
	opts := appLogsCommandOptions{Build: true, TailLines: 200}
	cmd := &cobra.Command{
		Use:   "build <app>",
		Short: "Read build logs",
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
			return c.renderBuildLogs(client, app.ID, opts)
		},
	}
	cmd.Flags().BoolVar(&opts.Follow, "follow", false, "Follow logs in real time")
	cmd.Flags().StringVar(&opts.OperationID, "operation", "", "Specific build operation ID")
	cmd.Flags().IntVar(&opts.TailLines, "tail", opts.TailLines, "Number of log lines to read")
	return cmd
}

func (c *CLI) renderBuildLogs(client *Client, appID string, opts appLogsCommandOptions) error {
	if opts.Follow {
		return c.streamBuildLogs(client, appID, opts)
	}
	logs, err := client.GetBuildLogs(appID, opts.OperationID, opts.TailLines)
	if err != nil {
		return err
	}
	logs.ArtifactSummary = c.collectBuildArtifactReport(client, appID, logs)
	if strings.TrimSpace(logs.JobName) == "" {
		logs.JobName = buildLogsFallbackJobName(logs)
	}
	if c.wantsJSON() {
		return writeJSON(c.stdout, logs)
	}
	return renderBuildLogsReport(c.stdout, logs)
}

func (c *CLI) renderRuntimeLogs(client *Client, appID string, opts runtimeLogsOptions, follow bool) error {
	if follow {
		return c.streamRuntimeLogs(client, appID, opts)
	}
	logs, err := client.GetRuntimeLogs(appID, opts)
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
}

func (c *CLI) streamBuildLogs(client *Client, appID string, opts appLogsCommandOptions) error {
	var end logStreamEndEvent
	err := client.StreamBuildLogs(appID, opts.OperationID, opts.TailLines, true, func(event sseEvent) error {
		switch event.Event {
		case "log":
			var payload logStreamLogEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			if c.wantsJSON() {
				return c.writeStreamJSON(event)
			}
			_, err := fmt.Fprintln(c.stdout, payload.Line)
			return err
		case "warning":
			if c.wantsJSON() {
				return c.writeStreamJSON(event)
			}
			var payload logStreamWarningEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			c.progressf("warning=%s", payload.Message)
		case "status", "ready", "heartbeat":
			if c.wantsJSON() {
				return c.writeStreamJSON(event)
			}
		case "end":
			if err := json.Unmarshal(event.Data, &end); err != nil {
				return err
			}
			if c.wantsJSON() {
				return c.writeStreamJSON(event)
			}
		default:
			if c.wantsJSON() {
				return c.writeStreamJSON(event)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if strings.EqualFold(end.OperationStatus, model.OperationStatusFailed) {
		return fmt.Errorf("build operation failed")
	}
	return nil
}

func (c *CLI) streamRuntimeLogs(client *Client, appID string, opts runtimeLogsOptions) error {
	return client.StreamRuntimeLogs(appID, opts, true, func(event sseEvent) error {
		switch event.Event {
		case "log":
			var payload logStreamLogEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			if c.wantsJSON() {
				return c.writeStreamJSON(event)
			}
			_, err := fmt.Fprintln(c.stdout, payload.Line)
			return err
		case "warning":
			if c.wantsJSON() {
				return c.writeStreamJSON(event)
			}
			var payload logStreamWarningEvent
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				return err
			}
			c.progressf("warning=%s", payload.Message)
		default:
			if c.wantsJSON() {
				return c.writeStreamJSON(event)
			}
		}
		return nil
	})
}

func (c *CLI) writeStreamJSON(event sseEvent) error {
	decoded, err := decodeSSEEventData(event.Data)
	if err != nil {
		return err
	}
	return writeJSON(c.stdout, map[string]any{
		"event": event.Event,
		"id":    event.ID,
		"data":  decoded,
	})
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
		pairs = append(pairs, kvPair{Key: "app", Value: formatDisplayName(result.App.Name, result.App.ID, c.showIDs())})
		pairs = append(pairs, kvPair{Key: "phase", Value: strings.TrimSpace(result.App.Status.Phase)})
		runtimeID := strings.TrimSpace(result.App.Status.CurrentRuntimeID)
		if runtimeID == "" {
			runtimeID = strings.TrimSpace(result.App.Spec.RuntimeID)
		}
		pairs = append(pairs, kvPair{Key: "runtime", Value: runtimeID})
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
	if result.AlreadyDisabled {
		pairs = append(pairs, kvPair{Key: "already_disabled", Value: "true"})
	}
	if result.Deleted {
		pairs = append(pairs, kvPair{Key: "deleted", Value: "true"})
	}
	if result.AlreadyDeleting {
		pairs = append(pairs, kvPair{Key: "already_deleting", Value: "true"})
	}
	return writeKeyValues(c.stdout, pairs...)
}

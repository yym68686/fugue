package cli

import (
	"fmt"
	"reflect"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppDatabaseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "db",
		Aliases: []string{"database"},
		Short:   "Inspect and manage an app-owned managed Postgres database",
	}
	cmd.AddCommand(
		c.newAppDatabaseShowCommand(),
		c.newAppDatabaseQueryCommand(),
		c.newAppDatabaseConfigureCommand(),
		c.newAppDatabaseDisableCommand(),
		c.newAppDatabaseSwitchoverCommand(),
	)
	return cmd
}

func (c *CLI) newAppDatabaseShowCommand() *cobra.Command {
	opts := struct {
		ShowSecrets bool
	}{}
	cmd := &cobra.Command{
		Use:     "show <app>",
		Aliases: []string{"get", "status"},
		Short:   "Show the app managed Postgres configuration",
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
			return c.renderAppDatabaseState(app, nil, false, opts.ShowSecrets)
		},
	}
	cmd.Flags().BoolVar(&opts.ShowSecrets, "show-secrets", false, "Show passwords and other sensitive fields in JSON output")
	return cmd
}

func (c *CLI) newAppDatabaseConfigureCommand() *cobra.Command {
	opts := struct {
		RuntimeName         string
		RuntimeID           string
		Database            string
		User                string
		Password            string
		Image               string
		ServiceName         string
		StorageSize         string
		StorageClass        string
		Instances           int
		SynchronousReplicas int
		FailoverRuntimeName string
		FailoverRuntimeID   string
		ShowSecrets         bool
		Wait                bool
	}{
		Wait: true,
	}
	cmd := &cobra.Command{
		Use:     "configure <app>",
		Aliases: []string{"set", "enable"},
		Short:   "Enable or update the app managed Postgres configuration",
		Long: strings.TrimSpace(`
Run without extra flags to enable a managed Postgres database with Fugue
defaults. When you omit --password, Fugue generates a strong random password
for the managed database. Add flags only for the parts you want to customize.
`),
		Args: cobra.ExactArgs(1),
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

			spec := cloneAppSpec(app.Spec)
			before := cloneAppPostgresSpec(spec.Postgres)
			if spec.Postgres == nil {
				spec.Postgres = &model.AppPostgresSpec{}
			}
			if strings.TrimSpace(opts.RuntimeName) != "" || strings.TrimSpace(opts.RuntimeID) != "" {
				runtimeID, err := resolveRuntimeSelection(client, opts.RuntimeID, opts.RuntimeName)
				if err != nil {
					return err
				}
				spec.Postgres.RuntimeID = runtimeID
			}
			if strings.TrimSpace(opts.FailoverRuntimeName) != "" || strings.TrimSpace(opts.FailoverRuntimeID) != "" {
				runtimeID, err := resolveRuntimeSelection(client, opts.FailoverRuntimeID, opts.FailoverRuntimeName)
				if err != nil {
					return err
				}
				spec.Postgres.FailoverTargetRuntimeID = runtimeID
			}
			if flagChanged(cmd, "database") {
				spec.Postgres.Database = strings.TrimSpace(opts.Database)
			}
			if flagChanged(cmd, "user") {
				spec.Postgres.User = strings.TrimSpace(opts.User)
			}
			if flagChanged(cmd, "password") {
				spec.Postgres.Password = opts.Password
			}
			if flagChanged(cmd, "image") {
				spec.Postgres.Image = strings.TrimSpace(opts.Image)
			}
			if flagChanged(cmd, "service-name") {
				spec.Postgres.ServiceName = strings.TrimSpace(opts.ServiceName)
			}
			if flagChanged(cmd, "storage-size") {
				spec.Postgres.StorageSize = strings.TrimSpace(opts.StorageSize)
			}
			if flagChanged(cmd, "storage-class") {
				spec.Postgres.StorageClassName = strings.TrimSpace(opts.StorageClass)
			}
			if flagChanged(cmd, "instances") {
				spec.Postgres.Instances = opts.Instances
			}
			if flagChanged(cmd, "sync-replicas") {
				spec.Postgres.SynchronousReplicas = opts.SynchronousReplicas
			}
			if err := ensureManagedPostgresPassword(spec.Postgres); err != nil {
				return err
			}
			if err := model.ValidateManagedPostgresUser(app.Name, *spec.Postgres); err != nil {
				return err
			}
			if reflect.DeepEqual(before, spec.Postgres) {
				return c.renderAppDatabaseState(app, nil, true, opts.ShowSecrets)
			}

			response, err := client.DeployApp(app.ID, &spec)
			if err != nil {
				return err
			}
			finalApp := app
			if opts.Wait {
				waitedApp, err := c.waitForSingleApp(client, app.ID, response.Operation, true)
				if err != nil {
					return err
				}
				if waitedApp != nil {
					finalApp = *waitedApp
				}
			} else {
				finalApp.Spec = spec
			}
			return c.renderAppDatabaseState(finalApp, &response.Operation, false, opts.ShowSecrets)
		},
	}
	cmd.Flags().StringVar(&opts.RuntimeName, "runtime", "", "Runtime name for managed Postgres")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Runtime ID for managed Postgres")
	cmd.Flags().StringVar(&opts.Database, "database", "", "Database name")
	cmd.Flags().StringVar(&opts.User, "user", "", "Database user")
	cmd.Flags().StringVar(&opts.Password, "password", "", "Database password. Generates a random password when omitted")
	cmd.Flags().StringVar(&opts.Image, "image", "", "Managed Postgres image override")
	cmd.Flags().StringVar(&opts.ServiceName, "service-name", "", "Service name override")
	cmd.Flags().StringVar(&opts.StorageSize, "storage-size", "", "Persistent storage size")
	cmd.Flags().StringVar(&opts.StorageClass, "storage-class", "", "Persistent storage class")
	cmd.Flags().IntVar(&opts.Instances, "instances", 0, "Managed Postgres instance count")
	cmd.Flags().IntVar(&opts.SynchronousReplicas, "sync-replicas", 0, "Managed Postgres synchronous replica count")
	cmd.Flags().StringVar(&opts.FailoverRuntimeName, "failover-to", "", "Runtime name for managed Postgres failover")
	cmd.Flags().StringVar(&opts.FailoverRuntimeID, "failover-runtime-id", "", "Runtime ID for managed Postgres failover")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	cmd.Flags().BoolVar(&opts.ShowSecrets, "show-secrets", false, "Show passwords and other sensitive fields in JSON output")
	_ = cmd.Flags().MarkHidden("runtime-id")
	_ = cmd.Flags().MarkHidden("failover-runtime-id")
	return cmd
}

func (c *CLI) newAppDatabaseDisableCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "disable <app>",
		Aliases: []string{"off"},
		Short:   "Disable the app managed Postgres database",
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
			if app.Spec.Postgres == nil {
				return c.renderAppDatabaseState(app, nil, true, false)
			}

			spec := cloneAppSpec(app.Spec)
			spec.Postgres = nil
			response, err := client.DeployApp(app.ID, &spec)
			if err != nil {
				return err
			}
			finalApp := app
			if opts.Wait {
				waitedApp, err := c.waitForSingleApp(client, app.ID, response.Operation, true)
				if err != nil {
					return err
				}
				if waitedApp != nil {
					finalApp = *waitedApp
				}
			} else {
				finalApp.Spec = spec
			}
			return c.renderAppDatabaseState(finalApp, &response.Operation, false, false)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newAppDatabaseSwitchoverCommand() *cobra.Command {
	opts := struct {
		RuntimeName string
		RuntimeID   string
		Wait        bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "switchover <app> [runtime]",
		Short: "Switchover the app managed Postgres primary to another runtime",
		Args:  cobra.RangeArgs(1, 2),
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
			if app.Spec.Postgres == nil {
				return fmt.Errorf("managed postgres is not configured for this app")
			}

			runtimeName := strings.TrimSpace(opts.RuntimeName)
			if len(args) == 2 {
				if runtimeName != "" || strings.TrimSpace(opts.RuntimeID) != "" {
					return fmt.Errorf("target runtime must be provided either as an argument or with --to")
				}
				runtimeName = strings.TrimSpace(args[1])
			}
			targetRuntimeID := strings.TrimSpace(app.Spec.Postgres.FailoverTargetRuntimeID)
			switch {
			case runtimeName != "" || strings.TrimSpace(opts.RuntimeID) != "":
				targetRuntimeID, err = resolveRuntimeSelection(client, opts.RuntimeID, runtimeName)
				if err != nil {
					return err
				}
			case targetRuntimeID == "":
				return fmt.Errorf("target runtime is required; pass one explicitly or configure database failover first")
			}

			response, err := client.SwitchoverAppDatabase(app.ID, targetRuntimeID)
			if err != nil {
				return err
			}
			finalApp := app
			if opts.Wait {
				waitedApp, err := c.waitForSingleApp(client, app.ID, response.Operation, true)
				if err != nil {
					return err
				}
				if waitedApp != nil {
					finalApp = *waitedApp
				}
			}
			if c.wantsJSON() {
				payload := map[string]any{
					"app":               finalApp,
					"operation":         response.Operation,
					"target_runtime_id": targetRuntimeID,
				}
				return writeJSON(c.stdout, payload)
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "operation_id", Value: response.Operation.ID},
				kvPair{Key: "target_runtime_id", Value: targetRuntimeID},
			)
		},
	}
	cmd.Flags().StringVar(&opts.RuntimeName, "to", "", "Target runtime name")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Target runtime ID")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	_ = cmd.Flags().MarkHidden("runtime-id")
	return cmd
}

func (c *CLI) renderAppDatabaseState(app model.App, operation *model.Operation, alreadyCurrent bool, showSecrets bool) error {
	if c.wantsJSON() {
		payloadApp := app
		var payloadOp *model.Operation
		if operation != nil {
			opCopy := *operation
			payloadOp = &opCopy
		}
		if !showSecrets {
			payloadApp = redactAppForOutput(payloadApp)
			if payloadOp != nil {
				redactedOp := redactOperationForOutput(*payloadOp)
				payloadOp = &redactedOp
			}
		}
		payload := map[string]any{
			"app":             payloadApp,
			"database":        cloneAppPostgresSpec(payloadApp.Spec.Postgres),
			"already_current": alreadyCurrent,
		}
		if payloadOp != nil {
			payload["operation"] = payloadOp
		}
		return writeJSON(c.stdout, payload)
	}
	pairs := []kvPair{
		{Key: "app_id", Value: app.ID},
		{Key: "database_enabled", Value: fmt.Sprintf("%t", app.Spec.Postgres != nil)},
	}
	if operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: operation.ID})
	}
	if alreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	if app.Spec.Postgres != nil {
		pairs = append(pairs,
			kvPair{Key: "runtime_id", Value: strings.TrimSpace(app.Spec.Postgres.RuntimeID)},
			kvPair{Key: "database", Value: strings.TrimSpace(app.Spec.Postgres.Database)},
			kvPair{Key: "user", Value: strings.TrimSpace(app.Spec.Postgres.User)},
			kvPair{Key: "service_name", Value: strings.TrimSpace(app.Spec.Postgres.ServiceName)},
			kvPair{Key: "storage_size", Value: strings.TrimSpace(app.Spec.Postgres.StorageSize)},
			kvPair{Key: "storage_class", Value: strings.TrimSpace(app.Spec.Postgres.StorageClassName)},
			kvPair{Key: "instances", Value: fmt.Sprintf("%d", app.Spec.Postgres.Instances)},
			kvPair{Key: "sync_replicas", Value: fmt.Sprintf("%d", app.Spec.Postgres.SynchronousReplicas)},
			kvPair{Key: "failover_target_runtime_id", Value: strings.TrimSpace(app.Spec.Postgres.FailoverTargetRuntimeID)},
			kvPair{Key: "pending_rebalance", Value: fmt.Sprintf("%t", app.Spec.Postgres.PrimaryPlacementPendingRebalance)},
			kvPair{Key: "image", Value: strings.TrimSpace(app.Spec.Postgres.Image)},
		)
	}
	return writeKeyValues(c.stdout, pairs...)
}

func flagChanged(cmd *cobra.Command, name string) bool {
	flag := cmd.Flags().Lookup(name)
	return flag != nil && flag.Changed
}

package cli

import (
	"fmt"
	"reflect"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

const (
	defaultAppDatabaseInstances = 1
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
		c.newAppDatabaseLocalizeCommand(),
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
		PrimaryNodeName     string
		FailoverRuntimeName string
		FailoverRuntimeID   string
		ClearFailover       bool
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
			before := cloneAppPostgresSpec(ownedManagedPostgresSpec(app))
			if before != nil {
				spec.Postgres = cloneAppPostgresSpec(before)
			} else if spec.Postgres == nil {
				spec.Postgres = &model.AppPostgresSpec{}
			}
			if opts.ClearFailover && (strings.TrimSpace(opts.FailoverRuntimeName) != "" || strings.TrimSpace(opts.FailoverRuntimeID) != "") {
				return fmt.Errorf("--clear-failover cannot be combined with --failover-to or --failover-runtime-id")
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
			if opts.ClearFailover {
				spec.Postgres.FailoverTargetRuntimeID = ""
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
			if flagChanged(cmd, "primary-node") {
				spec.Postgres.PrimaryNodeName = strings.TrimSpace(opts.PrimaryNodeName)
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
	cmd.Flags().StringVar(&opts.PrimaryNodeName, "primary-node", "", "Kubernetes node name to pin the managed Postgres primary on shared runtimes")
	cmd.Flags().StringVar(&opts.FailoverRuntimeName, "failover-to", "", "Runtime name for managed Postgres failover")
	cmd.Flags().StringVar(&opts.FailoverRuntimeID, "failover-runtime-id", "", "Runtime ID for managed Postgres failover")
	cmd.Flags().BoolVar(&opts.ClearFailover, "clear-failover", false, "Clear the managed Postgres failover target")
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
			database := ownedManagedPostgresSpec(app)
			if database == nil {
				return fmt.Errorf("managed postgres is not configured for this app")
			}

			runtimeName := strings.TrimSpace(opts.RuntimeName)
			if len(args) == 2 {
				if runtimeName != "" || strings.TrimSpace(opts.RuntimeID) != "" {
					return fmt.Errorf("target runtime must be provided either as an argument or with --to")
				}
				runtimeName = strings.TrimSpace(args[1])
			}
			targetRuntimeID := strings.TrimSpace(database.FailoverTargetRuntimeID)
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

func (c *CLI) newAppDatabaseLocalizeCommand() *cobra.Command {
	opts := struct {
		TargetNodeName string
		Wait           bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "localize <app>",
		Short: "Move the app managed Postgres primary to the app runtime and keep one local instance",
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
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}
			if ownedManagedPostgresSpec(app) == nil {
				return fmt.Errorf("managed postgres is not configured for this app")
			}

			response, err := client.LocalizeAppDatabase(app.ID, opts.TargetNodeName)
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
					"app":              finalApp,
					"operation":        response.Operation,
					"target_node_name": strings.TrimSpace(opts.TargetNodeName),
				}
				return writeJSON(c.stdout, payload)
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "operation_id", Value: response.Operation.ID},
				kvPair{Key: "target_runtime_id", Value: strings.TrimSpace(response.Operation.TargetRuntimeID)},
				kvPair{Key: "target_node_name", Value: strings.TrimSpace(opts.TargetNodeName)},
			)
		},
	}
	cmd.Flags().StringVar(&opts.TargetNodeName, "node", "", "Explicit Kubernetes node name for the localized Postgres primary")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) renderAppDatabaseState(app model.App, operation *model.Operation, alreadyCurrent bool, showSecrets bool) error {
	database := ownedManagedPostgresSpec(app)
	if c.wantsJSON() {
		payloadApp := app
		var payloadOp *model.Operation
		if operation != nil {
			opCopy := *operation
			payloadOp = &opCopy
		}
		if !showSecrets {
			payloadApp = redactAppForOutput(payloadApp)
			database = ownedManagedPostgresSpec(payloadApp)
			if payloadOp != nil {
				redactedOp := redactOperationForOutput(*payloadOp)
				payloadOp = &redactedOp
			}
		}
		payload := map[string]any{
			"app":             payloadApp,
			"database":        cloneAppPostgresSpec(database),
			"already_current": alreadyCurrent,
		}
		if payloadOp != nil {
			payload["operation"] = payloadOp
		}
		return writeJSON(c.stdout, payload)
	}
	pairs := []kvPair{
		{Key: "app_id", Value: app.ID},
		{Key: "database_enabled", Value: fmt.Sprintf("%t", database != nil)},
	}
	if operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: operation.ID})
	}
	if alreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	if database != nil {
		pairs = append(pairs,
			kvPair{Key: "runtime_id", Value: strings.TrimSpace(database.RuntimeID)},
			kvPair{Key: "database", Value: strings.TrimSpace(database.Database)},
			kvPair{Key: "user", Value: strings.TrimSpace(database.User)},
			kvPair{Key: "service_name", Value: strings.TrimSpace(database.ServiceName)},
			kvPair{Key: "storage_size", Value: strings.TrimSpace(database.StorageSize)},
			kvPair{Key: "storage_class", Value: strings.TrimSpace(database.StorageClassName)},
			kvPair{Key: "instances", Value: fmt.Sprintf("%d", database.Instances)},
			kvPair{Key: "sync_replicas", Value: fmt.Sprintf("%d", database.SynchronousReplicas)},
			kvPair{Key: "failover_target_runtime_id", Value: strings.TrimSpace(database.FailoverTargetRuntimeID)},
			kvPair{Key: "primary_node_name", Value: strings.TrimSpace(database.PrimaryNodeName)},
			kvPair{Key: "pending_rebalance", Value: fmt.Sprintf("%t", database.PrimaryPlacementPendingRebalance)},
			kvPair{Key: "image", Value: strings.TrimSpace(database.Image)},
		)
	}
	return writeKeyValues(c.stdout, pairs...)
}

func ownedManagedPostgresSpec(app model.App) *model.AppPostgresSpec {
	if app.Spec.Postgres != nil {
		normalized := normalizeAppDatabasePostgresSpec(app.Name, app.Spec.RuntimeID, *app.Spec.Postgres)
		return &normalized
	}

	for _, service := range app.BackingServices {
		if strings.TrimSpace(service.OwnerAppID) != strings.TrimSpace(app.ID) {
			continue
		}
		if !isManagedPostgresBackingService(service) || service.Spec.Postgres == nil {
			continue
		}
		normalized := normalizeAppDatabasePostgresSpec(appNameForDatabaseService(&service, &app), app.Spec.RuntimeID, *service.Spec.Postgres)
		return &normalized
	}

	return nil
}

func normalizeAppDatabasePostgresSpec(appName, appRuntimeID string, spec model.AppPostgresSpec) model.AppPostgresSpec {
	out := spec
	resourceName := postgresServiceNameForApp(appName)
	out.Image = model.NormalizeManagedPostgresImage(out.Image)
	if strings.TrimSpace(out.Database) == "" {
		out.Database = serviceResourceName(appName)
	}
	if strings.TrimSpace(out.User) == "" {
		out.User = model.DefaultManagedPostgresUser(appName)
	}
	if strings.TrimSpace(out.ServiceName) == "" {
		out.ServiceName = resourceName
	}
	out.RuntimeID = strings.TrimSpace(out.RuntimeID)
	if out.RuntimeID == "" {
		out.RuntimeID = strings.TrimSpace(appRuntimeID)
	}
	out.FailoverTargetRuntimeID = strings.TrimSpace(out.FailoverTargetRuntimeID)
	out.PrimaryNodeName = strings.TrimSpace(out.PrimaryNodeName)
	if strings.TrimSpace(out.StorageSize) == "" {
		out.StorageSize = model.DefaultManagedPostgresStorageSize
	}
	out.StorageClassName = strings.TrimSpace(out.StorageClassName)
	if out.Instances <= 0 {
		out.Instances = defaultAppDatabaseInstances
	}
	if out.FailoverTargetRuntimeID != "" && out.Instances < 2 {
		out.Instances = 2
	}
	if out.SynchronousReplicas < 0 {
		out.SynchronousReplicas = 0
	}
	if out.FailoverTargetRuntimeID != "" && out.SynchronousReplicas < 1 {
		out.SynchronousReplicas = 1
	}
	if out.SynchronousReplicas >= out.Instances {
		out.SynchronousReplicas = out.Instances - 1
	}
	out.Resources = normalizeAppDatabaseResources(out.Resources, model.DefaultManagedPostgresResources())
	return out
}

func normalizeAppDatabaseResources(spec *model.ResourceSpec, defaults model.ResourceSpec) *model.ResourceSpec {
	if spec == nil {
		value := defaults
		return &value
	}
	out := *spec
	if out.CPUMilliCores < 0 || out.MemoryMebibytes < 0 {
		value := defaults
		return &value
	}
	if out.CPUMilliCores == 0 {
		out.CPUMilliCores = defaults.CPUMilliCores
	}
	if out.MemoryMebibytes == 0 {
		out.MemoryMebibytes = defaults.MemoryMebibytes
	}
	if out.CPUMilliCores <= 0 || out.MemoryMebibytes <= 0 {
		value := defaults
		return &value
	}
	return &out
}

func isManagedPostgresBackingService(service model.BackingService) bool {
	if !strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(service.Status), model.BackingServiceStatusDeleted) {
		return false
	}
	provisioner := strings.TrimSpace(strings.ToLower(service.Provisioner))
	return provisioner == "" || provisioner == model.BackingServiceProvisionerManaged
}

func appNameForDatabaseService(service *model.BackingService, app *model.App) string {
	if app != nil {
		return app.Name
	}
	if service != nil && strings.TrimSpace(service.Name) != "" {
		name := service.Name
		if strings.HasSuffix(name, "-postgres") {
			return strings.TrimSuffix(name, "-postgres")
		}
		return name
	}
	return "service"
}

func postgresServiceNameForApp(appName string) string {
	return serviceResourceName(appName) + "-postgres"
}

func serviceResourceName(name string) string {
	name = model.Slugify(name)
	if len(name) > 50 {
		return name[:50]
	}
	return name
}

func flagChanged(cmd *cobra.Command, name string) bool {
	flag := cmd.Flags().Lookup(name)
	return flag != nil && flag.Changed
}

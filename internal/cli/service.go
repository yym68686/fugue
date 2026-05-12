package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type postgresServiceCreateOptions struct {
	Description         string
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
}

func (c *CLI) newServiceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "service",
		Aliases: []string{"services", "db", "database"},
		Short:   "Inspect and manage backing services",
		Long: strings.TrimSpace(`
Use service names for normal operations.

Service creation defaults to the default project when you omit --project and
your tenant already has one.

Use "fugue service postgres create <name>" for the primary creation flow.

Use "fugue app binding" to attach or detach a backing service from an app.
`),
	}
	cmd.AddCommand(
		c.newServiceListCommand(),
		c.newServicePostgresCommand(),
		hideCompatCommand(c.newServiceCreateCommand(), "fugue service postgres create"),
		c.newServiceShowCommand(),
		c.newServiceMoveCommand(),
		c.newServiceLocalizeCommand(),
		c.newServiceRemoveCommand(),
	)
	return cmd
}

func (c *CLI) newServiceListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List visible backing services",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, projectID, err := c.resolveFilterSelections(client)
			if err != nil {
				return err
			}
			services, err := client.ListBackingServices()
			if err != nil {
				return err
			}
			filtered := filterServices(services, tenantID, projectID)
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"backing_services": cloneBackingServicesForOutput(filtered, true)})
			}
			projectNames := c.loadProjectNames(client, tenantID)
			var (
				appNames     map[string]string
				runtimeNames map[string]string
				needsApps    bool
				needsRuntime bool
			)
			for _, service := range filtered {
				if strings.TrimSpace(service.OwnerAppID) != "" {
					needsApps = true
				}
				if service.Spec.Postgres != nil && strings.TrimSpace(service.Spec.Postgres.RuntimeID) != "" {
					needsRuntime = true
				}
			}
			if needsApps {
				appNames = c.loadAppNames(client)
			}
			if needsRuntime {
				runtimeNames = c.loadRuntimeNames(client)
			}
			return writeServiceTableWithContext(c.stdout, filtered, projectNames, appNames, runtimeNames, c.showIDs())
		},
	}
}

func (c *CLI) newServiceCreateCommand() *cobra.Command {
	opts := struct {
		postgresServiceCreateOptions
		Type string
	}{Type: model.BackingServiceTypePostgres}
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Compatibility alias for service postgres create",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !strings.EqualFold(strings.TrimSpace(opts.Type), model.BackingServiceTypePostgres) {
				return fmt.Errorf("unsupported service type %q", opts.Type)
			}
			return c.createPostgresService(args[0], opts.postgresServiceCreateOptions)
		},
	}
	cmd.Flags().StringVar(&opts.Description, "description", "", "Service description")
	cmd.Flags().StringVar(&opts.Type, "type", opts.Type, "Compatibility service type flag")
	cmd.Flags().StringVar(&opts.RuntimeName, "runtime", "", "Runtime name for managed postgres")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Runtime ID for managed postgres")
	cmd.Flags().StringVar(&opts.Database, "database", "", "Database name")
	cmd.Flags().StringVar(&opts.User, "user", "", "Database user")
	cmd.Flags().StringVar(&opts.Password, "password", "", "Database password")
	cmd.Flags().StringVar(&opts.Image, "image", "", "Postgres image override")
	cmd.Flags().StringVar(&opts.ServiceName, "service-name", "", "Kubernetes service name override")
	cmd.Flags().StringVar(&opts.StorageSize, "storage-size", "", "Persistent storage size")
	cmd.Flags().StringVar(&opts.StorageClass, "storage-class", "", "Persistent storage class")
	cmd.Flags().IntVar(&opts.Instances, "instances", 0, "Number of postgres instances")
	cmd.Flags().IntVar(&opts.SynchronousReplicas, "sync-replicas", 0, "Number of synchronous postgres replicas")
	_ = cmd.Flags().MarkHidden("runtime-id")
	return cmd
}

func (c *CLI) newServicePostgresCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "postgres",
		Aliases: []string{"pg"},
		Short:   "Create postgres backing services",
	}
	cmd.AddCommand(c.newServicePostgresCreateCommand())
	return cmd
}

func (c *CLI) newServicePostgresCreateCommand() *cobra.Command {
	opts := postgresServiceCreateOptions{}
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a postgres backing service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.createPostgresService(args[0], opts)
		},
	}
	cmd.Flags().StringVar(&opts.Description, "description", "", "Service description")
	cmd.Flags().StringVar(&opts.RuntimeName, "runtime", "", "Runtime name for managed postgres")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Runtime ID for managed postgres")
	cmd.Flags().StringVar(&opts.Database, "database", "", "Database name")
	cmd.Flags().StringVar(&opts.User, "user", "", "Database user")
	cmd.Flags().StringVar(&opts.Password, "password", "", "Database password")
	cmd.Flags().StringVar(&opts.Image, "image", "", "Postgres image override")
	cmd.Flags().StringVar(&opts.ServiceName, "service-name", "", "Kubernetes service name override")
	cmd.Flags().StringVar(&opts.StorageSize, "storage-size", "", "Persistent storage size")
	cmd.Flags().StringVar(&opts.StorageClass, "storage-class", "", "Persistent storage class")
	cmd.Flags().IntVar(&opts.Instances, "instances", 0, "Number of postgres instances")
	cmd.Flags().IntVar(&opts.SynchronousReplicas, "sync-replicas", 0, "Number of synchronous postgres replicas")
	_ = cmd.Flags().MarkHidden("runtime-id")
	return cmd
}

func (c *CLI) createPostgresService(name string, opts postgresServiceCreateOptions) error {
	client, err := c.newClient()
	if err != nil {
		return err
	}
	tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
	if err != nil {
		return err
	}
	projectID, err := resolveProjectReference(client, tenantID, c.effectiveProjectID(), c.effectiveProjectName())
	if err != nil {
		return err
	}
	if strings.TrimSpace(projectID) == "" {
		project, ok, err := resolveDefaultProjectForCreate(client, tenantID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("project is required; pass --project or create a default project first")
		}
		projectID = project.ID
	}
	runtimeID, err := resolveRuntimeSelection(client, opts.RuntimeID, opts.RuntimeName)
	if err != nil {
		return err
	}
	spec := model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			Image:               strings.TrimSpace(opts.Image),
			Database:            strings.TrimSpace(opts.Database),
			User:                strings.TrimSpace(opts.User),
			Password:            opts.Password,
			ServiceName:         strings.TrimSpace(opts.ServiceName),
			RuntimeID:           strings.TrimSpace(runtimeID),
			StorageSize:         strings.TrimSpace(opts.StorageSize),
			StorageClassName:    strings.TrimSpace(opts.StorageClass),
			Instances:           opts.Instances,
			SynchronousReplicas: opts.SynchronousReplicas,
		},
	}
	service, err := client.CreateBackingService(createBackingServiceRequest{
		TenantID:    tenantID,
		ProjectID:   projectID,
		Name:        name,
		Description: opts.Description,
		Spec:        spec,
	})
	if err != nil {
		return err
	}
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{"backing_service": redactBackingServiceForOutput(service)})
	}
	return c.renderBackingServiceDetail(client, service)
}

func (c *CLI) newServiceShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <service>",
		Aliases: []string{"get", "status", "info"},
		Short:   "Show a backing service",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			service, err := c.resolveNamedService(client, args[0])
			if err != nil {
				return err
			}
			service, err = client.GetBackingService(service.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"backing_service": redactBackingServiceForOutput(service)})
			}
			return c.renderBackingServiceDetail(client, service)
		},
	}
}

func (c *CLI) newServiceMoveCommand() *cobra.Command {
	opts := struct {
		RuntimeName string
		RuntimeID   string
		Wait        bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "move <service>",
		Aliases: []string{"migrate"},
		Short:   "Move a backing service to another runtime",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
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
			service, err := c.resolveNamedService(client, args[0])
			if err != nil {
				return err
			}
			service, err = client.GetBackingService(service.ID)
			if err != nil {
				return err
			}
			response, err := client.MigrateBackingService(service.ID, runtimeID)
			if err != nil {
				return err
			}
			service = response.BackingService
			if response.Operation != nil && opts.Wait {
				if _, err := c.waitForOperations(client, []model.Operation{*response.Operation}); err != nil {
					return err
				}
				service, err = client.GetBackingService(service.ID)
				if err != nil {
					return err
				}
			}
			if c.wantsJSON() {
				payload := map[string]any{
					"backing_service": redactBackingServiceForOutput(service),
					"already_current": response.AlreadyCurrent,
				}
				if response.Operation != nil {
					payload["operation"] = response.Operation
				}
				return writeJSON(c.stdout, payload)
			}
			if response.Operation != nil && !opts.Wait {
				if _, err := fmt.Fprintf(c.stdout, "operation=%s\n\n", response.Operation.ID); err != nil {
					return err
				}
			}
			return c.renderBackingServiceDetail(client, service)
		},
	}
	cmd.Flags().StringVar(&opts.RuntimeName, "to", "", "Target runtime name")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Target runtime ID")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for backing service switchover completion")
	_ = cmd.Flags().MarkHidden("runtime-id")
	return cmd
}

func (c *CLI) newServiceLocalizeCommand() *cobra.Command {
	opts := struct {
		RuntimeName string
		RuntimeID   string
		NodeName    string
		Wait        bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "localize <service>",
		Short: "Move a managed Postgres backing service primary to one local instance",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
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
			service, err := c.resolveNamedService(client, args[0])
			if err != nil {
				return err
			}
			service, err = client.GetBackingService(service.ID)
			if err != nil {
				return err
			}
			response, err := client.LocalizeBackingService(service.ID, runtimeID, opts.NodeName)
			if err != nil {
				return err
			}
			service = response.BackingService
			if response.Operation != nil && opts.Wait {
				if _, err := c.waitForOperations(client, []model.Operation{*response.Operation}); err != nil {
					return err
				}
				service, err = client.GetBackingService(service.ID)
				if err != nil {
					return err
				}
			}
			if c.wantsJSON() {
				payload := map[string]any{
					"backing_service":  redactBackingServiceForOutput(service),
					"already_current":  response.AlreadyCurrent,
					"target_node_name": strings.TrimSpace(opts.NodeName),
				}
				if response.Operation != nil {
					payload["operation"] = response.Operation
				}
				return writeJSON(c.stdout, payload)
			}
			if response.Operation != nil && !opts.Wait {
				if _, err := fmt.Fprintf(c.stdout, "operation=%s\n\n", response.Operation.ID); err != nil {
					return err
				}
			}
			if response.Operation != nil {
				if err := writeKeyValues(c.stdout,
					kvPair{Key: "operation_id", Value: response.Operation.ID},
					kvPair{Key: "target_runtime_id", Value: strings.TrimSpace(response.Operation.TargetRuntimeID)},
					kvPair{Key: "target_node_name", Value: strings.TrimSpace(opts.NodeName)},
				); err != nil {
					return err
				}
				if _, err := fmt.Fprintln(c.stdout); err != nil {
					return err
				}
			}
			return c.renderBackingServiceDetail(client, service)
		},
	}
	cmd.Flags().StringVar(&opts.RuntimeName, "to", "", "Target runtime name")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Target runtime ID")
	cmd.Flags().StringVar(&opts.NodeName, "node", "", "Explicit Kubernetes node name for the localized Postgres primary")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for backing service localize completion")
	_ = cmd.Flags().MarkHidden("runtime-id")
	return cmd
}

func (c *CLI) newServiceRemoveCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <service>",
		Aliases: []string{"rm", "remove"},
		Short:   "Delete a backing service",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			service, err := c.resolveNamedService(client, args[0])
			if err != nil {
				return err
			}
			service, err = client.DeleteBackingService(service.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"deleted":         true,
					"backing_service": redactBackingServiceForOutput(service),
				})
			}
			if err := c.renderBackingServiceDetail(client, service); err != nil {
				return err
			}
			_, err = fmt.Fprintln(c.stdout, "deleted=true")
			return err
		},
	}
}

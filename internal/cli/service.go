package cli

import (
	"fmt"
	"io"
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
				return writeJSON(c.stdout, map[string]any{"backing_services": filtered})
			}
			return writeServiceTable(c.stdout, filtered)
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
		return writeJSON(c.stdout, map[string]any{"backing_service": service})
	}
	return renderBackingService(c.stdout, service)
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
				return writeJSON(c.stdout, map[string]any{"backing_service": service})
			}
			return renderBackingService(c.stdout, service)
		},
	}
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
					"backing_service": service,
				})
			}
			if err := renderBackingService(c.stdout, service); err != nil {
				return err
			}
			_, err = fmt.Fprintln(c.stdout, "deleted=true")
			return err
		},
	}
}

func renderBackingService(w io.Writer, service model.BackingService) error {
	pairs := []kvPair{
		{Key: "service_id", Value: service.ID},
		{Key: "name", Value: service.Name},
		{Key: "type", Value: service.Type},
		{Key: "status", Value: service.Status},
		{Key: "project_id", Value: service.ProjectID},
		{Key: "current_resource_usage", Value: formatResourceUsageSummary(service.CurrentResourceUsage)},
	}
	if strings.TrimSpace(service.OwnerAppID) != "" {
		pairs = append(pairs, kvPair{Key: "owner_app_id", Value: service.OwnerAppID})
	}
	if service.Spec.Postgres != nil {
		pairs = append(pairs,
			kvPair{Key: "runtime_id", Value: service.Spec.Postgres.RuntimeID},
			kvPair{Key: "database", Value: service.Spec.Postgres.Database},
			kvPair{Key: "user", Value: service.Spec.Postgres.User},
			kvPair{Key: "service_name", Value: service.Spec.Postgres.ServiceName},
			kvPair{Key: "storage_size", Value: service.Spec.Postgres.StorageSize},
		)
		if service.Spec.Postgres.Instances > 0 {
			pairs = append(pairs, kvPair{Key: "instances", Value: fmt.Sprintf("%d", service.Spec.Postgres.Instances)})
		}
	}
	return writeKeyValues(w, pairs...)
}

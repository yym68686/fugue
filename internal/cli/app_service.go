package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppServiceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "service",
		Aliases: []string{"services"},
		Short:   "Attach, detach, and inspect app service connections",
	}
	cmd.AddCommand(
		c.newAppServiceListCommand(),
		c.newAppServiceAttachCommand(),
		c.newAppServiceDetachCommand(),
	)
	return cmd
}

func (c *CLI) newAppBindingCompatCommand() *cobra.Command {
	cmd := c.newAppServiceCommand()
	cmd.Use = "binding"
	cmd.Aliases = []string{"bindings"}
	cmd.Short = "Compatibility alias for app service connections"
	return cmd
}

func (c *CLI) newAppServiceListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls <app>",
		Aliases: []string{"list"},
		Short:   "List service bindings for an app",
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
			response, err := client.ListAppBindings(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app_id":           app.ID,
					"bindings":         response.Bindings,
					"backing_services": response.BackingServices,
				})
			}
			if err := writeKeyValues(c.stdout, kvPair{Key: "app_id", Value: app.ID}); err != nil {
				return err
			}
			if len(response.Bindings) == 0 {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeBindingTable(c.stdout, response.Bindings, response.BackingServices)
		},
	}
}

func (c *CLI) newAppServiceAttachCommand() *cobra.Command {
	opts := struct {
		Alias string
		Env   []string
		Wait  bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "attach <app> <service>",
		Aliases: []string{"bind"},
		Short:   "Attach a backing service to an app",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			service, err := c.resolveNamedService(client, args[1])
			if err != nil {
				return err
			}
			env, err := parseEnvAssignments(opts.Env)
			if err != nil {
				return err
			}
			response, err := client.CreateAppBinding(app.ID, service.ID, opts.Alias, env)
			if err != nil {
				return err
			}
			if err := c.waitForOptionalOperation(client, &response.Operation, opts.Wait); err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app_id":          app.ID,
					"binding":         response.Binding,
					"backing_service": response.BackingService,
					"operation":       response.Operation,
				})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "binding_id", Value: response.Binding.ID},
				kvPair{Key: "service_id", Value: response.Binding.ServiceID},
				kvPair{Key: "service_name", Value: response.BackingService.Name},
				kvPair{Key: "alias", Value: response.Binding.Alias},
				kvPair{Key: "operation_id", Value: response.Operation.ID},
			)
		},
	}
	cmd.Flags().StringVar(&opts.Alias, "alias", "", "Binding alias used to namespace credentials")
	cmd.Flags().StringArrayVarP(&opts.Env, "env", "e", nil, "Extra KEY=VALUE env pairs to inject with the binding")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newAppServiceDetachCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "detach <app> <binding-or-service>",
		Aliases: []string{"unbind"},
		Short:   "Detach a backing service from an app",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			bindings, err := client.ListAppBindings(app.ID)
			if err != nil {
				return err
			}
			binding, service, err := resolveBindingReference(bindings, args[1])
			if err != nil {
				return err
			}
			response, err := client.DeleteAppBinding(app.ID, binding.ID)
			if err != nil {
				return err
			}
			if err := c.waitForOptionalOperation(client, &response.Operation, opts.Wait); err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app_id":          app.ID,
					"binding":         response.Binding,
					"backing_service": response.BackingService,
					"operation":       response.Operation,
				})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "binding_id", Value: binding.ID},
				kvPair{Key: "service_id", Value: binding.ServiceID},
				kvPair{Key: "service_name", Value: service.Name},
				kvPair{Key: "operation_id", Value: response.Operation.ID},
			)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func resolveBindingReference(response appBindingsResponse, ref string) (model.ServiceBinding, model.BackingService, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return model.ServiceBinding{}, model.BackingService{}, fmt.Errorf("binding or service is required")
	}
	serviceByID := make(map[string]model.BackingService, len(response.BackingServices))
	for _, service := range response.BackingServices {
		serviceByID[service.ID] = service
	}
	var matches []model.ServiceBinding
	for _, binding := range response.Bindings {
		service := serviceByID[binding.ServiceID]
		switch {
		case strings.EqualFold(binding.ID, ref):
			matches = append(matches, binding)
		case strings.TrimSpace(binding.Alias) != "" && strings.EqualFold(binding.Alias, ref):
			matches = append(matches, binding)
		case strings.EqualFold(binding.ServiceID, ref):
			matches = append(matches, binding)
		case strings.TrimSpace(service.Name) != "" && strings.EqualFold(service.Name, ref):
			matches = append(matches, binding)
		}
	}
	if len(matches) == 0 {
		return model.ServiceBinding{}, model.BackingService{}, fmt.Errorf("binding or service %q not found", ref)
	}
	if len(matches) > 1 {
		return model.ServiceBinding{}, model.BackingService{}, fmt.Errorf("multiple bindings match %q; use a binding id", ref)
	}
	return matches[0], serviceByID[matches[0].ServiceID], nil
}

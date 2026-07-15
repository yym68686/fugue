package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newServiceSuspendCommand() *cobra.Command {
	return c.newServiceLifecycleCommand(true)
}

func (c *CLI) newServiceResumeCommand() *cobra.Command {
	return c.newServiceLifecycleCommand(false)
}

func (c *CLI) newServiceLifecycleCommand(suspended bool) *cobra.Command {
	action := "resume"
	short := "Resume a suspended managed Postgres backing service"
	if suspended {
		action = "suspend"
		short = "Suspend a managed Postgres backing service while retaining its storage"
	}
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   action + " <service>",
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.setServiceSuspended(args[0], suspended, opts.Wait)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the backing service lifecycle operation to complete")
	return cmd
}

func (c *CLI) setServiceSuspended(ref string, suspended, wait bool) error {
	client, err := c.newClient()
	if err != nil {
		return err
	}
	service, err := c.resolveNamedService(client, ref)
	if err != nil {
		return err
	}
	service, err = client.GetBackingService(service.ID)
	if err != nil {
		return err
	}
	if !isManagedPostgresBackingService(service) {
		return fmt.Errorf("backing service %q is not an active managed Postgres service", ref)
	}

	var response backingServiceLifecycleResponse
	if suspended {
		response, err = client.SuspendBackingService(service.ID)
	} else {
		response, err = client.ResumeBackingService(service.ID)
	}
	if err != nil {
		return err
	}
	service = response.BackingService
	if response.Operation != nil && wait {
		final, waitErr := c.waitForOperations(client, []model.Operation{*response.Operation})
		if waitErr != nil {
			return waitErr
		}
		if len(final) > 0 {
			operation := final[0]
			response.Operation = &operation
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
			payload["operation"] = redactOperationForOutput(*response.Operation)
		}
		return writeJSON(c.stdout, payload)
	}

	pairs := []kvPair{{Key: "already_current", Value: fmt.Sprintf("%t", response.AlreadyCurrent)}}
	if response.Operation != nil {
		pairs = append(pairs,
			kvPair{Key: "operation_id", Value: response.Operation.ID},
			kvPair{Key: "operation_status", Value: response.Operation.Status},
		)
	}
	if err := writeKeyValues(c.stdout, pairs...); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(c.stdout); err != nil {
		return err
	}
	return c.renderBackingServiceDetail(client, service)
}

func (c *CLI) newServicePostgresOrphanCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "orphan",
		Aliases: []string{"orphans"},
		Short:   "Inspect and adopt retained orphan managed Postgres resources",
	}
	cmd.AddCommand(
		c.newServicePostgresOrphanListCommand(),
		c.newServicePostgresOrphanAdoptCommand(),
	)
	return cmd
}

func (c *CLI) newServicePostgresOrphanListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List retained orphan managed Postgres resources",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			orphans, err := client.ListOrphanManagedApps()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"orphans": orphans})
			}
			return writeOrphanManagedAppTable(c.stdout, orphans)
		},
	}
}

func (c *CLI) newServicePostgresOrphanAdoptCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "adopt <app-id>",
		Short: "Adopt retained orphan managed Postgres resources into the Fugue store",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			appID := strings.TrimSpace(args[0])
			if appID == "" {
				return fmt.Errorf("app id is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.AdoptOrphanManagedApp(appID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app":              redactAppForOutput(response.App),
					"backing_services": cloneBackingServicesForOutput(response.BackingServices, true),
					"already_adopted":  response.AlreadyAdopted,
				})
			}
			if err := writeKeyValues(c.stdout,
				kvPair{Key: "app", Value: formatDisplayName(response.App.Name, response.App.ID, c.showIDs())},
				kvPair{Key: "tenant_id", Value: response.App.TenantID},
				kvPair{Key: "project_id", Value: response.App.ProjectID},
				kvPair{Key: "already_adopted", Value: fmt.Sprintf("%t", response.AlreadyAdopted)},
				kvPair{Key: "backing_services", Value: formatInt(len(response.BackingServices))},
			); err != nil {
				return err
			}
			if len(response.BackingServices) == 0 {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeServiceTable(c.stdout, response.BackingServices)
		},
	}
}

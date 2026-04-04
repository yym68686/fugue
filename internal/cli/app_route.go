package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppRouteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "route",
		Aliases: []string{"routes"},
		Short:   "Inspect and manage the primary app route",
	}
	cmd.AddCommand(
		c.newAppRouteShowCommand(),
		c.newAppRouteCheckCommand(),
		c.newAppRouteSetCommand(),
	)
	return cmd
}

func (c *CLI) newAppRouteShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <app>",
		Aliases: []string{"status", "get"},
		Short:   "Show the app's primary route",
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
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app_id": app.ID,
					"route":  app.Route,
				})
			}
			pairs := []kvPair{{Key: "app_id", Value: app.ID}}
			if app.Route != nil {
				pairs = append(pairs,
					kvPair{Key: "hostname", Value: app.Route.Hostname},
					kvPair{Key: "base_domain", Value: app.Route.BaseDomain},
					kvPair{Key: "public_url", Value: app.Route.PublicURL},
				)
				if app.Route.ServicePort > 0 {
					pairs = append(pairs, kvPair{Key: "service_port", Value: fmt.Sprintf("%d", app.Route.ServicePort)})
				}
			}
			return writeKeyValues(c.stdout, pairs...)
		},
	}
}

func (c *CLI) newAppRouteCheckCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "check <app> <hostname>",
		Short: "Check whether a primary route hostname is available",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			availability, err := client.GetAppRouteAvailability(app.ID, args[1])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app_id":       app.ID,
					"availability": availability,
				})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "input", Value: availability.Input},
				kvPair{Key: "label", Value: availability.Label},
				kvPair{Key: "hostname", Value: availability.Hostname},
				kvPair{Key: "base_domain", Value: availability.BaseDomain},
				kvPair{Key: "public_url", Value: availability.PublicURL},
				kvPair{Key: "valid", Value: fmt.Sprintf("%t", availability.Valid)},
				kvPair{Key: "available", Value: fmt.Sprintf("%t", availability.Available)},
				kvPair{Key: "current", Value: fmt.Sprintf("%t", availability.Current)},
				kvPair{Key: "reason", Value: availability.Reason},
			)
		},
	}
}

func (c *CLI) newAppRouteSetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "set <app> <hostname>",
		Short: "Update the app's primary route hostname",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.PatchAppRoute(app.ID, args[1])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app_id":          app.ID,
					"app":             response.App,
					"availability":    response.Availability,
					"already_current": response.AlreadyCurrent,
				})
			}
			pairs := []kvPair{
				{Key: "app_id", Value: response.App.ID},
				{Key: "hostname", Value: response.Availability.Hostname},
				{Key: "public_url", Value: response.Availability.PublicURL},
				{Key: "available", Value: fmt.Sprintf("%t", response.Availability.Available)},
				{Key: "current", Value: fmt.Sprintf("%t", response.Availability.Current)},
			}
			if response.AlreadyCurrent {
				pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
			}
			if value := strings.TrimSpace(response.Availability.Reason); value != "" {
				pairs = append(pairs, kvPair{Key: "reason", Value: value})
			}
			return writeKeyValues(c.stdout, pairs...)
		},
	}
}

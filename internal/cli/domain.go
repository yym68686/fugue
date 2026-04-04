package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type domainListResult struct {
	AppID   string            `json:"app_id,omitempty"`
	Domains []model.AppDomain `json:"domains"`
}

type domainAvailabilityResult struct {
	AppID        string                `json:"app_id,omitempty"`
	Availability appDomainAvailability `json:"availability"`
}

type domainMutationResult struct {
	AppID          string                 `json:"app_id,omitempty"`
	Domain         *model.AppDomain       `json:"domain,omitempty"`
	Availability   *appDomainAvailability `json:"availability,omitempty"`
	AlreadyCurrent bool                   `json:"already_current,omitempty"`
	Verified       bool                   `json:"verified,omitempty"`
	Removed        bool                   `json:"removed,omitempty"`
}

func (c *CLI) newDomainCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "domain",
		Aliases: []string{"domains"},
		Short:   "Inspect and manage app custom domains",
	}
	cmd.AddCommand(
		c.newDomainListCommand(),
		c.newDomainCheckCommand(),
		c.newDomainAddCommand(),
		c.newDomainVerifyCommand(),
		c.newDomainRemoveCommand(),
	)
	return cmd
}

func (c *CLI) newDomainCompatCommand() *cobra.Command {
	return hideCompatCommand(c.newDomainCommand(), "fugue app domain")
}

func (c *CLI) newDomainListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls <app>",
		Aliases: []string{"list"},
		Short:   "List custom domains for an app",
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
			domains, err := client.ListAppDomains(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, domainListResult{
					AppID:   app.ID,
					Domains: domains,
				})
			}
			return writeDomainTable(c.stdout, domains)
		},
	}
}

func (c *CLI) newDomainCheckCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "check <app> <hostname>",
		Aliases: []string{"status", "availability"},
		Short:   "Check whether a custom domain can be attached",
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
			availability, err := client.GetAppDomainAvailability(app.ID, args[1])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, domainAvailabilityResult{
					AppID:        app.ID,
					Availability: availability,
				})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "input", Value: availability.Input},
				kvPair{Key: "hostname", Value: availability.Hostname},
				kvPair{Key: "valid", Value: fmt.Sprintf("%t", availability.Valid)},
				kvPair{Key: "available", Value: fmt.Sprintf("%t", availability.Available)},
				kvPair{Key: "current", Value: fmt.Sprintf("%t", availability.Current)},
				kvPair{Key: "reason", Value: availability.Reason},
			)
		},
	}
}

func (c *CLI) newDomainAddCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "add <app> <hostname>",
		Aliases: []string{"attach", "connect"},
		Short:   "Attach a custom domain to an app",
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
			response, err := client.PutAppDomain(app.ID, args[1])
			if err != nil {
				return err
			}
			return c.renderDomainMutation(domainMutationResult{
				AppID:          app.ID,
				Domain:         &response.Domain,
				Availability:   &response.Availability,
				AlreadyCurrent: response.AlreadyCurrent,
			})
		},
	}
}

func (c *CLI) newDomainVerifyCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "verify <app> <hostname>",
		Short: "Re-check DNS verification for a custom domain",
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
			response, err := client.VerifyAppDomain(app.ID, args[1])
			if err != nil {
				return err
			}
			return c.renderDomainMutation(domainMutationResult{
				AppID:    app.ID,
				Domain:   &response.Domain,
				Verified: response.Verified,
			})
		},
	}
}

func (c *CLI) newDomainRemoveCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <app> <hostname>",
		Aliases: []string{"rm", "remove", "detach"},
		Short:   "Remove a custom domain from an app",
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
			domain, err := client.DeleteAppDomain(app.ID, args[1])
			if err != nil {
				return err
			}
			return c.renderDomainMutation(domainMutationResult{
				AppID:   app.ID,
				Domain:  &domain,
				Removed: true,
			})
		},
	}
}

func (c *CLI) renderDomainMutation(result domainMutationResult) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, result)
	}

	pairs := make([]kvPair, 0, 12)
	if strings.TrimSpace(result.AppID) != "" {
		pairs = append(pairs, kvPair{Key: "app_id", Value: result.AppID})
	}
	if result.Domain != nil {
		pairs = append(pairs, kvPair{Key: "hostname", Value: result.Domain.Hostname})
		if value := strings.TrimSpace(result.Domain.Status); value != "" {
			pairs = append(pairs, kvPair{Key: "status", Value: value})
		}
		if value := strings.TrimSpace(result.Domain.TLSStatus); value != "" {
			pairs = append(pairs, kvPair{Key: "tls_status", Value: value})
		}
		if value := strings.TrimSpace(result.Domain.RouteTarget); value != "" {
			pairs = append(pairs, kvPair{Key: "route_target", Value: value})
		}
		if value := strings.TrimSpace(result.Domain.LastMessage); value != "" {
			pairs = append(pairs, kvPair{Key: "last_message", Value: value})
		}
	}
	if result.Availability != nil {
		pairs = append(pairs,
			kvPair{Key: "available", Value: fmt.Sprintf("%t", result.Availability.Available)},
			kvPair{Key: "current", Value: fmt.Sprintf("%t", result.Availability.Current)},
		)
		if value := strings.TrimSpace(result.Availability.Reason); value != "" {
			pairs = append(pairs, kvPair{Key: "reason", Value: value})
		}
	}
	if result.AlreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	if result.Verified {
		pairs = append(pairs, kvPair{Key: "verified", Value: "true"})
	}
	if result.Removed {
		pairs = append(pairs, kvPair{Key: "removed", Value: "true"})
	}
	return writeKeyValues(c.stdout, pairs...)
}

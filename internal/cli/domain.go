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

type domainDiagnosisResult struct {
	AppID     string             `json:"app_id,omitempty"`
	Diagnosis appDomainDiagnosis `json:"diagnosis"`
}

type domainRepairResult struct {
	AppID     string             `json:"app_id,omitempty"`
	Domain    *model.AppDomain   `json:"domain,omitempty"`
	Diagnosis appDomainDiagnosis `json:"diagnosis"`
}

func (c *CLI) newDomainCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "domain",
		Aliases: []string{"domains"},
		Short:   "Inspect and manage app custom domains",
	}
	cmd.AddCommand(
		c.newDomainPrimaryCommand(),
		c.newDomainListCommand(),
		c.newDomainCheckCommand(),
		c.newDomainAddCommand(),
		c.newDomainVerifyCommand(),
		c.newDomainDiagnoseCommand(),
		c.newDomainRepairCommand(),
		c.newDomainRemoveCommand(),
	)
	return cmd
}

func (c *CLI) newDomainCompatCommand() *cobra.Command {
	return hideCompatCommand(c.newDomainCommand(), "fugue app domain")
}

func (c *CLI) newDomainPrimaryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "primary",
		Short: "Inspect and manage the primary app domain",
	}
	cmd.AddCommand(
		c.newDomainPrimaryShowCommand(),
		c.newDomainPrimaryCheckCommand(),
		c.newDomainPrimarySetCommand(),
	)
	return cmd
}

func (c *CLI) newDomainPrimaryShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <app>",
		Aliases: []string{"get", "status"},
		Short:   "Show the app's primary domain",
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
			pairs := []kvPair{
				{Key: "app", Value: app.Name},
				{Key: "app_id", Value: app.ID},
			}
			if app.Route != nil {
				pairs = append(pairs,
					kvPair{Key: "hostname", Value: app.Route.Hostname},
					kvPair{Key: "path_prefix", Value: model.NormalizeAppRoutePathPrefix(app.Route.PathPrefix)},
					kvPair{Key: "base_domain", Value: app.Route.BaseDomain},
					kvPair{Key: "public_url", Value: app.Route.PublicURL},
					kvPair{Key: "domain_name", Value: app.Route.DomainName},
					kvPair{Key: "entrypoint_name", Value: app.Route.EntrypointName},
				)
				if app.Route.ServicePort > 0 {
					pairs = append(pairs, kvPair{Key: "service_port", Value: fmt.Sprintf("%d", app.Route.ServicePort)})
				}
			}
			return writeKeyValues(c.stdout, pairs...)
		},
	}
}

func (c *CLI) newDomainPrimaryCheckCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "check <app> <hostname>",
		Short: "Check whether a primary domain hostname is available",
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
			availability, err := client.GetAppRouteAvailability(app.ID, args[1], "")
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
				kvPair{Key: "path_prefix", Value: availability.PathPrefix},
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

func (c *CLI) newDomainPrimarySetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "set <app> <hostname>",
		Short: "Update the app's primary domain hostname",
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
			response, err := client.PatchAppRoute(app.ID, args[1], "")
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
				{Key: "app", Value: response.App.Name},
				{Key: "app_id", Value: response.App.ID},
				{Key: "hostname", Value: response.Availability.Hostname},
				{Key: "path_prefix", Value: response.Availability.PathPrefix},
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

func (c *CLI) newDomainDiagnoseCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "diagnose <app> <hostname>",
		Aliases: []string{"diagnosis"},
		Short:   "Show DNS, TLS, and shared certificate health for a custom domain",
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
			response, err := client.GetAppDomainDiagnosis(app.ID, args[1])
			if err != nil {
				return err
			}
			return c.renderDomainDiagnosis(domainDiagnosisResult{
				AppID:     app.ID,
				Diagnosis: response.Diagnosis,
			})
		},
	}
}

func (c *CLI) newDomainRepairCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "repair <app> <hostname>",
		Short: "Re-verify and repair a custom domain's DNS and shared TLS state",
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
			response, err := client.RepairAppDomain(app.ID, args[1])
			if err != nil {
				return err
			}
			return c.renderDomainRepair(domainRepairResult{
				AppID:     app.ID,
				Domain:    &response.Domain,
				Diagnosis: response.Diagnosis,
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
		if value := strings.TrimSpace(result.Domain.DNSStatus); value != "" {
			pairs = append(pairs, kvPair{Key: "dns_status", Value: value})
		}
		if value := strings.TrimSpace(result.Domain.DNSRecordKind); value != "" {
			pairs = append(pairs, kvPair{Key: "dns_record_kind", Value: value})
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

func (c *CLI) renderDomainDiagnosis(result domainDiagnosisResult) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, result)
	}
	pairs := []kvPair{
		{Key: "app_id", Value: result.AppID},
		{Key: "hostname", Value: result.Diagnosis.Domain.Hostname},
		{Key: "status", Value: strings.TrimSpace(result.Diagnosis.Domain.Status)},
		{Key: "dns_status", Value: strings.TrimSpace(result.Diagnosis.Domain.DNSStatus)},
		{Key: "dns_record_kind", Value: strings.TrimSpace(result.Diagnosis.Domain.DNSRecordKind)},
		{Key: "tls_status", Value: strings.TrimSpace(result.Diagnosis.Domain.TLSStatus)},
		{Key: "route_target", Value: strings.TrimSpace(result.Diagnosis.Domain.RouteTarget)},
		{Key: "dns_verified", Value: fmt.Sprintf("%t", result.Diagnosis.DNSObservation.Verified)},
		{Key: "dns_record_kind_observed", Value: strings.TrimSpace(result.Diagnosis.DNSObservation.RecordKind)},
		{Key: "dns_cname", Value: strings.TrimSpace(result.Diagnosis.DNSObservation.CNAME)},
		{Key: "dns_matched_target", Value: strings.TrimSpace(result.Diagnosis.DNSObservation.MatchedTarget)},
		{Key: "dns_message", Value: strings.TrimSpace(result.Diagnosis.DNSObservation.Message)},
		{Key: "shared_tls_certificate_present", Value: fmt.Sprintf("%t", result.Diagnosis.SharedTLSCertificate.Present)},
		{Key: "shared_tls_certificate_sha256", Value: strings.TrimSpace(result.Diagnosis.SharedTLSCertificate.CertificateSHA256)},
		{Key: "shared_tls_issuer_storage", Value: strings.TrimSpace(result.Diagnosis.SharedTLSCertificate.IssuerStorage)},
	}
	if len(result.Diagnosis.DNSTargets) > 0 {
		pairs = append(pairs, kvPair{Key: "dns_targets", Value: strings.Join(result.Diagnosis.DNSTargets, ", ")})
	}
	for _, check := range result.Diagnosis.Checks {
		name := strings.TrimSpace(check.Name)
		if name == "" {
			continue
		}
		value := strings.TrimSpace(check.Status)
		if msg := strings.TrimSpace(check.Message); msg != "" {
			value += " (" + msg + ")"
		}
		pairs = append(pairs, kvPair{Key: "check_" + name, Value: value})
	}
	if len(result.Diagnosis.RecommendedActions) > 0 {
		pairs = append(pairs, kvPair{Key: "recommended_actions", Value: strings.Join(result.Diagnosis.RecommendedActions, " | ")})
	}
	return writeKeyValues(c.stdout, pairs...)
}

func (c *CLI) renderDomainRepair(result domainRepairResult) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, result)
	}
	if err := c.renderDomainMutation(domainMutationResult{
		AppID:  result.AppID,
		Domain: result.Domain,
	}); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(c.stdout); err != nil {
		return err
	}
	return c.renderDomainDiagnosis(domainDiagnosisResult{
		AppID:     result.AppID,
		Diagnosis: result.Diagnosis,
	})
}

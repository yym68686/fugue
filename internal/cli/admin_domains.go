package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminDomainsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "domains",
		Short: "Manage platform-owned hostname bindings",
		Long: strings.TrimSpace(`
Platform domains are hostnames inside the Fugue app domain, such as fugue.pro
or www.fugue.pro. A binding attaches one hostname to one app and lets the
authoritative DNS bundle and edge route bundle derive the data-plane state.
`),
		Example: strings.TrimSpace(`
  fugue admin domains bind fugue.pro --app fugue-web
  fugue admin domains bind www.fugue.pro --app fugue-web
  fugue admin domains get fugue.pro
`),
	}
	cmd.AddCommand(
		c.newAdminDomainsListCommand(),
		c.newAdminDomainsGetCommand(),
		c.newAdminDomainsBindCommand(),
		c.newAdminDomainsUnbindCommand(),
	)
	return cmd
}

func (c *CLI) newAdminDomainsListCommand() *cobra.Command {
	opts := struct {
		Zone string
	}{}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List platform-owned domain bindings",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			bindings, err := client.ListPlatformDomainBindings(opts.Zone)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"bindings": bindings})
			}
			return writePlatformDomainBindingTable(c.stdout, bindings)
		},
	}
	cmd.Flags().StringVar(&opts.Zone, "zone", "", "Only show bindings inside this DNS zone")
	return cmd
}

func (c *CLI) newAdminDomainsGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "get <hostname>",
		Aliases: []string{"show", "status"},
		Short:   "Show one platform-owned domain binding",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			binding, err := client.GetPlatformDomainBinding(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"binding": binding})
			}
			return writePlatformDomainBinding(c.stdout, binding)
		},
	}
}

func (c *CLI) newAdminDomainsBindCommand() *cobra.Command {
	opts := struct {
		AppRef      string
		RoutePolicy string
		EdgeGroupID string
	}{}
	cmd := &cobra.Command{
		Use:   "bind <hostname>",
		Short: "Bind a platform-owned hostname to an app",
		Args:  cobra.ExactArgs(1),
		Example: strings.TrimSpace(`
  fugue admin domains bind fugue.pro --app fugue-web
  fugue admin domains bind www.fugue.pro --app fugue-web
  fugue admin domains bind preview.fugue.pro --app demo --policy edge_canary --edge-group edge-group-country-us
`),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.AppRef) == "" {
				return fmt.Errorf("--app is required")
			}
			policy := model.NormalizeEdgeRoutePolicy(opts.RoutePolicy)
			if strings.TrimSpace(opts.RoutePolicy) == "" {
				policy = model.EdgeRoutePolicyEnabled
			}
			if policy == "" {
				return fmt.Errorf("--policy must be route_a_only, edge_canary, or edge_enabled")
			}
			if policy == model.EdgeRoutePolicyCanary && strings.TrimSpace(opts.EdgeGroupID) == "" {
				return fmt.Errorf("--edge-group is required when --policy=edge_canary")
			}
			if policy == model.EdgeRoutePolicyEnabled && strings.TrimSpace(opts.EdgeGroupID) != "" {
				return fmt.Errorf("--edge-group is only supported when --policy=edge_canary")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, opts.AppRef)
			if err != nil {
				return err
			}
			binding, err := client.PutPlatformDomainBinding(args[0], putPlatformDomainBindingRequest{
				AppID:       app.ID,
				RoutePolicy: policy,
				EdgeGroupID: opts.EdgeGroupID,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"binding": binding})
			}
			return writePlatformDomainBinding(c.stdout, binding)
		},
	}
	cmd.Flags().StringVar(&opts.AppRef, "app", "", "App name or ID to serve this hostname")
	cmd.Flags().StringVar(&opts.RoutePolicy, "policy", model.EdgeRoutePolicyEnabled, "Policy to apply: route_a_only, edge_canary, or edge_enabled")
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Edge group for edge_canary")
	return cmd
}

func (c *CLI) newAdminDomainsUnbindCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "unbind <hostname>",
		Aliases: []string{"delete", "rm", "remove"},
		Short:   "Remove a platform-owned domain binding",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.DeletePlatformDomainBinding(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writePlatformDomainBinding(c.stdout, response.Binding); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "deleted=%t\n", response.Deleted)
			return err
		},
	}
}

func writePlatformDomainBinding(w io.Writer, binding model.PlatformDomainBinding) error {
	return writeKeyValues(w,
		kvPair{Key: "hostname", Value: strings.TrimSpace(binding.Hostname)},
		kvPair{Key: "zone", Value: strings.TrimSpace(binding.Zone)},
		kvPair{Key: "app", Value: strings.TrimSpace(firstNonEmptyTrimmed(binding.AppName, binding.AppID))},
		kvPair{Key: "app_id", Value: strings.TrimSpace(binding.AppID)},
		kvPair{Key: "project", Value: strings.TrimSpace(binding.ProjectID)},
		kvPair{Key: "tenant", Value: strings.TrimSpace(binding.TenantID)},
		kvPair{Key: "status", Value: strings.TrimSpace(binding.Status)},
		kvPair{Key: "tls", Value: strings.TrimSpace(binding.TLSStatus)},
		kvPair{Key: "route_policy", Value: strings.TrimSpace(binding.RoutePolicy)},
		kvPair{Key: "edge_group", Value: strings.TrimSpace(binding.EdgeGroupID)},
		kvPair{Key: "dns_kind", Value: strings.TrimSpace(binding.DNSKind)},
		kvPair{Key: "updated", Value: formatTime(binding.UpdatedAt)},
	)
}

func writePlatformDomainBindingTable(w io.Writer, bindings []model.PlatformDomainBinding) error {
	sorted := append([]model.PlatformDomainBinding(nil), bindings...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Hostname < sorted[j].Hostname
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "HOSTNAME\tAPP\tPOLICY\tEDGE_GROUP\tSTATUS\tTLS\tDNS\tUPDATED"); err != nil {
		return err
	}
	for _, binding := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			strings.TrimSpace(binding.Hostname),
			strings.TrimSpace(firstNonEmptyTrimmed(binding.AppName, binding.AppID)),
			strings.TrimSpace(binding.RoutePolicy),
			strings.TrimSpace(binding.EdgeGroupID),
			strings.TrimSpace(binding.Status),
			strings.TrimSpace(binding.TLSStatus),
			strings.TrimSpace(binding.DNSKind),
			formatTime(binding.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

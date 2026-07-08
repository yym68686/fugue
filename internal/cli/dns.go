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

func (c *CLI) newDNSCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dns",
		Short: "Manage Fugue hosted DNS zones and records",
	}
	cmd.AddCommand(c.newDNSZoneCommand(), c.newDNSRecordCommand())
	return cmd
}

func (c *CLI) newDNSZoneCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "zone",
		Short: "Manage hosted DNS zones",
	}
	cmd.AddCommand(
		c.newDNSZoneAddCommand(),
		c.newDNSZoneListCommand(),
		c.newDNSZoneShowCommand(),
		c.newDNSZonePreflightCommand(),
		c.newDNSZoneDeleteCommand(),
	)
	return cmd
}

func (c *CLI) newDNSZoneAddCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "add <zone>",
		Short: "Create a hosted DNS zone",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			zone, err := client.CreateHostedDNSZone(createHostedDNSZoneClientRequest{
				ZoneName:  args[0],
				TenantID:  c.root.TenantID,
				ProjectID: c.root.ProjectID,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"zone": zone})
			}
			pairs := hostedDNSZoneKeyValues(zone)
			if len(zone.ExpectedNameservers) > 0 {
				pairs = append(pairs, kvPair{Key: "set_nameservers", Value: strings.Join(zone.ExpectedNameservers, ", ")})
			}
			return writeKeyValues(c.stdout, pairs...)
		},
	}
}

func (c *CLI) newDNSZoneListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List hosted DNS zones",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			zones, err := client.ListHostedDNSZones()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"zones": zones})
			}
			return writeHostedDNSZoneTable(c.stdout, zones)
		},
	}
}

func (c *CLI) newDNSZoneShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <zone>",
		Short: "Show a hosted DNS zone",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			zone, err := client.GetHostedDNSZone(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"zone": zone})
			}
			return writeKeyValues(c.stdout, hostedDNSZoneKeyValues(zone)...)
		},
	}
}

func (c *CLI) newDNSZonePreflightCommand() *cobra.Command {
	var minHealthy int
	cmd := &cobra.Command{
		Use:     "preflight <zone>",
		Aliases: []string{"diagnose"},
		Short:   "Check hosted DNS delegation and node health",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.HostedDNSZonePreflight(args[0], minHealthy)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeKeyValues(c.stdout, hostedDNSZoneKeyValues(response.Zone)...); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeDNSDelegationPreflight(c.stdout, response.Preflight)
		},
	}
	cmd.Flags().IntVar(&minHealthy, "min-healthy-nodes", 0, "Minimum healthy DNS nodes required")
	return cmd
}

func (c *CLI) newDNSZoneDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <zone>",
		Aliases: []string{"rm", "remove"},
		Short:   "Delete a hosted DNS zone",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.DeleteHostedDNSZone(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "deleted", Value: fmt.Sprintf("%t", response.Deleted)},
				kvPair{Key: "zone", Value: response.Zone.ZoneName},
				kvPair{Key: "id", Value: response.Zone.ID},
			)
		},
	}
}

func (c *CLI) newDNSRecordCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "record",
		Short: "Manage hosted DNS records",
	}
	cmd.AddCommand(
		c.newDNSRecordListCommand(),
		c.newDNSRecordAddCommand(),
		c.newDNSRecordEditCommand(),
		c.newDNSRecordDiagnoseCommand(),
		c.newDNSRecordDeleteCommand(),
	)
	return cmd
}

func (c *CLI) newDNSRecordListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls <zone>",
		Aliases: []string{"list"},
		Short:   "List hosted DNS records",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			records, err := client.ListHostedDNSRecords(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"records": records})
			}
			return writeHostedDNSRecordTable(c.stdout, records)
		},
	}
}

func (c *CLI) newDNSRecordAddCommand() *cobra.Command {
	var opts hostedDNSRecordCLIOptions
	cmd := &cobra.Command{
		Use:   "add <zone> <name> <type> <value...>",
		Short: "Create a hosted DNS record",
		Args:  cobra.MinimumNArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			req := opts.createRequest(args[1], args[2], args[3:])
			record, err := client.CreateHostedDNSRecord(args[0], req)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"record": record})
			}
			return writeKeyValues(c.stdout, hostedDNSRecordKeyValues(record)...)
		},
	}
	addHostedDNSRecordFlags(cmd, &opts, true)
	return cmd
}

func (c *CLI) newDNSRecordEditCommand() *cobra.Command {
	var opts hostedDNSRecordCLIOptions
	cmd := &cobra.Command{
		Use:   "edit <zone> <record-id-or-name>",
		Short: "Edit a hosted DNS record",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			recordID, err := resolveHostedDNSRecordID(client, args[0], args[1], opts.RecordType)
			if err != nil {
				return err
			}
			req := opts.patchRequest()
			record, err := client.PatchHostedDNSRecord(args[0], recordID, req)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"record": record})
			}
			return writeKeyValues(c.stdout, hostedDNSRecordKeyValues(record)...)
		},
	}
	addHostedDNSRecordFlags(cmd, &opts, false)
	cmd.Flags().StringVar(&opts.RecordType, "type", "", "Record type when resolving a record name")
	return cmd
}

func (c *CLI) newDNSRecordDeleteCommand() *cobra.Command {
	var recordType string
	cmd := &cobra.Command{
		Use:     "delete <zone> <record-id-or-name>",
		Aliases: []string{"rm", "remove"},
		Short:   "Delete a hosted DNS record",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			recordID, err := resolveHostedDNSRecordID(client, args[0], args[1], recordType)
			if err != nil {
				return err
			}
			response, err := client.DeleteHostedDNSRecord(args[0], recordID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "deleted", Value: fmt.Sprintf("%t", response.Deleted)},
				kvPair{Key: "record_id", Value: response.Record.ID},
				kvPair{Key: "name", Value: response.Record.Name},
				kvPair{Key: "type", Value: response.Record.Type},
			)
		},
	}
	cmd.Flags().StringVar(&recordType, "type", "", "Record type when resolving a record name")
	return cmd
}

func (c *CLI) newDNSRecordDiagnoseCommand() *cobra.Command {
	var recordType string
	cmd := &cobra.Command{
		Use:   "diagnose <zone> <record-id-or-name>",
		Short: "Show hosted DNS record publish and flattening diagnostics",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			recordID, err := resolveHostedDNSRecordID(client, args[0], args[1], recordType)
			if err != nil {
				return err
			}
			records, err := client.ListHostedDNSRecords(args[0])
			if err != nil {
				return err
			}
			for _, record := range records {
				if record.ID != recordID {
					continue
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"record": record})
				}
				return writeKeyValues(c.stdout, hostedDNSRecordKeyValues(record)...)
			}
			return fmt.Errorf("record %s not found", args[1])
		},
	}
	cmd.Flags().StringVar(&recordType, "type", "", "Record type when resolving a record name")
	return cmd
}

type hostedDNSRecordCLIOptions struct {
	RecordType            string
	Values                []string
	TTL                   int
	Flatten               bool
	FlattenSet            bool
	FlattenMode           string
	FlattenTarget         string
	FlattenIPv4Policy     string
	FlattenIPv6Policy     string
	FlattenTTLPolicy      string
	FlattenFallbackPolicy string
	Status                string
	Overwrite             bool
}

func (o hostedDNSRecordCLIOptions) createRequest(name, recordType string, values []string) createHostedDNSRecordClientRequest {
	return createHostedDNSRecordClientRequest{
		Name:                  name,
		Type:                  recordType,
		Values:                append([]string(nil), values...),
		TTL:                   o.TTL,
		Flatten:               o.Flatten,
		FlattenMode:           o.FlattenMode,
		FlattenTarget:         o.FlattenTarget,
		FlattenIPv4Policy:     o.FlattenIPv4Policy,
		FlattenIPv6Policy:     o.FlattenIPv6Policy,
		FlattenTTLPolicy:      o.FlattenTTLPolicy,
		FlattenFallbackPolicy: o.FlattenFallbackPolicy,
		Overwrite:             o.Overwrite,
	}
}

func (o hostedDNSRecordCLIOptions) patchRequest() patchHostedDNSRecordClientRequest {
	var flatten *bool
	if o.FlattenSet {
		flatten = &o.Flatten
	}
	return patchHostedDNSRecordClientRequest{
		Values:                append([]string(nil), o.Values...),
		TTL:                   o.TTL,
		Flatten:               flatten,
		FlattenMode:           o.FlattenMode,
		FlattenTarget:         o.FlattenTarget,
		FlattenIPv4Policy:     o.FlattenIPv4Policy,
		FlattenIPv6Policy:     o.FlattenIPv6Policy,
		FlattenTTLPolicy:      o.FlattenTTLPolicy,
		FlattenFallbackPolicy: o.FlattenFallbackPolicy,
		Status:                o.Status,
		Overwrite:             o.Overwrite,
	}
}

func addHostedDNSRecordFlags(cmd *cobra.Command, opts *hostedDNSRecordCLIOptions, includeFlattenBool bool) {
	cmd.Flags().IntVar(&opts.TTL, "ttl", 0, "Record TTL seconds")
	if includeFlattenBool {
		cmd.Flags().BoolVar(&opts.Flatten, "flatten", false, "Flatten CNAME/ALIAS/ANAME to A/AAAA")
	} else {
		cmd.Flags().BoolVar(&opts.Flatten, "flatten", false, "Enable flattening")
		cmd.Flags().BoolVar(&opts.FlattenSet, "set-flatten", false, "Apply the --flatten value")
	}
	cmd.Flags().StringVar(&opts.FlattenMode, "flatten-mode", "", "Flatten mode: none, apex, always, app")
	cmd.Flags().StringVar(&opts.FlattenTarget, "flatten-target", "", "Flatten target hostname")
	cmd.Flags().StringVar(&opts.FlattenIPv4Policy, "flatten-ipv4-policy", "", "Flatten IPv4 policy")
	cmd.Flags().StringVar(&opts.FlattenIPv6Policy, "flatten-ipv6-policy", "", "Flatten IPv6 policy")
	cmd.Flags().StringVar(&opts.FlattenTTLPolicy, "flatten-ttl-policy", "", "Flatten TTL policy")
	cmd.Flags().StringVar(&opts.FlattenFallbackPolicy, "flatten-fallback", "", "Flatten fallback policy")
	cmd.Flags().StringArrayVar(&opts.Values, "value", nil, "Replacement value for edit; repeatable")
	cmd.Flags().StringVar(&opts.Status, "status", "", "Record status for edit")
	cmd.Flags().BoolVar(&opts.Overwrite, "overwrite", false, "Allow safe replacement of conflicting records")
}

func resolveHostedDNSRecordID(client *Client, zone, ident, recordType string) (string, error) {
	ident = strings.TrimSpace(ident)
	if strings.HasPrefix(ident, "dnsrec_") {
		return ident, nil
	}
	records, err := client.ListHostedDNSRecords(zone)
	if err != nil {
		return "", err
	}
	matches := []model.DNSRecord{}
	for _, record := range records {
		if record.Name != ident && record.FQDN != ident {
			continue
		}
		if recordType != "" && !strings.EqualFold(record.Type, recordType) {
			continue
		}
		matches = append(matches, record)
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("record %s not found", ident)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("record %s is ambiguous; pass --type", ident)
	}
	return matches[0].ID, nil
}

func hostedDNSZoneKeyValues(zone model.HostedZone) []kvPair {
	return []kvPair{
		{Key: "id", Value: zone.ID},
		{Key: "zone", Value: zone.ZoneName},
		{Key: "tenant_id", Value: zone.TenantID},
		{Key: "status", Value: zone.Status},
		{Key: "delegation_status", Value: zone.DelegationStatus},
		{Key: "parent_nameservers", Value: strings.Join(zone.ParentNameservers, ", ")},
		{Key: "expected_nameservers", Value: strings.Join(zone.ExpectedNameservers, ", ")},
		{Key: "last_message", Value: zone.LastMessage},
	}
}

func hostedDNSRecordKeyValues(record model.DNSRecord) []kvPair {
	return []kvPair{
		{Key: "id", Value: record.ID},
		{Key: "name", Value: record.Name},
		{Key: "fqdn", Value: record.FQDN},
		{Key: "type", Value: record.Type},
		{Key: "values", Value: strings.Join(record.Values, ", ")},
		{Key: "ttl", Value: fmt.Sprintf("%d", record.TTL)},
		{Key: "source", Value: record.Source},
		{Key: "status", Value: record.Status},
		{Key: "flatten_mode", Value: record.FlattenMode},
		{Key: "flatten_target", Value: record.FlattenTarget},
		{Key: "flatten_status", Value: record.FlattenStatus},
		{Key: "flattened_a", Value: strings.Join(record.FlattenedA, ", ")},
		{Key: "flattened_aaaa", Value: strings.Join(record.FlattenedAAAA, ", ")},
		{Key: "last_resolved_at", Value: formatOptionalTimePtr(record.LastResolvedAt)},
		{Key: "last_published_at", Value: formatOptionalTimePtr(record.LastPublishedAt)},
		{Key: "resolve_error", Value: record.ResolveError},
		{Key: "last_message", Value: record.LastMessage},
	}
}

func writeHostedDNSZoneTable(w io.Writer, zones []model.HostedZone) error {
	sorted := append([]model.HostedZone(nil), zones...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ZoneName < sorted[j].ZoneName })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ZONE\tSTATUS\tDELEGATION\tNAMESERVERS\tMESSAGE"); err != nil {
		return err
	}
	for _, zone := range sorted {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", zone.ZoneName, zone.Status, zone.DelegationStatus, strings.Join(zone.ExpectedNameservers, ","), zone.LastMessage); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeHostedDNSRecordTable(w io.Writer, records []model.DNSRecord) error {
	sorted := append([]model.DNSRecord(nil), records...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].FQDN != sorted[j].FQDN {
			return sorted[i].FQDN < sorted[j].FQDN
		}
		return sorted[i].Type < sorted[j].Type
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tTYPE\tVALUES\tTTL\tSOURCE\tSTATUS\tFLATTEN\tERROR"); err != nil {
		return err
	}
	for _, record := range sorted {
		flatten := record.FlattenMode
		if record.FlattenStatus != "" {
			flatten = strings.TrimSpace(flatten + "/" + record.FlattenStatus)
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\n", record.Name, record.Type, strings.Join(record.Values, ","), record.TTL, record.Source, record.Status, flatten, record.ResolveError); err != nil {
			return err
		}
	}
	return tw.Flush()
}

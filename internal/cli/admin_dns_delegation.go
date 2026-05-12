package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

const defaultCloudflareAPIBaseURL = "https://api.cloudflare.com/client/v4"

type dnsDelegationCommandOptions struct {
	dnsDelegationPreflightOptions

	ParentZone           string
	CloudflareAPIBaseURL string
	CloudflareZoneID     string
	CloudflareToken      string
	CloudflareEnvFile    string
	Confirm              bool
	Timeout              time.Duration
}

type dnsDelegationCloudflareAction struct {
	Operation string `json:"operation"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Value     string `json:"value"`
	TTL       int    `json:"ttl,omitempty"`
	Result    string `json:"result"`
	RecordID  string `json:"record_id,omitempty"`
}

func (c *CLI) newAdminDNSDelegationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delegation",
		Short: "Plan, apply, or roll back dns.fugue.pro parent-zone delegation",
	}
	cmd.AddCommand(
		c.newAdminDNSDelegationPlanCommand(),
		c.newAdminDNSDelegationApplyCommand(),
		c.newAdminDNSDelegationRollbackCommand(),
	)
	return cmd
}

func (c *CLI) newAdminDNSDelegationPlanCommand() *cobra.Command {
	opts := defaultDNSDelegationCommandOptions()
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show the DNS delegation plan without changing Cloudflare",
		RunE: func(cmd *cobra.Command, args []string) error {
			response, err := c.loadDNSDelegationPreflight(opts)
			if err != nil {
				return err
			}
			if err := validateDNSDelegationPlanSafety(response.Zone, response.DelegationPlan); err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"dry_run":   true,
					"operation": "plan",
					"preflight": response,
					"actions":   dnsDelegationPlannedActions("apply", response.DelegationPlan),
				})
			}
			return writeDNSDelegationCommandPlan(c.stdout, "plan", true, response, dnsDelegationPlannedActions("apply", response.DelegationPlan))
		},
	}
	addDNSDelegationPreflightFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newAdminDNSDelegationApplyCommand() *cobra.Command {
	opts := defaultDNSDelegationCommandOptions()
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply the planned dns.fugue.pro delegation records in Cloudflare",
		RunE: func(cmd *cobra.Command, args []string) error {
			response, err := c.loadDNSDelegationPreflight(opts)
			if err != nil {
				return err
			}
			if err := validateDNSDelegationPreflightForApply(response); err != nil {
				return err
			}
			if err := validateDNSDelegationPlanSafety(response.Zone, response.DelegationPlan); err != nil {
				return err
			}
			if !opts.Confirm {
				actions := dnsDelegationPlannedActions("apply", response.DelegationPlan)
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{
						"dry_run":   true,
						"operation": "apply",
						"preflight": response,
						"actions":   actions,
					})
				}
				return writeDNSDelegationCommandPlan(c.stdout, "apply", true, response, actions)
			}
			cloudflare, err := newCloudflareDNSClientFromOptions(opts)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
			defer cancel()
			actions, err := cloudflare.applyDNSDelegationPlan(ctx, opts.ParentZone, response.DelegationPlan)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"dry_run":   false,
					"operation": "apply",
					"preflight": response,
					"actions":   actions,
				})
			}
			return writeDNSDelegationCommandPlan(c.stdout, "apply", false, response, actions)
		},
	}
	addDNSDelegationPreflightFlags(cmd, &opts)
	addDNSDelegationCloudflareFlags(cmd, &opts)
	cmd.Flags().BoolVar(&opts.Confirm, "confirm", false, "Actually write Cloudflare records; without this flag apply is a dry-run")
	return cmd
}

func (c *CLI) newAdminDNSDelegationRollbackCommand() *cobra.Command {
	opts := defaultDNSDelegationCommandOptions()
	cmd := &cobra.Command{
		Use:   "rollback",
		Short: "Remove only the delegation records created for dns.fugue.pro",
		RunE: func(cmd *cobra.Command, args []string) error {
			response, err := c.loadDNSDelegationPreflight(opts)
			if err != nil {
				return err
			}
			if err := validateDNSDelegationPlanSafety(response.Zone, response.DelegationPlan); err != nil {
				return err
			}
			if !opts.Confirm {
				actions := dnsDelegationPlannedActions("rollback", response.DelegationPlan)
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{
						"dry_run":   true,
						"operation": "rollback",
						"preflight": response,
						"actions":   actions,
					})
				}
				return writeDNSDelegationCommandPlan(c.stdout, "rollback", true, response, actions)
			}
			cloudflare, err := newCloudflareDNSClientFromOptions(opts)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
			defer cancel()
			actions, err := cloudflare.rollbackDNSDelegationPlan(ctx, opts.ParentZone, response.DelegationPlan)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"dry_run":   false,
					"operation": "rollback",
					"preflight": response,
					"actions":   actions,
				})
			}
			return writeDNSDelegationCommandPlan(c.stdout, "rollback", false, response, actions)
		},
	}
	addDNSDelegationPreflightFlags(cmd, &opts)
	addDNSDelegationCloudflareFlags(cmd, &opts)
	cmd.Flags().BoolVar(&opts.Confirm, "confirm", false, "Actually delete Cloudflare records; without this flag rollback is a dry-run")
	return cmd
}

func defaultDNSDelegationCommandOptions() dnsDelegationCommandOptions {
	apiBase := strings.TrimSpace(os.Getenv("CLOUDFLARE_API_BASE_URL"))
	if apiBase == "" {
		apiBase = defaultCloudflareAPIBaseURL
	}
	return dnsDelegationCommandOptions{
		dnsDelegationPreflightOptions: dnsDelegationPreflightOptions{
			Zone:            "dns.fugue.pro",
			ProbeName:       "d-test.dns.fugue.pro",
			MinHealthyNodes: 2,
		},
		ParentZone:           firstNonEmpty(os.Getenv("CLOUDFLARE_ZONE_NAME"), "fugue.pro"),
		CloudflareAPIBaseURL: apiBase,
		CloudflareZoneID:     strings.TrimSpace(os.Getenv("CLOUDFLARE_ZONE_ID")),
		CloudflareEnvFile:    strings.TrimSpace(os.Getenv("CLOUDFLARE_ENV_FILE")),
		Timeout:              10 * time.Second,
	}
}

func addDNSDelegationPreflightFlags(cmd *cobra.Command, opts *dnsDelegationCommandOptions) {
	cmd.Flags().StringVar(&opts.Zone, "zone", opts.Zone, "Delegated DNS zone to check")
	cmd.Flags().StringVar(&opts.ProbeName, "probe-name", opts.ProbeName, "A record each DNS node must answer")
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", opts.EdgeGroupID, "Only check DNS nodes in this edge group")
	cmd.Flags().IntVar(&opts.MinHealthyNodes, "min-healthy-nodes", opts.MinHealthyNodes, "Minimum healthy DNS nodes required")
}

func addDNSDelegationCloudflareFlags(cmd *cobra.Command, opts *dnsDelegationCommandOptions) {
	cmd.Flags().StringVar(&opts.ParentZone, "cloudflare-zone", opts.ParentZone, "Cloudflare parent zone name")
	cmd.Flags().StringVar(&opts.CloudflareZoneID, "cloudflare-zone-id", opts.CloudflareZoneID, "Cloudflare zone ID; if empty the CLI looks up --cloudflare-zone")
	cmd.Flags().StringVar(&opts.CloudflareEnvFile, "cloudflare-env-file", opts.CloudflareEnvFile, "Optional env file containing CLOUDFLARE_DNS_API_TOKEN")
	cmd.Flags().StringVar(&opts.CloudflareAPIBaseURL, "cloudflare-api-url", opts.CloudflareAPIBaseURL, "Cloudflare API base URL")
	cmd.Flags().StringVar(&opts.CloudflareToken, "cloudflare-token", "", "Cloudflare DNS API token; prefer CLOUDFLARE_DNS_API_TOKEN or --cloudflare-env-file")
	cmd.Flags().DurationVar(&opts.Timeout, "cloudflare-timeout", opts.Timeout, "Cloudflare API timeout")
}

func (c *CLI) loadDNSDelegationPreflight(opts dnsDelegationCommandOptions) (model.DNSDelegationPreflightResponse, error) {
	client, err := c.newClient()
	if err != nil {
		return model.DNSDelegationPreflightResponse{}, err
	}
	return client.DNSDelegationPreflight(opts.dnsDelegationPreflightOptions)
}

func validateDNSDelegationPreflightForApply(response model.DNSDelegationPreflightResponse) error {
	if !response.Pass {
		return fmt.Errorf("DNS delegation preflight did not pass; refusing to write Cloudflare records")
	}
	if response.MinHealthyNodes < 2 {
		return fmt.Errorf("DNS delegation requires at least two healthy DNS nodes; got min_healthy_nodes=%d", response.MinHealthyNodes)
	}
	if response.HealthyNodeCount < 2 {
		return fmt.Errorf("DNS delegation requires at least two healthy DNS nodes; got %d", response.HealthyNodeCount)
	}
	passingNodes := 0
	for _, node := range response.Nodes {
		if !node.Pass {
			continue
		}
		passingNodes++
		if node.CacheWriteErrors != 0 || node.CacheLoadErrors != 0 {
			return fmt.Errorf("DNS node %s has cache errors; refusing to apply delegation", node.DNSNodeID)
		}
		if !node.NodeReady || node.NodeDiskPressure {
			return fmt.Errorf("DNS node %s failed Kubernetes health gate; refusing to apply delegation", node.DNSNodeID)
		}
	}
	if passingNodes < 2 {
		return fmt.Errorf("DNS delegation requires at least two passing DNS nodes; got %d", passingNodes)
	}
	return nil
}

func validateDNSDelegationPlanSafety(zone string, plan model.DNSDelegationPlan) error {
	zone = normalizeDNSName(zone)
	if zone == "" {
		return fmt.Errorf("delegated zone is empty")
	}
	ns1 := "ns1." + zone
	ns2 := "ns2." + zone
	allowedGlueNames := map[string]bool{ns1: true, ns2: true}
	allowedNSValues := map[string]bool{ns1: true, ns2: true}
	for _, record := range plan.PlannedARecords {
		name := normalizeDNSName(record.Name)
		recordType := strings.ToUpper(strings.TrimSpace(record.Type))
		if !allowedGlueNames[name] {
			return fmt.Errorf("unsafe delegation plan: refusing to change %s %s", record.Name, record.Type)
		}
		if recordType != "A" && recordType != "AAAA" {
			return fmt.Errorf("unsafe delegation plan: glue record %s has unsupported type %s", record.Name, record.Type)
		}
		if err := validateDNSDelegationValues(record); err != nil {
			return err
		}
	}
	for _, record := range plan.PlannedNSRecords {
		name := normalizeDNSName(record.Name)
		if name != zone || strings.ToUpper(strings.TrimSpace(record.Type)) != "NS" {
			return fmt.Errorf("unsafe delegation plan: refusing to change %s %s", record.Name, record.Type)
		}
		for _, value := range record.Values {
			if !allowedNSValues[normalizeDNSName(value)] {
				return fmt.Errorf("unsafe delegation plan: refusing NS target %s", value)
			}
		}
		if err := validateDNSDelegationValues(record); err != nil {
			return err
		}
	}
	for _, record := range plan.RollbackDeleteRecords {
		name := normalizeDNSName(record.Name)
		recordType := strings.ToUpper(strings.TrimSpace(record.Type))
		switch recordType {
		case "A", "AAAA":
			if !allowedGlueNames[name] {
				return fmt.Errorf("unsafe rollback plan: refusing to delete %s %s", record.Name, record.Type)
			}
		case "NS":
			if name != zone {
				return fmt.Errorf("unsafe rollback plan: refusing to delete %s %s", record.Name, record.Type)
			}
			for _, value := range record.Values {
				if !allowedNSValues[normalizeDNSName(value)] {
					return fmt.Errorf("unsafe rollback plan: refusing NS target %s", value)
				}
			}
		default:
			return fmt.Errorf("unsafe rollback plan: unsupported type %s for %s", record.Type, record.Name)
		}
		if err := validateDNSDelegationValues(record); err != nil {
			return err
		}
	}
	return nil
}

func validateDNSDelegationValues(record model.DNSDelegationRecord) error {
	if strings.TrimSpace(record.Name) == "" || strings.TrimSpace(record.Type) == "" {
		return fmt.Errorf("unsafe delegation plan: record name and type are required")
	}
	if len(record.Values) == 0 {
		return fmt.Errorf("unsafe delegation plan: %s %s has no values", record.Name, record.Type)
	}
	for _, value := range record.Values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("unsafe delegation plan: %s %s has an empty value", record.Name, record.Type)
		}
	}
	return nil
}

func normalizeDNSName(value string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(value)), ".")
}

func dnsDelegationPlannedActions(operation string, plan model.DNSDelegationPlan) []dnsDelegationCloudflareAction {
	actions := []dnsDelegationCloudflareAction{}
	switch operation {
	case "apply":
		for _, record := range dnsDelegationApplyRecords(plan) {
			for _, value := range record.Values {
				actions = append(actions, dnsDelegationCloudflareAction{
					Operation: "upsert",
					Name:      record.Name,
					Type:      strings.ToUpper(record.Type),
					Value:     value,
					TTL:       record.TTL,
					Result:    "dry-run",
				})
			}
		}
	case "rollback":
		for _, record := range plan.RollbackDeleteRecords {
			for _, value := range record.Values {
				actions = append(actions, dnsDelegationCloudflareAction{
					Operation: "delete",
					Name:      record.Name,
					Type:      strings.ToUpper(record.Type),
					Value:     value,
					Result:    "dry-run",
				})
			}
		}
	}
	sortDNSDelegationActions(actions)
	return actions
}

func sortDNSDelegationActions(actions []dnsDelegationCloudflareAction) {
	sort.SliceStable(actions, func(i, j int) bool {
		left := actions[i]
		right := actions[j]
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		if left.Type != right.Type {
			return left.Type < right.Type
		}
		if left.Value != right.Value {
			return left.Value < right.Value
		}
		return left.Operation < right.Operation
	})
}

func dnsDelegationApplyRecords(plan model.DNSDelegationPlan) []model.DNSDelegationRecord {
	records := make([]model.DNSDelegationRecord, 0, len(plan.PlannedARecords)+len(plan.PlannedNSRecords))
	records = append(records, plan.PlannedARecords...)
	records = append(records, plan.PlannedNSRecords...)
	return mergeDNSDelegationRecordSets(records)
}

func mergeDNSDelegationRecordSets(records []model.DNSDelegationRecord) []model.DNSDelegationRecord {
	merged := make([]model.DNSDelegationRecord, 0, len(records))
	byKey := map[string]int{}
	seenValues := map[string]map[string]bool{}
	for _, record := range records {
		name := strings.TrimSpace(record.Name)
		recordType := strings.ToUpper(strings.TrimSpace(record.Type))
		values := record.Values
		key := normalizeDNSName(name) + "\x00" + recordType
		index, ok := byKey[key]
		if !ok {
			record.Name = name
			record.Type = recordType
			record.Values = nil
			byKey[key] = len(merged)
			seenValues[key] = map[string]bool{}
			merged = append(merged, record)
			index = len(merged) - 1
		}
		if merged[index].TTL == 0 && record.TTL != 0 {
			merged[index].TTL = record.TTL
		}
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value == "" || seenValues[key][value] {
				continue
			}
			seenValues[key][value] = true
			merged[index].Values = append(merged[index].Values, value)
		}
	}
	return merged
}

func writeDNSDelegationCommandPlan(w io.Writer, operation string, dryRun bool, response model.DNSDelegationPreflightResponse, actions []dnsDelegationCloudflareAction) error {
	if err := writeKeyValues(w,
		kvPair{Key: "operation", Value: operation},
		kvPair{Key: "dry_run", Value: fmt.Sprintf("%t", dryRun)},
		kvPair{Key: "preflight_pass", Value: fmt.Sprintf("%t", response.Pass)},
		kvPair{Key: "zone", Value: response.Zone},
		kvPair{Key: "healthy_nodes", Value: fmt.Sprintf("%d/%d", response.HealthyNodeCount, response.MinHealthyNodes)},
		kvPair{Key: "dns_bundle_version", Value: firstNonEmpty(response.DNSBundleVersion, "-")},
	); err != nil {
		return err
	}
	if dryRun {
		if _, err := fmt.Fprintln(w, "note=pass --confirm to write Cloudflare records"); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeDNSDelegationActionTable(w, actions)
}

func writeDNSDelegationActionTable(w io.Writer, actions []dnsDelegationCloudflareAction) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "OPERATION\tNAME\tTYPE\tVALUE\tTTL\tRESULT"); err != nil {
		return err
	}
	for _, action := range actions {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n",
			action.Operation,
			action.Name,
			action.Type,
			action.Value,
			action.TTL,
			action.Result,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

type cloudflareDNSClient struct {
	baseURL string
	token   string
	zoneID  string
	http    *http.Client
}

type cloudflareDNSRecord struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Name    string `json:"name"`
	Content string `json:"content"`
	TTL     int    `json:"ttl"`
	Proxied bool   `json:"proxied,omitempty"`
}

type cloudflareDNSRecordPayload struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	Content string `json:"content"`
	TTL     int    `json:"ttl,omitempty"`
	Proxied *bool  `json:"proxied,omitempty"`
	Comment string `json:"comment,omitempty"`
}

type cloudflareZone struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type cloudflareAPIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func newCloudflareDNSClientFromOptions(opts dnsDelegationCommandOptions) (*cloudflareDNSClient, error) {
	token, err := resolveCloudflareToken(opts)
	if err != nil {
		return nil, err
	}
	return &cloudflareDNSClient{
		baseURL: strings.TrimRight(firstNonEmpty(opts.CloudflareAPIBaseURL, defaultCloudflareAPIBaseURL), "/"),
		token:   token,
		zoneID:  strings.TrimSpace(opts.CloudflareZoneID),
		http:    &http.Client{},
	}, nil
}

func resolveCloudflareToken(opts dnsDelegationCommandOptions) (string, error) {
	if token := strings.TrimSpace(opts.CloudflareToken); token != "" {
		return token, nil
	}
	for _, key := range []string{"CLOUDFLARE_DNS_API_TOKEN", "CLOUDFLARE_API_TOKEN"} {
		if token := strings.TrimSpace(os.Getenv(key)); token != "" {
			return token, nil
		}
	}
	if strings.TrimSpace(opts.CloudflareEnvFile) != "" {
		values, err := loadSimpleEnvFile(opts.CloudflareEnvFile)
		if err != nil {
			return "", err
		}
		for _, key := range []string{"CLOUDFLARE_DNS_API_TOKEN", "CLOUDFLARE_API_TOKEN"} {
			if token := strings.TrimSpace(values[key]); token != "" {
				return token, nil
			}
		}
	}
	return "", fmt.Errorf("Cloudflare token is required; set CLOUDFLARE_DNS_API_TOKEN or pass --cloudflare-env-file")
}

func loadSimpleEnvFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read Cloudflare env file: %w", err)
	}
	out := map[string]string{}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"'`)
		if key != "" {
			out[key] = value
		}
	}
	return out, nil
}

func (c *cloudflareDNSClient) applyDNSDelegationPlan(ctx context.Context, parentZone string, plan model.DNSDelegationPlan) ([]dnsDelegationCloudflareAction, error) {
	zoneID, err := c.ensureZoneID(ctx, parentZone)
	if err != nil {
		return nil, err
	}
	actions := []dnsDelegationCloudflareAction{}
	for _, record := range dnsDelegationApplyRecords(plan) {
		next, err := c.reconcileRecordSet(ctx, zoneID, record)
		if err != nil {
			return actions, err
		}
		actions = append(actions, next...)
	}
	sortDNSDelegationActions(actions)
	return actions, nil
}

func (c *cloudflareDNSClient) rollbackDNSDelegationPlan(ctx context.Context, parentZone string, plan model.DNSDelegationPlan) ([]dnsDelegationCloudflareAction, error) {
	zoneID, err := c.ensureZoneID(ctx, parentZone)
	if err != nil {
		return nil, err
	}
	actions := []dnsDelegationCloudflareAction{}
	for _, record := range plan.RollbackDeleteRecords {
		next, err := c.deleteRecordValues(ctx, zoneID, record)
		if err != nil {
			return actions, err
		}
		actions = append(actions, next...)
	}
	sortDNSDelegationActions(actions)
	return actions, nil
}

func (c *cloudflareDNSClient) ensureZoneID(ctx context.Context, parentZone string) (string, error) {
	if strings.TrimSpace(c.zoneID) != "" {
		return strings.TrimSpace(c.zoneID), nil
	}
	values := url.Values{}
	values.Set("name", strings.TrimSpace(parentZone))
	values.Set("status", "active")
	values.Set("per_page", "1")
	var zones []cloudflareZone
	if err := c.do(ctx, http.MethodGet, "/zones?"+values.Encode(), nil, &zones); err != nil {
		return "", fmt.Errorf("look up Cloudflare zone %s: %w; pass --cloudflare-zone-id if this token cannot list zones", parentZone, err)
	}
	if len(zones) == 0 || strings.TrimSpace(zones[0].ID) == "" {
		return "", fmt.Errorf("Cloudflare zone %s not found", parentZone)
	}
	c.zoneID = zones[0].ID
	return c.zoneID, nil
}

func (c *cloudflareDNSClient) reconcileRecordSet(ctx context.Context, zoneID string, record model.DNSDelegationRecord) ([]dnsDelegationCloudflareAction, error) {
	recordType := strings.ToUpper(strings.TrimSpace(record.Type))
	name := strings.TrimSpace(record.Name)
	existing, err := c.listDNSRecords(ctx, zoneID, recordType, name)
	if err != nil {
		return nil, err
	}
	desired := map[string]bool{}
	for _, value := range record.Values {
		desired[strings.TrimSpace(value)] = true
	}
	actions := []dnsDelegationCloudflareAction{}
	for _, existingRecord := range existing {
		if desired[existingRecord.Content] {
			continue
		}
		if err := c.deleteDNSRecord(ctx, zoneID, existingRecord.ID); err != nil {
			return actions, err
		}
		actions = append(actions, dnsDelegationCloudflareAction{
			Operation: "delete",
			Name:      name,
			Type:      recordType,
			Value:     existingRecord.Content,
			TTL:       existingRecord.TTL,
			Result:    "deleted-extra",
			RecordID:  existingRecord.ID,
		})
	}
	existingByContent := map[string]cloudflareDNSRecord{}
	for _, existingRecord := range existing {
		if desired[existingRecord.Content] {
			existingByContent[existingRecord.Content] = existingRecord
		}
	}
	for _, value := range record.Values {
		value = strings.TrimSpace(value)
		payload := cloudflarePayloadForRecord(record, value)
		if current, ok := existingByContent[value]; ok {
			if !cloudflareRecordNeedsUpdate(current, payload) {
				actions = append(actions, dnsDelegationCloudflareAction{
					Operation: "upsert",
					Name:      name,
					Type:      recordType,
					Value:     value,
					TTL:       firstNonZero(payload.TTL, current.TTL),
					Result:    "unchanged",
					RecordID:  current.ID,
				})
				continue
			}
			updated, err := c.updateDNSRecord(ctx, zoneID, current.ID, payload)
			if err != nil {
				return actions, err
			}
			actions = append(actions, dnsDelegationCloudflareAction{
				Operation: "upsert",
				Name:      name,
				Type:      recordType,
				Value:     value,
				TTL:       updated.TTL,
				Result:    "updated",
				RecordID:  updated.ID,
			})
			continue
		}
		created, err := c.createDNSRecord(ctx, zoneID, payload)
		if err != nil {
			return actions, err
		}
		actions = append(actions, dnsDelegationCloudflareAction{
			Operation: "upsert",
			Name:      name,
			Type:      recordType,
			Value:     value,
			TTL:       created.TTL,
			Result:    "created",
			RecordID:  created.ID,
		})
	}
	return actions, nil
}

func cloudflareRecordNeedsUpdate(current cloudflareDNSRecord, payload cloudflareDNSRecordPayload) bool {
	if payload.TTL != 0 && current.TTL != payload.TTL {
		return true
	}
	if payload.Proxied != nil && current.Proxied != *payload.Proxied {
		return true
	}
	return false
}

func (c *cloudflareDNSClient) deleteRecordValues(ctx context.Context, zoneID string, record model.DNSDelegationRecord) ([]dnsDelegationCloudflareAction, error) {
	recordType := strings.ToUpper(strings.TrimSpace(record.Type))
	name := strings.TrimSpace(record.Name)
	existing, err := c.listDNSRecords(ctx, zoneID, recordType, name)
	if err != nil {
		return nil, err
	}
	values := map[string]bool{}
	for _, value := range record.Values {
		values[strings.TrimSpace(value)] = true
	}
	actions := []dnsDelegationCloudflareAction{}
	for _, existingRecord := range existing {
		if !values[existingRecord.Content] {
			continue
		}
		if err := c.deleteDNSRecord(ctx, zoneID, existingRecord.ID); err != nil {
			return actions, err
		}
		actions = append(actions, dnsDelegationCloudflareAction{
			Operation: "delete",
			Name:      name,
			Type:      recordType,
			Value:     existingRecord.Content,
			TTL:       existingRecord.TTL,
			Result:    "deleted",
			RecordID:  existingRecord.ID,
		})
	}
	for _, value := range record.Values {
		value = strings.TrimSpace(value)
		found := false
		for _, action := range actions {
			if action.Value == value {
				found = true
				break
			}
		}
		if !found {
			actions = append(actions, dnsDelegationCloudflareAction{
				Operation: "delete",
				Name:      name,
				Type:      recordType,
				Value:     value,
				Result:    "not-found",
			})
		}
	}
	return actions, nil
}

func (c *cloudflareDNSClient) listDNSRecords(ctx context.Context, zoneID, recordType, name string) ([]cloudflareDNSRecord, error) {
	values := url.Values{}
	values.Set("type", recordType)
	values.Set("name", name)
	values.Set("per_page", "100")
	var records []cloudflareDNSRecord
	if err := c.do(ctx, http.MethodGet, "/zones/"+url.PathEscape(zoneID)+"/dns_records?"+values.Encode(), nil, &records); err != nil {
		return nil, fmt.Errorf("list Cloudflare DNS records for %s %s: %w", name, recordType, err)
	}
	return records, nil
}

func (c *cloudflareDNSClient) createDNSRecord(ctx context.Context, zoneID string, payload cloudflareDNSRecordPayload) (cloudflareDNSRecord, error) {
	var record cloudflareDNSRecord
	if err := c.do(ctx, http.MethodPost, "/zones/"+url.PathEscape(zoneID)+"/dns_records", payload, &record); err != nil {
		return cloudflareDNSRecord{}, fmt.Errorf("create Cloudflare DNS record %s %s %s: %w", payload.Name, payload.Type, payload.Content, err)
	}
	return record, nil
}

func (c *cloudflareDNSClient) updateDNSRecord(ctx context.Context, zoneID, recordID string, payload cloudflareDNSRecordPayload) (cloudflareDNSRecord, error) {
	var record cloudflareDNSRecord
	if err := c.do(ctx, http.MethodPut, "/zones/"+url.PathEscape(zoneID)+"/dns_records/"+url.PathEscape(recordID), payload, &record); err != nil {
		return cloudflareDNSRecord{}, fmt.Errorf("update Cloudflare DNS record %s %s %s: %w", payload.Name, payload.Type, payload.Content, err)
	}
	return record, nil
}

func (c *cloudflareDNSClient) deleteDNSRecord(ctx context.Context, zoneID, recordID string) error {
	if err := c.do(ctx, http.MethodDelete, "/zones/"+url.PathEscape(zoneID)+"/dns_records/"+url.PathEscape(recordID), nil, nil); err != nil {
		return fmt.Errorf("delete Cloudflare DNS record %s: %w", recordID, err)
	}
	return nil
}

func cloudflarePayloadForRecord(record model.DNSDelegationRecord, value string) cloudflareDNSRecordPayload {
	recordType := strings.ToUpper(strings.TrimSpace(record.Type))
	payload := cloudflareDNSRecordPayload{
		Type:    recordType,
		Name:    strings.TrimSpace(record.Name),
		Content: strings.TrimSpace(value),
		TTL:     record.TTL,
		Comment: strings.TrimSpace(record.Comment),
	}
	if cloudflareRecordSupportsProxied(recordType) {
		proxied := false
		payload.Proxied = &proxied
	}
	return payload
}

func cloudflareRecordSupportsProxied(recordType string) bool {
	switch strings.ToUpper(strings.TrimSpace(recordType)) {
	case "A", "AAAA", "CNAME":
		return true
	default:
		return false
	}
}

func (c *cloudflareDNSClient) do(ctx context.Context, method, apiPath string, requestBody any, result any) error {
	var body io.Reader
	if requestBody != nil {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(requestBody); err != nil {
			return err
		}
		body = &buf
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiPath, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var envelope struct {
		Success bool                 `json:"success"`
		Errors  []cloudflareAPIError `json:"errors"`
		Result  json.RawMessage      `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !envelope.Success {
		messages := make([]string, 0, len(envelope.Errors))
		for _, apiErr := range envelope.Errors {
			if strings.TrimSpace(apiErr.Message) != "" {
				messages = append(messages, apiErr.Message)
			}
		}
		if len(messages) == 0 {
			messages = append(messages, resp.Status)
		}
		return fmt.Errorf("%s", strings.Join(messages, "; "))
	}
	if result == nil {
		return nil
	}
	if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return nil
	}
	return json.Unmarshal(envelope.Result, result)
}

func firstNonZero(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

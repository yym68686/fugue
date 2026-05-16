package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminDiscoveryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "discovery",
		Short: "Inspect signed discovery state",
	}
	cmd.AddCommand(c.newAdminDiscoveryBundleCommand())
	return cmd
}

func (c *CLI) newAdminDiscoveryBundleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bundle",
		Short: "Inspect and save the signed discovery bundle",
	}
	cmd.AddCommand(
		c.newAdminDiscoveryBundleShowCommand(),
		c.newAdminDiscoveryBundleDownloadCommand(),
		c.newAdminDiscoveryBundleDiffCommand(),
	)
	return cmd
}

func (c *CLI) newAdminDiscoveryBundleShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show",
		Aliases: []string{"get"},
		Short:   "Show the current signed discovery bundle",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			bundle, err := client.GetDiscoveryBundle()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, bundle)
			}
			return writeDiscoveryBundleSummary(c.stdout, bundle)
		},
	}
}

func (c *CLI) newAdminDiscoveryBundleDownloadCommand() *cobra.Command {
	opts := struct {
		Output string
	}{}
	cmd := &cobra.Command{
		Use:   "download [path]",
		Short: "Save the current signed discovery bundle as JSON",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			outputPath := strings.TrimSpace(opts.Output)
			if len(args) > 0 {
				if outputPath != "" && outputPath != strings.TrimSpace(args[0]) {
					return fmt.Errorf("pass either [path] or --output, not both")
				}
				outputPath = strings.TrimSpace(args[0])
			}
			if outputPath == "" {
				return fmt.Errorf("output path is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			bundle, err := client.GetDiscoveryBundle()
			if err != nil {
				return err
			}
			raw, err := json.MarshalIndent(bundle, "", "  ")
			if err != nil {
				return fmt.Errorf("encode discovery bundle: %w", err)
			}
			raw = append(raw, '\n')
			if dir := filepath.Dir(outputPath); dir != "" && dir != "." {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					return fmt.Errorf("create output directory: %w", err)
				}
			}
			if err := os.WriteFile(outputPath, raw, 0o600); err != nil {
				return fmt.Errorf("write discovery bundle: %w", err)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"path": outputPath, "generation": bundle.Generation})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "path", Value: outputPath},
				kvPair{Key: "generation", Value: strings.TrimSpace(bundle.Generation)},
				kvPair{Key: "valid_until", Value: formatTime(bundle.ValidUntil)},
			)
		},
	}
	cmd.Flags().StringVarP(&opts.Output, "output", "o", "", "Output file path")
	return cmd
}

func (c *CLI) newAdminDiscoveryBundleDiffCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "diff <path>",
		Short: "Compare a saved discovery bundle with the current bundle",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			localBundle, err := readDiscoveryBundleFile(args[0])
			if err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			currentBundle, err := client.GetDiscoveryBundle()
			if err != nil {
				return err
			}
			changed := discoveryBundleDiffs(localBundle, currentBundle)
			response := map[string]any{
				"same":                 len(changed) == 0,
				"changed":              changed,
				"local_generation":     localBundle.Generation,
				"current_generation":   currentBundle.Generation,
				"local_valid_until":    localBundle.ValidUntil,
				"current_valid_until":  currentBundle.ValidUntil,
				"local_generated_at":   localBundle.GeneratedAt,
				"current_generated_at": currentBundle.GeneratedAt,
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeDiscoveryBundleDiff(c.stdout, localBundle, currentBundle, changed)
		},
	}
}

func readDiscoveryBundleFile(path string) (model.DiscoveryBundle, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return model.DiscoveryBundle{}, fmt.Errorf("read discovery bundle: %w", err)
	}
	var bundle model.DiscoveryBundle
	if err := json.Unmarshal(raw, &bundle); err != nil {
		return model.DiscoveryBundle{}, fmt.Errorf("decode discovery bundle: %w", err)
	}
	return bundle, nil
}

func writeDiscoveryBundleSummary(w io.Writer, bundle model.DiscoveryBundle) error {
	if err := writeKeyValues(w,
		kvPair{Key: "schema_version", Value: strings.TrimSpace(bundle.SchemaVersion)},
		kvPair{Key: "generation", Value: strings.TrimSpace(bundle.Generation)},
		kvPair{Key: "previous_generation", Value: firstNonEmpty(strings.TrimSpace(bundle.PreviousGeneration), "-")},
		kvPair{Key: "issuer", Value: strings.TrimSpace(bundle.Issuer)},
		kvPair{Key: "key_id", Value: firstNonEmpty(strings.TrimSpace(bundle.KeyID), "-")},
		kvPair{Key: "generated_at", Value: formatTime(bundle.GeneratedAt)},
		kvPair{Key: "valid_until", Value: formatTime(bundle.ValidUntil)},
		kvPair{Key: "api_endpoints", Value: formatInt(len(bundle.APIEndpoints))},
		kvPair{Key: "kubernetes", Value: formatInt(len(bundle.Kubernetes))},
		kvPair{Key: "registry", Value: formatInt(len(bundle.Registry))},
		kvPair{Key: "edge_groups", Value: formatInt(len(bundle.EdgeGroups))},
		kvPair{Key: "edge_nodes", Value: formatInt(len(bundle.EdgeNodes))},
		kvPair{Key: "dns_nodes", Value: formatInt(len(bundle.DNSNodes))},
		kvPair{Key: "platform_routes", Value: formatInt(len(bundle.PlatformRoutes))},
	); err != nil {
		return err
	}
	if len(bundle.APIEndpoints) > 0 {
		if _, err := fmt.Fprintln(w, "\n[api_endpoints]"); err != nil {
			return err
		}
		if err := writeDiscoveryEndpointTable(w, bundle.APIEndpoints); err != nil {
			return err
		}
	}
	if len(bundle.Kubernetes) > 0 {
		if _, err := fmt.Fprintln(w, "\n[kubernetes]"); err != nil {
			return err
		}
		if err := writeDiscoveryKubernetesTable(w, bundle.Kubernetes); err != nil {
			return err
		}
	}
	if len(bundle.Registry) > 0 {
		if _, err := fmt.Fprintln(w, "\n[registry]"); err != nil {
			return err
		}
		if err := writeDiscoveryRegistryTable(w, bundle.Registry); err != nil {
			return err
		}
	}
	if len(bundle.EdgeGroups) > 0 {
		if _, err := fmt.Fprintln(w, "\n[edge_groups]"); err != nil {
			return err
		}
		if err := writeEdgeGroupTable(w, bundle.EdgeGroups); err != nil {
			return err
		}
	}
	if len(bundle.EdgeNodes) > 0 {
		if _, err := fmt.Fprintln(w, "\n[edge_nodes]"); err != nil {
			return err
		}
		if err := writeEdgeNodeTable(w, bundle.EdgeNodes); err != nil {
			return err
		}
	}
	if len(bundle.DNSNodes) > 0 {
		if _, err := fmt.Fprintln(w, "\n[dns_nodes]"); err != nil {
			return err
		}
		if err := writeDNSNodeTable(w, bundle.DNSNodes); err != nil {
			return err
		}
	}
	if len(bundle.PlatformRoutes) > 0 {
		if _, err := fmt.Fprintln(w, "\n[platform_routes]"); err != nil {
			return err
		}
		if err := writeDiscoveryPlatformRouteTable(w, bundle.PlatformRoutes); err != nil {
			return err
		}
	}
	return nil
}

func writeDiscoveryEndpointTable(w io.Writer, endpoints []model.DiscoveryEndpoint) error {
	sorted := append([]model.DiscoveryEndpoint(nil), endpoints...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tURL"); err != nil {
		return err
	}
	for _, endpoint := range sorted {
		if _, err := fmt.Fprintf(tw, "%s\t%s\n", endpoint.Name, endpoint.URL); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeDiscoveryKubernetesTable(w io.Writer, endpoints []model.DiscoveryKubernetesEndpoint) error {
	sorted := append([]model.DiscoveryKubernetesEndpoint(nil), endpoints...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tSERVER\tFALLBACKS\tREGISTRY\tCA_HASH"); err != nil {
		return err
	}
	for _, endpoint := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			endpoint.Name,
			firstNonEmpty(endpoint.Server, "-"),
			firstNonEmpty(strings.Join(endpoint.FallbackServers, ","), "-"),
			firstNonEmpty(endpoint.RegistryEndpoint, "-"),
			firstNonEmpty(endpoint.CAHash, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeDiscoveryRegistryTable(w io.Writer, endpoints []model.DiscoveryRegistryEndpoint) error {
	sorted := append([]model.DiscoveryRegistryEndpoint(nil), endpoints...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tPUSH_BASE\tPULL_BASE\tMIRROR"); err != nil {
		return err
	}
	for _, endpoint := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\n",
			endpoint.Name,
			firstNonEmpty(endpoint.PushBase, "-"),
			firstNonEmpty(endpoint.PullBase, "-"),
			firstNonEmpty(endpoint.Mirror, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeDiscoveryPlatformRouteTable(w io.Writer, routes []model.PlatformRoute) error {
	sorted := append([]model.PlatformRoute(nil), routes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Hostname < sorted[j].Hostname })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "HOSTNAME\tKIND\tUPSTREAM\tPOLICY\tEDGE_GROUP\tSTATUS"); err != nil {
		return err
	}
	for _, route := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			route.Hostname,
			firstNonEmpty(route.Kind, "-"),
			firstNonEmpty(route.UpstreamURL, "-"),
			firstNonEmpty(route.RoutePolicy, "-"),
			firstNonEmpty(route.EdgeGroupID, "-"),
			firstNonEmpty(route.Status, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func discoveryBundleDiffs(localBundle, currentBundle model.DiscoveryBundle) []string {
	changed := make([]string, 0)
	addIfChanged := func(name string, localValue, currentValue any) {
		if !jsonValuesEqual(localValue, currentValue) {
			changed = append(changed, name)
		}
	}
	addIfChanged("schema_version", localBundle.SchemaVersion, currentBundle.SchemaVersion)
	addIfChanged("generation", localBundle.Generation, currentBundle.Generation)
	addIfChanged("previous_generation", localBundle.PreviousGeneration, currentBundle.PreviousGeneration)
	addIfChanged("issuer", localBundle.Issuer, currentBundle.Issuer)
	addIfChanged("key_id", localBundle.KeyID, currentBundle.KeyID)
	addIfChanged("valid_until", localBundle.ValidUntil, currentBundle.ValidUntil)
	addIfChanged("api_endpoints", localBundle.APIEndpoints, currentBundle.APIEndpoints)
	addIfChanged("kubernetes", localBundle.Kubernetes, currentBundle.Kubernetes)
	addIfChanged("registry", localBundle.Registry, currentBundle.Registry)
	addIfChanged("edge_groups", localBundle.EdgeGroups, currentBundle.EdgeGroups)
	addIfChanged("edge_nodes", localBundle.EdgeNodes, currentBundle.EdgeNodes)
	addIfChanged("dns_nodes", localBundle.DNSNodes, currentBundle.DNSNodes)
	addIfChanged("platform_routes", localBundle.PlatformRoutes, currentBundle.PlatformRoutes)
	addIfChanged("public_runtime_env", localBundle.PublicRuntimeEnv, currentBundle.PublicRuntimeEnv)
	return changed
}

func jsonValuesEqual(left, right any) bool {
	leftRaw, leftErr := json.Marshal(left)
	rightRaw, rightErr := json.Marshal(right)
	if leftErr != nil || rightErr != nil {
		return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right)
	}
	return string(leftRaw) == string(rightRaw)
}

func writeDiscoveryBundleDiff(w io.Writer, localBundle, currentBundle model.DiscoveryBundle, changed []string) error {
	if err := writeKeyValues(w,
		kvPair{Key: "same", Value: fmt.Sprintf("%t", len(changed) == 0)},
		kvPair{Key: "local_generation", Value: firstNonEmpty(localBundle.Generation, "-")},
		kvPair{Key: "current_generation", Value: firstNonEmpty(currentBundle.Generation, "-")},
		kvPair{Key: "local_valid_until", Value: formatTime(localBundle.ValidUntil)},
		kvPair{Key: "current_valid_until", Value: formatTime(currentBundle.ValidUntil)},
	); err != nil {
		return err
	}
	if len(changed) == 0 {
		_, err := fmt.Fprintln(w, "changed=-")
		return err
	}
	sort.Strings(changed)
	_, err := fmt.Fprintf(w, "changed=%s\n", strings.Join(changed, ","))
	return err
}

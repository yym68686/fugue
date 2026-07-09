package cli

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminEdgeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "edge",
		Short: "Manage edge data-plane canary policy",
	}
	cmd.AddCommand(c.newAdminEdgeRoutePolicyCommand())
	cmd.AddCommand(c.newAdminEdgeRouteCheckCommand())
	cmd.AddCommand(c.newAdminEdgeCacheCheckCommand())
	cmd.AddCommand(c.newAdminEdgeQualityRankCommand())
	cmd.AddCommand(c.newAdminEdgeNodesCommand())
	return cmd
}

func (c *CLI) newAdminEdgeQualityRankCommand() *cobra.Command {
	opts := struct {
		TrafficClass     string
		RequestSizeClass string
		Method           string
		PathPrefix       string
		Scope            string
		Window           string
		Since            string
	}{
		Scope:  "global",
		Window: "30m",
	}
	cmd := &cobra.Command{
		Use:   "quality-rank <hostname>",
		Short: "Rank edge nodes for a hostname and client scope",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.GetEdgeQualityRank(args[0], opts.TrafficClass, opts.RequestSizeClass, opts.Method, opts.PathPrefix, opts.Scope, opts.Window, opts.Since)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeEdgeQualityRank(c.stdout, response)
		},
	}
	cmd.Flags().StringVar(&opts.TrafficClass, "traffic-class", "", "Traffic class such as dynamic_api, large_body_api, static_cacheable, streaming, sse, or websocket")
	cmd.Flags().StringVar(&opts.RequestSizeClass, "request-size-class", "", "Request body size class: no_body, body_le_64k, body_64k_1m, body_1m_16m, or body_gt_16m")
	cmd.Flags().StringVar(&opts.Method, "method", "", "HTTP method such as GET or POST")
	cmd.Flags().StringVar(&opts.PathPrefix, "path-prefix", "", "Path prefix to bucket before ranking")
	cmd.Flags().StringVar(&opts.Scope, "scope", opts.Scope, "Client scope: global, country:<country>, region:<country>:<region>, or asn:<asn>")
	cmd.Flags().StringVar(&opts.Window, "window", opts.Window, "Quality window such as 30m, 6h, or 24h")
	cmd.Flags().StringVar(&opts.Since, "since", "", "Explicit lower bound as a duration or RFC3339 timestamp; overrides --window")
	return cmd
}

func (c *CLI) newAdminEdgeRouteCheckCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "route-check <hostname>",
		Short: "Explain edge readiness for a hostname",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			explain, err := client.ExplainRoute(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"explain": explain})
			}
			return writeRouteExplain(c.stdout, explain)
		},
	}
}

func (c *CLI) newAdminEdgeNodesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "nodes",
		Aliases: []string{"node"},
		Short:   "Inspect registered edge nodes",
	}
	cmd.AddCommand(
		c.newAdminEdgeNodesListCommand(),
		c.newAdminEdgeNodesGetCommand(),
		c.newAdminEdgeNodesQualityCommand(),
		c.newAdminEdgeNodesTokenCommand(),
		c.newAdminEdgeNodesDesiredStateCommand(),
		c.newAdminEdgeNodesProbeCommand(),
		c.newAdminEdgeNodesCanaryCommand(),
		c.newAdminEdgeNodesDrainCommand(),
		c.newAdminEdgeNodesUndrainCommand(),
	)
	return cmd
}

func (c *CLI) newAdminEdgeNodesListCommand() *cobra.Command {
	opts := struct {
		EdgeGroupID string
	}{}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List registered edge nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.ListEdgeNodes(opts.EdgeGroupID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeEdgeGroupTable(c.stdout, response.Groups); err != nil {
				return err
			}
			if len(response.Nodes) == 0 {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeEdgeNodeTable(c.stdout, response.Nodes)
		},
	}
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Only show nodes in this edge group")
	return cmd
}

func (c *CLI) newAdminEdgeNodesGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "get <edge-id>",
		Aliases: []string{"show"},
		Short:   "Show one registered edge node",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.GetEdgeNode(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeEdgeNode(c.stdout, response.Node); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeEdgeGroup(c.stdout, response.Group)
		},
	}
}

func (c *CLI) newAdminEdgeNodesQualityCommand() *cobra.Command {
	opts := struct {
		Since string
	}{
		Since: "24h",
	}
	cmd := &cobra.Command{
		Use:   "quality <edge-id>",
		Short: "Show per-node edge quality metrics",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.GetEdgeNodeQuality(args[0], opts.Since)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeEdgeNodeQuality(c.stdout, response)
		},
	}
	cmd.Flags().StringVar(&opts.Since, "since", opts.Since, "Quality window as a duration such as 24h or an RFC3339 timestamp")
	return cmd
}

func (c *CLI) newAdminEdgeNodesTokenCommand() *cobra.Command {
	opts := createEdgeNodeTokenRequest{}
	cmd := &cobra.Command{
		Use:   "token <edge-id>",
		Short: "Create or rotate an edge-scoped token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.EdgeGroupID) == "" {
				return fmt.Errorf("--edge-group is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.CreateEdgeNodeToken(args[0], opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeEdgeNode(c.stdout, response.Node); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "token=%s\n", response.Token)
			return err
		},
	}
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Edge group this node token is allowed to serve")
	cmd.Flags().StringVar(&opts.WorkloadMode, "workload-mode", "", "Workload mode for the edge node: static or dynamic")
	cmd.Flags().StringVar(&opts.CanaryState, "canary-state", "", "Initial canary state: joined, warming, probing, canary, active, or drained")
	cmd.Flags().IntVar(&opts.CanaryWeight, "canary-weight", 0, "Initial canary weight")
	cmd.Flags().StringVar(&opts.Region, "region", "", "Region metadata for the edge node")
	cmd.Flags().StringVar(&opts.Country, "country", "", "Country metadata for the edge node")
	cmd.Flags().StringVar(&opts.PublicHostname, "public-hostname", "", "Public hostname for the edge node")
	cmd.Flags().StringVar(&opts.PublicIPv4, "public-ipv4", "", "Public IPv4 for the edge node")
	cmd.Flags().StringVar(&opts.PublicIPv6, "public-ipv6", "", "Public IPv6 for the edge node")
	cmd.Flags().StringVar(&opts.MeshIP, "mesh-ip", "", "Private mesh IP for the edge node")
	cmd.Flags().BoolVar(&opts.Draining, "draining", false, "Create the node in draining mode")
	return cmd
}

func (c *CLI) newAdminEdgeNodesDesiredStateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "desired-state <edge-id>",
		Short: "Show control-plane desired state for one edge node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.GetAdminEdgeNodeDesiredState(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeEdgeNodeDesiredState(c.stdout, response.DesiredState)
		},
	}
}

func (c *CLI) newAdminEdgeNodesProbeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "probe <edge-id>",
		Short: "Probe public 80/443 reachability for one edge node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.ProbeEdgeNode(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeEdgeNodeControlResult(c.stdout, response)
		},
	}
}

func (c *CLI) newAdminEdgeNodesCanaryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "canary",
		Short: "Manage edge node canary state",
	}
	opts := setEdgeNodeCanaryRequest{State: model.EdgeCanaryStateCanary, Weight: 1}
	setCmd := &cobra.Command{
		Use:   "set <edge-id>",
		Short: "Set one edge node canary state and weight",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.SetEdgeNodeCanary(args[0], opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeEdgeNodeControlResult(c.stdout, response)
		},
	}
	setCmd.Flags().StringVar(&opts.State, "state", opts.State, "Canary state: canary, active, or drained")
	setCmd.Flags().IntVar(&opts.Weight, "weight", opts.Weight, "Canary answer weight; canary state is capped by the control plane")
	cmd.AddCommand(setCmd)
	return cmd
}

func (c *CLI) newAdminEdgeNodesDrainCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "drain <edge-id>",
		Short: "Drain one edge node from DNS answers",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.DrainEdgeNode(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeEdgeNodeControlResult(c.stdout, response)
		},
	}
}

func (c *CLI) newAdminEdgeNodesUndrainCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "undrain <edge-id>",
		Short: "Return one drained edge node to canary eligibility",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.UndrainEdgeNode(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeEdgeNodeControlResult(c.stdout, response)
		},
	}
}

func (c *CLI) newAdminEdgeRoutePolicyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "route-policy",
		Short: "Manage hostname opt-in for regional edge routing",
		Long: strings.TrimSpace(`
Edge route policies are a platform-admin override. Generated platform hostnames
use the nearest healthy non-excluded edge group by default. Policies can pin a
hostname to one edge group, opt it into edge routing, or exclude specific edge
nodes and edge groups from serving that hostname.
`),
	}
	cmd.AddCommand(
		c.newAdminEdgeRoutePolicyListCommand(),
		c.newAdminEdgeRoutePolicyShowCommand(),
		c.newAdminEdgeRoutePolicySetCommand(),
		c.newAdminEdgeRoutePolicyExcludeEdgeCommand(),
		c.newAdminEdgeRoutePolicyAllowEdgeCommand(),
		c.newAdminEdgeRoutePolicyExcludeEdgeGroupCommand(),
		c.newAdminEdgeRoutePolicyAllowEdgeGroupCommand(),
		c.newAdminEdgeRoutePolicyDeleteCommand(),
	)
	return cmd
}

func (c *CLI) newAdminEdgeRoutePolicyListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List configured edge route policies",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			policies, err := client.ListEdgeRoutePolicies()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policies": policies})
			}
			return writeEdgeRoutePolicyTable(c.stdout, policies)
		},
	}
}

func (c *CLI) newAdminEdgeRoutePolicyShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <hostname>",
		Aliases: []string{"get", "status"},
		Short:   "Show one hostname edge route policy",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			policy, err := client.GetEdgeRoutePolicy(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policy": policy})
			}
			return writeEdgeRoutePolicy(c.stdout, policy)
		},
	}
}

func (c *CLI) newAdminEdgeRoutePolicySetCommand() *cobra.Command {
	opts := struct {
		EdgeGroupID     string
		RoutePolicy     string
		MinHealthyEdges int
	}{}
	cmd := &cobra.Command{
		Use:   "set <hostname>",
		Short: "Opt a hostname into one regional edge group",
		Args:  cobra.ExactArgs(1),
		Example: strings.TrimSpace(`
  fugue admin edge route-policy set app-canary.fugue.pro --edge-group edge-group-country-us --policy edge_canary
  fugue admin edge route-policy set app-canary.fugue.pro --edge-group edge-group-country-us --policy edge_enabled
`),
		RunE: func(cmd *cobra.Command, args []string) error {
			policy := model.NormalizeEdgeRoutePolicy(opts.RoutePolicy)
			if policy == "" || policy == model.EdgeRoutePolicyRouteAOnly {
				return fmt.Errorf("--policy must be edge_canary or edge_enabled")
			}
			if strings.TrimSpace(opts.EdgeGroupID) == "" {
				return fmt.Errorf("--edge-group is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			updated, err := client.PutEdgeRoutePolicyUpdate(args[0], edgeRoutePolicyUpdate{
				EdgeGroupID:         opts.EdgeGroupID,
				RoutePolicy:         policy,
				MinHealthyEdgeNodes: opts.MinHealthyEdges,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policy": updated})
			}
			return writeEdgeRoutePolicy(c.stdout, updated)
		},
	}
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Edge group allowed to serve this hostname")
	cmd.Flags().StringVar(&opts.RoutePolicy, "policy", model.EdgeRoutePolicyCanary, "Policy to apply: edge_canary or edge_enabled")
	cmd.Flags().IntVar(&opts.MinHealthyEdges, "min-healthy-edges", 0, "Minimum healthy edge nodes expected for this hostname; defaults to platform policy")
	return cmd
}

func (c *CLI) newAdminEdgeRoutePolicyExcludeEdgeCommand() *cobra.Command {
	opts := struct {
		EdgeID          string
		Reason          string
		TTL             string
		MinHealthyEdges int
	}{}
	cmd := &cobra.Command{
		Use:   "exclude-edge <hostname>",
		Short: "Exclude one edge node from serving a hostname",
		Args:  cobra.ExactArgs(1),
		Example: strings.TrimSpace(`
  fugue admin edge route-policy exclude-edge api.example.com --edge vps-84c8f0a9 --reason slow-upload --ttl 6h
`),
		RunE: func(cmd *cobra.Command, args []string) error {
			edgeID := normalizeEdgeRoutePolicyCLIID(opts.EdgeID)
			if edgeID == "" {
				return fmt.Errorf("--edge is required")
			}
			expiresAt, err := parseEdgeRoutePolicyTTL(opts.TTL)
			if err != nil {
				return err
			}
			result, err := c.mutateEdgeRoutePolicyExclusions(args[0], true, func(policy *model.EdgeRoutePolicy) {
				policy.ExcludedEdgeIDs = addEdgeRoutePolicyListValue(policy.ExcludedEdgeIDs, edgeID)
				if strings.TrimSpace(opts.Reason) != "" {
					policy.ExclusionReason = strings.TrimSpace(opts.Reason)
				}
				if expiresAt != nil {
					policy.ExclusionExpiresAt = expiresAt
				}
				if opts.MinHealthyEdges > 0 {
					policy.MinHealthyEdgeNodes = opts.MinHealthyEdges
				}
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return writeEdgeRoutePolicyMutation(c.stdout, result)
		},
	}
	cmd.Flags().StringVar(&opts.EdgeID, "edge", "", "Edge node ID to exclude")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Reason recorded on the exclusion policy")
	cmd.Flags().StringVar(&opts.TTL, "ttl", "", "Optional exclusion TTL such as 30m, 6h, or 168h")
	cmd.Flags().IntVar(&opts.MinHealthyEdges, "min-healthy-edges", 0, "Minimum healthy edge nodes expected after this exclusion")
	return cmd
}

func (c *CLI) newAdminEdgeRoutePolicyAllowEdgeCommand() *cobra.Command {
	opts := struct {
		EdgeID string
	}{}
	cmd := &cobra.Command{
		Use:   "allow-edge <hostname>",
		Short: "Remove one edge node exclusion from a hostname",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			edgeID := normalizeEdgeRoutePolicyCLIID(opts.EdgeID)
			if edgeID == "" {
				return fmt.Errorf("--edge is required")
			}
			result, err := c.mutateEdgeRoutePolicyExclusions(args[0], false, func(policy *model.EdgeRoutePolicy) {
				policy.ExcludedEdgeIDs = removeEdgeRoutePolicyListValue(policy.ExcludedEdgeIDs, edgeID)
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return writeEdgeRoutePolicyMutation(c.stdout, result)
		},
	}
	cmd.Flags().StringVar(&opts.EdgeID, "edge", "", "Edge node ID to allow again")
	return cmd
}

func (c *CLI) newAdminEdgeRoutePolicyExcludeEdgeGroupCommand() *cobra.Command {
	opts := struct {
		EdgeGroupID     string
		Reason          string
		TTL             string
		MinHealthyEdges int
	}{}
	cmd := &cobra.Command{
		Use:   "exclude-edge-group <hostname>",
		Short: "Exclude one edge group from serving a hostname",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			edgeGroupID := normalizeEdgeRoutePolicyCLIID(opts.EdgeGroupID)
			if edgeGroupID == "" {
				return fmt.Errorf("--edge-group is required")
			}
			expiresAt, err := parseEdgeRoutePolicyTTL(opts.TTL)
			if err != nil {
				return err
			}
			result, err := c.mutateEdgeRoutePolicyExclusions(args[0], true, func(policy *model.EdgeRoutePolicy) {
				policy.ExcludedEdgeGroupIDs = addEdgeRoutePolicyListValue(policy.ExcludedEdgeGroupIDs, edgeGroupID)
				if strings.TrimSpace(opts.Reason) != "" {
					policy.ExclusionReason = strings.TrimSpace(opts.Reason)
				}
				if expiresAt != nil {
					policy.ExclusionExpiresAt = expiresAt
				}
				if opts.MinHealthyEdges > 0 {
					policy.MinHealthyEdgeNodes = opts.MinHealthyEdges
				}
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return writeEdgeRoutePolicyMutation(c.stdout, result)
		},
	}
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Edge group ID to exclude")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Reason recorded on the exclusion policy")
	cmd.Flags().StringVar(&opts.TTL, "ttl", "", "Optional exclusion TTL such as 30m, 6h, or 168h")
	cmd.Flags().IntVar(&opts.MinHealthyEdges, "min-healthy-edges", 0, "Minimum healthy edge nodes expected after this exclusion")
	return cmd
}

func (c *CLI) newAdminEdgeRoutePolicyAllowEdgeGroupCommand() *cobra.Command {
	opts := struct {
		EdgeGroupID string
	}{}
	cmd := &cobra.Command{
		Use:   "allow-edge-group <hostname>",
		Short: "Remove one edge group exclusion from a hostname",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			edgeGroupID := normalizeEdgeRoutePolicyCLIID(opts.EdgeGroupID)
			if edgeGroupID == "" {
				return fmt.Errorf("--edge-group is required")
			}
			result, err := c.mutateEdgeRoutePolicyExclusions(args[0], false, func(policy *model.EdgeRoutePolicy) {
				policy.ExcludedEdgeGroupIDs = removeEdgeRoutePolicyListValue(policy.ExcludedEdgeGroupIDs, edgeGroupID)
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return writeEdgeRoutePolicyMutation(c.stdout, result)
		},
	}
	cmd.Flags().StringVar(&opts.EdgeGroupID, "edge-group", "", "Edge group ID to allow again")
	return cmd
}

func (c *CLI) newAdminEdgeRoutePolicyDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <hostname>",
		Aliases: []string{"rm", "remove", "disable"},
		Short:   "Remove a hostname edge opt-in policy",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.DeleteEdgeRoutePolicy(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeEdgeRoutePolicy(c.stdout, response.Policy); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "deleted=%t\n", response.Deleted)
			return err
		},
	}
}

type edgeRoutePolicyMutationResult struct {
	Policy        model.EdgeRoutePolicy            `json:"policy"`
	Deleted       bool                             `json:"deleted,omitempty"`
	TrafficSafety *model.ServiceTrafficSafetyState `json:"traffic_safety,omitempty"`
}

func (c *CLI) mutateEdgeRoutePolicyExclusions(hostname string, createIfMissing bool, mutate func(*model.EdgeRoutePolicy)) (edgeRoutePolicyMutationResult, error) {
	client, err := c.newClient()
	if err != nil {
		return edgeRoutePolicyMutationResult{}, err
	}
	policy, err := client.GetEdgeRoutePolicy(hostname)
	if err != nil {
		apiErr, ok := err.(*apiServerError)
		if !ok || apiErr.StatusCode != http.StatusNotFound || !createIfMissing {
			return edgeRoutePolicyMutationResult{}, err
		}
		policy = model.EdgeRoutePolicy{
			Hostname:    strings.TrimSpace(hostname),
			RoutePolicy: model.EdgeRoutePolicyEnabled,
		}
	}
	if policy.RoutePolicy == "" {
		policy.RoutePolicy = model.EdgeRoutePolicyEnabled
	}
	if model.NormalizeEdgeRoutePolicy(policy.RoutePolicy) == model.EdgeRoutePolicyRouteAOnly {
		return edgeRoutePolicyMutationResult{}, fmt.Errorf("hostname is route_a_only; enable edge routing before excluding individual edges")
	}
	mutate(&policy)
	policy.ExcludedEdgeIDs = normalizeEdgeRoutePolicyCLIList(policy.ExcludedEdgeIDs)
	policy.ExcludedEdgeGroupIDs = normalizeEdgeRoutePolicyCLIList(policy.ExcludedEdgeGroupIDs)
	if len(policy.ExcludedEdgeIDs) == 0 && len(policy.ExcludedEdgeGroupIDs) == 0 {
		policy.ExclusionReason = ""
		policy.ExclusionExpiresAt = nil
		if strings.TrimSpace(policy.EdgeGroupID) == "" {
			response, err := client.DeleteEdgeRoutePolicy(hostname)
			if err != nil {
				return edgeRoutePolicyMutationResult{}, err
			}
			return edgeRoutePolicyMutationResult{Policy: response.Policy, Deleted: response.Deleted}, nil
		}
	}
	updated, err := client.PutEdgeRoutePolicyUpdate(hostname, edgeRoutePolicyUpdate{
		EdgeGroupID:          policy.EdgeGroupID,
		ExcludedEdgeIDs:      policy.ExcludedEdgeIDs,
		ExcludedEdgeGroupIDs: policy.ExcludedEdgeGroupIDs,
		ExclusionReason:      policy.ExclusionReason,
		ExclusionExpiresAt:   policy.ExclusionExpiresAt,
		MinHealthyEdgeNodes:  policy.MinHealthyEdgeNodes,
		RoutePolicy:          policy.RoutePolicy,
	})
	if err != nil {
		return edgeRoutePolicyMutationResult{}, err
	}
	result := edgeRoutePolicyMutationResult{Policy: updated}
	if updated.MinHealthyEdgeNodes > 0 {
		if state, err := client.ExplainTrafficSafety(hostname, updated.MinHealthyEdgeNodes); err == nil {
			result.TrafficSafety = &state
		}
	}
	return result, nil
}

func parseEdgeRoutePolicyTTL(raw string) (*time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		return nil, fmt.Errorf("--ttl must be a positive duration such as 30m or 6h")
	}
	expiresAt := time.Now().UTC().Add(duration)
	return &expiresAt, nil
}

func addEdgeRoutePolicyListValue(values []string, value string) []string {
	value = normalizeEdgeRoutePolicyCLIID(value)
	if value == "" {
		return normalizeEdgeRoutePolicyCLIList(values)
	}
	for _, existing := range values {
		if normalizeEdgeRoutePolicyCLIID(existing) == value {
			return normalizeEdgeRoutePolicyCLIList(values)
		}
	}
	return normalizeEdgeRoutePolicyCLIList(append(append([]string(nil), values...), value))
}

func removeEdgeRoutePolicyListValue(values []string, value string) []string {
	value = normalizeEdgeRoutePolicyCLIID(value)
	if value == "" {
		return normalizeEdgeRoutePolicyCLIList(values)
	}
	out := make([]string, 0, len(values))
	for _, existing := range values {
		if normalizeEdgeRoutePolicyCLIID(existing) == value {
			continue
		}
		out = append(out, existing)
	}
	return normalizeEdgeRoutePolicyCLIList(out)
}

func normalizeEdgeRoutePolicyCLIList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = normalizeEdgeRoutePolicyCLIID(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeEdgeRoutePolicyCLIID(value string) string {
	return strings.TrimSpace(strings.ToLower(value))
}

func writeEdgeRoutePolicyMutation(w io.Writer, result edgeRoutePolicyMutationResult) error {
	if err := writeEdgeRoutePolicy(w, result.Policy); err != nil {
		return err
	}
	if result.TrafficSafety != nil {
		if err := writeKeyValues(w,
			kvPair{Key: "traffic_safety", Value: edgeRoutePolicyTrafficSafetyStatus(*result.TrafficSafety)},
			kvPair{Key: "healthy_edges", Value: fmt.Sprintf("%d/%d", result.TrafficSafety.HealthyEdgeCount, result.TrafficSafety.MinHealthyEdgeCount)},
			kvPair{Key: "traffic_safety_blockers", Value: stringsJoin(result.TrafficSafety.Blockers)},
		); err != nil {
			return err
		}
	}
	if result.Deleted {
		_, err := fmt.Fprintln(w, "deleted=true")
		return err
	}
	return nil
}

func edgeRoutePolicyTrafficSafetyStatus(state model.ServiceTrafficSafetyState) string {
	if state.Pass {
		return "ok"
	}
	return "at_risk"
}

func writeEdgeRoutePolicy(w io.Writer, policy model.EdgeRoutePolicy) error {
	return writeKeyValues(w,
		kvPair{Key: "hostname", Value: strings.TrimSpace(policy.Hostname)},
		kvPair{Key: "app", Value: strings.TrimSpace(policy.AppID)},
		kvPair{Key: "tenant", Value: strings.TrimSpace(policy.TenantID)},
		kvPair{Key: "edge_group", Value: strings.TrimSpace(policy.EdgeGroupID)},
		kvPair{Key: "excluded_edges", Value: strings.Join(policy.ExcludedEdgeIDs, ",")},
		kvPair{Key: "excluded_edge_groups", Value: strings.Join(policy.ExcludedEdgeGroupIDs, ",")},
		kvPair{Key: "exclusion_reason", Value: strings.TrimSpace(policy.ExclusionReason)},
		kvPair{Key: "exclusion_expires", Value: formatOptionalEdgeTime(policy.ExclusionExpiresAt)},
		kvPair{Key: "min_healthy_edges", Value: fmt.Sprintf("%d", policy.MinHealthyEdgeNodes)},
		kvPair{Key: "route_policy", Value: strings.TrimSpace(policy.RoutePolicy)},
		kvPair{Key: "enabled", Value: fmt.Sprintf("%t", policy.Enabled)},
		kvPair{Key: "updated", Value: formatTime(policy.UpdatedAt)},
	)
}

func writeEdgeRoutePolicyTable(w io.Writer, policies []model.EdgeRoutePolicy) error {
	sorted := append([]model.EdgeRoutePolicy(nil), policies...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Hostname < sorted[j].Hostname
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "HOSTNAME\tPOLICY\tMIN_HEALTHY\tEDGE_GROUP\tEXCLUDED_EDGES\tEXCLUDED_GROUPS\tAPP\tENABLED\tUPDATED"); err != nil {
		return err
	}
	for _, policy := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%d\t%s\t%s\t%s\t%s\t%t\t%s\n",
			strings.TrimSpace(policy.Hostname),
			strings.TrimSpace(policy.RoutePolicy),
			policy.MinHealthyEdgeNodes,
			strings.TrimSpace(policy.EdgeGroupID),
			strings.Join(policy.ExcludedEdgeIDs, ","),
			strings.Join(policy.ExcludedEdgeGroupIDs, ","),
			strings.TrimSpace(policy.AppID),
			policy.Enabled,
			formatTime(policy.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeEdgeNode(w io.Writer, node model.EdgeNode) error {
	return writeKeyValues(w,
		kvPair{Key: "edge_id", Value: strings.TrimSpace(node.ID)},
		kvPair{Key: "edge_group", Value: strings.TrimSpace(node.EdgeGroupID)},
		kvPair{Key: "workload_mode", Value: cliEdgeNodeWorkloadMode(node)},
		kvPair{Key: "canary_state", Value: cliEdgeNodeCanaryState(node)},
		kvPair{Key: "canary_weight", Value: fmt.Sprintf("%d", cliEdgeNodeCanaryWeight(node))},
		kvPair{Key: "public_probe", Value: cliEdgeNodePublicProbeStatus(node)},
		kvPair{Key: "dns_eligible", Value: fmt.Sprintf("%t", cliEdgeNodeDNSEligible(node))},
		kvPair{Key: "dns_gate_reason", Value: cliEdgeNodeDNSGateReason(node)},
		kvPair{Key: "status", Value: strings.TrimSpace(node.Status)},
		kvPair{Key: "healthy", Value: fmt.Sprintf("%t", node.Healthy)},
		kvPair{Key: "draining", Value: fmt.Sprintf("%t", node.Draining)},
		kvPair{Key: "region", Value: strings.TrimSpace(node.Region)},
		kvPair{Key: "country", Value: strings.TrimSpace(node.Country)},
		kvPair{Key: "public_ipv4", Value: strings.TrimSpace(node.PublicIPv4)},
		kvPair{Key: "public_ipv6", Value: strings.TrimSpace(node.PublicIPv6)},
		kvPair{Key: "mesh_ip", Value: strings.TrimSpace(node.MeshIP)},
		kvPair{Key: "route_bundle", Value: strings.TrimSpace(node.RouteBundleVersion)},
		kvPair{Key: "dns_bundle", Value: strings.TrimSpace(node.DNSBundleVersion)},
		kvPair{Key: "caddy_routes", Value: fmt.Sprintf("%d", node.CaddyRouteCount)},
		kvPair{Key: "caddy_applied", Value: strings.TrimSpace(node.CaddyAppliedVersion)},
		kvPair{Key: "cache_status", Value: strings.TrimSpace(node.CacheStatus)},
		kvPair{Key: "tls_status", Value: strings.TrimSpace(node.TLSStatus)},
		kvPair{Key: "tls_last_message", Value: strings.TrimSpace(node.TLSLastMessage)},
		kvPair{Key: "tls_ready", Value: formatOptionalEdgeTime(node.TLSReadyAt)},
		kvPair{Key: "last_seen", Value: formatOptionalEdgeTime(node.LastSeenAt)},
		kvPair{Key: "last_heartbeat", Value: formatOptionalEdgeTime(node.LastHeartbeatAt)},
		kvPair{Key: "updated", Value: formatTime(node.UpdatedAt)},
	)
}

func writeEdgeNodeDesiredState(w io.Writer, state edgeNodeDesiredState) error {
	return writeKeyValues(w,
		kvPair{Key: "edge_id", Value: strings.TrimSpace(state.EdgeID)},
		kvPair{Key: "edge_group", Value: strings.TrimSpace(state.EdgeGroupID)},
		kvPair{Key: "workload_mode", Value: strings.TrimSpace(state.WorkloadMode)},
		kvPair{Key: "canary_state", Value: strings.TrimSpace(state.CanaryState)},
		kvPair{Key: "canary_weight", Value: fmt.Sprintf("%d", state.CanaryWeight)},
		kvPair{Key: "public_probe", Value: strings.TrimSpace(state.PublicProbeStatus)},
		kvPair{Key: "dns_eligible", Value: fmt.Sprintf("%t", state.DNSEligible)},
		kvPair{Key: "draining", Value: fmt.Sprintf("%t", state.Draining)},
		kvPair{Key: "route_ready", Value: fmt.Sprintf("%t", state.RouteReady)},
		kvPair{Key: "tls_ready", Value: fmt.Sprintf("%t", state.TLSReady)},
		kvPair{Key: "token_prefix", Value: strings.TrimSpace(state.TokenPrefix)},
		kvPair{Key: "last_heartbeat", Value: formatOptionalEdgeTime(state.LastHeartbeatAt)},
	)
}

func writeEdgeNodeControlResult(w io.Writer, response edgeNodeControlResponse) error {
	if err := writeEdgeNode(w, response.Node); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeEdgeNodeDesiredState(w, response.DesiredState)
}

func writeEdgeNodeQuality(w io.Writer, response model.EdgeNodeQualityResponse) error {
	summary := response.Summary
	if err := writeKeyValues(w,
		kvPair{Key: "edge_id", Value: strings.TrimSpace(summary.EdgeID)},
		kvPair{Key: "edge_group", Value: strings.TrimSpace(summary.EdgeGroupID)},
		kvPair{Key: "status", Value: strings.TrimSpace(response.Node.Status)},
		kvPair{Key: "healthy", Value: fmt.Sprintf("%t", response.Node.Healthy)},
		kvPair{Key: "draining", Value: fmt.Sprintf("%t", response.Node.Draining)},
		kvPair{Key: "since", Value: formatTime(summary.Since)},
		kvPair{Key: "sample_records", Value: fmt.Sprintf("%d", summary.SampleRecordCount)},
		kvPair{Key: "requests", Value: fmt.Sprintf("%d", summary.RequestCount)},
		kvPair{Key: "errors", Value: fmt.Sprintf("%d", summary.ErrorCount)},
		kvPair{Key: "error_rate", Value: formatEdgeNodeQualityRate(summary.ErrorRate)},
		kvPair{Key: "avg_ttfb_ms", Value: formatEdgeNodeQualityMetric(summary.AvgTTFBMS)},
		kvPair{Key: "avg_tls_ms", Value: formatEdgeNodeQualityMetric(summary.AvgTLSHandshakeMS)},
		kvPair{Key: "avg_upstream_ms", Value: formatEdgeNodeQualityMetric(summary.AvgUpstreamMS)},
		kvPair{Key: "avg_total_ms", Value: formatEdgeNodeQualityMetric(summary.AvgTotalMS)},
		kvPair{Key: "avg_upload_bps", Value: formatEdgeNodeQualityMetric(summary.AvgUploadBPS)},
		kvPair{Key: "min_upload_bps", Value: fmt.Sprintf("%d", summary.MinUploadBPS)},
		kvPair{Key: "avg_body_read_ms", Value: formatEdgeNodeQualityMetric(summary.AvgBodyReadMS)},
		kvPair{Key: "avg_max_read_gap_ms", Value: formatEdgeNodeQualityMetric(summary.AvgMaxReadGapMS)},
		kvPair{Key: "body_incomplete", Value: fmt.Sprintf("%d", summary.BodyIncompleteCount)},
		kvPair{Key: "body_read_errors", Value: fmt.Sprintf("%d", summary.BodyReadErrorCount)},
		kvPair{Key: "avg_response_egress_bps", Value: formatEdgeNodeQualityMetric(summary.AvgResponseEgressBPS)},
		kvPair{Key: "avg_response_write_ms", Value: formatEdgeNodeQualityMetric(summary.AvgResponseWriteMS)},
		kvPair{Key: "avg_origin_dns_ms", Value: formatEdgeNodeQualityMetric(summary.AvgOriginDNSMS)},
		kvPair{Key: "avg_origin_connect_ms", Value: formatEdgeNodeQualityMetric(summary.AvgOriginConnectMS)},
		kvPair{Key: "avg_origin_write_ms", Value: formatEdgeNodeQualityMetric(summary.AvgOriginWriteMS)},
		kvPair{Key: "avg_origin_wait_ms", Value: formatEdgeNodeQualityMetric(summary.AvgOriginWaitMS)},
		kvPair{Key: "avg_origin_ttfb_ms", Value: formatEdgeNodeQualityMetric(summary.AvgOriginTTFBMS)},
		kvPair{Key: "avg_origin_total_ms", Value: formatEdgeNodeQualityMetric(summary.AvgOriginTotalMS)},
		kvPair{Key: "avg_active_requests", Value: formatEdgeNodeQualityMetric(summary.AvgActiveRequests)},
		kvPair{Key: "avg_active_body_buffers", Value: formatEdgeNodeQualityMetric(summary.AvgActiveBodyBuffers)},
		kvPair{Key: "avg_client_tcp_rtt_ms", Value: formatEdgeNodeQualityMetric(summary.AvgClientTCPRTTMS)},
		kvPair{Key: "avg_client_tcp_rttvar_ms", Value: formatEdgeNodeQualityMetric(summary.AvgClientTCPRTTVarMS)},
		kvPair{Key: "client_tcp_retrans_rate", Value: formatEdgeNodeQualityRate(summary.ClientTCPRetransRate)},
		kvPair{Key: "client_tcp_rto_rate", Value: formatEdgeNodeQualityRate(summary.ClientTCPRTORate)},
		kvPair{Key: "cache_hit_rate", Value: formatEdgeNodeQualityRate(summary.CacheHitRate)},
		kvPair{Key: "tls_status", Value: strings.TrimSpace(summary.TLSStatus)},
		kvPair{Key: "cache_status", Value: strings.TrimSpace(summary.CacheStatus)},
		kvPair{Key: "caddy_routes", Value: fmt.Sprintf("%d", summary.CaddyRouteCount)},
		kvPair{Key: "route_bundle", Value: strings.TrimSpace(summary.RouteBundleVersion)},
		kvPair{Key: "dns_bundle", Value: strings.TrimSpace(summary.DNSBundleVersion)},
		kvPair{Key: "last_sampled", Value: formatOptionalEdgeTime(summary.LastSampledAt)},
		kvPair{Key: "generated_at", Value: formatTime(response.GeneratedAt)},
	); err != nil {
		return err
	}
	if len(response.Routes) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeEdgeNodeQualityRouteTable(w, response.Routes)
}

func writeEdgeQualityRank(w io.Writer, response model.EdgeQualityRankResponse) error {
	if err := writeKeyValues(w,
		kvPair{Key: "hostname", Value: strings.TrimSpace(response.Hostname)},
		kvPair{Key: "traffic_class", Value: strings.TrimSpace(response.TrafficClass)},
		kvPair{Key: "request_size_class", Value: strings.TrimSpace(response.RequestSizeClass)},
		kvPair{Key: "method", Value: strings.TrimSpace(response.Method)},
		kvPair{Key: "path_bucket", Value: strings.TrimSpace(response.PathPrefixBucket)},
		kvPair{Key: "requested_scope", Value: strings.TrimSpace(response.RequestedScope)},
		kvPair{Key: "selected_scope", Value: strings.TrimSpace(response.SelectedScope)},
		kvPair{Key: "fallback_level", Value: fmt.Sprintf("%d", response.FallbackLevel)},
		kvPair{Key: "fallback_reason", Value: strings.TrimSpace(response.FallbackReason)},
		kvPair{Key: "window", Value: strings.TrimSpace(response.Window)},
		kvPair{Key: "since", Value: formatTime(response.Since)},
		kvPair{Key: "generated_at", Value: formatTime(response.GeneratedAt)},
	); err != nil {
		return err
	}
	if response.ShadowComparison != nil {
		if err := writeKeyValues(w,
			kvPair{Key: "legacy_edge_group", Value: strings.TrimSpace(response.ShadowComparison.LegacySelectedEdgeGroupID)},
			kvPair{Key: "quality_edge_group", Value: strings.TrimSpace(response.ShadowComparison.QualitySelectedEdgeGroupID)},
			kvPair{Key: "shadow_changed", Value: fmt.Sprintf("%t", response.ShadowComparison.Changed)},
			kvPair{Key: "shadow_reason", Value: strings.TrimSpace(response.ShadowComparison.Reason)},
		); err != nil {
			return err
		}
	}
	if len(response.Candidates) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeEdgeQualityRankCandidateTable(w, response.Candidates); err != nil {
			return err
		}
	}
	if len(response.HardGated) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "\nHard-gated candidates:"); err != nil {
		return err
	}
	return writeEdgeQualityRankCandidateTable(w, response.HardGated)
}

func writeEdgeQualityRankCandidateTable(w io.Writer, candidates []model.EdgeQualityRankCandidate) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "RANK\tEDGE\tGROUP\tSCORE\tCONF\tREQUESTS\tERROR\tTTFB_MS\tUPLOAD_BPS\tTCP_RETRANS\tTCP_RTO\tBREAKDOWN\tREASON"); err != nil {
		return err
	}
	for _, candidate := range candidates {
		rank := "-"
		if candidate.Rank > 0 {
			rank = fmt.Sprintf("%d", candidate.Rank)
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			rank,
			strings.TrimSpace(candidate.EdgeID),
			strings.TrimSpace(candidate.EdgeGroupID),
			formatEdgeNodeQualityMetric(candidate.Score),
			formatEdgeNodeQualityRate(candidate.Confidence),
			candidate.RequestCount,
			formatEdgeNodeQualityRate(candidate.ErrorRate),
			formatEdgeNodeQualityMetric(candidate.AvgTTFBMS),
			formatEdgeNodeQualityMetric(candidate.AvgUploadBPS),
			formatEdgeNodeQualityRate(candidate.ClientTCPRetransRate),
			formatEdgeNodeQualityRate(candidate.ClientTCPRTORate),
			formatEdgeQualityScoreBreakdown(candidate.ScoreBreakdown),
			strings.TrimSpace(candidate.Reason),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatEdgeQualityScoreBreakdown(values map[string]float64) string {
	if len(values) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		value := values[key]
		if value == 0 {
			continue
		}
		parts = append(parts, key+"="+formatEdgeNodeQualityMetric(value))
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, ",")
}

func writeEdgeNodeQualityRouteTable(w io.Writer, routes []model.EdgeNodeQualityRoute) error {
	sorted := append([]model.EdgeNodeQualityRoute(nil), routes...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Hostname != sorted[j].Hostname {
			return sorted[i].Hostname < sorted[j].Hostname
		}
		if sorted[i].PathPrefix != sorted[j].PathPrefix {
			return sorted[i].PathPrefix < sorted[j].PathPrefix
		}
		if sorted[i].Method != sorted[j].Method {
			return sorted[i].Method < sorted[j].Method
		}
		return sorted[i].TrafficClass < sorted[j].TrafficClass
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "HOSTNAME\tPATH\tMETHOD\tCLASS\tREQUESTS\tERROR_RATE\tAVG_TTFB_MS\tUPLOAD_BPS\tBODY_READ_MS\tORIGIN_TTFB_MS\tRESP_BPS\tCACHE_HIT\tLAST_SAMPLED"); err != nil {
		return err
	}
	for _, route := range sorted {
		path := strings.TrimSpace(route.PathPrefix)
		if path == "" {
			path = "/"
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			strings.TrimSpace(route.Hostname),
			path,
			strings.TrimSpace(route.Method),
			strings.TrimSpace(route.TrafficClass),
			route.RequestCount,
			formatEdgeNodeQualityRate(route.ErrorRate),
			formatEdgeNodeQualityMetric(route.AvgTTFBMS),
			formatEdgeNodeQualityMetric(route.AvgUploadBPS),
			formatEdgeNodeQualityMetric(route.AvgBodyReadMS),
			formatEdgeNodeQualityMetric(route.AvgOriginTTFBMS),
			formatEdgeNodeQualityMetric(route.AvgResponseEgressBPS),
			formatEdgeNodeQualityRate(route.CacheHitRate),
			formatOptionalEdgeTime(route.LastSampledAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatEdgeNodeQualityRate(value float64) string {
	return fmt.Sprintf("%.2f%%", value*100)
}

func formatEdgeNodeQualityMetric(value float64) string {
	return fmt.Sprintf("%.1f", value)
}

func writeEdgeGroup(w io.Writer, group model.EdgeGroup) error {
	return writeKeyValues(w,
		kvPair{Key: "edge_group", Value: strings.TrimSpace(group.ID)},
		kvPair{Key: "status", Value: strings.TrimSpace(group.Status)},
		kvPair{Key: "region", Value: strings.TrimSpace(group.Region)},
		kvPair{Key: "country", Value: strings.TrimSpace(group.Country)},
		kvPair{Key: "nodes", Value: fmt.Sprintf("%d", group.NodeCount)},
		kvPair{Key: "healthy_nodes", Value: fmt.Sprintf("%d", group.HealthyNodeCount)},
		kvPair{Key: "has_healthy_nodes", Value: fmt.Sprintf("%t", group.HasHealthyNodes)},
		kvPair{Key: "last_seen", Value: formatOptionalEdgeTime(group.LastSeenAt)},
	)
}

func writeEdgeNodeTable(w io.Writer, nodes []model.EdgeNode) error {
	sorted := append([]model.EdgeNode(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].EdgeGroupID != sorted[j].EdgeGroupID {
			return sorted[i].EdgeGroupID < sorted[j].EdgeGroupID
		}
		return sorted[i].ID < sorted[j].ID
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "EDGE_ID\tGROUP\tMODE\tCANARY\tWEIGHT\tPROBE\tDNS_ELIGIBLE\tGATE\tSTATUS\tHEALTHY\tDRAINING\tROUTE_BUNDLE\tTLS\tCADDY_ROUTES\tLAST_SEEN"); err != nil {
		return err
	}
	for _, node := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%d\t%s\t%t\t%s\t%s\t%t\t%t\t%s\t%s\t%d\t%s\n",
			strings.TrimSpace(node.ID),
			strings.TrimSpace(node.EdgeGroupID),
			cliEdgeNodeWorkloadMode(node),
			cliEdgeNodeCanaryState(node),
			cliEdgeNodeCanaryWeight(node),
			cliEdgeNodePublicProbeStatus(node),
			cliEdgeNodeDNSEligible(node),
			cliEdgeNodeDNSGateReason(node),
			strings.TrimSpace(node.Status),
			node.Healthy,
			node.Draining,
			strings.TrimSpace(node.RouteBundleVersion),
			strings.TrimSpace(node.TLSStatus),
			node.CaddyRouteCount,
			formatOptionalEdgeTime(node.LastSeenAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func cliEdgeNodeWorkloadMode(node model.EdgeNode) string {
	mode := model.NormalizeEdgeWorkloadMode(node.WorkloadMode)
	if mode == "" {
		mode = model.EdgeWorkloadModeStatic
	}
	return mode
}

func cliEdgeNodeCanaryState(node model.EdgeNode) string {
	state := model.NormalizeEdgeCanaryState(node.CanaryState)
	if state != "" {
		return state
	}
	if cliEdgeNodeWorkloadMode(node) == model.EdgeWorkloadModeDynamic {
		return model.EdgeCanaryStateJoined
	}
	return model.EdgeCanaryStateActive
}

func cliEdgeNodeCanaryWeight(node model.EdgeNode) int {
	if node.CanaryWeight > 0 {
		return node.CanaryWeight
	}
	if cliEdgeNodeCanaryState(node) == model.EdgeCanaryStateActive {
		return 100
	}
	if cliEdgeNodeCanaryState(node) == model.EdgeCanaryStateCanary {
		return 1
	}
	return 0
}

func cliEdgeNodePublicProbeStatus(node model.EdgeNode) string {
	status := model.NormalizeEdgePublicProbeStatus(node.PublicProbeStatus)
	if status == "" {
		status = model.EdgePublicProbeStatusUnknown
	}
	return status
}

func cliEdgeNodeRouteReady(node model.EdgeNode) bool {
	return strings.TrimSpace(node.RouteBundleVersion) != "" ||
		strings.TrimSpace(node.ServingGeneration) != "" ||
		strings.TrimSpace(node.CaddyAppliedVersion) != ""
}

func cliEdgeNodeTLSReady(node model.EdgeNode) bool {
	status := strings.ToLower(strings.TrimSpace(node.TLSStatus))
	return status == "" || status == "ready" || status == "ok" || status == "healthy"
}

func cliEdgeNodeDNSEligible(node model.EdgeNode) bool {
	return cliEdgeNodeDNSGateReason(node) == "eligible"
}

func cliEdgeNodeDNSGateReason(node model.EdgeNode) string {
	if node.Draining {
		return "draining"
	}
	if !node.Healthy {
		return "not_healthy"
	}
	if !cliEdgeNodeRouteReady(node) {
		return "route_not_ready"
	}
	if !cliEdgeNodeTLSReady(node) {
		return "tls_not_ready"
	}
	if cliEdgeNodeWorkloadMode(node) != model.EdgeWorkloadModeDynamic {
		return "eligible"
	}
	switch cliEdgeNodeCanaryState(node) {
	case model.EdgeCanaryStateCanary, model.EdgeCanaryStateActive:
	default:
		return "not_canary"
	}
	if cliEdgeNodeCanaryWeight(node) <= 0 {
		return "zero_weight"
	}
	if cliEdgeNodePublicProbeStatus(node) == model.EdgePublicProbeStatusFailing {
		return "probe_failed"
	}
	return "eligible"
}

func writeEdgeGroupTable(w io.Writer, groups []model.EdgeGroup) error {
	sorted := append([]model.EdgeGroup(nil), groups...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ID < sorted[j].ID
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "GROUP\tSTATUS\tNODES\tHEALTHY\tHAS_HEALTHY\tREGION\tCOUNTRY\tLAST_SEEN"); err != nil {
		return err
	}
	for _, group := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%d\t%d\t%t\t%s\t%s\t%s\n",
			strings.TrimSpace(group.ID),
			strings.TrimSpace(group.Status),
			group.NodeCount,
			group.HealthyNodeCount,
			group.HasHealthyNodes,
			strings.TrimSpace(group.Region),
			strings.TrimSpace(group.Country),
			formatOptionalEdgeTime(group.LastSeenAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatOptionalEdgeTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return ""
	}
	return formatTime(*value)
}

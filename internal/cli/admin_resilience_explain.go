package cli

import (
	"fmt"
	"io"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminReleaseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "release",
		Short: "Inspect release safety gates",
	}
	guard := &cobra.Command{
		Use:   "guard",
		Short: "Inspect release guard decisions",
	}
	guard.AddCommand(c.newAdminReleaseGuardStatusCommand(), c.newAdminReleaseGuardSignalsCommand())
	cmd.AddCommand(guard)
	return cmd
}

func (c *CLI) newAdminReleaseGuardStatusCommand() *cobra.Command {
	opts := struct{ Subject string }{}
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show release guard status",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			status, err := client.GetReleaseGuardStatus(opts.Subject)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"status": status})
			}
			return writeReleaseGuardStatus(c.stdout, status)
		},
	}
	cmd.Flags().StringVar(&opts.Subject, "subject", "", "Optional hostname, app, node, or subsystem scope")
	return cmd
}

func (c *CLI) newAdminTrafficSafetyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "traffic-safety",
		Short: "Explain service traffic safety gates",
	}
	opts := struct{ MinHealthyEdges int }{MinHealthyEdges: 1}
	explain := &cobra.Command{
		Use:   "explain <hostname>",
		Short: "Explain whether a hostname has enough healthy eligible edges",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			state, err := client.ExplainTrafficSafety(args[0], opts.MinHealthyEdges)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"state": state})
			}
			return writeTrafficSafetyState(c.stdout, state)
		},
	}
	explain.Flags().IntVar(&opts.MinHealthyEdges, "min-healthy-edges", opts.MinHealthyEdges, "Minimum healthy eligible edge groups required")
	cmd.AddCommand(explain)
	return cmd
}

func (c *CLI) newAdminRequestCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "request",
		Short: "Explain request attribution",
	}
	opts := struct{ Since string }{}
	explain := &cobra.Command{
		Use:   "explain <request-id>",
		Short: "Explain one observed request without exposing request body or secrets",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			explain, err := client.ExplainRequest(args[0], opts.Since)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"explain": explain})
			}
			return writeRequestExplain(c.stdout, explain)
		},
	}
	explain.Flags().StringVar(&opts.Since, "since", "", "Lookback window such as 24h or RFC3339 timestamp")
	cmd.AddCommand(explain)
	return cmd
}

func writeReleaseGuardStatus(w io.Writer, status model.ReleaseGuardStatus) error {
	if err := writeKeyValues(w,
		kvPair{Key: "pass", Value: fmt.Sprintf("%t", status.Pass)},
		kvPair{Key: "block_rollout", Value: fmt.Sprintf("%t", status.BlockRollout)},
		kvPair{Key: "mode", Value: firstNonEmpty(status.Mode, "-")},
		kvPair{Key: "failure_contracts", Value: fmt.Sprintf("%d", status.FailureContractCount)},
		kvPair{Key: "artifact_kinds", Value: stringsJoin(status.PlatformArtifactKinds)},
		kvPair{Key: "artifact_validation_failures", Value: fmt.Sprintf("%d", status.PlatformArtifactFailures)},
		kvPair{Key: "consumer_drift", Value: fmt.Sprintf("%d", status.PlatformConsumerDrift)},
		kvPair{Key: "release_signals", Value: fmt.Sprintf("%d", len(status.ReleaseSignals))},
		kvPair{Key: "blocked_reasons", Value: stringsJoin(status.BlockedReasons)},
		kvPair{Key: "next_steps", Value: stringsJoin(status.RecommendedOperatorSteps)},
		kvPair{Key: "generated_at", Value: formatTime(status.GeneratedAt)},
	); err != nil {
		return err
	}
	if len(status.RobustnessBaseline.Checks) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeRobustnessCheckTable(w, status.RobustnessBaseline.Checks)
}

func writeTrafficSafetyState(w io.Writer, state model.ServiceTrafficSafetyState) error {
	if err := writeKeyValues(w,
		kvPair{Key: "hostname", Value: state.Hostname},
		kvPair{Key: "pass", Value: fmt.Sprintf("%t", state.Pass)},
		kvPair{Key: "healthy_edges", Value: fmt.Sprintf("%d/%d", state.HealthyEdgeCount, state.MinHealthyEdgeCount)},
		kvPair{Key: "eligible_edge_groups", Value: stringsJoin(state.EligibleEdgeGroups)},
		kvPair{Key: "hard_gated_edge_groups", Value: stringsJoin(state.HardGatedEdgeGroups)},
		kvPair{Key: "blockers", Value: stringsJoin(state.Blockers)},
		kvPair{Key: "route_mode", Value: firstNonEmpty(state.RouteExplain.ServingMode, "-")},
		kvPair{Key: "generated_at", Value: formatTime(state.GeneratedAt)},
	); err != nil {
		return err
	}
	if len(state.RouteExplain.Routes) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeEdgeRouteBindingTable(w, state.RouteExplain.Routes)
}

func writeRequestExplain(w io.Writer, explain model.RequestExplainResponse) error {
	pairs := []kvPair{
		{Key: "request_id", Value: explain.RequestID},
		{Key: "found", Value: fmt.Sprintf("%t", explain.Found)},
		{Key: "error_class", Value: firstNonEmpty(explain.ErrorClass, "-")},
		{Key: "secret_safe", Value: fmt.Sprintf("%t", explain.SecretSafe)},
		{Key: "generated_at", Value: formatTime(explain.GeneratedAt)},
	}
	if explain.Found {
		pairs = append(pairs,
			kvPair{Key: "edge", Value: firstNonEmpty(explain.EdgeID, "-")},
			kvPair{Key: "edge_group", Value: firstNonEmpty(explain.EdgeGroupID, "-")},
			kvPair{Key: "hostname", Value: firstNonEmpty(explain.Hostname, "-")},
			kvPair{Key: "method", Value: firstNonEmpty(explain.Method, "-")},
			kvPair{Key: "path_prefix", Value: firstNonEmpty(explain.PathPrefix, "-")},
			kvPair{Key: "traffic_class", Value: firstNonEmpty(explain.TrafficClass, "-")},
			kvPair{Key: "route_generation", Value: firstNonEmpty(explain.RouteGeneration, "-")},
			kvPair{Key: "status_code", Value: fmt.Sprintf("%d", explain.StatusCode)},
			kvPair{Key: "body_read_block_ms", Value: fmt.Sprintf("%d", explain.BodyReadBlockMS)},
			kvPair{Key: "upload_effective", Value: formatBytes(explain.UploadEffectiveBPS) + "/s"},
			kvPair{Key: "min_window", Value: formatBytes(explain.MinWindowBPS) + "/s"},
			kvPair{Key: "max_read_gap_ms", Value: fmt.Sprintf("%d", explain.MaxReadGapMS)},
			kvPair{Key: "body_read_bytes", Value: fmt.Sprintf("%d/%d", explain.RequestBodyReadBytes, explain.RequestBodyBytes)},
			kvPair{Key: "origin_dns_ms", Value: fmt.Sprintf("%d", explain.OriginDNSMS)},
			kvPair{Key: "origin_connect_ms", Value: fmt.Sprintf("%d", explain.OriginConnectMS)},
			kvPair{Key: "origin_write_ms", Value: fmt.Sprintf("%d", explain.OriginRequestWriteMS)},
			kvPair{Key: "origin_wait_ms", Value: fmt.Sprintf("%d", explain.OriginResponseWaitMS)},
			kvPair{Key: "origin_ttfb_ms", Value: fmt.Sprintf("%d", explain.OriginTTFBMS)},
			kvPair{Key: "origin_total_ms", Value: fmt.Sprintf("%d", explain.OriginTotalMS)},
			kvPair{Key: "client_tcp_rtt_ms", Value: fmt.Sprintf("%.2f", explain.ClientTCPRTTMS)},
			kvPair{Key: "client_tcp_retrans_rate", Value: fmt.Sprintf("%.4f", explain.ClientTCPRetransRate)},
			kvPair{Key: "client_tcp_rto_rate", Value: fmt.Sprintf("%.4f", explain.ClientTCPRTORate)},
			kvPair{Key: "client_tcp_delivery", Value: formatBytes(explain.ClientTCPDeliveryBPS) + "/s"},
			kvPair{Key: "attribution", Value: stringsJoin(explain.Attribution)},
			kvPair{Key: "sampled_at", Value: formatTime(explain.SampledAt)},
		)
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	if len(explain.Evidence) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeStringMap(w, explain.Evidence)
}

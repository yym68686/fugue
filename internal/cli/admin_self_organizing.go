package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminControlPlaneCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "control-plane", Short: "Inspect control-plane readiness gates"}
	storeCmd := &cobra.Command{Use: "store", Short: "Inspect and gate control-plane store promotion"}
	storeCmd.AddCommand(c.newAdminControlPlaneStoreStatusCommand(), c.newAdminControlPlaneStorePromoteCommand())
	cmd.AddCommand(storeCmd)
	return cmd
}

func (c *CLI) newAdminControlPlaneStoreStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show authoritative store status",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			status, err := client.GetControlPlaneStoreStatus()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"status": status})
			}
			return writeControlPlaneStoreStatus(c.stdout, status)
		},
	}
}

func (c *CLI) newAdminControlPlaneStorePromoteCommand() *cobra.Command {
	opts := cliStorePromoteRequest{}
	cmd := &cobra.Command{
		Use:   "promote",
		Short: "Run or confirm store promotion gates",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.DryRun == opts.Confirm {
				return fmt.Errorf("set exactly one of --dry-run or --confirm")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.PromoteControlPlaneStore(opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeStorePromotion(c.stdout, response.Promotion)
		},
	}
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "Run promotion gates without cutover")
	cmd.Flags().BoolVar(&opts.Confirm, "confirm", false, "Confirm a previously passing dry-run")
	cmd.Flags().StringVar(&opts.TargetStore, "target-store", "", "Target store identifier")
	cmd.Flags().StringVar(&opts.SourceKind, "source-kind", "", "Source store kind")
	cmd.Flags().StringVar(&opts.SourceFingerprint, "source-fingerprint", "", "Expected source fingerprint")
	cmd.Flags().StringVar(&opts.Generation, "generation", "", "Expected store generation")
	cmd.Flags().StringVar(&opts.BackupRef, "backup-ref", "", "Protective backup path or object ref")
	cmd.Flags().StringVar(&opts.RollbackRef, "rollback-ref", "", "Rollback manifest or restore ref")
	return cmd
}

func (c *CLI) newAdminRoutesCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "routes", Short: "Inspect platform route decisions"}
	cmd.AddCommand(&cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List hostname serving modes",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			routes, err := client.ListRouteServingModes()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"routes": routes})
			}
			return writeRouteServingModes(c.stdout, routes)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "explain <hostname>",
		Short: "Explain edge selection for a hostname",
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
	})
	return cmd
}

func writeRouteServingModes(w io.Writer, routes []model.RouteServingMode) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "HOSTNAME\tMODE\tEDGE_GROUP\tRUNTIME_GROUP\tREASON"); err != nil {
		return err
	}
	for _, route := range routes {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			route.Hostname,
			route.ServingMode,
			firstNonEmpty(route.SelectedEdgeGroup, "-"),
			firstNonEmpty(route.RuntimeEdgeGroup, "-"),
			firstNonEmpty(route.Reason, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func (c *CLI) newAdminPlatformCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "platform", Short: "Inspect platform autonomy gates"}
	autonomy := &cobra.Command{Use: "autonomy", Short: "Inspect self-organizing platform status"}
	autonomy.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Show self-organizing readiness summary",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			status, err := client.GetPlatformAutonomyStatus()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"status": status})
			}
			return writePlatformAutonomyStatus(c.stdout, status)
		},
	})
	drillOpts := model.PlatformFailureDrillRequest{DryRun: true}
	drill := &cobra.Command{
		Use:   "failure-drill",
		Short: "Run a non-destructive self-organizing failure drill",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			report, err := client.RunPlatformFailureDrill(drillOpts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"report": report})
			}
			return writePlatformFailureDrill(c.stdout, report)
		},
	}
	drill.Flags().BoolVar(&drillOpts.DryRun, "dry-run", true, "Only report what would be tested")
	drill.Flags().StringVar(&drillOpts.Target, "target", "", "Control-plane node target or random selector")
	cmd.AddCommand(drill)
	cmd.AddCommand(autonomy)
	return cmd
}

func (c *CLI) newAdminSecurityCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "security", Short: "Inspect token and signing key lifecycle gates"}
	opts := model.KeyRotationPreflightRequest{}
	rotation := &cobra.Command{
		Use:   "key-rotation",
		Short: "Dry-run or stage bundle signing key rotation",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !opts.DryRun && !opts.Stage && !opts.ConfirmRevoke {
				opts.DryRun = true
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			preflight, err := client.PreflightKeyRotation(opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"preflight": preflight})
			}
			return writeKeyRotationPreflight(c.stdout, preflight)
		},
	}
	rotation.Flags().BoolVar(&opts.DryRun, "dry-run", false, "Check key rotation readiness without changing rollout phase")
	rotation.Flags().BoolVar(&opts.Stage, "stage", false, "Require a staged dual-sign window before rollout")
	rotation.Flags().BoolVar(&opts.ConfirmRevoke, "confirm-revoke", false, "Require all nodes to accept the new key before revoking the previous key")
	rotation.Flags().StringVar(&opts.NewKeyID, "new-key-id", "", "Expected active bundle signing key id")
	rotation.Flags().StringVar(&opts.PreviousKeyID, "previous-key-id", "", "Expected previous bundle signing key id")
	cmd.AddCommand(rotation)
	return cmd
}

func writeControlPlaneStoreStatus(w io.Writer, status model.ControlPlaneStoreStatus) error {
	if err := writeKeyValues(w,
		kvPair{Key: "authoritative_store", Value: status.AuthoritativeStore},
		kvPair{Key: "store_generation", Value: status.StoreGeneration},
		kvPair{Key: "source_fingerprint", Value: status.SourceFingerprint},
		kvPair{Key: "permission_verification", Value: status.PermissionVerificationStatus},
		kvPair{Key: "restore_readiness", Value: status.RestoreReadiness},
		kvPair{Key: "block_rollout", Value: fmt.Sprintf("%t", status.BlockRollout)},
		kvPair{Key: "gate_reason", Value: firstNonEmpty(status.GateReason, "-")},
	); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(w)
	return writeInvariantChecks(w, status.Invariants)
}

func writeStorePromotion(w io.Writer, promotion model.StorePromotion) error {
	return writeKeyValues(w,
		kvPair{Key: "id", Value: promotion.ID},
		kvPair{Key: "target_store", Value: promotion.TargetStore},
		kvPair{Key: "generation", Value: promotion.Generation},
		kvPair{Key: "status", Value: promotion.Status},
		kvPair{Key: "dry_run", Value: fmt.Sprintf("%t", promotion.DryRun)},
		kvPair{Key: "message", Value: firstNonEmpty(promotion.Message, "-")},
	)
}

func writeRouteExplain(w io.Writer, explain model.RouteExplainResponse) error {
	pairs := []kvPair{
		{Key: "hostname", Value: explain.Hostname},
		{Key: "serving_mode", Value: explain.ServingMode},
		{Key: "fallback_chain", Value: stringsJoin(explain.FallbackChain)},
		{Key: "reasons", Value: stringsJoin(explain.Reasons)},
	}
	if explain.Route != nil {
		pairs = append(pairs,
			kvPair{Key: "selected_edge_group", Value: firstNonEmpty(explain.Route.SelectedEdgeGroup, "-")},
			kvPair{Key: "runtime_edge_group", Value: firstNonEmpty(explain.Route.RuntimeEdgeGroup, "-")},
			kvPair{Key: "fallback_reason", Value: firstNonEmpty(explain.Route.FallbackReason, "-")},
		)
	}
	return writeKeyValues(w, pairs...)
}

func writePlatformAutonomyStatus(w io.Writer, status model.PlatformAutonomyStatus) error {
	if err := writeKeyValues(w,
		kvPair{Key: "pass", Value: fmt.Sprintf("%t", status.Pass)},
		kvPair{Key: "block_rollout", Value: fmt.Sprintf("%t", status.BlockRollout)},
		kvPair{Key: "control_plane_store", Value: status.ControlPlaneStore.RestoreReadiness},
		kvPair{Key: "discovery_bundle", Value: status.DiscoveryBundle},
		kvPair{Key: "node_policy", Value: status.NodePolicy},
		kvPair{Key: "edge", Value: status.Edge},
		kvPair{Key: "dns", Value: status.DNS},
		kvPair{Key: "registry", Value: status.Registry},
		kvPair{Key: "headscale", Value: status.Headscale},
		kvPair{Key: "route_fallback", Value: status.RouteFallback},
	); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(w)
	return writeInvariantChecks(w, status.Checks)
}

func writePlatformFailureDrill(w io.Writer, report model.PlatformFailureDrillReport) error {
	if err := writeKeyValues(w,
		kvPair{Key: "id", Value: report.ID},
		kvPair{Key: "target", Value: report.Target},
		kvPair{Key: "dry_run", Value: fmt.Sprintf("%t", report.DryRun)},
		kvPair{Key: "status", Value: report.Status},
		kvPair{Key: "block_rollout", Value: fmt.Sprintf("%t", report.BlockRollout)},
		kvPair{Key: "report_ref", Value: report.ReportRef},
		kvPair{Key: "backlog", Value: stringsJoin(report.Backlog)},
	); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(w)
	return writeInvariantChecks(w, report.Checks)
}

func writeKeyRotationPreflight(w io.Writer, preflight model.KeyRotationPreflight) error {
	if err := writeKeyValues(w,
		kvPair{Key: "dry_run", Value: fmt.Sprintf("%t", preflight.DryRun)},
		kvPair{Key: "stage", Value: fmt.Sprintf("%t", preflight.Stage)},
		kvPair{Key: "confirm_revoke", Value: fmt.Sprintf("%t", preflight.ConfirmRevoke)},
		kvPair{Key: "current_key_id", Value: preflight.CurrentKeyID},
		kvPair{Key: "new_key_id", Value: preflight.NewKeyID},
		kvPair{Key: "previous_key_id", Value: preflight.PreviousKeyID},
		kvPair{Key: "can_stage", Value: fmt.Sprintf("%t", preflight.CanStage)},
		kvPair{Key: "can_revoke_previous", Value: fmt.Sprintf("%t", preflight.CanRevokePrevious)},
		kvPair{Key: "block_rollout", Value: fmt.Sprintf("%t", preflight.BlockRollout)},
	); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(w)
	return writeInvariantChecks(w, preflight.Checks)
}

func writeInvariantChecks(w io.Writer, checks []model.StoreInvariantCheck) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "CHECK\tPASS\tCOUNT\tMESSAGE"); err != nil {
		return err
	}
	for _, check := range checks {
		if _, err := fmt.Fprintf(tw, "%s\t%t\t%d\t%s\n", check.Name, check.Pass, check.Count, firstNonEmpty(check.Message, "-")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func stringsJoin(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ",")
}

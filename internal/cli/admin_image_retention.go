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

func (c *CLI) newAdminImageRetentionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "image-retention",
		Short: "Plan distributed image-store retention reconciliation",
		Long: strings.TrimSpace(`
Inspect how distributed image-store retention would reconcile app image generations.

plan is observe-only: it computes keep/drop decisions, stale system pins, pending
replication tasks, and historical 2/2 replica policy convergence without changing
control-plane metadata or node-local files.

reconcile --dry-run is an alias for the same non-mutating plan output. A future
non-dry-run reconcile must still flow through the control plane release path and
node-updater tasks; do not use SSH deletion as a substitute.
`),
	}
	cmd.AddCommand(
		c.newAdminImageRetentionPlanCommand(),
		c.newAdminImageRetentionReconcileCommand(),
	)
	return cmd
}

func (c *CLI) newAdminImageRetentionPlanCommand() *cobra.Command {
	opts := struct {
		App       string
		All       bool
		Decisions bool
	}{}
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Observe distributed image retention keep/drop decisions",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			plans, err := c.fetchImageRetentionPlans(opts.App, opts.All)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"plans": plans})
			}
			return writeImageRetentionPlans(c.stdout, plans, opts.Decisions || !opts.All)
		},
	}
	cmd.Flags().StringVar(&opts.App, "app", "", "App ID or name to plan")
	cmd.Flags().BoolVar(&opts.All, "all", false, "Plan all apps")
	cmd.Flags().BoolVar(&opts.Decisions, "decisions", false, "Print per-image keep/drop decisions")
	return cmd
}

func (c *CLI) newAdminImageRetentionReconcileCommand() *cobra.Command {
	opts := struct {
		App       string
		All       bool
		DryRun    bool
		Decisions bool
	}{DryRun: true}
	cmd := &cobra.Command{
		Use:   "reconcile",
		Short: "Dry-run distributed image retention reconciliation",
		Long: strings.TrimSpace(`
Dry-run distributed image retention reconciliation.

This command intentionally refuses mutating execution today. It is used to audit
which image generations would be kept/dropped, which historical pins would expire,
and which obsolete replication tasks would be canceled before enabling automated
metadata reconciliation in the controller.
`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !opts.DryRun {
				return fmt.Errorf("only --dry-run reconciliation is supported by this CLI command")
			}
			plans, err := c.fetchImageRetentionPlans(opts.App, opts.All)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"dry_run": true, "plans": plans})
			}
			if _, err := fmt.Fprintln(c.stdout, "dry_run=true"); err != nil {
				return err
			}
			return writeImageRetentionPlans(c.stdout, plans, opts.Decisions || !opts.All)
		},
	}
	cmd.Flags().StringVar(&opts.App, "app", "", "App ID or name to dry-run reconcile")
	cmd.Flags().BoolVar(&opts.All, "all", false, "Dry-run reconcile all apps")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", true, "Do not mutate metadata; required")
	cmd.Flags().BoolVar(&opts.Decisions, "decisions", false, "Print per-image keep/drop decisions")
	return cmd
}

func (c *CLI) fetchImageRetentionPlans(app string, all bool) ([]model.DistributedImageRetentionPlan, error) {
	if !all && strings.TrimSpace(app) == "" {
		return nil, fmt.Errorf("--app or --all is required")
	}
	client, err := c.newClient()
	if err != nil {
		return nil, err
	}
	return client.GetImageRetentionPlan(app, all)
}

func writeImageRetentionPlans(w io.Writer, plans []model.DistributedImageRetentionPlan, includeDecisions bool) error {
	sorted := append([]model.DistributedImageRetentionPlan(nil), plans...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].AppName != sorted[j].AppName {
			return sorted[i].AppName < sorted[j].AppName
		}
		return sorted[i].AppID < sorted[j].AppID
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "APP\tAPP_ID\tLIMIT\tKEEP\tDROP\tPIN_DRYRUN\tTASK_DRYRUN\t2/2_DRYRUN"); err != nil {
		return err
	}
	for _, plan := range sorted {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\n",
			firstNonEmpty(strings.TrimSpace(plan.AppName), "-"),
			firstNonEmpty(strings.TrimSpace(plan.AppID), "-"),
			plan.EffectiveLimit,
			len(plan.KeepImageIDs),
			len(plan.DropImageIDs),
			plan.WouldDeletePins,
			plan.WouldCancelTasks,
			plan.WouldNormalizeImages,
		); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if !includeDecisions {
		return nil
	}
	for _, plan := range sorted {
		if _, err := fmt.Fprintf(w, "\n[decisions app=%s]\n", firstNonEmpty(strings.TrimSpace(plan.AppName), strings.TrimSpace(plan.AppID), "-")); err != nil {
			return err
		}
		if err := writeImageRetentionDecisionTable(w, plan.ImageDecisions); err != nil {
			return err
		}
	}
	return nil
}

func writeImageRetentionDecisionTable(w io.Writer, decisions []model.ImageRetentionDecision) error {
	sorted := append([]model.ImageRetentionDecision(nil), decisions...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Rank != sorted[j].Rank {
			return sorted[i].Rank < sorted[j].Rank
		}
		return sorted[i].ImageID < sorted[j].ImageID
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "RANK\tIMAGE_ID\tKEEP\tREASON\tCURRENT\tACTIVE_OP\tUSER_PIN\tIMAGE_REF\tLAST_DEPLOYED"); err != nil {
		return err
	}
	for _, decision := range sorted {
		last := "-"
		if decision.LastDeployedAt != nil {
			last = formatTime(*decision.LastDeployedAt)
		}
		if _, err := fmt.Fprintf(tw, "%d\t%s\t%t\t%s\t%t\t%t\t%t\t%s\t%s\n",
			decision.Rank,
			firstNonEmpty(strings.TrimSpace(decision.ImageID), "-"),
			decision.Keep,
			firstNonEmpty(strings.TrimSpace(decision.Reason), "-"),
			decision.CurrentWorkload,
			decision.ActiveOperation,
			decision.UserPinned,
			firstNonEmpty(strings.TrimSpace(decision.ImageRef), "-"),
			last,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

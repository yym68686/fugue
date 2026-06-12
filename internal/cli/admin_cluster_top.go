package cli

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	climonitor "fugue/internal/cli/monitor"
	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type clusterTopPayload struct {
	ClusterNodes      []model.ClusterNode                   `json:"cluster_nodes"`
	Runtimes          []model.Runtime                       `json:"runtimes"`
	ControlPlane      *model.ControlPlaneStatus             `json:"control_plane,omitempty"`
	NodePolicySummary *model.ClusterNodePolicyStatusSummary `json:"node_policy_summary,omitempty"`
	NodePolicies      []model.ClusterNodePolicyStatus       `json:"node_policies,omitempty"`
}

func (c *CLI) newAdminClusterTopCommand() *cobra.Command {
	opts := monitorOptions{Interval: 3 * time.Second, Sort: "NODE"}
	cmd := &cobra.Command{
		Use:   "top",
		Short: "Watch high-density cluster, runtime, and control-plane status",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			if c.wantsJSON() {
				payload, err := c.loadClusterTopPayload(client)
				if err != nil {
					return err
				}
				return writeJSON(c.stdout, payload)
			}
			if opts.Once || !c.shouldUseInteractiveMonitor(opts.Plain) {
				if !opts.Once {
					c.progressf("watch_fallback=plain_snapshot reason=non_interactive")
				}
				payload, err := c.loadClusterTopPayload(client)
				if err != nil {
					return err
				}
				return c.renderMonitorSnapshot(clusterTopPayloadSnapshot(payload, opts))
			}
			return c.watchClusterTopMonitor(ctx, client, opts)
		},
	}
	cmd.Flags().DurationVar(&opts.Interval, "interval", opts.Interval, "Monitor refresh interval")
	cmd.Flags().BoolVar(&opts.Once, "once", false, "Render one monitor snapshot and exit")
	cmd.Flags().BoolVar(&opts.Plain, "plain", false, "Force plain monitor output without interactive terminal mode")
	cmd.Flags().BoolVar(&opts.AltScreen, "alt-screen", false, "Use the alternate screen for interactive monitor output")
	cmd.Flags().StringVar(&opts.Filter, "filter", "", "Filter monitor table rows")
	cmd.Flags().StringVar(&opts.Search, "search", "", "Search monitor table rows")
	cmd.Flags().StringVar(&opts.Sort, "sort", opts.Sort, "Sort monitor table rows by column")
	return cmd
}

func (c *CLI) watchClusterTopMonitor(ctx context.Context, client *Client, opts monitorOptions) error {
	if opts.Interval <= 0 {
		opts.Interval = 3 * time.Second
	}
	session := climonitor.Session{Controls: monitorControls(opts)}
	for {
		payload, err := c.loadClusterTopPayload(client)
		if err != nil {
			if session.HaveLast {
				if renderErr := c.renderMonitorSnapshot(session.SnapshotWithError(err, time.Now().UTC())); renderErr != nil {
					return renderErr
				}
				if waitErr := waitMonitorInterval(ctx, opts.Interval); waitErr != nil {
					return c.printMonitorSummary(session.Last)
				}
				continue
			}
			return err
		}
		snapshot := clusterTopPayloadSnapshot(payload, opts)
		if session.Accept(snapshot) {
			if err := c.renderMonitorSnapshot(snapshot); err != nil {
				return err
			}
		}
		if err := waitMonitorInterval(ctx, opts.Interval); err != nil {
			return c.printMonitorSummary(snapshot)
		}
	}
}

func (c *CLI) loadClusterTopPayload(client *Client) (clusterTopPayload, error) {
	nodes, err := client.ListClusterNodes()
	if err != nil {
		return clusterTopPayload{}, err
	}
	runtimes, err := client.ListRuntimes()
	if err != nil {
		return clusterTopPayload{}, err
	}
	controlPlane, err := client.GetControlPlaneStatus()
	if err != nil {
		return clusterTopPayload{}, err
	}
	summary, policies, err := client.GetClusterNodePolicyStatus()
	if err != nil {
		return clusterTopPayload{}, err
	}
	return clusterTopPayload{
		ClusterNodes:      nodes,
		Runtimes:          runtimes,
		ControlPlane:      &controlPlane,
		NodePolicySummary: &summary,
		NodePolicies:      policies,
	}, nil
}

func clusterTopPayloadSnapshot(payload clusterTopPayload, opts monitorOptions) climonitor.Snapshot {
	snapshot := buildClusterTopSnapshot(payload.ClusterNodes, payload.Runtimes, payload.ControlPlane, payload.NodePolicySummary, opts)
	if payload.NodePolicySummary != nil && len(payload.NodePolicies) > 0 {
		rows := make([][]string, 0, len(payload.NodePolicies))
		for _, policy := range payload.NodePolicies {
			rows = append(rows, []string{
				firstNonEmptyTrimmed(policy.NodeName, "-"),
				fmt.Sprintf("%t", policy.Ready),
				fmt.Sprintf("%t", policy.Reconciled),
				fmt.Sprintf("%t", policy.BlockRollout),
				firstNonEmptyTrimmed(policy.GateReason, stringsJoinWithSeparator(policy.ReconcileReasons, "; "), "-"),
			})
		}
		snapshot.Sections = append(snapshot.Sections, climonitor.Section{
			Title:   "node policy",
			Headers: []string{"NODE", "READY", "RECONCILED", "BLOCK", "REASON"},
			Rows:    rows,
		})
	}
	return snapshot
}

func stringsJoinWithSeparator(values []string, sep string) string {
	if len(values) == 0 {
		return ""
	}
	out := ""
	for _, value := range values {
		if value == "" {
			continue
		}
		if out != "" {
			out += sep
		}
		out += value
	}
	return out
}

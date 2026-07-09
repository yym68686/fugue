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

func (c *CLI) newAdminNodeUpdaterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "node-updater",
		Aliases: []string{"node-updaters"},
		Short:   "Inspect host node updaters and maintenance tasks",
	}
	cmd.AddCommand(
		c.newAdminNodeUpdaterListCommand(),
		c.newAdminNodeUpdaterHealthCommand(),
		c.newAdminNodeUpdaterTaskCommand(),
		c.newAdminNodeUpdaterRepairHistoryCommand(),
		c.newAdminNodeUpdaterRolloutCommand(),
	)
	return cmd
}

func (c *CLI) newAdminNodeUpdaterRolloutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rollout",
		Short: "Inspect and control node-updater generation rollout",
	}
	cmd.AddCommand(
		c.newAdminNodeUpdaterRolloutStatusCommand(),
		c.newAdminNodeUpdaterRolloutPauseCommand(),
		c.newAdminNodeUpdaterRolloutResumeCommand(),
	)
	return cmd
}

func (c *CLI) newAdminNodeUpdaterRolloutStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show node-updater rollout status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			updaters, err := client.ListNodeUpdaters()
			if err != nil {
				return err
			}
			gate, err := client.GetGatePolicy("node_updater.generation_rollout")
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"target_generation": model.NodeUpdaterCurrentVersion,
					"gate":              gate,
					"node_updaters":     updaters,
				})
			}
			return writeNodeUpdaterRolloutStatus(c.stdout, gate, updaters)
		},
	}
}

func (c *CLI) newAdminNodeUpdaterRolloutPauseCommand() *cobra.Command {
	opts := struct{ Reason string }{}
	cmd := &cobra.Command{
		Use:   "pause",
		Short: "Pause node-updater generation rollout",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.PromoteGatePolicy("node_updater.generation_rollout", model.GatePolicyPromoteRequest{
				Mode:   model.GatePolicyModeDisabled,
				Reason: firstNonEmpty(opts.Reason, "pause node-updater rollout"),
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeGatePolicy(c.stdout, response.Policy)
		},
	}
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Pause reason")
	return cmd
}

func (c *CLI) newAdminNodeUpdaterRolloutResumeCommand() *cobra.Command {
	opts := struct {
		Reason string
		Canary []string
	}{}
	cmd := &cobra.Command{
		Use:   "resume",
		Short: "Resume node-updater generation rollout in canary mode",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			canary := opts.Canary
			if len(canary) == 0 {
				canary = []string{"single-node"}
			}
			response, err := client.PromoteGatePolicy("node_updater.generation_rollout", model.GatePolicyPromoteRequest{
				Mode:         model.GatePolicyModeCanary,
				Reason:       firstNonEmpty(opts.Reason, "resume node-updater rollout canary"),
				CanaryScopes: canary,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeGatePolicy(c.stdout, response.Policy)
		},
	}
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Resume reason")
	cmd.Flags().StringArrayVar(&opts.Canary, "canary-scope", nil, "Canary failure-domain scope (repeatable)")
	return cmd
}

func (c *CLI) newAdminNodeUpdaterListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List registered host node updaters",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			updaters, err := client.ListNodeUpdaters()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"node_updaters": updaters})
			}
			return writeNodeUpdaterTable(c.stdout, updaters)
		},
	}
}

func (c *CLI) newAdminNodeUpdaterTaskCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "task",
		Aliases: []string{"tasks"},
		Short:   "Inspect and enqueue host maintenance tasks",
	}
	cmd.AddCommand(
		c.newAdminNodeUpdaterTaskListCommand(),
		c.newAdminNodeUpdaterTaskCreateCommand(),
	)
	return cmd
}

func (c *CLI) newAdminNodeUpdaterRepairHistoryCommand() *cobra.Command {
	opts := struct {
		NodeUpdaterID string
		Status        string
	}{}
	cmd := &cobra.Command{
		Use:   "repair-history",
		Short: "Show node-local autonomy and repair task history",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tasks, err := client.ListNodeUpdateTasks(opts.NodeUpdaterID, opts.Status)
			if err != nil {
				return err
			}
			filtered := make([]model.NodeUpdateTask, 0, len(tasks))
			for _, task := range tasks {
				if nodeUpdateTaskIsRepairHistory(task) {
					filtered = append(filtered, task)
				}
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"tasks": filtered})
			}
			return writeNodeUpdateTaskTable(c.stdout, filtered)
		},
	}
	cmd.Flags().StringVar(&opts.NodeUpdaterID, "node-updater", "", "Filter by node updater ID")
	cmd.Flags().StringVar(&opts.Status, "status", "", "Filter by task status")
	return cmd
}

func (c *CLI) newAdminNodeUpdaterHealthCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Inspect node deep health and quarantine state",
	}
	list := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List latest node deep health reports",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			results, err := client.ListNodeDeepHealthResults()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"results": results})
			}
			return writeNodeDeepHealthTable(c.stdout, results)
		},
	}
	show := &cobra.Command{
		Use:   "show <node-updater-id>",
		Short: "Show one node deep health report",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			result, err := client.GetNodeDeepHealthResult(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"result": result})
			}
			return writeNodeDeepHealth(c.stdout, result)
		},
	}
	cmd.AddCommand(list, show)
	return cmd
}

func (c *CLI) newAdminNodeUpdaterTaskListCommand() *cobra.Command {
	opts := struct {
		NodeUpdaterID string
		Status        string
	}{}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List host maintenance tasks",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tasks, err := client.ListNodeUpdateTasks(opts.NodeUpdaterID, opts.Status)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"tasks": tasks})
			}
			return writeNodeUpdateTaskTable(c.stdout, tasks)
		},
	}
	cmd.Flags().StringVar(&opts.NodeUpdaterID, "node-updater", "", "Filter by node updater ID")
	cmd.Flags().StringVar(&opts.NodeUpdaterID, "node-updater-id", "", "Filter by node updater ID")
	cmd.Flags().StringVar(&opts.Status, "status", "", "Filter by task status")
	_ = cmd.Flags().MarkHidden("node-updater-id")
	return cmd
}

func (c *CLI) newAdminNodeUpdaterTaskCreateCommand() *cobra.Command {
	opts := struct {
		NodeUpdaterID          string
		ClusterNodeName        string
		RuntimeRef             string
		Type                   string
		Payload                []string
		DryRun                 bool
		AllowDelete            bool
		ExpectedLVCount        int
		ExpectedPVCount        int
		ExpectedImageSizeBytes int64
	}{}
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Enqueue a host maintenance task",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			taskType := normalizeNodeUpdateTaskType(opts.Type)
			if taskType == "" {
				return fmt.Errorf("--type must be one of %s", strings.Join(nodeUpdateTaskTypes(), ", "))
			}
			if strings.TrimSpace(opts.NodeUpdaterID) == "" && strings.TrimSpace(opts.ClusterNodeName) == "" && strings.TrimSpace(opts.RuntimeRef) == "" {
				return fmt.Errorf("one of --node-updater, --cluster-node, or --runtime is required")
			}
			payload, err := parseEnvAssignments(opts.Payload)
			if err != nil {
				return err
			}
			if flagChanged(cmd, "dry-run") {
				payload["dry_run"] = fmt.Sprintf("%t", opts.DryRun)
			}
			if flagChanged(cmd, "allow-delete") {
				payload["allow_delete"] = fmt.Sprintf("%t", opts.AllowDelete)
			}
			if flagChanged(cmd, "expected-lv-count") {
				payload["expected_lv_count"] = fmt.Sprintf("%d", opts.ExpectedLVCount)
			}
			if flagChanged(cmd, "expected-bound-pv-count") {
				payload["expected_bound_pv_count"] = fmt.Sprintf("%d", opts.ExpectedPVCount)
			}
			if flagChanged(cmd, "expected-image-size-bytes") {
				payload["expected_image_size_bytes"] = fmt.Sprintf("%d", opts.ExpectedImageSizeBytes)
			}
			if taskType == model.NodeUpdateTaskTypeDecommissionLocalPV && (flagChanged(cmd, "dry-run") || flagChanged(cmd, "allow-delete")) {
				payload["allow_localpv_decommission"] = "true"
				if flagChanged(cmd, "allow-delete") && opts.AllowDelete && !flagChanged(cmd, "dry-run") {
					payload["dry_run"] = "false"
				}
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			runtimeID := strings.TrimSpace(opts.RuntimeRef)
			if runtimeID != "" {
				runtimeObj, err := c.resolveNamedRuntime(client, runtimeID)
				if err != nil {
					return err
				}
				runtimeID = runtimeObj.ID
			}
			task, err := client.CreateNodeUpdateTask(nodeUpdateTaskCreateRequest{
				NodeUpdaterID:   strings.TrimSpace(opts.NodeUpdaterID),
				ClusterNodeName: strings.TrimSpace(opts.ClusterNodeName),
				RuntimeID:       runtimeID,
				Type:            taskType,
				Payload:         payload,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"task": task})
			}
			return writeNodeUpdateTask(c.stdout, task)
		},
	}
	cmd.Flags().StringVar(&opts.NodeUpdaterID, "node-updater", "", "Target node updater ID")
	cmd.Flags().StringVar(&opts.NodeUpdaterID, "node-updater-id", "", "Target node updater ID")
	cmd.Flags().StringVar(&opts.ClusterNodeName, "cluster-node", "", "Target cluster node name")
	cmd.Flags().StringVar(&opts.RuntimeRef, "runtime", "", "Target runtime name or ID")
	cmd.Flags().StringVar(&opts.Type, "type", "", "Task type")
	cmd.Flags().StringArrayVar(&opts.Payload, "payload", nil, "Task payload as KEY=VALUE (repeatable)")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", true, "Set dry_run task payload")
	cmd.Flags().BoolVar(&opts.AllowDelete, "allow-delete", false, "Set allow_delete task payload")
	cmd.Flags().IntVar(&opts.ExpectedLVCount, "expected-lv-count", 0, "Expected LocalPV LV count for decommission preflight")
	cmd.Flags().IntVar(&opts.ExpectedPVCount, "expected-bound-pv-count", 0, "Expected bound LocalPV PV count for decommission preflight")
	cmd.Flags().Int64Var(&opts.ExpectedImageSizeBytes, "expected-image-size-bytes", 0, "Expected LocalPV backing file size in bytes")
	_ = cmd.Flags().MarkHidden("node-updater-id")
	return cmd
}

func nodeUpdateTaskTypes() []string {
	return []string{
		model.NodeUpdateTaskTypeRefreshJoinConfig,
		model.NodeUpdateTaskTypeUpgradeK3SAgent,
		model.NodeUpdateTaskTypeUpgradeUpdater,
		model.NodeUpdateTaskTypeRestartK3SAgent,
		model.NodeUpdateTaskTypeDiagnoseNode,
		model.NodeUpdateTaskTypeInstallNFSClient,
		model.NodeUpdateTaskTypePrepullSystemImages,
		model.NodeUpdateTaskTypePrepullAppImages,
		model.NodeUpdateTaskTypeReplicateAppImage,
		model.NodeUpdateTaskTypeVerifyImageCache,
		model.NodeUpdateTaskTypePruneImageCache,
		model.NodeUpdateTaskTypeReportImageCache,
		model.NodeUpdateTaskTypeReportLocalPV,
		model.NodeUpdateTaskTypeDecommissionLocalPV,
		model.NodeUpdateTaskTypeVerifySystemdEscape,
		model.NodeUpdateTaskTypeRepairManagedIPTables,
		model.NodeUpdateTaskTypeRefreshDesiredState,
		model.NodeUpdateTaskTypeReloadLKGBundle,
		model.NodeUpdateTaskTypeRestartStatelessNodeService,
		model.NodeUpdateTaskTypeRunDeepHealth,
	}
}

func normalizeNodeUpdateTaskType(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	for _, candidate := range nodeUpdateTaskTypes() {
		if raw == candidate {
			return candidate
		}
	}
	return ""
}

func writeNodeUpdaterTable(w io.Writer, updaters []model.NodeUpdater) error {
	sorted := append([]model.NodeUpdater(nil), updaters...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].ClusterNodeName != sorted[j].ClusterNodeName {
			return sorted[i].ClusterNodeName < sorted[j].ClusterNodeName
		}
		return sorted[i].ID < sorted[j].ID
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "UPDATER\tSTATUS\tNODE\tRUNTIME\tVERSION\tK3S\tDISCOVERY\tLAST_HEARTBEAT\tERROR"); err != nil {
		return err
	}
	for _, updater := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			firstNonEmpty(strings.TrimSpace(updater.ID), "-"),
			firstNonEmpty(strings.TrimSpace(updater.Status), "-"),
			firstNonEmpty(strings.TrimSpace(updater.ClusterNodeName), "-"),
			firstNonEmpty(strings.TrimSpace(updater.RuntimeID), "-"),
			firstNonEmpty(strings.TrimSpace(updater.UpdaterVersion), "-"),
			firstNonEmpty(strings.TrimSpace(updater.K3SVersion), "-"),
			firstNonEmpty(strings.TrimSpace(updater.DiscoveryGeneration), "-"),
			formatOptionalTimePtr(updater.LastHeartbeatAt),
			firstNonEmpty(strings.TrimSpace(updater.LastError), "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeNodeUpdaterRolloutStatus(w io.Writer, gate model.GatePolicy, updaters []model.NodeUpdater) error {
	current := 0
	outdated := 0
	missing := 0
	for _, updater := range updaters {
		switch strings.TrimSpace(updater.UpdaterVersion) {
		case model.NodeUpdaterCurrentVersion:
			current++
		case "":
			missing++
		default:
			outdated++
		}
	}
	if err := writeKeyValues(w,
		kvPair{Key: "target_generation", Value: model.NodeUpdaterCurrentVersion},
		kvPair{Key: "gate_mode", Value: firstNonEmpty(gate.Mode, "-")},
		kvPair{Key: "canary_scopes", Value: stringsJoin(gate.CanaryFailureDomains)},
		kvPair{Key: "total_nodes", Value: fmt.Sprintf("%d", len(updaters))},
		kvPair{Key: "current", Value: fmt.Sprintf("%d", current)},
		kvPair{Key: "outdated", Value: fmt.Sprintf("%d", outdated)},
		kvPair{Key: "missing_version", Value: fmt.Sprintf("%d", missing)},
		kvPair{Key: "kill_switch", Value: firstNonEmpty(gate.KillSwitchEnv, "-")},
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeNodeUpdaterTable(w, updaters)
}

func writeNodeDeepHealthTable(w io.Writer, results []model.NodeDeepHealthResult) error {
	sorted := append([]model.NodeDeepHealthResult(nil), results...)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].UpdatedAt.Equal(sorted[j].UpdatedAt) {
			return sorted[i].UpdatedAt.After(sorted[j].UpdatedAt)
		}
		return sorted[i].NodeUpdaterID < sorted[j].NodeUpdaterID
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "UPDATER\tNODE\tSTATUS\tQUARANTINE\tREASON\tEXPIRES\tUPDATED"); err != nil {
		return err
	}
	for _, result := range sorted {
		expires := "-"
		if result.QuarantineExpiresAt != nil {
			expires = formatTime(*result.QuarantineExpiresAt)
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			firstNonEmpty(strings.TrimSpace(result.NodeUpdaterID), "-"),
			firstNonEmpty(strings.TrimSpace(result.ClusterNodeName), "-"),
			firstNonEmpty(strings.TrimSpace(result.OverallStatus), "-"),
			firstNonEmpty(strings.TrimSpace(result.QuarantineState), "-"),
			firstNonEmpty(strings.TrimSpace(result.QuarantineReason), "-"),
			expires,
			formatTime(result.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeNodeDeepHealth(w io.Writer, result model.NodeDeepHealthResult) error {
	expires := "-"
	if result.QuarantineExpiresAt != nil {
		expires = formatTime(*result.QuarantineExpiresAt)
	}
	if err := writeKeyValues(w,
		kvPair{Key: "node_updater", Value: firstNonEmpty(strings.TrimSpace(result.NodeUpdaterID), "-")},
		kvPair{Key: "cluster_node", Value: firstNonEmpty(strings.TrimSpace(result.ClusterNodeName), "-")},
		kvPair{Key: "runtime", Value: firstNonEmpty(strings.TrimSpace(result.RuntimeID), "-")},
		kvPair{Key: "machine", Value: firstNonEmpty(strings.TrimSpace(result.MachineID), "-")},
		kvPair{Key: "observed_only", Value: fmt.Sprintf("%t", result.ObservedOnly)},
		kvPair{Key: "overall_status", Value: firstNonEmpty(strings.TrimSpace(result.OverallStatus), "-")},
		kvPair{Key: "quarantine_state", Value: firstNonEmpty(strings.TrimSpace(result.QuarantineState), "-")},
		kvPair{Key: "quarantine_reason", Value: firstNonEmpty(strings.TrimSpace(result.QuarantineReason), "-")},
		kvPair{Key: "quarantine_expires", Value: expires},
		kvPair{Key: "reported_at", Value: formatTime(result.ReportedAt)},
		kvPair{Key: "updated_at", Value: formatTime(result.UpdatedAt)},
	); err != nil {
		return err
	}
	if len(result.RecoveryConditions) > 0 {
		if _, err := fmt.Fprintln(w, "\nRecovery conditions:"); err != nil {
			return err
		}
		for _, condition := range result.RecoveryConditions {
			if _, err := fmt.Fprintf(w, "- %s\n", strings.TrimSpace(condition)); err != nil {
				return err
			}
		}
	}
	if len(result.Checks) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "\nChecks:"); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "STATUS\tHARD\tCATEGORY\tCHECK\tOBSERVED\tMESSAGE"); err != nil {
		return err
	}
	for _, check := range result.Checks {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%t\t%s\t%s\t%s\t%s\n",
			firstNonEmpty(strings.TrimSpace(check.Status), "-"),
			check.HardFail,
			firstNonEmpty(strings.TrimSpace(check.Category), "-"),
			firstNonEmpty(strings.TrimSpace(check.Name), "-"),
			firstNonEmpty(strings.TrimSpace(check.Observed), "-"),
			firstNonEmpty(strings.TrimSpace(check.Message), "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeNodeUpdateTaskTable(w io.Writer, tasks []model.NodeUpdateTask) error {
	sorted := append([]model.NodeUpdateTask(nil), tasks...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TASK\tTYPE\tSTATUS\tUPDATER\tNODE\tRUNTIME\tUPDATED\tRESULT"); err != nil {
		return err
	}
	for _, task := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			firstNonEmpty(strings.TrimSpace(task.ID), "-"),
			firstNonEmpty(strings.TrimSpace(task.Type), "-"),
			firstNonEmpty(strings.TrimSpace(task.Status), "-"),
			firstNonEmpty(strings.TrimSpace(task.NodeUpdaterID), "-"),
			firstNonEmpty(strings.TrimSpace(task.ClusterNodeName), "-"),
			firstNonEmpty(strings.TrimSpace(task.RuntimeID), "-"),
			formatTime(task.UpdatedAt),
			firstNonEmpty(strings.TrimSpace(task.ResultMessage), strings.TrimSpace(task.ErrorMessage), "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeNodeUpdateTask(w io.Writer, task model.NodeUpdateTask) error {
	if err := writeKeyValues(w,
		kvPair{Key: "task", Value: strings.TrimSpace(task.ID)},
		kvPair{Key: "type", Value: strings.TrimSpace(task.Type)},
		kvPair{Key: "status", Value: strings.TrimSpace(task.Status)},
		kvPair{Key: "node_updater", Value: firstNonEmpty(strings.TrimSpace(task.NodeUpdaterID), "-")},
		kvPair{Key: "cluster_node", Value: firstNonEmpty(strings.TrimSpace(task.ClusterNodeName), "-")},
		kvPair{Key: "runtime", Value: firstNonEmpty(strings.TrimSpace(task.RuntimeID), "-")},
		kvPair{Key: "created_at", Value: formatTime(task.CreatedAt)},
		kvPair{Key: "updated_at", Value: formatTime(task.UpdatedAt)},
		kvPair{Key: "result", Value: firstNonEmpty(strings.TrimSpace(task.ResultMessage), "-")},
		kvPair{Key: "error", Value: firstNonEmpty(strings.TrimSpace(task.ErrorMessage), "-")},
	); err != nil {
		return err
	}
	if len(task.Payload) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "\n[payload]"); err != nil {
		return err
	}
	return writeStringMap(w, task.Payload)
}

func nodeUpdateTaskIsRepairHistory(task model.NodeUpdateTask) bool {
	switch strings.TrimSpace(task.Type) {
	case model.NodeUpdateTaskTypeRestartK3SAgent,
		model.NodeUpdateTaskTypeRepairManagedIPTables,
		model.NodeUpdateTaskTypeRefreshDesiredState,
		model.NodeUpdateTaskTypeReloadLKGBundle,
		model.NodeUpdateTaskTypeRestartStatelessNodeService,
		model.NodeUpdateTaskTypeRunDeepHealth:
		return true
	default:
		return false
	}
}

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
		c.newAdminNodeUpdaterTaskCommand(),
	)
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
		NodeUpdaterID   string
		ClusterNodeName string
		RuntimeRef      string
		Type            string
		Payload         []string
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
		model.NodeUpdateTaskTypeVerifySystemdEscape,
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

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

func (c *CLI) newAdminImageCacheCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "image-cache",
		Short: "Inspect and plan node-local image-cache cleanup",
	}
	cmd.AddCommand(
		c.newAdminImageCacheInventoryCommand(),
		c.newAdminImageCachePrunePlanCommand(),
		c.newAdminImageCachePruneCommand(),
	)
	return cmd
}

type adminImageCacheNodeFilter struct {
	NodeID          string
	ClusterNodeName string
	RuntimeID       string
}

func (c *CLI) newAdminImageCacheInventoryCommand() *cobra.Command {
	opts := adminImageCacheNodeFilter{}
	cmd := &cobra.Command{
		Use:   "inventory",
		Short: "List latest reported node-local image-cache inventories",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			nodes, manifests, err := client.ListImageCacheInventory(opts.NodeID, opts.ClusterNodeName, opts.RuntimeID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"nodes": nodes, "manifests": manifests})
			}
			if err := writeImageCacheInventoryTable(c.stdout, nodes); err != nil {
				return err
			}
			if len(manifests) == 0 || (opts.NodeID == "" && opts.ClusterNodeName == "" && opts.RuntimeID == "") {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout, "\n[manifests]"); err != nil {
				return err
			}
			return writeImageCacheManifestTable(c.stdout, manifests)
		},
	}
	addImageCacheNodeFilterFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newAdminImageCachePrunePlanCommand() *cobra.Command {
	opts := struct {
		adminImageCacheNodeFilter
		Mode    string
		Persist bool
	}{}
	cmd := &cobra.Command{
		Use:   "prune-plan",
		Short: "Compute an image-cache orphan prune plan without executing it",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			plan, err := client.GetImageCachePrunePlan(opts.NodeID, opts.ClusterNodeName, opts.RuntimeID, opts.Mode, opts.Persist)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"plan": plan})
			}
			return writeImageCachePrunePlan(c.stdout, plan)
		},
	}
	addImageCacheNodeFilterFlags(cmd, &opts.adminImageCacheNodeFilter)
	cmd.Flags().StringVar(&opts.Mode, "mode", "observe", "Plan mode: observe, dry-run, or delete")
	cmd.Flags().BoolVar(&opts.Persist, "persist", false, "Persist the computed observe plan")
	return cmd
}

func (c *CLI) newAdminImageCachePruneCommand() *cobra.Command {
	opts := struct {
		adminImageCacheNodeFilter
		DryRun         bool
		AllowDelete    bool
		MaxDeleteBytes int64
	}{DryRun: true}
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Create a node-updater task for an image-cache prune plan",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.NodeID == "" && opts.ClusterNodeName == "" && opts.RuntimeID == "" {
				return fmt.Errorf("--node, --node-id, or --runtime is required")
			}
			mode := model.ImageCachePruneModeDryRun
			if opts.AllowDelete {
				mode = model.ImageCachePruneModeDelete
			}
			dryRun := true
			if opts.AllowDelete {
				dryRun = false
			}
			if flagChanged(cmd, "dry-run") {
				dryRun = opts.DryRun
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			plan, task, err := client.CreateImageCachePrunePlanTask(createImageCachePrunePlanTaskRequest{
				NodeID:          opts.NodeID,
				ClusterNodeName: opts.ClusterNodeName,
				RuntimeID:       opts.RuntimeID,
				Mode:            mode,
				AllowDelete:     opts.AllowDelete,
				MaxDeleteBytes:  opts.MaxDeleteBytes,
				DryRun:          &dryRun,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"plan": plan, "task": task})
			}
			if err := writeImageCachePrunePlan(c.stdout, plan); err != nil {
				return err
			}
			if strings.TrimSpace(task.ID) == "" {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout, "\n[task]"); err != nil {
				return err
			}
			return writeNodeUpdateTask(c.stdout, task)
		},
	}
	addImageCacheNodeFilterFlags(cmd, &opts.adminImageCacheNodeFilter)
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", true, "Create a non-mutating dry-run prune task")
	cmd.Flags().BoolVar(&opts.AllowDelete, "allow-delete", false, "Allow delete-mode prune task creation")
	cmd.Flags().Int64Var(&opts.MaxDeleteBytes, "max-delete-bytes", 0, "Override per-node prune byte budget")
	return cmd
}

func addImageCacheNodeFilterFlags(cmd *cobra.Command, opts *adminImageCacheNodeFilter) {
	cmd.Flags().StringVar(&opts.ClusterNodeName, "node", "", "Filter by cluster node name")
	cmd.Flags().StringVar(&opts.ClusterNodeName, "cluster-node", "", "Filter by cluster node name")
	cmd.Flags().StringVar(&opts.NodeID, "node-id", "", "Filter by machine/node ID")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime", "", "Filter by runtime ID")
}

func writeImageCacheInventoryTable(w io.Writer, nodes []model.ImageCacheNodeInventory) error {
	sorted := append([]model.ImageCacheNodeInventory(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].ClusterNodeName != sorted[j].ClusterNodeName {
			return sorted[i].ClusterNodeName < sorted[j].ClusterNodeName
		}
		return sorted[i].NodeID < sorted[j].NodeID
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NODE\tRUNTIME\tMANIFESTS\tCACHE\tFREE\tUSED%\tPINS\tOBSERVED\tENDPOINT\tSTATUS"); err != nil {
		return err
	}
	for _, node := range sorted {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%.1f\t%d\t%s\t%s\t%s\n",
			firstNonEmpty(strings.TrimSpace(node.ClusterNodeName), strings.TrimSpace(node.NodeID), "-"),
			firstNonEmpty(strings.TrimSpace(node.RuntimeID), "-"),
			node.ManifestCount,
			formatBytes(node.CacheBytes),
			formatBytes(node.FilesystemFreeBytes),
			node.FilesystemUsedPercent,
			node.PinCount,
			formatTime(node.ObservedAt),
			firstNonEmpty(strings.TrimSpace(node.CacheEndpoint), "-"),
			firstNonEmpty(strings.TrimSpace(node.Status), "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeImageCacheManifestTable(w io.Writer, manifests []model.ImageCacheManifest) error {
	sorted := append([]model.ImageCacheManifest(nil), manifests...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Repo != sorted[j].Repo {
			return sorted[i].Repo < sorted[j].Repo
		}
		return sorted[i].Target < sorted[j].Target
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "REPO\tTARGET\tDIGEST\tBYTES\tPINNED\tLAST_SEEN"); err != nil {
		return err
	}
	for _, manifest := range sorted {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%t\t%s\n",
			firstNonEmpty(strings.TrimSpace(manifest.Repo), "-"),
			firstNonEmpty(strings.TrimSpace(manifest.Target), "-"),
			firstNonEmpty(shortDigest(manifest.Digest), "-"),
			formatBytes(firstNonZeroCLIInt64(manifest.TotalBlobBytes, manifest.ManifestSizeBytes)),
			manifest.PinnedLocally,
			formatTime(manifest.LastSeenAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeImageCachePrunePlan(w io.Writer, plan model.ImageCachePrunePlan) error {
	if err := writeKeyValues(w,
		kvPair{Key: "plan", Value: firstNonEmpty(strings.TrimSpace(plan.ID), "-")},
		kvPair{Key: "mode", Value: firstNonEmpty(strings.TrimSpace(plan.Mode), "-")},
		kvPair{Key: "node", Value: firstNonEmpty(strings.TrimSpace(plan.ClusterNodeName), strings.TrimSpace(plan.NodeID), "-")},
		kvPair{Key: "status", Value: firstNonEmpty(strings.TrimSpace(plan.Status), "-")},
		kvPair{Key: "candidate_manifests", Value: formatInt(plan.CandidateManifestCount)},
		kvPair{Key: "protected_manifests", Value: formatInt(plan.ProtectedManifestCount)},
		kvPair{Key: "planned_delete_bytes", Value: formatBytes(plan.PlannedDeleteBytes)},
		kvPair{Key: "max_delete_bytes", Value: formatBytes(plan.MaxDeleteBytes)},
		kvPair{Key: "min_manifest_age", Value: firstNonEmpty(strings.TrimSpace(plan.MinManifestAge), "-")},
		kvPair{Key: "protection_summary", Value: formatImageCacheSummary(plan.ProtectionSummary)},
		kvPair{Key: "candidate_summary", Value: formatImageCacheSummary(plan.CandidateSummary)},
		kvPair{Key: "created_at", Value: formatTime(plan.CreatedAt)},
		kvPair{Key: "error", Value: firstNonEmpty(strings.TrimSpace(plan.Error), "-")},
	); err != nil {
		return err
	}
	if len(plan.Candidates) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "\n[candidates]"); err != nil {
		return err
	}
	return writeImageCachePruneCandidateTable(w, plan.Candidates)
}

func writeImageCachePruneCandidateTable(w io.Writer, candidates []model.ImageCachePruneCandidate) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "REPO\tTARGET\tDIGEST\tREASON\tPROTECTED\tSKIP\tBYTES\tLAST_SEEN"); err != nil {
		return err
	}
	for _, candidate := range candidates {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%t\t%s\t%s\t%s\n",
			firstNonEmpty(strings.TrimSpace(candidate.Repo), "-"),
			firstNonEmpty(strings.TrimSpace(candidate.Target), "-"),
			firstNonEmpty(shortDigest(candidate.Digest), "-"),
			firstNonEmpty(strings.TrimSpace(candidate.Reason), "-"),
			candidate.Protected,
			firstNonEmpty(strings.TrimSpace(candidate.SkipReason), "-"),
			formatBytes(candidate.PlannedDeleteBytes),
			firstNonEmpty(strings.TrimSpace(candidate.LastSeenAt), "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatImageCacheSummary(summary map[string]int) string {
	if len(summary) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(summary))
	for key := range summary {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, summary[key]))
	}
	return strings.Join(parts, ",")
}

func firstNonZeroCLIInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func shortDigest(digest string) string {
	digest = strings.TrimSpace(digest)
	if len(digest) <= 19 {
		return digest
	}
	if strings.HasPrefix(digest, "sha256:") {
		return digest[:19]
	}
	return digest[:12]
}

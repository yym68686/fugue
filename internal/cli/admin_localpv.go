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

func (c *CLI) newAdminClusterNodeLocalPVCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "localpv",
		Short: "Inspect Fugue LVM LocalPV inventory and decommission eligibility",
	}
	cmd.AddCommand(
		c.newAdminClusterNodeLocalPVListCommand(),
		c.newAdminClusterNodeLocalPVShowCommand(),
	)
	return cmd
}

func (c *CLI) newAdminClusterNodeLocalPVListCommand() *cobra.Command {
	opts := adminImageCacheNodeFilter{}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List LocalPV inventories for cluster nodes",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			inventories, err := client.ListLocalPVInventories(opts.NodeID, opts.ClusterNodeName, opts.RuntimeID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"inventories": inventories})
			}
			return writeLocalPVInventoryTable(c.stdout, inventories)
		},
	}
	addImageCacheNodeFilterFlags(cmd, &opts)
	return cmd
}

func (c *CLI) newAdminClusterNodeLocalPVShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <node>",
		Short: "Show one node's LocalPV inventory and safety gates",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			inventories, err := client.ListLocalPVInventories("", args[0], "")
			if err != nil {
				return err
			}
			if len(inventories) == 0 {
				return fmt.Errorf("no LocalPV inventory found for node %s", args[0])
			}
			inventory := newestLocalPVInventory(inventories)
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"inventory": inventory})
			}
			return writeLocalPVInventoryDetail(c.stdout, inventory)
		},
	}
}

func writeLocalPVInventoryTable(w io.Writer, inventories []model.LocalPVInventory) error {
	sorted := append([]model.LocalPVInventory(nil), inventories...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].ClusterNodeName != sorted[j].ClusterNodeName {
			return sorted[i].ClusterNodeName < sorted[j].ClusterNodeName
		}
		return sorted[i].ObservedAt.After(sorted[j].ObservedAt)
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NODE\tELIGIBILITY\tVG\tIMAGE\tIMAGE_SIZE\tFREE\tLVS\tACTIVE_LVS\tBOUND_PVS\tOBSERVED\tUNSAFE_REASONS"); err != nil {
		return err
	}
	for _, inventory := range sorted {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\n",
			firstNonEmpty(strings.TrimSpace(inventory.ClusterNodeName), strings.TrimSpace(inventory.NodeID), "-"),
			localPVEligibility(inventory),
			firstNonEmpty(strings.TrimSpace(inventory.VGName), "-"),
			firstNonEmpty(strings.TrimSpace(inventory.ImagePath), "-"),
			formatBytes(inventory.ImageSizeBytes),
			formatBytes(inventory.PVFreeBytes),
			inventory.LVCount,
			inventory.ActiveLVCount,
			inventory.BoundPVCount,
			formatTime(inventory.ObservedAt),
			firstNonEmpty(strings.Join(inventory.UnsafeReasons, ","), "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeLocalPVInventoryDetail(w io.Writer, inventory model.LocalPVInventory) error {
	if err := writeKeyValues(w,
		kvPair{Key: "node", Value: firstNonEmpty(strings.TrimSpace(inventory.ClusterNodeName), strings.TrimSpace(inventory.NodeID), "-")},
		kvPair{Key: "eligibility", Value: localPVEligibility(inventory)},
		kvPair{Key: "vg_name", Value: firstNonEmpty(strings.TrimSpace(inventory.VGName), "-")},
		kvPair{Key: "image_path", Value: firstNonEmpty(strings.TrimSpace(inventory.ImagePath), "-")},
		kvPair{Key: "image_size", Value: formatBytes(inventory.ImageSizeBytes)},
		kvPair{Key: "loop_device", Value: firstNonEmpty(strings.TrimSpace(inventory.LoopDevice), "-")},
		kvPair{Key: "loop_backing_file", Value: firstNonEmpty(strings.TrimSpace(inventory.LoopBackingFile), "-")},
		kvPair{Key: "pv_size", Value: formatBytes(inventory.PVSizeBytes)},
		kvPair{Key: "pv_free", Value: formatBytes(inventory.PVFreeBytes)},
		kvPair{Key: "lv_count", Value: formatInt(inventory.LVCount)},
		kvPair{Key: "active_lv_count", Value: formatInt(inventory.ActiveLVCount)},
		kvPair{Key: "bound_pv_count", Value: formatInt(inventory.BoundPVCount)},
		kvPair{Key: "observed_at", Value: formatTime(inventory.ObservedAt)},
		kvPair{Key: "unsafe_reasons", Value: firstNonEmpty(strings.Join(inventory.UnsafeReasons, ","), "-")},
	); err != nil {
		return err
	}
	if len(inventory.LVNames) > 0 {
		if _, err := fmt.Fprintln(w, "\n[lv_names]"); err != nil {
			return err
		}
		for _, name := range inventory.LVNames {
			if _, err := fmt.Fprintf(w, "- %s\n", name); err != nil {
				return err
			}
		}
	}
	if len(inventory.BoundPVCRefs) > 0 {
		if _, err := fmt.Fprintln(w, "\n[bound_pvcs]"); err != nil {
			return err
		}
		for _, ref := range inventory.BoundPVCRefs {
			if _, err := fmt.Fprintf(w, "- %s\n", ref); err != nil {
				return err
			}
		}
	}
	return nil
}

func newestLocalPVInventory(inventories []model.LocalPVInventory) model.LocalPVInventory {
	out := inventories[0]
	for _, inventory := range inventories[1:] {
		if inventory.ObservedAt.After(out.ObservedAt) {
			out = inventory
		}
	}
	return out
}

func localPVEligibility(inventory model.LocalPVInventory) string {
	if inventory.SafeToDecommission {
		return "eligible"
	}
	return "not eligible"
}

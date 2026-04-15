package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminClusterNodeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Inspect one node with host-level disk, journal, and metrics diagnostics",
	}
	cmd.AddCommand(
		c.newAdminClusterNodeInspectCommand(),
		c.newAdminClusterNodeDiskCommand(),
		c.newAdminClusterNodeJournalCommand(),
		c.newAdminClusterNodeMetricsCommand(),
	)
	return cmd
}

func (c *CLI) newAdminClusterNodeInspectCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect <node>",
		Short: "Collect disk, kubelet journal, metrics, and related event evidence for one node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			diagnosis, err := client.GetClusterNodeDiagnosis(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"diagnosis": diagnosis})
			}
			return renderClusterNodeDiagnosis(c.stdout, diagnosis)
		},
	}
}

func (c *CLI) newAdminClusterNodeDiskCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "disk <node>",
		Short: "Show node filesystem totals and the largest host paths without SSH",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			diagnosis, err := client.GetClusterNodeDiagnosis(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"diagnosis": diagnosis})
			}
			if err := writeClusterNodeDiagnosisHeader(c.stdout, diagnosis); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return renderClusterNodeDiskDiagnosis(c.stdout, diagnosis)
		},
	}
}

func (c *CLI) newAdminClusterNodeJournalCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "journal <node>",
		Short: "Show recent kubelet eviction and metrics evidence from the host journal",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			diagnosis, err := client.GetClusterNodeDiagnosis(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"diagnosis": diagnosis})
			}
			if err := writeClusterNodeDiagnosisHeader(c.stdout, diagnosis); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return renderClusterNodeJournalDiagnosis(c.stdout, diagnosis)
		},
	}
}

func (c *CLI) newAdminClusterNodeMetricsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "metrics <node>",
		Short: "Explain why node CPU, memory, or storage metrics are present or missing",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			diagnosis, err := client.GetClusterNodeDiagnosis(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"diagnosis": diagnosis})
			}
			if err := writeClusterNodeDiagnosisHeader(c.stdout, diagnosis); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return renderClusterNodeMetricsDiagnosis(c.stdout, diagnosis)
		},
	}
}

func renderClusterNodeDiagnosis(w io.Writer, diagnosis clusterNodeDiagnosis) error {
	if err := writeClusterNodeDiagnosisHeader(w, diagnosis); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "disk"); err != nil {
		return err
	}
	if err := renderClusterNodeDiskDiagnosis(w, diagnosis); err != nil {
		return err
	}
	if diagnosis.Metrics != nil {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "metrics"); err != nil {
			return err
		}
		if err := renderClusterNodeMetricsDiagnosis(w, diagnosis); err != nil {
			return err
		}
	}
	if len(diagnosis.Journal) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "journal"); err != nil {
			return err
		}
		if err := renderClusterNodeJournalDiagnosis(w, diagnosis); err != nil {
			return err
		}
	}
	if len(diagnosis.Events) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "events"); err != nil {
			return err
		}
		return writeClusterEventTable(w, diagnosis.Events)
	}
	return nil
}

func writeClusterNodeDiagnosisHeader(w io.Writer, diagnosis clusterNodeDiagnosis) error {
	nodeName := ""
	nodeStatus := ""
	nodeRegion := ""
	nodeRuntime := ""
	if diagnosis.Node != nil {
		nodeName = strings.TrimSpace(diagnosis.Node.Name)
		nodeStatus = strings.TrimSpace(diagnosis.Node.Status)
		nodeRegion = strings.TrimSpace(diagnosis.Node.Region)
		nodeRuntime = strings.TrimSpace(diagnosis.Node.RuntimeID)
	}
	pairs := []kvPair{
		{Key: "node", Value: nodeName},
		{Key: "status", Value: nodeStatus},
		{Key: "region", Value: nodeRegion},
		{Key: "runtime_id", Value: nodeRuntime},
		{Key: "summary", Value: strings.TrimSpace(diagnosis.Summary)},
		{Key: "janitor_namespace", Value: strings.TrimSpace(diagnosis.JanitorNamespace)},
		{Key: "janitor_pod", Value: strings.TrimSpace(diagnosis.JanitorPod)},
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	for _, warning := range diagnosis.Warnings {
		if _, err := fmt.Fprintf(w, "warning=%s\n", warning); err != nil {
			return err
		}
	}
	return nil
}

func renderClusterNodeDiskDiagnosis(w io.Writer, diagnosis clusterNodeDiagnosis) error {
	if len(diagnosis.Filesystems) > 0 {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(tw, "MOUNT\tFILESYSTEM\tUSED\tTOTAL\tAVAILABLE\tUSE%"); err != nil {
			return err
		}
		for _, item := range diagnosis.Filesystems {
			if _, err := fmt.Fprintf(
				tw,
				"%s\t%s\t%s\t%s\t%s\t%s\n",
				item.MountPath,
				item.Filesystem,
				formatOptionalBytes(item.UsedBytes),
				formatOptionalBytes(item.SizeBytes),
				formatOptionalBytes(item.AvailableBytes),
				formatOptionalPercent(item.UsedPercent),
			); err != nil {
				return err
			}
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	if len(diagnosis.HotPaths) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PATH\tSIZE"); err != nil {
		return err
	}
	for _, item := range diagnosis.HotPaths {
		if _, err := fmt.Fprintf(tw, "%s\t%s\n", item.Path, formatBytes(item.Bytes)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func renderClusterNodeJournalDiagnosis(w io.Writer, diagnosis clusterNodeDiagnosis) error {
	if len(diagnosis.Journal) == 0 {
		return nil
	}
	for _, entry := range diagnosis.Journal {
		prefix := ""
		if entry.Timestamp != nil && !entry.Timestamp.IsZero() {
			prefix = formatTime(entry.Timestamp.UTC()) + " "
		}
		if _, err := fmt.Fprintln(w, strings.TrimSpace(prefix+entry.Message)); err != nil {
			return err
		}
	}
	return nil
}

func renderClusterNodeMetricsDiagnosis(w io.Writer, diagnosis clusterNodeDiagnosis) error {
	if diagnosis.Metrics == nil {
		return nil
	}
	if err := writeKeyValues(w,
		kvPair{Key: "status", Value: strings.TrimSpace(diagnosis.Metrics.Status)},
		kvPair{Key: "summary", Value: strings.TrimSpace(diagnosis.Metrics.Summary)},
	); err != nil {
		return err
	}
	evidence := append([]string(nil), diagnosis.Metrics.Evidence...)
	sort.Strings(evidence)
	for _, item := range evidence {
		if _, err := fmt.Fprintf(w, "evidence=%s\n", item); err != nil {
			return err
		}
	}
	for _, warning := range diagnosis.Metrics.Warnings {
		if _, err := fmt.Fprintf(w, "warning=%s\n", warning); err != nil {
			return err
		}
	}
	return nil
}

func formatOptionalBytes(value *int64) string {
	if value == nil {
		return ""
	}
	return formatBytes(*value)
}

func formatOptionalPercent(value *float64) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%.0f%%", *value)
}

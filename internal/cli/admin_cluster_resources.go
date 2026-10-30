package cli

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

const clusterResourceSortFields = "cpu-request, cpu-usage, cpu-recommended, cpu-capacity, memory-request, memory-usage, memory-recommended, name, namespace, pods"

func (c *CLI) newAdminClusterResourcesCommand() *cobra.Command {
	var node, field, order string
	var limit, window, minimum int
	var memory bool
	cmd := &cobra.Command{
		Use:     "resources --node <node>",
		Short:   "Rank node services by resource requests, live CPU/memory and recommended requests (admin)",
		Long:    "Report all nonterminal pods on one node, grouped by managed app, database or Kubernetes controller. Includes candidate replicas, sidecars, init-container peaks and overhead. CPU-REC is the total recommended request; CPU-CAP is the separate capacity estimate. Unknown values are shown as '-'. This command does not change resources.",
		Example: "  fugue admin cluster resources --node worker-a\n  fugue admin cluster resources --node worker-a --limit 0 --memory\n  fugue admin cluster resources --node worker-a --sort memory-usage --order desc --limit 50 --json",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(node) == "" {
				return fmt.Errorf("--node is required")
			}
			if limit < 0 {
				return fmt.Errorf("--limit must be zero (all services) or positive")
			}
			if window < 1 || window > 168 || minimum < 1 {
				return fmt.Errorf("--window-hours must be 1..168 and --min-samples must be positive")
			}
			if err := sortClusterServiceResources(nil, field, order); err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			query := url.Values{"node": {strings.TrimSpace(node)}, "window_hours": {strconv.Itoa(window)}, "min_samples": {strconv.Itoa(minimum)}}
			var report model.ClusterResourceReport
			if err := client.doJSON(http.MethodGet, "/v1/cluster/resources?"+query.Encode(), nil, &report); err != nil {
				return err
			}
			if err := sortClusterServiceResources(report.Services, field, order); err != nil {
				return err
			}
			if limit > 0 && len(report.Services) > limit {
				report.Services = report.Services[:limit]
			}
			if c.wantsJSON() {
				return c.writeJSON(report)
			}
			return writeClusterResourceTable(c.stdout, report, memory || strings.HasPrefix(field, "memory-"))
		},
	}
	cmd.Flags().StringVar(&node, "node", "", "Exact Kubernetes node name (required)")
	cmd.Flags().IntVar(&limit, "limit", 30, "Maximum services to display; 0 shows all")
	cmd.Flags().StringVar(&field, "sort", "cpu-request", "Sort field: "+clusterResourceSortFields)
	cmd.Flags().StringVar(&order, "order", "desc", "Sort order: asc or desc; unknown values always last")
	cmd.Flags().BoolVar(&memory, "memory", false, "Include memory request, actual working set and recommendation columns (MiB)")
	cmd.Flags().IntVar(&window, "window-hours", 168, "Right-sizing history window in hours (1..168)")
	cmd.Flags().IntVar(&minimum, "min-samples", 12, "Minimum historical samples for recommendations")
	return cmd
}

func sortClusterServiceResources(rows []model.ClusterServiceResources, field, order string) error {
	valid := false
	for _, candidate := range strings.Split(clusterResourceSortFields, ", ") {
		if field == candidate {
			valid = true
		}
	}
	if !valid {
		return fmt.Errorf("--sort must be one of: %s", clusterResourceSortFields)
	}
	if order != "asc" && order != "desc" {
		return fmt.Errorf("--order must be asc or desc")
	}
	value := func(row model.ClusterServiceResources) (float64, bool) {
		var pointer *int64
		switch field {
		case "cpu-request":
			return float64(row.CPURequestMilliCores), true
		case "cpu-usage":
			if row.CPUUsageMilliCores != nil {
				return *row.CPUUsageMilliCores, true
			}
			return 0, false
		case "cpu-recommended":
			pointer = row.CPURecommendedMilliCores
		case "cpu-capacity":
			pointer = row.CPUCapacityMilliCores
		case "memory-request":
			return float64(row.MemoryRequestBytes), true
		case "memory-usage":
			pointer = row.MemoryUsageBytes
		case "memory-recommended":
			pointer = row.MemoryRecommendedBytes
		case "pods":
			return float64(row.PodCount), true
		}
		if pointer == nil {
			return 0, false
		}
		return float64(*pointer), true
	}
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if field == "name" || field == "namespace" {
			left, right := a.Name, b.Name
			if field == "namespace" {
				left, right = a.Namespace, b.Namespace
			}
			if left != right {
				if order == "asc" {
					return left < right
				}
				return left > right
			}
		} else {
			left, knownLeft := value(a)
			right, knownRight := value(b)
			if knownLeft != knownRight {
				return knownLeft
			}
			if knownLeft && left != right {
				if order == "asc" {
					return left < right
				}
				return left > right
			}
		}
		return a.Namespace+"\x00"+a.Name+"\x00"+a.Kind+"\x00"+a.ID < b.Namespace+"\x00"+b.Name+"\x00"+b.Kind+"\x00"+b.ID
	})
	return nil
}

func writeClusterResourceTable(out io.Writer, report model.ClusterResourceReport, memory bool) error {
	if _, err := fmt.Fprintf(out, "Node: %s  Services: %d/%d  Pods: %d  CPU request: %dm  Memory request: %.1f MiB\n", report.Node, len(report.Services), report.TotalServices, report.TotalPods, report.CPURequestMilliCores, float64(report.MemoryRequestBytes)/1024/1024); err != nil {
		return err
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	header := "RANK\tNAMESPACE\tKIND\tSERVICE\tPODS\tCPU-REQ(m)\tCPU-USED(m)\tCPU-REC(m)\tCPU-CAP(m)"
	if memory {
		header += "\tMEM-REQ(MiB)\tMEM-USED(MiB)\tMEM-REC(MiB)"
	}
	fmt.Fprintln(w, header+"\tMETRICS\tRECOMMENDATION")
	integer := func(value *int64, divisor float64) string {
		if value == nil {
			return "-"
		}
		if divisor == 1 {
			return strconv.FormatInt(*value, 10)
		}
		return fmt.Sprintf("%.1f", float64(*value)/divisor)
	}
	for i, row := range report.Services {
		usage := "-"
		if row.CPUUsageMilliCores != nil {
			usage = fmt.Sprintf("%.2f", *row.CPUUsageMilliCores)
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s", i+1, row.Namespace, row.Kind, row.Name, row.PodCount, row.CPURequestMilliCores, usage, integer(row.CPURecommendedMilliCores, 1), integer(row.CPUCapacityMilliCores, 1))
		if memory {
			fmt.Fprintf(w, "\t%.1f\t%s\t%s", float64(row.MemoryRequestBytes)/1024/1024, integer(row.MemoryUsageBytes, 1024*1024), integer(row.MemoryRecommendedBytes, 1024*1024))
		}
		fmt.Fprintf(w, "\t%d/%d\t%s\n", row.MeasuredPods, row.PodCount, row.RecommendationReason)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(out, "CPU-REC = request target; CPU-CAP = capacity estimate. Totals include all node services; recommendations retain non-main-container reservations. History: %dh, minimum samples: %d.\n", report.WindowHours, report.MinSamples)
	for _, warning := range report.Warnings {
		if _, err := fmt.Fprintln(out, "Warning: "+warning); err != nil {
			return err
		}
	}
	return nil
}

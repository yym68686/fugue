package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newProjectOverviewCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "overview [project]",
		Aliases: []string{"status"},
		Short:   "Show project live overview",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			if len(args) == 0 {
				gallery, err := client.GetConsoleGallery()
				if err != nil {
					return err
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, gallery)
				}
				return writeConsoleProjectTable(c.stdout, gallery.Projects)
			}

			summary, detail, err := c.loadConsoleProjectOverview(client, args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				payload := map[string]any{"project": detail}
				if summary != nil {
					payload["summary"] = summary
				}
				return writeJSON(c.stdout, payload)
			}
			return renderConsoleProjectOverview(c.stdout, summary, detail)
		},
	}
}

func (c *CLI) newProjectAppsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "apps <project>",
		Short: "List apps in a project with live context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			_, detail, err := c.loadConsoleProjectOverview(client, args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"apps": detail.Apps})
			}
			return writeAppTable(c.stdout, detail.Apps)
		},
	}
}

func (c *CLI) newProjectOpsCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ops <project>",
		Aliases: []string{"operations"},
		Short:   "List project operations with app context",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			_, detail, err := c.loadConsoleProjectOverview(client, args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"operations": detail.Operations})
			}
			return writeOperationTableWithApps(c.stdout, detail.Operations, mapAppNames(detail.Apps))
		},
	}
}

func (c *CLI) newProjectWatchCommand() *cobra.Command {
	opts := struct {
		Interval time.Duration
	}{Interval: 5 * time.Second}
	cmd := &cobra.Command{
		Use:   "watch [project]",
		Short: "Watch project overview changes",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			var previousHash [32]byte
			first := true
			for {
				snapshot, hashValue, err := c.loadProjectWatchSnapshot(ctx, client, args)
				if err != nil {
					return err
				}
				if first || hashValue != previousHash {
					if !first {
						if _, err := fmt.Fprintln(c.stdout); err != nil {
							return err
						}
					}
					previousHash = hashValue
					if c.wantsJSON() {
						if err := writeJSON(c.stdout, snapshot); err != nil {
							return err
						}
					} else {
						if _, err := fmt.Fprintf(c.stdout, "observed_at=%s\n", formatTime(time.Now().UTC())); err != nil {
							return err
						}
						switch value := snapshot.(type) {
						case consoleGalleryResponse:
							if err := writeConsoleProjectTable(c.stdout, value.Projects); err != nil {
								return err
							}
						case projectOverviewSnapshot:
							if err := renderConsoleProjectOverview(c.stdout, value.Summary, value.Detail); err != nil {
								return err
							}
						}
					}
					first = false
				}
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(opts.Interval):
				}
			}
		},
	}
	cmd.Flags().DurationVar(&opts.Interval, "interval", opts.Interval, "Polling interval")
	return cmd
}

type projectOverviewSnapshot struct {
	Summary *consoleProjectSummary       `json:"summary,omitempty"`
	Detail  consoleProjectDetailResponse `json:"detail"`
}

func (c *CLI) loadProjectWatchSnapshot(ctx context.Context, client *Client, args []string) (any, [32]byte, error) {
	if len(args) == 0 {
		gallery, err := client.GetConsoleGallery()
		if err != nil {
			return nil, [32]byte{}, err
		}
		sum, err := json.Marshal(gallery)
		if err != nil {
			return nil, [32]byte{}, err
		}
		return gallery, sha256.Sum256(sum), nil
	}
	summary, detail, err := c.loadConsoleProjectOverview(client, args[0])
	if err != nil {
		return nil, [32]byte{}, err
	}
	snapshot := projectOverviewSnapshot{Summary: summary, Detail: detail}
	sum, err := json.Marshal(snapshot)
	if err != nil {
		return nil, [32]byte{}, err
	}
	select {
	case <-ctx.Done():
		return nil, [32]byte{}, ctx.Err()
	default:
	}
	return snapshot, sha256.Sum256(sum), nil
}

func (c *CLI) loadConsoleProjectOverview(client *Client, ref string) (*consoleProjectSummary, consoleProjectDetailResponse, error) {
	project, err := c.resolveNamedProject(client, ref)
	if err != nil {
		return nil, consoleProjectDetailResponse{}, err
	}
	detail, err := client.GetConsoleProject(project.ID)
	if err != nil {
		return nil, consoleProjectDetailResponse{}, err
	}
	gallery, err := client.GetConsoleGallery()
	if err != nil {
		return nil, detail, nil
	}
	for _, summary := range gallery.Projects {
		if strings.EqualFold(strings.TrimSpace(summary.ID), strings.TrimSpace(project.ID)) {
			summaryCopy := summary
			return &summaryCopy, detail, nil
		}
	}
	return nil, detail, nil
}

func writeConsoleProjectTable(w io.Writer, projects []consoleProjectSummary) error {
	sorted := append([]consoleProjectSummary(nil), projects...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Name, sorted[j].Name) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PROJECT\tLIFECYCLE\tSYNC\tAPPS\tSERVICES\tUSAGE\tBADGES"); err != nil {
		return err
	}
	for _, project := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%d\t%d\t%s\t%s\n",
			project.Name,
			project.Lifecycle.Label,
			project.Lifecycle.SyncMode,
			project.AppCount,
			project.ServiceCount,
			formatResourceUsageSummary(&project.ResourceUsageSnapshot),
			formatConsoleBadges(project.ServiceBadges),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func renderConsoleProjectOverview(w io.Writer, summary *consoleProjectSummary, detail consoleProjectDetailResponse) error {
	projectName := strings.TrimSpace(detail.ProjectName)
	if projectName == "" && detail.Project != nil {
		projectName = strings.TrimSpace(detail.Project.Name)
	}
	if projectName == "" {
		projectName = strings.TrimSpace(detail.ProjectID)
	}
	pairs := []kvPair{
		{Key: "project", Value: projectName},
		{Key: "project_id", Value: detail.ProjectID},
	}
	if summary != nil {
		pairs = append(pairs,
			kvPair{Key: "lifecycle", Value: summary.Lifecycle.Label},
			kvPair{Key: "sync_mode", Value: summary.Lifecycle.SyncMode},
			kvPair{Key: "app_count", Value: fmt.Sprintf("%d", summary.AppCount)},
			kvPair{Key: "service_count", Value: fmt.Sprintf("%d", summary.ServiceCount)},
			kvPair{Key: "resource_usage", Value: formatResourceUsageSummary(&summary.ResourceUsageSnapshot)},
			kvPair{Key: "badges", Value: formatConsoleBadges(summary.ServiceBadges)},
		)
	} else {
		pairs = append(pairs,
			kvPair{Key: "app_count", Value: fmt.Sprintf("%d", len(detail.Apps))},
			kvPair{Key: "operation_count", Value: fmt.Sprintf("%d", len(detail.Operations))},
			kvPair{Key: "cluster_nodes", Value: fmt.Sprintf("%d", len(detail.ClusterNodes))},
		)
	}
	if detail.Project != nil && strings.TrimSpace(detail.Project.Description) != "" {
		pairs = append(pairs, kvPair{Key: "description", Value: detail.Project.Description})
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	if len(detail.Apps) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[apps]"); err != nil {
			return err
		}
		if err := writeAppTable(w, detail.Apps); err != nil {
			return err
		}
	}
	if len(detail.Operations) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[operations]"); err != nil {
			return err
		}
		if err := writeOperationTableWithApps(w, detail.Operations, mapAppNames(detail.Apps)); err != nil {
			return err
		}
	}
	if len(detail.ClusterNodes) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[cluster_nodes]"); err != nil {
			return err
		}
		if err := writeClusterNodeTable(w, detail.ClusterNodes); err != nil {
			return err
		}
	}
	return nil
}

func writeOperationTableWithApps(w io.Writer, operations []model.Operation, appNames map[string]string) error {
	sorted := append([]model.Operation(nil), operations...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "OPERATION\tSTATUS\tTYPE\tAPP\tTARGET\tUPDATED"); err != nil {
		return err
	}
	for _, op := range sorted {
		appLabel := strings.TrimSpace(appNames[op.AppID])
		if appLabel == "" {
			appLabel = strings.TrimSpace(op.AppID)
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			op.ID,
			op.Status,
			op.Type,
			appLabel,
			firstNonEmpty(op.TargetRuntimeID, op.AssignedRuntimeID),
			formatTime(op.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func mapAppNames(apps []model.App) map[string]string {
	out := make(map[string]string, len(apps))
	for _, app := range apps {
		if strings.TrimSpace(app.ID) == "" {
			continue
		}
		out[app.ID] = app.Name
	}
	return out
}

func formatConsoleBadges(badges []consoleProjectBadge) string {
	if len(badges) == 0 {
		return ""
	}
	parts := make([]string, 0, len(badges))
	for _, badge := range badges {
		label := strings.TrimSpace(badge.Label)
		if label == "" {
			continue
		}
		if meta := strings.TrimSpace(badge.Meta); meta != "" {
			parts = append(parts, label+" ("+meta+")")
			continue
		}
		parts = append(parts, label)
	}
	return strings.Join(parts, ", ")
}

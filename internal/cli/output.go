package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"
)

type kvPair struct {
	Key   string
	Value string
}

func writeJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func writeKeyValues(w io.Writer, pairs ...kvPair) error {
	for _, pair := range pairs {
		if strings.TrimSpace(pair.Key) == "" {
			continue
		}
		if _, err := fmt.Fprintf(w, "%s=%s\n", pair.Key, pair.Value); err != nil {
			return err
		}
	}
	return nil
}

func writeStringMap(w io.Writer, values map[string]string) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := fmt.Fprintf(w, "%s=%s\n", key, values[key]); err != nil {
			return err
		}
	}
	return nil
}

func writeAppTable(w io.Writer, apps []model.App) error {
	return writeAppTableWithRuntimeNames(w, apps, nil, false)
}

func writeAppTableWithRuntimeNames(w io.Writer, apps []model.App, runtimeNames map[string]string, showIDs bool) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "APP\tSTATUS\tREPLICAS\tRUNTIME\tUSAGE\tURL\tDETAIL"); err != nil {
		return err
	}
	for _, app := range apps {
		runtimeID := strings.TrimSpace(app.Status.CurrentRuntimeID)
		if runtimeID == "" {
			runtimeID = strings.TrimSpace(app.Spec.RuntimeID)
		}
		url := ""
		if app.Route != nil {
			url = strings.TrimSpace(app.Route.PublicURL)
		}
		runtimeName := firstNonEmptyTrimmed(runtimeNames[runtimeID], runtimeID)
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
			formatDisplayName(app.Name, app.ID, showIDs),
			strings.TrimSpace(app.Status.Phase),
			maxInt(app.Status.CurrentReplicas, app.Spec.Replicas),
			formatDisplayName(runtimeName, runtimeID, showIDs),
			formatResourceUsageSummary(app.CurrentResourceUsage),
			url,
			formatAppListDetail(app.Status.LastMessage),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatAppListDetail(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return ""
	}
	const maxLen = 96
	if len(message) <= maxLen {
		return message
	}
	return strings.TrimSpace(message[:maxLen-3]) + "..."
}

func writeDomainTable(w io.Writer, domains []model.AppDomain) error {
	sorted := append([]model.AppDomain(nil), domains...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Hostname < sorted[j].Hostname
	})

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "HOSTNAME\tSTATUS\tTLS\tTARGET\tUPDATED"); err != nil {
		return err
	}
	for _, domain := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			domain.Hostname,
			strings.TrimSpace(domain.Status),
			strings.TrimSpace(domain.TLSStatus),
			strings.TrimSpace(domain.RouteTarget),
			formatTime(domain.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeAppFileTable(w io.Writer, files []model.AppFile) error {
	sorted := append([]model.AppFile(nil), files...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Path < sorted[j].Path
	})

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PATH\tSECRET\tMODE\tBYTES"); err != nil {
		return err
	}
	for _, appFile := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%t\t%s\t%d\n",
			appFile.Path,
			appFile.Secret,
			formatFileMode(appFile.Mode),
			len(appFile.Content),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeWorkspaceTree(w io.Writer, tree appFilesystemTreeResponse) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tTYPE\tSIZE\tMODE\tMODIFIED\tPATH"); err != nil {
		return err
	}
	for _, entry := range tree.Entries {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%d\t%s\t%s\t%s\n",
			entry.Name,
			entry.Kind,
			entry.Size,
			formatFileMode(entry.Mode),
			formatTime(entry.ModifiedAt),
			entry.Path,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeMultiAppSummary(w io.Writer, apps []model.App) error {
	sorted := append([]model.App(nil), apps...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Name, sorted[j].Name) < 0
	})
	return writeAppTable(w, sorted)
}

func writeAppStatus(w io.Writer, app model.App) error {
	return writeAppStatusWithContext(w, app, nil, nil, nil, false)
}

func writeAppStatusWithContext(w io.Writer, app model.App, tenantNames, projectNames, runtimeNames map[string]string, showIDs bool) error {
	url := ""
	if app.Route != nil {
		url = strings.TrimSpace(app.Route.PublicURL)
	}
	runtimeID := strings.TrimSpace(app.Status.CurrentRuntimeID)
	if runtimeID == "" {
		runtimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	sourceType := ""
	if app.Source != nil {
		sourceType = strings.TrimSpace(app.Source.Type)
	}
	failoverTarget := ""
	if app.Spec.Failover != nil {
		failoverTarget = strings.TrimSpace(app.Spec.Failover.TargetRuntimeID)
	}
	workspaceRoot := ""
	if app.Spec.Workspace != nil {
		workspaceRoot = strings.TrimSpace(app.Spec.Workspace.MountPath)
	}
	postgresRuntime := ""
	if app.Spec.Postgres != nil {
		postgresRuntime = strings.TrimSpace(app.Spec.Postgres.RuntimeID)
	}
	tenantName := firstNonEmptyTrimmed(tenantNames[app.TenantID], app.TenantID)
	projectName := firstNonEmptyTrimmed(projectNames[app.ProjectID], app.ProjectID)
	runtimeName := firstNonEmptyTrimmed(runtimeNames[runtimeID], runtimeID)
	failoverTargetName := firstNonEmptyTrimmed(runtimeNames[failoverTarget], failoverTarget)
	postgresRuntimeName := firstNonEmptyTrimmed(runtimeNames[postgresRuntime], postgresRuntime)
	pairs := []kvPair{
		kvPair{Key: "app", Value: formatDisplayName(app.Name, app.ID, showIDs)},
		kvPair{Key: "tenant", Value: formatDisplayName(tenantName, app.TenantID, showIDs)},
		kvPair{Key: "project", Value: formatDisplayName(projectName, app.ProjectID, showIDs)},
		kvPair{Key: "phase", Value: strings.TrimSpace(app.Status.Phase)},
		kvPair{Key: "desired_replicas", Value: fmt.Sprintf("%d", app.Spec.Replicas)},
		kvPair{Key: "current_replicas", Value: fmt.Sprintf("%d", app.Status.CurrentReplicas)},
		kvPair{Key: "runtime", Value: formatDisplayName(runtimeName, runtimeID, showIDs)},
		kvPair{Key: "source", Value: sourceType},
		kvPair{Key: "source_ref", Value: sourceRef(app.Source)},
		kvPair{Key: "failover_target_runtime", Value: formatDisplayName(failoverTargetName, failoverTarget, showIDs)},
		kvPair{Key: "workspace_root", Value: workspaceRoot},
		kvPair{Key: "postgres_runtime", Value: formatDisplayName(postgresRuntimeName, postgresRuntime, showIDs)},
		kvPair{Key: "current_resource_usage", Value: formatResourceUsageSummary(app.CurrentResourceUsage)},
		kvPair{Key: "image_mirror_limit", Value: formatImageMirrorLimit(app.Spec.ImageMirrorLimit)},
		kvPair{Key: "bindings", Value: fmt.Sprintf("%d", len(app.Bindings))},
		kvPair{Key: "last_operation_id", Value: app.Status.LastOperationID},
		kvPair{Key: "last_message", Value: app.Status.LastMessage},
		kvPair{Key: "url", Value: url},
		kvPair{Key: "release_started_at", Value: formatModeTime(app.Status.CurrentReleaseStartedAt)},
		kvPair{Key: "release_ready_at", Value: formatModeTime(app.Status.CurrentReleaseReadyAt)},
		kvPair{Key: "updated_at", Value: formatTime(app.UpdatedAt)},
	}
	if app.Route != nil {
		pairs = append(pairs,
			kvPair{Key: "route_hostname", Value: strings.TrimSpace(app.Route.Hostname)},
			kvPair{Key: "route_base_domain", Value: strings.TrimSpace(app.Route.BaseDomain)},
		)
	}
	if app.Source != nil {
		pairs = append(pairs,
			kvPair{Key: "repo_branch", Value: strings.TrimSpace(app.Source.RepoBranch)},
			kvPair{Key: "commit_sha", Value: strings.TrimSpace(app.Source.CommitSHA)},
			kvPair{Key: "commit_committed_at", Value: strings.TrimSpace(app.Source.CommitCommittedAt)},
			kvPair{Key: "image_ref", Value: strings.TrimSpace(app.Source.ImageRef)},
			kvPair{Key: "resolved_image_ref", Value: strings.TrimSpace(app.Source.ResolvedImageRef)},
			kvPair{Key: "build_strategy", Value: strings.TrimSpace(app.Source.BuildStrategy)},
			kvPair{Key: "source_dir", Value: strings.TrimSpace(app.Source.SourceDir)},
			kvPair{Key: "dockerfile_path", Value: strings.TrimSpace(app.Source.DockerfilePath)},
			kvPair{Key: "build_context_dir", Value: strings.TrimSpace(app.Source.BuildContextDir)},
			kvPair{Key: "compose_service", Value: strings.TrimSpace(app.Source.ComposeService)},
			kvPair{Key: "detected_provider", Value: strings.TrimSpace(app.Source.DetectedProvider)},
			kvPair{Key: "detected_stack", Value: strings.TrimSpace(app.Source.DetectedStack)},
		)
	}
	return writeKeyValues(w, pairs...)
}

func sourceRef(source *model.AppSource) string {
	if source == nil {
		return ""
	}
	switch {
	case strings.TrimSpace(source.RepoURL) != "":
		return source.RepoURL
	case strings.TrimSpace(source.ImageRef) != "":
		return source.ImageRef
	case strings.TrimSpace(source.ResolvedImageRef) != "":
		return source.ResolvedImageRef
	default:
		return ""
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func formatModeTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return formatTime(*t)
}

func formatFileMode(mode int32) string {
	if mode <= 0 {
		return ""
	}
	return strconv.FormatInt(int64(mode), 8)
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

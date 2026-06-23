package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"

	corev1 "k8s.io/api/core/v1"
)

const (
	clusterNodeJanitorSelector        = "app.kubernetes.io/component=node-janitor"
	clusterNodeJanitorContainer       = "node-janitor"
	clusterNodeDiagnosisEventLimit    = 20
	clusterNodeDiagnosisHotPathLimit  = 20
	clusterNodeDiagnosisJournalLimit  = 80
	clusterNodeDiagnosisCommandTimout = 20 * time.Second
)

const clusterNodeDiagnosisFilesystemScript = `
set -euo pipefail
chroot /host /bin/sh -lc '
for path in / /var/lib /var/log /var/lib/fugue /var/lib/containerd /var/lib/rancher /var/log/pods; do
  [ -e "$path" ] || continue
  df -P -B1 "$path" 2>/dev/null | awk "NR>1 {printf \"%s\t%s\t%s\t%s\t%s\t%s\n\", \$1, \$2, \$3, \$4, \$5, \$6}"
done' | awk '!seen[$0]++'
`

const clusterNodeDiagnosisHotPathsScript = `
set -euo pipefail
chroot /host /bin/sh -lc '
for base in /var/lib /var/log /var/lib/fugue /var/lib/containerd /var/lib/rancher /var/log/pods; do
  [ -d "$base" ] || continue
  du -x -B1 -d1 "$base" 2>/dev/null
done' | sort -rn -k1,1 | awk '!seen[$2]++' | head -n 20
`

const clusterNodeDiagnosisJournalScript = `
set -euo pipefail
if chroot /host /bin/sh -lc 'command -v journalctl >/dev/null 2>&1'; then
  chroot /host /bin/sh -lc 'journalctl -u k3s -u k3s-agent --no-pager -n 400 -o short-iso 2>/dev/null | grep -Ei "eviction|disk pressure|ephemeral-storage|imagefs|nodefs|stats/summary|metrics-server|summary" | tail -n 120' || true
fi
`

const clusterNodeControlPlaneIncidentScript = `
set -euo pipefail
chroot /host /bin/sh -lc '
base=/var/log/fugue/incidents
[ -d "$base" ] || exit 0
latest=""
if [ -e "$base/latest-k3s" ]; then
  latest="$base/latest-k3s"
elif command -v find >/dev/null 2>&1; then
  latest="$(find "$base" -maxdepth 1 -type d -name "k3s-*" -printf "%T@ %p\n" 2>/dev/null | sort -nr | awk "NR == 1 {print \$2}")"
fi
[ -n "$latest" ] && [ -d "$latest" ] || exit 0
if command -v readlink >/dev/null 2>&1; then
  resolved="$(readlink -f "$latest" 2>/dev/null || true)"
  [ -n "$resolved" ] && latest="$resolved"
fi
name="$(basename "$latest")"
archive="$base/${name}.tar.gz"
printf "__FUGUE_FIELD__\tname\t%s\n" "$name"
printf "__FUGUE_FIELD__\tpath\t%s\n" "$latest"
[ -f "$archive" ] && printf "__FUGUE_FIELD__\tarchive_path\t%s\n" "$archive"
if [ -f "$latest/diagnosis.txt" ]; then
  printf "__FUGUE_DIAGNOSIS_BEGIN__\n"
  sed "s/\r$//" "$latest/diagnosis.txt" | head -200
fi
'
`

type clusterNodeFilesystemUsage struct {
	Filesystem     string   `json:"filesystem,omitempty"`
	MountPath      string   `json:"mount_path"`
	SizeBytes      *int64   `json:"size_bytes,omitempty"`
	UsedBytes      *int64   `json:"used_bytes,omitempty"`
	AvailableBytes *int64   `json:"available_bytes,omitempty"`
	UsedPercent    *float64 `json:"used_percent,omitempty"`
}

type clusterNodePathUsage struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
}

type clusterNodeJournalEntry struct {
	Timestamp *time.Time `json:"timestamp,omitempty"`
	Unit      string     `json:"unit,omitempty"`
	Message   string     `json:"message"`
}

type clusterNodeMetricsDiagnosis struct {
	Status   string   `json:"status"`
	Summary  string   `json:"summary"`
	Evidence []string `json:"evidence"`
	Warnings []string `json:"warnings"`
}

type clusterNodeControlPlaneIncident struct {
	Name                 string         `json:"name,omitempty"`
	Path                 string         `json:"path,omitempty"`
	ArchivePath          string         `json:"archive_path,omitempty"`
	PrimaryFailureSignal string         `json:"primary_failure_signal,omitempty"`
	RootCauseStatus      string         `json:"root_cause_status,omitempty"`
	EvidenceCounts       map[string]int `json:"evidence_counts,omitempty"`
	BaselineFirst        string         `json:"baseline_first,omitempty"`
	BaselineLast         string         `json:"baseline_last,omitempty"`
	SelectedEvidence     []string       `json:"selected_evidence,omitempty"`
	NextChecks           []string       `json:"next_checks,omitempty"`
	DiagnosisText        string         `json:"diagnosis_text,omitempty"`
}

type clusterNodeDiagnosis struct {
	Node                 *model.ClusterNode               `json:"node,omitempty"`
	Summary              string                           `json:"summary"`
	JanitorNamespace     string                           `json:"janitor_namespace,omitempty"`
	JanitorPod           string                           `json:"janitor_pod,omitempty"`
	Filesystems          []clusterNodeFilesystemUsage     `json:"filesystems"`
	HotPaths             []clusterNodePathUsage           `json:"hot_paths"`
	Journal              []clusterNodeJournalEntry        `json:"journal"`
	Events               []model.ClusterEvent             `json:"events"`
	Metrics              *clusterNodeMetricsDiagnosis     `json:"metrics,omitempty"`
	ControlPlaneIncident *clusterNodeControlPlaneIncident `json:"control_plane_incident,omitempty"`
	Warnings             []string                         `json:"warnings"`
}

func (s *Server) handleGetClusterNodeDiagnosis(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}

	nodeName := strings.TrimSpace(r.PathValue("name"))
	if nodeName == "" {
		httpx.WriteError(w, http.StatusBadRequest, "node name is required")
		return
	}

	client, err := s.requireClusterNodeClient()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	defer client.closeIdleConnections()

	snapshots, err := s.loadClusterNodeInventory(r.Context())
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	snapshot, found := findClusterNodeSnapshotByName(snapshots, nodeName)
	if !found {
		httpx.WriteError(w, http.StatusNotFound, "cluster node not found")
		return
	}

	diagnosis, err := s.diagnoseClusterNode(r.Context(), client, snapshot)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	s.appendAudit(principal, "cluster.node.diagnosis.read", "node", nodeName, principal.TenantID, map[string]string{
		"node": nodeName,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"diagnosis": diagnosis})
}

func findClusterNodeSnapshotByName(snapshots []clusterNodeSnapshot, nodeName string) (clusterNodeSnapshot, bool) {
	nodeName = strings.TrimSpace(nodeName)
	for _, snapshot := range snapshots {
		if strings.EqualFold(strings.TrimSpace(snapshot.node.Name), nodeName) {
			return snapshot, true
		}
	}
	return clusterNodeSnapshot{}, false
}

func (s *Server) diagnoseClusterNode(ctx context.Context, client *clusterNodeClient, snapshot clusterNodeSnapshot) (clusterNodeDiagnosis, error) {
	diagnosis := clusterNodeDiagnosis{
		Node:        cloneClusterNodeForDiagnosis(snapshot.node),
		Filesystems: []clusterNodeFilesystemUsage{},
		HotPaths:    []clusterNodePathUsage{},
		Journal:     []clusterNodeJournalEntry{},
		Events:      []model.ClusterEvent{},
		Warnings:    []string{},
	}

	events, err := client.listClusterEvents(ctx, "")
	if err != nil {
		diagnosis.Warnings = append(diagnosis.Warnings, fmt.Sprintf("cluster events unavailable: %v", err))
	} else {
		diagnosis.Events = filterClusterNodeDiagnosisEvents(events, snapshot)
	}

	freshSummary, summaryErr := client.getNodeSummary(ctx, snapshot.node.Name)
	diagnosis.Metrics = buildClusterNodeMetricsDiagnosis(snapshot.node, freshSummary, summaryErr)
	if summaryErr != nil {
		diagnosis.Warnings = append(diagnosis.Warnings, fmt.Sprintf("fresh node summary unavailable: %v", summaryErr))
	}

	janitorNamespace, janitorPod, err := s.findNodeJanitorPod(ctx, client, snapshot.node.Name)
	if err != nil {
		diagnosis.Summary = buildClusterNodeDiagnosisSummary(snapshot.node, diagnosis.Metrics)
		diagnosis.Warnings = append(diagnosis.Warnings, err.Error())
		return diagnosis, nil
	}
	diagnosis.JanitorNamespace = janitorNamespace
	diagnosis.JanitorPod = janitorPod

	commandCtx, cancel := context.WithTimeout(ctx, clusterNodeDiagnosisCommandTimout)
	defer cancel()

	if output, err := s.runNodeJanitorCommand(commandCtx, janitorNamespace, janitorPod, clusterNodeDiagnosisFilesystemScript); err != nil {
		diagnosis.Warnings = append(diagnosis.Warnings, fmt.Sprintf("filesystem inventory unavailable: %v", err))
	} else {
		diagnosis.Filesystems = parseClusterNodeFilesystemUsage(output)
	}
	if output, err := s.runNodeJanitorCommand(commandCtx, janitorNamespace, janitorPod, clusterNodeDiagnosisHotPathsScript); err != nil {
		diagnosis.Warnings = append(diagnosis.Warnings, fmt.Sprintf("hot-path inventory unavailable: %v", err))
	} else {
		diagnosis.HotPaths = parseClusterNodePathUsage(output, clusterNodeDiagnosisHotPathLimit)
	}
	if output, err := s.runNodeJanitorCommand(commandCtx, janitorNamespace, janitorPod, clusterNodeDiagnosisJournalScript); err != nil {
		diagnosis.Warnings = append(diagnosis.Warnings, fmt.Sprintf("kubelet journal evidence unavailable: %v", err))
	} else {
		diagnosis.Journal = parseClusterNodeJournalEntries(output, clusterNodeDiagnosisJournalLimit)
		if diagnosis.Metrics != nil {
			diagnosis.Metrics.Evidence = append(diagnosis.Metrics.Evidence, selectMetricsEvidence(diagnosis.Journal)...)
		}
	}
	if output, err := s.runNodeJanitorCommand(commandCtx, janitorNamespace, janitorPod, clusterNodeControlPlaneIncidentScript); err != nil {
		diagnosis.Warnings = append(diagnosis.Warnings, fmt.Sprintf("control-plane incident summary unavailable: %v", err))
	} else {
		diagnosis.ControlPlaneIncident = parseClusterNodeControlPlaneIncident(output)
	}

	diagnosis.Summary = buildClusterNodeDiagnosisSummary(snapshot.node, diagnosis.Metrics)
	return diagnosis, nil
}

func cloneClusterNodeForDiagnosis(node model.ClusterNode) *model.ClusterNode {
	cloned := node
	if len(node.Roles) > 0 {
		cloned.Roles = append([]string(nil), node.Roles...)
	}
	if len(node.Workloads) > 0 {
		workloads := make([]model.ClusterNodeWorkload, len(node.Workloads))
		copy(workloads, node.Workloads)
		cloned.Workloads = workloads
	}
	if len(node.Conditions) > 0 {
		conditions := make(map[string]model.ClusterNodeCondition, len(node.Conditions))
		for key, value := range node.Conditions {
			conditions[key] = value
		}
		cloned.Conditions = conditions
	}
	return &cloned
}

func buildClusterNodeMetricsDiagnosis(node model.ClusterNode, summary *kubeNodeSummary, summaryErr error) *clusterNodeMetricsDiagnosis {
	evidence := []string{}
	warnings := []string{}
	if clusterNodeConditionIsTrue(node, clusterNodeConditionDisk) {
		evidence = append(evidence, "node condition DiskPressure=True")
	}
	if clusterNodeConditionIsTrue(node, clusterNodeConditionMemory) {
		evidence = append(evidence, "node condition MemoryPressure=True")
	}
	if summaryErr != nil {
		warnings = append(warnings, summaryErr.Error())
	}
	if summary != nil {
		evidence = append(evidence, "kubelet stats/summary responded successfully")
		return &clusterNodeMetricsDiagnosis{
			Status:   "available",
			Summary:  fmt.Sprintf("kubelet stats/summary is available for node %s", strings.TrimSpace(node.Name)),
			Evidence: evidence,
			Warnings: warnings,
		}
	}
	if node.CPU == nil && node.Memory == nil && node.EphemeralStorage == nil {
		evidence = append(evidence, "cluster inventory currently shows '-' for node metrics")
	}
	summaryText := fmt.Sprintf("kubelet stats/summary is unavailable for node %s", strings.TrimSpace(node.Name))
	if summaryErr != nil {
		summaryText = fmt.Sprintf("%s: %v", summaryText, summaryErr)
	}
	return &clusterNodeMetricsDiagnosis{
		Status:   "missing",
		Summary:  summaryText,
		Evidence: evidence,
		Warnings: warnings,
	}
}

func buildClusterNodeDiagnosisSummary(node model.ClusterNode, metrics *clusterNodeMetricsDiagnosis) string {
	switch {
	case clusterNodeConditionIsTrue(node, clusterNodeConditionDisk):
		return fmt.Sprintf("node %s is reporting DiskPressure; inspect hot paths and kubelet journal evidence below", strings.TrimSpace(node.Name))
	case metrics != nil && strings.EqualFold(strings.TrimSpace(metrics.Status), "missing"):
		return fmt.Sprintf("node %s currently lacks kubelet stats/summary data; inspect the metrics evidence and journal excerpts below", strings.TrimSpace(node.Name))
	default:
		return fmt.Sprintf("collected host-level disk, journal, and metrics diagnostics for node %s", strings.TrimSpace(node.Name))
	}
}

func clusterNodeConditionIsTrue(node model.ClusterNode, conditionType string) bool {
	if len(node.Conditions) == 0 {
		return false
	}
	condition, ok := node.Conditions[strings.TrimSpace(conditionType)]
	return ok && strings.EqualFold(strings.TrimSpace(condition.Status), "True")
}

func (s *Server) findNodeJanitorPod(ctx context.Context, client *clusterNodeClient, nodeName string) (string, string, error) {
	namespaces := []string{}
	if namespace := strings.TrimSpace(s.controlPlaneNamespace); namespace != "" {
		namespaces = append(namespaces, namespace)
	}
	namespaces = append(namespaces, "")

	seen := map[string]struct{}{}
	var lastErr error
	for _, namespace := range namespaces {
		if _, ok := seen[namespace]; ok {
			continue
		}
		seen[namespace] = struct{}{}
		pods, err := client.listCorePods(ctx, namespace, clusterNodeJanitorSelector)
		if err != nil {
			lastErr = err
			continue
		}
		var notReadyPod string
		for _, pod := range pods {
			if !strings.EqualFold(strings.TrimSpace(pod.Spec.NodeName), strings.TrimSpace(nodeName)) {
				continue
			}
			if nodeJanitorPodCanExec(pod) {
				return strings.TrimSpace(pod.Namespace), strings.TrimSpace(pod.Name), nil
			}
			if notReadyPod == "" {
				notReadyPod = describeNodeJanitorPodReadiness(pod)
			}
		}
		if notReadyPod != "" {
			return "", "", fmt.Errorf("node-janitor pod for node %s is not ready (%s)", strings.TrimSpace(nodeName), notReadyPod)
		}
	}
	if lastErr != nil {
		return "", "", fmt.Errorf("node-janitor pod lookup failed: %v", lastErr)
	}
	return "", "", fmt.Errorf("node-janitor pod for node %s was not found", strings.TrimSpace(nodeName))
}

func nodeJanitorPodCanExec(pod corev1.Pod) bool {
	if !strings.EqualFold(strings.TrimSpace(string(pod.Status.Phase)), string(corev1.PodRunning)) {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if strings.TrimSpace(status.Name) == clusterNodeJanitorContainer && status.State.Running != nil {
			return true
		}
	}
	return false
}

func describeNodeJanitorPodReadiness(pod corev1.Pod) string {
	parts := []string{}
	if name := strings.TrimSpace(pod.Name); name != "" {
		parts = append(parts, "pod="+name)
	}
	if namespace := strings.TrimSpace(pod.Namespace); namespace != "" {
		parts = append(parts, "namespace="+namespace)
	}
	if phase := strings.TrimSpace(string(pod.Status.Phase)); phase != "" {
		parts = append(parts, "phase="+phase)
	}
	if reason := strings.TrimSpace(pod.Status.Reason); reason != "" {
		parts = append(parts, "reason="+reason)
	}
	for _, status := range pod.Status.ContainerStatuses {
		if strings.TrimSpace(status.Name) != clusterNodeJanitorContainer {
			continue
		}
		switch {
		case status.State.Waiting != nil && strings.TrimSpace(status.State.Waiting.Reason) != "":
			parts = append(parts, "container_reason="+strings.TrimSpace(status.State.Waiting.Reason))
		case status.State.Terminated != nil && strings.TrimSpace(status.State.Terminated.Reason) != "":
			parts = append(parts, "container_reason="+strings.TrimSpace(status.State.Terminated.Reason))
		case status.State.Running == nil:
			parts = append(parts, "container_reason=NotRunning")
		}
		break
	}
	if len(parts) == 0 {
		return "pod status unavailable"
	}
	return strings.Join(parts, " ")
}

func (s *Server) runNodeJanitorCommand(ctx context.Context, namespace, podName, script string) ([]byte, error) {
	runner := s.filesystemExecRunner
	if runner == nil {
		runner = kubeFilesystemExecRunner{}
	}
	output, _, err := runClusterExecWithRetries(
		ctx,
		runner,
		namespace,
		podName,
		clusterNodeJanitorContainer,
		[]string{"/bin/bash", "-lc", script},
		2,
		250*time.Millisecond,
		clusterNodeDiagnosisCommandTimout,
	)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func parseClusterNodeFilesystemUsage(raw []byte) []clusterNodeFilesystemUsage {
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	out := make([]clusterNodeFilesystemUsage, 0, len(lines))
	seen := map[string]struct{}{}
	for _, line := range lines {
		fields := strings.Split(strings.TrimSpace(line), "\t")
		if len(fields) < 6 {
			continue
		}
		key := strings.Join(fields, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, clusterNodeFilesystemUsage{
			Filesystem:     strings.TrimSpace(fields[0]),
			SizeBytes:      parseOptionalInt64(strings.TrimSpace(fields[1])),
			UsedBytes:      parseOptionalInt64(strings.TrimSpace(fields[2])),
			AvailableBytes: parseOptionalInt64(strings.TrimSpace(fields[3])),
			UsedPercent:    parsePercent(strings.TrimSpace(fields[4])),
			MountPath:      strings.TrimSpace(fields[5]),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i].UsedBytes
		right := out[j].UsedBytes
		switch {
		case left != nil && right != nil && *left != *right:
			return *left > *right
		case out[i].MountPath != out[j].MountPath:
			return out[i].MountPath < out[j].MountPath
		default:
			return out[i].Filesystem < out[j].Filesystem
		}
	})
	return out
}

func parseClusterNodePathUsage(raw []byte, limit int) []clusterNodePathUsage {
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	out := make([]clusterNodePathUsage, 0, len(lines))
	seen := map[string]struct{}{}
	for _, line := range lines {
		fields := strings.SplitN(strings.TrimSpace(line), "\t", 2)
		if len(fields) != 2 {
			continue
		}
		path := strings.TrimSpace(fields[1])
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		bytesValue, err := strconv.ParseInt(strings.TrimSpace(fields[0]), 10, 64)
		if err != nil {
			continue
		}
		out = append(out, clusterNodePathUsage{Path: path, Bytes: bytesValue})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Bytes != out[j].Bytes {
			return out[i].Bytes > out[j].Bytes
		}
		return out[i].Path < out[j].Path
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func parseClusterNodeJournalEntries(raw []byte, limit int) []clusterNodeJournalEntry {
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	out := make([]clusterNodeJournalEntry, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		entry := clusterNodeJournalEntry{Message: line}
		if timestamp, rest, ok := parseJournalTimestampPrefix(line); ok {
			entry.Timestamp = &timestamp
			entry.Message = rest
		}
		out = append(out, entry)
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

func parseClusterNodeControlPlaneIncident(raw []byte) *clusterNodeControlPlaneIncident {
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return nil
	}
	incident := &clusterNodeControlPlaneIncident{
		EvidenceCounts: map[string]int{},
	}
	diagnosisLines := []string{}
	section := ""
	inDiagnosis := false

	for _, rawLine := range strings.Split(text, "\n") {
		line := strings.TrimRight(rawLine, "\r")
		if strings.HasPrefix(line, "__FUGUE_FIELD__\t") {
			fields := strings.SplitN(strings.TrimPrefix(line, "__FUGUE_FIELD__\t"), "\t", 2)
			if len(fields) != 2 {
				continue
			}
			switch strings.TrimSpace(fields[0]) {
			case "name":
				incident.Name = strings.TrimSpace(fields[1])
			case "path":
				incident.Path = strings.TrimSpace(fields[1])
			case "archive_path":
				incident.ArchivePath = strings.TrimSpace(fields[1])
			}
			continue
		}
		if line == "__FUGUE_DIAGNOSIS_BEGIN__" {
			inDiagnosis = true
			continue
		}
		if !inDiagnosis {
			continue
		}
		diagnosisLines = append(diagnosisLines, line)
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		switch trimmed {
		case "evidence_counts:":
			section = "evidence_counts"
			continue
		case "selected_evidence:":
			section = "selected_evidence"
			continue
		case "next_checks:":
			section = "next_checks"
			continue
		}
		if key, value, ok := strings.Cut(trimmed, "="); ok {
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			switch key {
			case "primary_failure_signal":
				incident.PrimaryFailureSignal = value
			case "root_cause_status":
				incident.RootCauseStatus = value
			case "baseline_first":
				incident.BaselineFirst = value
			case "baseline_last":
				incident.BaselineLast = value
			default:
				if section == "evidence_counts" {
					if count, err := strconv.Atoi(value); err == nil {
						incident.EvidenceCounts[key] = count
					}
				}
			}
			continue
		}
		switch section {
		case "selected_evidence":
			if len(incident.SelectedEvidence) < 40 {
				incident.SelectedEvidence = append(incident.SelectedEvidence, trimmed)
			}
		case "next_checks":
			if len(incident.NextChecks) < 12 {
				incident.NextChecks = append(incident.NextChecks, strings.TrimSpace(strings.TrimPrefix(trimmed, "- ")))
			}
		}
	}

	incident.DiagnosisText = strings.TrimSpace(strings.Join(diagnosisLines, "\n"))
	if len(incident.EvidenceCounts) == 0 {
		incident.EvidenceCounts = nil
	}
	if incident.Name == "" &&
		incident.Path == "" &&
		incident.ArchivePath == "" &&
		incident.PrimaryFailureSignal == "" &&
		incident.DiagnosisText == "" {
		return nil
	}
	return incident
}

func parseJournalTimestampPrefix(line string) (time.Time, string, bool) {
	if len(line) < len("2006-01-02T15:04:05Z07:00") {
		return time.Time{}, line, false
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return time.Time{}, line, false
	}
	candidate := strings.TrimSpace(fields[0] + "T" + fields[1])
	if value, err := time.Parse(time.RFC3339, candidate); err == nil {
		return value.UTC(), strings.TrimSpace(strings.TrimPrefix(line, fields[0]+" "+fields[1])), true
	}
	if value, err := time.Parse("2006-01-02T15:04:05-0700", candidate); err == nil {
		return value.UTC(), strings.TrimSpace(strings.TrimPrefix(line, fields[0]+" "+fields[1])), true
	}
	return time.Time{}, line, false
}

func parseOptionalInt64(raw string) *int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil
	}
	return &value
}

func parsePercent(raw string) *float64 {
	raw = strings.TrimSuffix(strings.TrimSpace(raw), "%")
	if raw == "" {
		return nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil
	}
	return &value
}

func filterClusterNodeDiagnosisEvents(events []corev1.Event, snapshot clusterNodeSnapshot) []model.ClusterEvent {
	podNames := map[string]struct{}{}
	for _, pod := range snapshot.pods {
		if name := strings.TrimSpace(pod.Metadata.Name); name != "" {
			podNames[name] = struct{}{}
		}
	}

	out := make([]model.ClusterEvent, 0, len(events))
	for _, event := range events {
		if eventRelevantToClusterNodeDiagnosis(event, snapshot.node.Name, podNames) {
			out = append(out, clusterEventFromCore(event))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		left := clusterEventSortTime(out[i])
		right := clusterEventSortTime(out[j])
		if !left.Equal(right) {
			return left.After(right)
		}
		return out[i].Name < out[j].Name
	})
	if len(out) > clusterNodeDiagnosisEventLimit {
		out = out[:clusterNodeDiagnosisEventLimit]
	}
	return out
}

func eventRelevantToClusterNodeDiagnosis(event corev1.Event, nodeName string, podNames map[string]struct{}) bool {
	switch strings.TrimSpace(event.InvolvedObject.Kind) {
	case "Node":
		return strings.EqualFold(strings.TrimSpace(event.InvolvedObject.Name), strings.TrimSpace(nodeName))
	case "Pod":
		_, ok := podNames[strings.TrimSpace(event.InvolvedObject.Name)]
		return ok
	default:
		return false
	}
}

func selectMetricsEvidence(entries []clusterNodeJournalEntry) []string {
	out := make([]string, 0, 4)
	for _, entry := range entries {
		text := strings.ToLower(strings.TrimSpace(entry.Message))
		if text == "" {
			continue
		}
		if strings.Contains(text, "stats/summary") || strings.Contains(text, "metrics-server") || strings.Contains(text, "summary") {
			out = append(out, entry.Message)
		}
		if len(out) >= 4 {
			break
		}
	}
	return out
}

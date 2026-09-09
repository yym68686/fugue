package cli

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/tui"
)

func tuiRedactToken(value, token string) string {
	if token != "" {
		return strings.ReplaceAll(value, token, "[redacted]")
	}
	return value
}

func (p *tuiProvider) loadTUICluster(client *Client, request tui.Request) (tui.Snapshot, error) {
	s := tui.Snapshot{Target: request.Target, Title: "Cluster", Status: "ready", ObservedAt: time.Now()}
	kind, scope := request.Target.Kind, request.Target.Scope
	if request.Section != "overview" && request.Section != "" {
		return s, nil
	}
	nodesView := kind == "node" || (kind == "cluster" && (scope == "" || scope == "all" || scope == "nodes"))
	controlView := kind == "component" || kind == "component-pod" || kind == "release" || (kind == "cluster" && (scope == "" || scope == "all" || scope == "control-plane"))
	runtimesView := kind == "cluster" && (scope == "" || scope == "all" || scope == "runtimes")
	if nodesView {
		nodes, err := client.listTUINodes()
		s.Sources = append(s.Sources, tuiSource("nodes", err, s.ObservedAt))
		if err == nil {
			table := tui.Table{ID: "nodes", Title: "Nodes", Columns: []string{"Node", "Status", "CPU", "Memory", "Disk", "Workloads", "Region"}}
			ready := 0
			for _, node := range nodes {
				if kind == "node" && node.Name != request.Target.ID {
					continue
				}
				if strings.EqualFold(node.Status, "ready") {
					ready++
				}
				var cpu, mem, disk *float64
				if node.CPU != nil {
					cpu = node.CPU.UsagePercent
				}
				if node.Memory != nil {
					mem = node.Memory.UsagePercent
				}
				if node.EphemeralStorage != nil {
					disk = node.EphemeralStorage.UsagePercent
				}
				target := tui.Target{Kind: "node", ID: node.Name, Name: node.Name}
				numbers := map[int]float64{5: float64(len(node.Workloads))}
				for i, v := range []*float64{cpu, mem, disk} {
					if v != nil {
						numbers[i+2] = *v
					}
				}
				table.Rows = append(table.Rows, tui.Row{ID: node.Name, Cells: []string{node.Name, node.Status, tuiPercent(cpu), tuiPercent(mem), tuiPercent(disk), strconv.Itoa(len(node.Workloads)), node.Region}, Numbers: numbers, Target: &target})
				if kind == "node" {
					appendTUINodeDetails(&s, node)
					appendTUINodeSeries(&s, node)
				}
			}
			if kind == "cluster" {
				appendTUIClusterSeries(&s, nodes)
			}
			s.Tables = append([]tui.Table{table}, s.Tables...)
			s.Fields = append(s.Fields, tui.Field{Label: "Nodes ready", Value: fmt.Sprintf("%d / %d", ready, len(table.Rows))})
			if kind == "node" && len(table.Rows) == 0 {
				s.Status = "not found"
			}
		}
		summary, policies, err := client.GetClusterNodePolicyStatus()
		s.Sources = append(s.Sources, tuiSource("policies", err, s.ObservedAt))
		if err == nil {
			s.Fields = append(s.Fields, tui.Field{Label: "Policy drift", Value: strconv.Itoa(summary.Drifted)}, tui.Field{Label: "Health blocked", Value: strconv.Itoa(summary.BlockedByHealth)})
			table := tui.Table{ID: "policies", Title: "Node policy", Columns: []string{"Node", "Ready", "Reconciled", "Blocked", "Reason"}}
			for _, policy := range policies {
				if kind == "node" && policy.NodeName != request.Target.ID {
					continue
				}
				target := tui.Target{Kind: "node", ID: policy.NodeName, Name: policy.NodeName}
				table.Rows = append(table.Rows, tui.Row{ID: policy.NodeName, Target: &target, Cells: []string{policy.NodeName, strconv.FormatBool(policy.Ready), strconv.FormatBool(policy.Reconciled), strconv.FormatBool(policy.BlockRollout), firstNonEmptyTrimmed(policy.GateReason, policy.ReconcileError, strings.Join(policy.ReconcileReasons, "; "))}})
			}
			s.Tables = append(s.Tables, table)
		}
	}
	if controlView {
		control, err := client.GetControlPlaneStatus()
		s.Sources = append(s.Sources, tuiSource("components", err, control.ObservedAt))
		if err == nil {
			appendTUIControlDetails(&s, control, request.Target)
		}
	}
	if runtimesView {
		runtimes, err := client.ListRuntimes()
		s.Sources = append(s.Sources, tuiSource("runtimes", err, s.ObservedAt))
		if err == nil {
			table := tui.Table{ID: "runtimes", Title: "Runtimes", Columns: []string{"Runtime", "Status", "Type", "Access", "Node", "Heartbeat"}}
			for _, r := range runtimes {
				target := tui.Target{Kind: "runtime", ID: r.ID, Name: r.Name}
				table.Rows = append(table.Rows, tui.Row{ID: r.ID, Target: &target, Cells: []string{r.Name, r.Status, r.Type, r.AccessMode, r.ClusterNodeName, formatOptionalTime(r.LastHeartbeatAt)}})
			}
			s.Tables = append(s.Tables, table)
		}
	}
	for _, source := range s.Sources {
		if source.State != "available" {
			s.Status = "partial"
		}
	}
	return s, nil
}

func tuiPercent(v *float64) string {
	if v == nil {
		return "--"
	}
	return fmt.Sprintf("%.1f%%", *v)
}
func appendTUINodeDetails(s *tui.Snapshot, node model.ClusterNode) {
	s.Title, s.Status = node.Name, node.Status
	s.Fields = append(s.Fields, tui.Field{Label: "Region / zone", Value: node.Region + " / " + node.Zone}, tui.Field{Label: "Kubelet", Value: node.KubeletVersion}, tui.Field{Label: "OS", Value: node.OSImage}, tui.Field{Label: "Container runtime", Value: node.ContainerRuntime})
	if node.CPU != nil {
		appendTUINumber(s, "CPU capacity mCPU", node.CPU.CapacityMilliCores)
		appendTUINumber(s, "CPU allocatable mCPU", node.CPU.AllocatableMilliCores)
		appendTUINumber(s, "CPU schedulable mCPU", node.CPU.SchedulableFreeMilliCores)
	}
	if node.Memory != nil {
		appendTUINumber(s, "Memory capacity B", node.Memory.CapacityBytes)
		appendTUINumber(s, "Memory schedulable B", node.Memory.SchedulableFreeBytes)
	}
	if node.EphemeralStorage != nil {
		appendTUINumber(s, "Disk capacity B", node.EphemeralStorage.CapacityBytes)
		appendTUINumber(s, "Disk used B", node.EphemeralStorage.UsedBytes)
	}
	table := tui.Table{ID: "workloads", Title: "Workloads", Columns: []string{"Workload", "Kind", "Runtime", "Pods"}}
	for _, workload := range node.Workloads {
		row := tui.Row{ID: workload.ID, Cells: []string{workload.Name, workload.Kind, workload.RuntimeID, strconv.Itoa(workload.PodCount)}}
		appID := workload.OwnerAppID
		if workload.Kind == model.ClusterNodeWorkloadKindApp {
			appID = workload.ID
		}
		if appID != "" {
			target := tui.Target{Kind: "app", ID: appID, Name: workload.Name, TenantID: workload.TenantID, ProjectID: workload.ProjectID}
			row.Target = &target
		}
		table.Rows = append(table.Rows, row)
	}
	s.Tables = append(s.Tables, table)
}
func appendTUINumber(s *tui.Snapshot, label string, v *int64) {
	value := "unavailable"
	if v != nil {
		value = strconv.FormatInt(*v, 10)
	}
	s.Fields = append(s.Fields, tui.Field{Label: label, Value: value})
}

func appendTUIControlDetails(s *tui.Snapshot, control model.ControlPlaneStatus, target tui.Target) {
	s.Fields = append(s.Fields, tui.Field{Label: "Control plane", Value: control.Status}, tui.Field{Label: "Declared version", Value: control.Version}, tui.Field{Label: "Observed version", Value: control.LiveVersion}, tui.Field{Label: "Observed at", Value: control.ObservedAt.Format(time.RFC3339)})
	table := tui.Table{ID: "components", Title: "Control plane", Columns: []string{"Component", "Status", "Ready", "Version"}}
	for _, component := range control.Components {
		if (target.Kind == "component" && component.DeploymentName != target.ID) || (target.Kind == "component-pod" && component.DeploymentName != target.ProjectID) {
			continue
		}
		t := tui.Target{Kind: "component", ID: component.DeploymentName, Name: component.Component}
		table.Rows = append(table.Rows, tui.Row{ID: component.DeploymentName, Target: &t, Cells: []string{component.Component, component.Status, fmt.Sprintf("%d/%d", component.ReadyReplicas, component.DesiredReplicas), component.ImageTag}})
		if target.Kind == "component" || target.Kind == "component-pod" {
			s.Title, s.Status = component.Component, component.Status
			s.Fields = append(s.Fields, tui.Field{Label: "Deployment", Value: component.DeploymentName}, tui.Field{Label: "Image", Value: component.Image})
			pods := tui.Table{ID: "component-pods", Title: "Component workload", Columns: []string{"Pod", "Phase", "Ready", "Node", "Image"}}
			for _, pod := range component.ObservedPods {
				if target.Kind == "component-pod" && pod.Name != target.ID {
					continue
				}
				t := tui.Target{Kind: "component-pod", ID: pod.Name, Name: pod.Name, ProjectID: component.DeploymentName}
				pods.Rows = append(pods.Rows, tui.Row{ID: pod.Name, Target: &t, Cells: []string{pod.Name, pod.Phase, strconv.FormatBool(pod.Ready), pod.NodeName, pod.ImageTag}})
				if target.Kind == "component-pod" {
					s.Title = pod.Name
					s.Fields = append(s.Fields, tui.Field{Label: "Started", Value: formatOptionalTime(pod.StartTime)}, tui.Field{Label: "Pod image", Value: pod.Image})
				}
			}
			s.Tables = append(s.Tables, pods)
		}
	}
	s.Tables = append(s.Tables, table)
	if control.DeployWorkflow != nil {
		run := control.DeployWorkflow
		t := tui.Target{Kind: "release", ID: run.HeadSHA, Name: "Release " + run.HeadSHA[:min(12, len(run.HeadSHA))]}
		s.Tables = append(s.Tables, tui.Table{ID: "release", Title: "Release evidence", Columns: []string{"Workflow", "State", "Conclusion", "Commit", "URL"}, Rows: []tui.Row{{ID: run.HeadSHA, Target: &t, Cells: []string{run.Workflow, run.Status, run.Conclusion, run.HeadSHA, run.HTMLURL}}}})
		if target.Kind == "release" {
			s.Title = target.Name
			s.Status = run.Status
			s.Fields = append(s.Fields, tui.Field{Label: "Requested commit", Value: target.ID}, tui.Field{Label: "Observed commit", Value: run.HeadSHA}, tui.Field{Label: "Workflow URL", Value: run.HTMLURL}, tui.Field{Label: "Conclusion", Value: run.Conclusion}, tui.Field{Label: "Updated", Value: formatOptionalTime(run.UpdatedAt)})
			if target.ID != run.HeadSHA {
				s.Status = "superseded"
			}
		}
	}
	for _, warning := range control.Warnings {
		s.Events = append(s.Events, tui.Event{ID: warning.Name, At: control.ObservedAt, Severity: warning.Severity, Message: warning.Name + ": " + warning.Reason + " " + warning.Message})
	}
	if control.Topology != nil {
		topology := control.Topology
		s.Fields = append(s.Fields, tui.Field{Label: "Topology", Value: topology.Mode}, tui.Field{Label: "Quorum ready", Value: strconv.FormatBool(topology.QuorumReady)}, tui.Field{Label: "Failure domains", Value: strings.Join(topology.FailureDomains, ", ")})
	}
}

func (c *Client) listTUINodes() ([]model.ClusterNode, error) {
	var result struct {
		Nodes []model.ClusterNode `json:"cluster_nodes"`
	}
	err := c.doJSON(http.MethodGet, "/v1/cluster/nodes?sync_locations=false", nil, &result)
	return result.Nodes, err
}
func appendTUINodeSeries(s *tui.Snapshot, node model.ClusterNode) {
	var cpu, mem, disk *float64
	if node.CPU != nil {
		cpu = node.CPU.UsagePercent
	}
	if node.Memory != nil {
		mem = node.Memory.UsagePercent
	}
	if node.EphemeralStorage != nil {
		disk = node.EphemeralStorage.UsagePercent
	}
	for i, v := range []*float64{cpu, mem, disk} {
		series := tui.Series{ID: []string{"cpu", "memory", "disk"}[i], Label: []string{"CPU", "Memory", "Disk"}[i], Unit: "%", Source: "kubelet node summary", State: "unavailable", Interval: 30 * time.Second}
		if node.ObservedAt != nil && v != nil {
			series.State = "available"
			series.Points = []tui.Point{{At: *node.ObservedAt, Value: v}}
		}
		s.Series = append(s.Series, series)
	}
	s.Series = append(s.Series, tui.Series{ID: "network", Label: "Network", Unit: "bytes/s", Source: "not exported by node telemetry", State: "unavailable"})
}

func appendTUIClusterSeries(s *tui.Snapshot, nodes []model.ClusterNode) {
	peak := model.ClusterNode{}
	var cpu, mem, disk *float64
	for _, node := range nodes {
		if node.ObservedAt == nil {
			continue
		}
		if peak.ObservedAt == nil || node.ObservedAt.Before(*peak.ObservedAt) {
			peak.ObservedAt = node.ObservedAt
		}
		if node.CPU != nil && node.CPU.UsagePercent != nil && (cpu == nil || *node.CPU.UsagePercent > *cpu) {
			cpu = node.CPU.UsagePercent
		}
		if node.Memory != nil && node.Memory.UsagePercent != nil && (mem == nil || *node.Memory.UsagePercent > *mem) {
			mem = node.Memory.UsagePercent
		}
		if node.EphemeralStorage != nil && node.EphemeralStorage.UsagePercent != nil && (disk == nil || *node.EphemeralStorage.UsagePercent > *disk) {
			disk = node.EphemeralStorage.UsagePercent
		}
	}
	peak.CPU = &model.ClusterNodeCPUStats{UsagePercent: cpu}
	peak.Memory = &model.ClusterNodeMemoryStats{UsagePercent: mem}
	peak.EphemeralStorage = &model.ClusterNodeStorageStats{UsagePercent: disk}
	appendTUINodeSeries(s, peak)
	for i := range s.Series {
		if s.Series[i].ID != "network" {
			s.Series[i].Label = "Peak node " + s.Series[i].Label
			s.Series[i].Source = "maximum across timestamped kubelet node samples"
		}
	}
}

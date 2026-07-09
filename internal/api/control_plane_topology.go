package api

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

const (
	controlPlaneEndpointModeUnavailable     = "unavailable"
	controlPlaneEndpointModeSingle          = "single_endpoint"
	controlPlaneEndpointModeMultiAddress    = "multi_address_endpoint"
	controlPlaneEndpointModeHAEndpointReady = "ha_endpoint_ready"
)

func buildControlPlaneTopologyFromKubeNodes(nodes []kubeNode, generatedAt time.Time) model.ControlPlaneTopology {
	topologyNodes := make([]model.ControlPlaneTopologyNode, 0, len(nodes))
	for _, node := range nodes {
		topologyNodes = append(topologyNodes, controlPlaneTopologyNodeFromKube(node))
	}
	return buildControlPlaneTopology(topologyNodes, generatedAt)
}

func buildControlPlaneTopologyFromNodeUpdaters(updaters []model.NodeUpdater, generatedAt time.Time) model.ControlPlaneTopology {
	nodes := make([]model.ControlPlaneTopologyNode, 0, len(updaters))
	for _, updater := range updaters {
		nodes = append(nodes, controlPlaneTopologyNodeFromUpdater(updater))
	}
	return buildControlPlaneTopology(nodes, generatedAt)
}

func buildControlPlaneTopology(nodes []model.ControlPlaneTopologyNode, generatedAt time.Time) model.ControlPlaneTopology {
	sort.SliceStable(nodes, func(i, j int) bool {
		return strings.TrimSpace(nodes[i].NodeName) < strings.TrimSpace(nodes[j].NodeName)
	})
	providers := map[string]struct{}{}
	regions := map[string]struct{}{}
	failureDomains := map[string]struct{}{}
	controlPlaneCapable := 0
	readyControlPlaneCapable := 0
	etcdVoterCapable := 0
	releaseRunnerCapable := 0
	for _, node := range nodes {
		if node.ControlPlaneCapable {
			controlPlaneCapable++
			if node.Ready {
				readyControlPlaneCapable++
			}
		}
		if node.EtcdVoterCapable {
			etcdVoterCapable++
		}
		if node.ReleaseRunnerCapable {
			releaseRunnerCapable++
		}
		addNonEmpty(providers, node.Provider)
		addNonEmpty(regions, node.Region)
		addNonEmpty(failureDomains, node.FailureDomain)
	}

	mode := model.ControlPlaneTopologyModeUnavailable
	status := model.ControlPlaneTopologyStatusUnavailable
	endpointMode := controlPlaneEndpointModeUnavailable
	warnings := []string{}
	missing := []string{}
	switch {
	case controlPlaneCapable == 0:
		status = model.ControlPlaneTopologyStatusNoCapableNode
		missing = append(missing, "mark at least one node control-plane-capable before control-plane topology can be explained")
	case controlPlaneCapable == 1:
		mode = model.ControlPlaneTopologyModeSingle
		status = model.ControlPlaneTopologyStatusSingleNodeRisk
		endpointMode = controlPlaneEndpointModeSingle
		warnings = append(warnings, "single-control-plane: data plane must rely on validated LKG and external watchdog during control-plane VM outage")
		missing = append(missing, "add at least two more control-plane-capable nodes for etcd quorum HA")
		missing = append(missing, "add a second release-runner-capable node outside the current failure domain")
	case controlPlaneCapable == 2:
		mode = model.ControlPlaneTopologyModeDualPartial
		status = model.ControlPlaneTopologyStatusPartialRedundancy
		endpointMode = controlPlaneEndpointModeMultiAddress
		warnings = append(warnings, "two control-plane-capable nodes improve API/controller redundancy but do not provide full etcd quorum HA")
		missing = append(missing, "add one more control-plane-capable node for 3-node etcd quorum")
	default:
		mode = model.ControlPlaneTopologyModeHAQuorum
		status = model.ControlPlaneTopologyStatusHAReady
		endpointMode = controlPlaneEndpointModeHAEndpointReady
	}
	if controlPlaneCapable >= 3 && len(failureDomains) < 2 {
		status = model.ControlPlaneTopologyStatusPartialRedundancy
		warnings = append(warnings, "control-plane-capable nodes do not span at least two failure domains")
		missing = append(missing, "spread control-plane-capable nodes across provider, region, or zone failure domains")
	}
	if releaseRunnerCapable < 2 {
		missing = appendMissing(missing, "need at least two release-runner-capable nodes for deploy runner de-single-point")
	}

	return model.ControlPlaneTopology{
		Mode:                          mode,
		Status:                        status,
		ControlPlaneCapableNodes:      controlPlaneCapable,
		ReadyControlPlaneCapableNodes: readyControlPlaneCapable,
		EtcdVoterCapableNodes:         etcdVoterCapable,
		ReleaseRunnerCapableNodes:     releaseRunnerCapable,
		FailureDomains:                sortedSet(failureDomains),
		Providers:                     sortedSet(providers),
		Regions:                       sortedSet(regions),
		EndpointMode:                  endpointMode,
		QuorumReady:                   controlPlaneCapable >= 3,
		HAReady:                       controlPlaneCapable >= 3 && readyControlPlaneCapable >= 2 && len(failureDomains) >= 2,
		RiskWarnings:                  dedupeStrings(warnings),
		MissingRedundancy:             dedupeStrings(missing),
		Nodes:                         nodes,
		GeneratedAt:                   generatedAt,
	}
}

func controlPlaneTopologyNodeFromKube(node kubeNode) model.ControlPlaneTopologyNode {
	labels := node.Metadata.Labels
	name := strings.TrimSpace(node.Metadata.Name)
	roles := kubeNodeRoles(labels)
	desiredRole := model.NormalizeMachineControlPlaneRole(firstNodeLabel(labels, runtimepkg.ControlPlaneDesiredRoleKey))
	effectiveRole := model.MachineControlPlaneRoleNone
	for _, role := range roles {
		if strings.EqualFold(strings.TrimSpace(role), "control-plane") {
			effectiveRole = model.MachineControlPlaneRoleMember
			break
		}
	}
	if effectiveRole == model.MachineControlPlaneRoleNone {
		switch desiredRole {
		case model.MachineControlPlaneRoleCandidate, model.MachineControlPlaneRoleMember:
			effectiveRole = model.MachineControlPlaneRoleCandidate
		}
	}
	controlPlaneCapable := effectiveRole == model.MachineControlPlaneRoleMember || effectiveRole == model.MachineControlPlaneRoleCandidate
	capabilities := controlPlaneCapabilitiesForLabels(labels, roles)
	provider := firstNodeLabel(labels, "fugue.io/provider", "topology.kubernetes.io/provider", "provider")
	region := kubeNodeRegion(labels, node.Metadata.Annotations)
	zone := kubeNodeZone(labels)
	return model.ControlPlaneTopologyNode{
		NodeName:                  name,
		Ready:                     strings.EqualFold(kubeNodeReadyStatus(node), "ready"),
		ControlPlaneCapable:       controlPlaneCapable,
		EtcdVoterCapable:          effectiveRole == model.MachineControlPlaneRoleMember,
		ReleaseRunnerCapable:      hasCapability(capabilities, "release-runner-capable") || strings.EqualFold(firstNodeLabel(labels, "fugue.io/role.release-runner"), runtimepkg.NodeRoleLabelValue),
		DesiredControlPlaneRole:   desiredRole,
		EffectiveControlPlaneRole: effectiveRole,
		Provider:                  provider,
		Region:                    region,
		Zone:                      zone,
		FailureDomain:             failureDomain(provider, region, zone, name),
		Roles:                     roles,
		Capabilities:              capabilities,
	}
}

func controlPlaneTopologyNodeFromUpdater(updater model.NodeUpdater) model.ControlPlaneTopologyNode {
	labels := updater.Labels
	desiredRole := model.NormalizeMachineControlPlaneRole(firstNodeLabel(labels, runtimepkg.ControlPlaneDesiredRoleKey))
	effectiveRole := model.MachineControlPlaneRoleNone
	if hasCapability(updater.Capabilities, "control-plane-capable") {
		effectiveRole = model.MachineControlPlaneRoleCandidate
	}
	if hasCapability(updater.Capabilities, "etcd-voter-capable") {
		effectiveRole = model.MachineControlPlaneRoleMember
	}
	if desiredRole == model.MachineControlPlaneRoleMember {
		effectiveRole = model.MachineControlPlaneRoleMember
	} else if desiredRole == model.MachineControlPlaneRoleCandidate && effectiveRole == model.MachineControlPlaneRoleNone {
		effectiveRole = model.MachineControlPlaneRoleCandidate
	}
	region := firstNodeLabel(labels, runtimepkg.RegionLabelKey, runtimepkg.LegacyRegionLabelKey, "region")
	zone := firstNodeLabel(labels, runtimepkg.ZoneLabelKey, runtimepkg.LegacyZoneLabelKey, "zone")
	provider := firstNodeLabel(labels, "fugue.io/provider", "topology.kubernetes.io/provider", "provider")
	nodeName := strings.TrimSpace(updater.ClusterNodeName)
	if nodeName == "" {
		nodeName = strings.TrimSpace(updater.MachineID)
	}
	return model.ControlPlaneTopologyNode{
		NodeName:                  nodeName,
		Ready:                     strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive),
		ControlPlaneCapable:       effectiveRole == model.MachineControlPlaneRoleMember || effectiveRole == model.MachineControlPlaneRoleCandidate,
		EtcdVoterCapable:          effectiveRole == model.MachineControlPlaneRoleMember || hasCapability(updater.Capabilities, "etcd-voter-capable"),
		ReleaseRunnerCapable:      hasCapability(updater.Capabilities, "release-runner-capable"),
		DesiredControlPlaneRole:   desiredRole,
		EffectiveControlPlaneRole: effectiveRole,
		Provider:                  provider,
		Region:                    region,
		Zone:                      zone,
		FailureDomain:             failureDomain(provider, region, zone, nodeName),
		Capabilities:              append([]string(nil), updater.Capabilities...),
	}
}

func controlPlaneCapabilitiesForLabels(labels map[string]string, roles []string) []string {
	capabilities := []string{}
	for _, role := range roles {
		if strings.EqualFold(strings.TrimSpace(role), "control-plane") {
			capabilities = append(capabilities, "control-plane-capable", "etcd-voter-capable")
			break
		}
	}
	if role := model.NormalizeMachineControlPlaneRole(firstNodeLabel(labels, runtimepkg.ControlPlaneDesiredRoleKey)); role == model.MachineControlPlaneRoleCandidate || role == model.MachineControlPlaneRoleMember {
		capabilities = append(capabilities, "control-plane-capable")
		if role == model.MachineControlPlaneRoleMember {
			capabilities = append(capabilities, "etcd-voter-capable")
		}
	}
	if strings.EqualFold(firstNodeLabel(labels, "fugue.io/role.release-runner"), runtimepkg.NodeRoleLabelValue) {
		capabilities = append(capabilities, "release-runner-capable")
	}
	return dedupeStrings(capabilities)
}

func failureDomain(provider, region, zone, fallback string) string {
	parts := []string{}
	if provider = strings.TrimSpace(provider); provider != "" {
		parts = append(parts, "provider:"+provider)
	}
	if region = strings.TrimSpace(region); region != "" {
		parts = append(parts, "region:"+region)
	}
	if zone = strings.TrimSpace(zone); zone != "" {
		parts = append(parts, "zone:"+zone)
	}
	if len(parts) > 0 {
		return strings.Join(parts, "/")
	}
	if fallback = strings.TrimSpace(fallback); fallback != "" {
		return "node:" + fallback
	}
	return ""
}

func hasCapability(capabilities []string, capability string) bool {
	capability = strings.TrimSpace(strings.ToLower(capability))
	for _, item := range capabilities {
		if strings.EqualFold(strings.TrimSpace(item), capability) {
			return true
		}
	}
	return false
}

func addNonEmpty(set map[string]struct{}, value string) {
	value = strings.TrimSpace(value)
	if value != "" {
		set[value] = struct{}{}
	}
}

func sortedSet(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func appendMissing(values []string, next string) []string {
	next = strings.TrimSpace(next)
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), next) {
			return values
		}
	}
	return append(values, next)
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func controlPlaneTopologyCheck(topology model.ControlPlaneTopology) model.RobustnessCheck {
	pass := topology.Status == model.ControlPlaneTopologyStatusHAReady || topology.Status == model.ControlPlaneTopologyStatusSingleNodeRisk || topology.Status == model.ControlPlaneTopologyStatusPartialRedundancy
	severity := model.RobustnessSeverityInfo
	if topology.Status == model.ControlPlaneTopologyStatusNoCapableNode || topology.Status == model.ControlPlaneTopologyStatusUnavailable {
		pass = false
		severity = model.RobustnessSeverityWarning
	} else if topology.Status == model.ControlPlaneTopologyStatusSingleNodeRisk || topology.Status == model.ControlPlaneTopologyStatusPartialRedundancy {
		severity = model.RobustnessSeverityWarning
	}
	return model.RobustnessCheck{
		Name:     "control_plane_topology",
		Pass:     pass,
		Severity: severity,
		Subject:  "control-plane",
		Expected: "topology is explainable; single-node physical limits are risk warnings, not release failures",
		Observed: fmt.Sprintf("mode=%s status=%s capable=%d ready=%d quorum_ready=%t", topology.Mode, topology.Status, topology.ControlPlaneCapableNodes, topology.ReadyControlPlaneCapableNodes, topology.QuorumReady),
		Message:  strings.Join(append([]string{}, append(topology.RiskWarnings, topology.MissingRedundancy...)...), "; "),
		Evidence: map[string]string{
			"mode":                  topology.Mode,
			"status":                topology.Status,
			"endpoint_mode":         topology.EndpointMode,
			"capable_nodes":         fmt.Sprintf("%d", topology.ControlPlaneCapableNodes),
			"ready_capable_nodes":   fmt.Sprintf("%d", topology.ReadyControlPlaneCapableNodes),
			"etcd_voter_capable":    fmt.Sprintf("%d", topology.EtcdVoterCapableNodes),
			"release_runner_nodes":  fmt.Sprintf("%d", topology.ReleaseRunnerCapableNodes),
			"failure_domain_count":  fmt.Sprintf("%d", len(topology.FailureDomains)),
			"single_node_not_block": fmt.Sprintf("%t", topology.Status == model.ControlPlaneTopologyStatusSingleNodeRisk),
		},
		RepairHint: "use fugue admin cluster status --json to inspect topology; add control-plane-capable nodes when physical capacity exists",
	}
}

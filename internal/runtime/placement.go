package runtime

import (
	"strings"

	"fugue/internal/model"
)

const (
	TenantTaintKey              = "fugue.io/tenant"
	TenantIDLabelKey            = "fugue.io/tenant-id"
	RuntimeIDLabelKey           = "fugue.io/runtime-id"
	NodeModeLabelKey            = "fugue.io/node-mode"
	NodeKeyIDLabelKey           = "fugue.io/node-key-id"
	MachineIDLabelKey           = "fugue.io/machine-id"
	MachineScopeLabelKey        = "fugue.io/machine-scope"
	SharedPoolLabelKey          = "fugue.io/shared-pool"
	SharedPoolLabelValue        = "internal"
	BuildNodeLabelKey           = "fugue.io/build"
	BuildNodeLabelValue         = "true"
	AppRuntimeRoleLabelKey      = "fugue.io/role.app-runtime"
	BuilderRoleLabelKey         = "fugue.io/role.builder"
	EdgeRoleLabelKey            = "fugue.io/role.edge"
	DNSRoleLabelKey             = "fugue.io/role.dns"
	InternalMaintenanceLabelKey = "fugue.io/role.internal-maintenance"
	NodeRoleLabelValue          = "true"
	DedicatedTaintKey           = "fugue.io/dedicated"
	DedicatedEdgeValue          = "edge"
	DedicatedDNSValue           = "dns"
	DedicatedInternalValue      = "internal"
	NodeSchedulableLabelKey     = "fugue.io/schedulable"
	NodeHealthLabelKey          = "fugue.io/node-health"
	NodeHealthReadyValue        = "ready"
	NodeHealthBlockedValue      = "blocked"
	NodeUnhealthyTaintKey       = "fugue.io/node-unhealthy"
	NodeUnhealthyTaintValue     = "true"
	ControlPlaneDesiredRoleKey  = "fugue.io/control-plane-desired-role"
	RegionLabelKey              = "topology.kubernetes.io/region"
	LegacyRegionLabelKey        = "failure-domain.beta.kubernetes.io/region"
	ZoneLabelKey                = "topology.kubernetes.io/zone"
	LegacyZoneLabelKey          = "failure-domain.beta.kubernetes.io/zone"
	LocationCountryCodeLabelKey = "fugue.io/location-country-code"
)

type Toleration struct {
	Key      string `json:"key,omitempty"`
	Operator string `json:"operator,omitempty"`
	Value    string `json:"value,omitempty"`
	Effect   string `json:"effect,omitempty"`
}

type SchedulingConstraints struct {
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	Tolerations  []Toleration      `json:"tolerations,omitempty"`
}

func JoinNodeLabels(runtimeObj model.Runtime) []string {
	labels := []string{
		RuntimeIDLabelKey + "=" + runtimeObj.ID,
		TenantIDLabelKey + "=" + runtimeObj.TenantID,
		NodeModeLabelKey + "=" + runtimeObj.Type,
	}
	if runtimeUsesInternalSharedPool(runtimeObj) {
		labels = append(labels, SharedPoolLabelKey+"="+SharedPoolLabelValue)
	}
	return labels
}

func JoinNodeLabelMap(runtimeObj model.Runtime) map[string]string {
	labels := map[string]string{
		RuntimeIDLabelKey: runtimeObj.ID,
		TenantIDLabelKey:  runtimeObj.TenantID,
		NodeModeLabelKey:  runtimeObj.Type,
	}
	if runtimeUsesInternalSharedPool(runtimeObj) {
		labels[SharedPoolLabelKey] = SharedPoolLabelValue
	}
	return labels
}

func JoinNodeTaints(runtimeObj model.Runtime) []string {
	if runtimeObj.TenantID == "" || runtimeUsesInternalSharedPool(runtimeObj) {
		return nil
	}
	return []string{
		TenantTaintKey + "=" + runtimeObj.TenantID + ":NoSchedule",
	}
}

func SchedulingForRuntime(runtimeObj model.Runtime) SchedulingConstraints {
	switch runtimeObj.Type {
	case model.RuntimeTypeManagedOwned:
		constraints := SchedulingConstraints{
			NodeSelector: map[string]string{
				RuntimeIDLabelKey: runtimeObj.ID,
				TenantIDLabelKey:  runtimeObj.TenantID,
			},
		}
		if runtimeObj.TenantID != "" && !runtimeUsesInternalSharedPool(runtimeObj) {
			constraints.Tolerations = []Toleration{
				{
					Key:      TenantTaintKey,
					Operator: "Equal",
					Value:    runtimeObj.TenantID,
					Effect:   "NoSchedule",
				},
			}
		}
		return constraints
	case model.RuntimeTypeManagedShared:
		return SchedulingConstraints{NodeSelector: ManagedSharedNodeSelector(runtimeObj)}
	default:
		return SchedulingConstraints{}
	}
}

func ManagedSharedNodeSelector(runtimeObj model.Runtime) map[string]string {
	selector := map[string]string{
		SharedPoolLabelKey: SharedPoolLabelValue,
	}
	for key, value := range PlacementNodeSelector(runtimeObj) {
		selector[key] = value
	}
	return selector
}

func PlacementNodeSelector(runtimeObj model.Runtime) map[string]string {
	return PlacementNodeSelectorForLabels(runtimeObj.Labels)
}

func PlacementNodeSelectorForLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}

	selector := map[string]string{}
	if value := firstPlacementLabelValue(labels, RegionLabelKey, LegacyRegionLabelKey, "region"); value != "" {
		selector[RegionLabelKey] = value
	}
	if value := firstPlacementLabelValue(labels, ZoneLabelKey, LegacyZoneLabelKey, "zone"); value != "" {
		selector[ZoneLabelKey] = value
	}
	if value := firstPlacementLabelValue(labels, LocationCountryCodeLabelKey, "country_code", "countryCode"); value != "" {
		selector[LocationCountryCodeLabelKey] = strings.ToLower(value)
	}
	if len(selector) == 0 {
		return nil
	}
	return selector
}

func firstPlacementLabelValue(labels map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(labels[key]); value != "" {
			return value
		}
	}
	return ""
}

func runtimeUsesInternalSharedPool(runtimeObj model.Runtime) bool {
	return model.NormalizeRuntimePoolMode(runtimeObj.Type, runtimeObj.PoolMode) == model.RuntimePoolModeInternalShared
}

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
	return []string{
		RuntimeIDLabelKey + "=" + runtimeObj.ID,
		TenantIDLabelKey + "=" + runtimeObj.TenantID,
		NodeModeLabelKey + "=" + runtimeObj.Type,
	}
}

func JoinNodeTaints(runtimeObj model.Runtime) []string {
	if runtimeObj.TenantID == "" {
		return nil
	}
	return []string{
		TenantTaintKey + "=" + runtimeObj.TenantID + ":NoSchedule",
	}
}

func SchedulingForRuntime(runtimeObj model.Runtime) SchedulingConstraints {
	switch runtimeObj.Type {
	case model.RuntimeTypeManagedOwned:
		return SchedulingConstraints{
			NodeSelector: map[string]string{
				RuntimeIDLabelKey: runtimeObj.ID,
				TenantIDLabelKey:  runtimeObj.TenantID,
			},
			Tolerations: []Toleration{
				{
					Key:      TenantTaintKey,
					Operator: "Equal",
					Value:    runtimeObj.TenantID,
					Effect:   "NoSchedule",
				},
			},
		}
	case model.RuntimeTypeManagedShared:
		selector := PlacementNodeSelector(runtimeObj)
		if len(selector) == 0 {
			return SchedulingConstraints{}
		}
		return SchedulingConstraints{NodeSelector: selector}
	default:
		return SchedulingConstraints{}
	}
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

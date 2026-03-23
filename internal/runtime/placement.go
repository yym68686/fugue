package runtime

import "fugue/internal/model"

const (
	TenantTaintKey    = "fugue.io/tenant"
	TenantIDLabelKey  = "fugue.io/tenant-id"
	RuntimeIDLabelKey = "fugue.io/runtime-id"
	NodeModeLabelKey  = "fugue.io/node-mode"
)

type Toleration struct {
	Key      string
	Operator string
	Value    string
	Effect   string
}

type SchedulingConstraints struct {
	NodeSelector map[string]string
	Tolerations  []Toleration
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
	if runtimeObj.Type != model.RuntimeTypeManagedOwned {
		return SchedulingConstraints{}
	}
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
}

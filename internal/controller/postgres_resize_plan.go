package controller

import (
	"fmt"
	"strings"

	"fugue/internal/config"
)

const (
	managedPostgresResizeStageLimitUpscale     = "limit_upscale"
	managedPostgresResizeStageRequestUpscale   = "request_upscale"
	managedPostgresResizeStageRequestDownscale = "request_downscale"
	managedPostgresResizeStageLimitDownscale   = "limit_downscale"
)

type managedPostgresResizePlanStage struct {
	Name             string
	Resources        kubeResourceRequirements
	ChangedResources []string
}

// planManagedPostgresResizeStages produces transitions that are valid at every
// intermediate point. Limits move up before requests, requests move down before
// limits, and increases and decreases never share one Kubernetes /resize call.
// This keeps a partial operation restartable from the live Pod resources.
func planManagedPostgresResizeStages(
	current, target kubeResourceRequirements,
) ([]managedPostgresResizePlanStage, error) {
	if err := validateCompleteManagedPostgresResizeEnvelope(current); err != nil {
		return nil, fmt.Errorf("validate current managed postgres resources: %w", err)
	}
	if err := validateCompleteManagedPostgresResizeEnvelope(target); err != nil {
		return nil, fmt.Errorf("validate target managed postgres resources: %w", err)
	}

	cursor := cloneKubeResourceRequirements(current)
	stages := make([]managedPostgresResizePlanStage, 0, 4)
	appendStage := func(name string, update func(*kubeResourceRequirements)) error {
		before := cloneKubeResourceRequirements(cursor)
		update(&cursor)
		changed, _, _ := compareResizeResources(before, cursor)
		if len(changed) == 0 {
			return nil
		}
		if err := validateCompleteManagedPostgresResizeEnvelope(cursor); err != nil {
			return fmt.Errorf("validate managed postgres resize stage %s: %w", name, err)
		}
		stages = append(stages, managedPostgresResizePlanStage{
			Name:             name,
			Resources:        managedPostgresCPUAndMemoryEnvelope(cursor),
			ChangedResources: append([]string(nil), changed...),
		})
		return nil
	}

	if err := appendStage(managedPostgresResizeStageLimitUpscale, func(next *kubeResourceRequirements) {
		copyManagedPostgresResizeDirection(next.Limits, cursor.Limits, target.Limits, true)
	}); err != nil {
		return nil, err
	}
	if err := appendStage(managedPostgresResizeStageRequestUpscale, func(next *kubeResourceRequirements) {
		copyManagedPostgresResizeDirection(next.Requests, cursor.Requests, target.Requests, true)
	}); err != nil {
		return nil, err
	}
	if err := appendStage(managedPostgresResizeStageRequestDownscale, func(next *kubeResourceRequirements) {
		copyManagedPostgresResizeDirection(next.Requests, cursor.Requests, target.Requests, false)
	}); err != nil {
		return nil, err
	}
	if err := appendStage(managedPostgresResizeStageLimitDownscale, func(next *kubeResourceRequirements) {
		copyManagedPostgresResizeDirection(next.Limits, cursor.Limits, target.Limits, false)
	}); err != nil {
		return nil, err
	}

	if !managedPostgresResizeResourcesEqual(cursor, target) {
		return nil, fmt.Errorf("managed postgres resize plan did not converge to the exact target")
	}
	return stages, nil
}

func copyManagedPostgresResizeDirection(
	destination, current, target map[string]string,
	increase bool,
) {
	for _, resourceName := range []string{"cpu", "memory"} {
		before, beforeOK := parseResizeQuantity(current[resourceName])
		after, afterOK := parseResizeQuantity(target[resourceName])
		if !beforeOK || !afterOK {
			continue
		}
		cmp := before.Cmp(after)
		if (increase && cmp < 0) || (!increase && cmp > 0) {
			destination[resourceName] = strings.TrimSpace(target[resourceName])
		}
	}
}

func managedPostgresCPUAndMemoryEnvelope(resources kubeResourceRequirements) kubeResourceRequirements {
	return kubeResourceRequirements{
		Requests: map[string]string{
			"cpu":    strings.TrimSpace(resources.Requests["cpu"]),
			"memory": strings.TrimSpace(resources.Requests["memory"]),
		},
		Limits: map[string]string{
			"cpu":    strings.TrimSpace(resources.Limits["cpu"]),
			"memory": strings.TrimSpace(resources.Limits["memory"]),
		},
	}
}

func validateCompleteManagedPostgresResizeEnvelope(resources kubeResourceRequirements) error {
	if err := validateMergedPodResourceRequirements(resources); err != nil {
		return err
	}
	for _, scope := range []struct {
		name   string
		values map[string]string
	}{
		{name: "requests", values: resources.Requests},
		{name: "limits", values: resources.Limits},
	} {
		for _, resourceName := range []string{"cpu", "memory"} {
			if _, ok := parseResizeQuantity(scope.values[resourceName]); !ok {
				return fmt.Errorf("%s.%s must be a positive explicit quantity", scope.name, resourceName)
			}
		}
	}
	return nil
}

func managedPostgresResizeResourcesEqual(left, right kubeResourceRequirements) bool {
	for _, scope := range []struct {
		left  map[string]string
		right map[string]string
	}{
		{left: left.Requests, right: right.Requests},
		{left: left.Limits, right: right.Limits},
	} {
		for _, resourceName := range []string{"cpu", "memory"} {
			leftQuantity, leftOK := parseResizeQuantity(scope.left[resourceName])
			rightQuantity, rightOK := parseResizeQuantity(scope.right[resourceName])
			if !leftOK || !rightOK || leftQuantity.Cmp(rightQuantity) != 0 {
				return false
			}
		}
	}
	return true
}

func managedPostgresResizeDirectionGate(
	resourceChange string,
	increase bool,
	gates config.ManagedPostgresInPlaceResizeConfig,
) (bool, string) {
	if !gates.Enabled {
		return false, "global_resize_disabled"
	}
	var enabled bool
	switch resourceChange {
	case "requests.cpu":
		if increase {
			enabled = gates.CPURequestUpscaleEnabled
		} else {
			enabled = gates.CPURequestDownscaleEnabled
		}
	case "requests.memory":
		if increase {
			enabled = gates.MemoryRequestUpscaleEnabled
		} else {
			enabled = gates.MemoryRequestDownscaleEnabled
		}
	case "limits.cpu":
		if increase {
			enabled = gates.CPULimitUpscaleEnabled
		} else {
			enabled = gates.CPULimitDownscaleEnabled
		}
	case "limits.memory":
		if increase {
			enabled = gates.MemoryLimitUpscaleEnabled
		} else {
			enabled = gates.MemoryLimitDownscaleEnabled
		}
	default:
		return false, "unsupported_resize_dimension"
	}
	if enabled {
		return true, "enabled"
	}
	direction := "downscale"
	if increase {
		direction = "upscale"
	}
	return false, strings.ReplaceAll(resourceChange, ".", "_") + "_" + direction + "_disabled"
}

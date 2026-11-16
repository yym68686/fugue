package controller

import (
	"fmt"
	"strconv"

	"fugue/internal/model"
)

// The target owns exactly one request. Old CPU journals remain readable.
type appRequestResizeTarget struct {
	Resource string
	Value    int64
}

func (t appRequestResizeTarget) quantity() string {
	if t.Resource == "memory" {
		return strconv.FormatInt(t.Value, 10) + "Mi"
	}
	return strconv.FormatInt(t.Value, 10) + "m"
}
func (t appRequestResizeTarget) requested() kubeResourceRequirements {
	return kubeResourceRequirements{Requests: map[string]string{t.Resource: t.quantity()}}
}
func appOperationRequestResizeTarget(op model.Operation) appRequestResizeTarget {
	if op.Type == model.OperationTypeAppMemoryResize {
		return appRequestResizeTarget{"memory", op.DesiredSpec.Resources.MemoryMebibytes}
	}
	return appRequestResizeTarget{"cpu", op.DesiredSpec.Resources.CPUMilliCores}
}
func (j appRequestResizeJournal) target() appRequestResizeTarget {
	if j.Memory > 0 {
		return appRequestResizeTarget{"memory", j.Memory}
	}
	return appRequestResizeTarget{"cpu", j.CPU}
}
func appRequestResizeEnvelopeMatches(current, baseline kubeResourceRequirements, target appRequestResizeTarget) bool {
	a, b := cloneKubeResourceRequirements(current), cloneKubeResourceRequirements(baseline)
	if !resizeQuantitiesEqual(a.Requests[target.Resource], b.Requests[target.Resource]) && !resizeQuantitiesEqual(a.Requests[target.Resource], target.quantity()) {
		return false
	}
	delete(a.Requests, target.Resource)
	delete(b.Requests, target.Resource)
	return appRequestResizeResourcesEqual(a, b)
}

func assessAppRequestResize(ob managedPostgresResizeObservation, target appRequestResizeTarget, pending bool) error {
	if ob.PodUID == "" || ob.ResourceVersion == "" || ob.DeletionTimestamp != "" || target.Value <= 0 || ob.PodResources != nil {
		return fmt.Errorf("request resize requires a live container-scoped Pod")
	}
	merged := cloneKubeResourceRequirements(ob.DesiredResources)
	if merged.Requests == nil || merged.Requests[target.Resource] == "" {
		return fmt.Errorf("missing %s resource request", target.Resource)
	}
	merged.Requests[target.Resource] = target.quantity()
	if appRequestResizeGuaranteed(ob, ob.DesiredResources) != appRequestResizeGuaranteed(ob, merged) {
		return fmt.Errorf("%s request resize would change Pod QoS class", target.Resource)
	}
	if pending {
		if ob.Phase != "Pending" {
			return fmt.Errorf("replacement Pod is not Pending")
		}
		if err := validateMergedPodResourceRequirements(merged); err != nil {
			return err
		}
		return validateManagedPostgresResizePolicy(ob.ResizePolicy, merged, ob.DesiredResources)
	}
	if ob.ActualResources == nil || ob.ContainerStartedAt == "" {
		return fmt.Errorf("actual container resources/start time not observed")
	}
	assessment := assessManagedPostgresResize(ob, target.requested(), managedPostgresResizeSafetyOptions{AllowRequestDownscale: true})
	if assessment.State != managedPostgresResizeStateReady && assessment.State != managedPostgresResizeStateNoop {
		return fmt.Errorf("%s resize blocked: %s (%s)", target.Resource, assessment.Reason, assessment.Message)
	}
	return nil
}

func appRequestResizeConverged(ob managedPostgresResizeObservation, target appRequestResizeTarget) bool {
	return appRequestResizeQuantityConverged(ob, target.Resource, target.quantity())
}

func appRequestResizeQuantityConverged(ob managedPostgresResizeObservation, resourceName, quantity string) bool {
	if ob.ActualResources == nil || !ob.PodReady || !ob.ContainerReady || ob.Phase != "Running" || resizePendingCondition(ob.Conditions) != nil ||
		(ob.Generation > 0 && ob.ObservedGeneration < ob.Generation) ||
		!resizeQuantitiesEqual(ob.DesiredResources.Requests[resourceName], quantity) || !resizeQuantitiesEqual(ob.ActualResources.Requests[resourceName], quantity) {
		return false
	}
	if allocated := ob.AllocatedResources[resourceName]; allocated != "" && !resizeQuantitiesEqual(allocated, quantity) {
		return false
	}
	for _, scope := range []struct{ desired, actual map[string]string }{{ob.DesiredResources.Requests, ob.ActualResources.Requests}, {ob.DesiredResources.Limits, ob.ActualResources.Limits}} {
		for _, name := range []string{"cpu", "memory"} {
			if scope.desired[name] == "" && scope.actual[name] == "" {
				continue
			}
			if !resizeQuantitiesEqual(scope.desired[name], scope.actual[name]) {
				return false
			}
		}
	}
	return true
}

func assessAppRequestResizeBaseline(ob managedPostgresResizeObservation, target appRequestResizeTarget, baseline appRequestResizePodBaseline) error {
	if baseline.Workload != "" && baseline.NotReady {
		if baseline.Restarts > 0 {
			return fmt.Errorf("revision container is not ready after restarts; wait for a stable observation")
		}
		ob.PodReady, ob.ContainerReady = true, true
	}
	return assessAppRequestResize(ob, target, baseline.Pending && ob.Phase == "Pending")
}
func appRequestResizeBaselineConverged(ob managedPostgresResizeObservation, baseline appRequestResizePodBaseline, target appRequestResizeTarget) bool {
	if baseline.Workload != "" && baseline.Pending && ob.Phase == "Pending" && ob.ContainerStartedAt == "" && ob.RestartCount == 0 {
		return ob.DeletionTimestamp == "" && resizePendingCondition(ob.Conditions) == nil && resizeQuantitiesEqual(ob.DesiredResources.Requests[target.Resource], target.quantity())
	}
	if baseline.Workload != "" && baseline.NotReady {
		ob.PodReady, ob.ContainerReady = true, true
	}
	return appRequestResizeConverged(ob, target)
}
func appRequestResizeOriginalConverged(ob managedPostgresResizeObservation, baseline appRequestResizePodBaseline, resourceName string) bool {
	quantity := baseline.Resources.Requests[resourceName]
	if baseline.Workload != "" && baseline.Pending && ob.Phase == "Pending" && ob.ContainerStartedAt == "" && ob.RestartCount == 0 {
		return ob.DeletionTimestamp == "" && resizePendingCondition(ob.Conditions) == nil && resizeQuantitiesEqual(ob.DesiredResources.Requests[resourceName], quantity)
	}
	if baseline.Workload != "" && baseline.NotReady {
		ob.PodReady, ob.ContainerReady = true, true
	}
	return appRequestResizeQuantityConverged(ob, resourceName, quantity)
}

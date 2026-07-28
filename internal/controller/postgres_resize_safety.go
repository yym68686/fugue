package controller

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Resize states are deliberately independent from Kubernetes Pod phases. A
// database may be Running while a resize is Deferred or Infeasible, and those
// conditions must remain visible to the caller instead of being interpreted as
// a successful resize.
const (
	managedPostgresResizeStateReady      = "Ready"
	managedPostgresResizeStateNoop       = "Noop"
	managedPostgresResizeStateBlocked    = "Blocked"
	managedPostgresResizeStateDeferred   = "Deferred"
	managedPostgresResizeStateInfeasible = "Infeasible"
	managedPostgresResizeStateInProgress = "InProgress"
	managedPostgresResizeStateFailed     = "Failed"
)

type managedPostgresResizeSafetyOptions struct {
	AllowRequestDownscale bool
	AllowLimitChanges     bool
	BaselineRestartCount  *int
}

type managedPostgresResizeAssessment struct {
	State             string
	Reason            string
	Message           string
	ChangedResources  []string
	IncreaseResources []string
	DecreaseResources []string
}

// assessManagedPostgresResize is the fail-closed policy gate used by future
// resize executors. It has no Kubernetes side effects. The gate intentionally
// permits only a complete, healthy Pod observation and rejects anything that
// could require a container restart or a destructive resource-map mutation.
func assessManagedPostgresResize(
	observation managedPostgresResizeObservation,
	target kubeResourceRequirements,
	options managedPostgresResizeSafetyOptions,
) managedPostgresResizeAssessment {
	assessment := managedPostgresResizeAssessment{State: managedPostgresResizeStateBlocked}
	if strings.TrimSpace(observation.PodUID) == "" || strings.TrimSpace(observation.ResourceVersion) == "" {
		return blockedResizeAssessment("missing_identity", "resize requires a Pod UID and resourceVersion")
	}
	if strings.TrimSpace(observation.Phase) != "Running" || !observation.PodReady || !observation.ContainerReady {
		return blockedResizeAssessment("pod_not_ready", "resize requires a Running and Ready PostgreSQL Pod")
	}
	if options.BaselineRestartCount != nil && observation.RestartCount != *options.BaselineRestartCount {
		return blockedResizeAssessment("restart_detected", fmt.Sprintf("container restart count changed from %d to %d", *options.BaselineRestartCount, observation.RestartCount))
	}
	if pending := resizePendingCondition(observation.Conditions); pending != nil {
		state := managedPostgresResizeStateInProgress
		reason := "kubernetes_resize_pending"
		switch strings.ToLower(strings.TrimSpace(pending.Reason)) {
		case "deferred":
			state = managedPostgresResizeStateDeferred
			reason = "kubernetes_resize_deferred"
		case "infeasible":
			state = managedPostgresResizeStateInfeasible
			reason = "kubernetes_resize_infeasible"
		}
		return managedPostgresResizeAssessment{State: state, Reason: reason, Message: pending.Message}
	}
	if err := validatePodResizeResources(target); err != nil {
		return blockedResizeAssessment("invalid_target", err.Error())
	}
	current := observation.DesiredResources
	if observation.ActualResources != nil {
		current = *observation.ActualResources
	}
	merged, err := mergePodResizeResources(current, target)
	if err != nil {
		return blockedResizeAssessment("invalid_target", err.Error())
	}
	if err := validateManagedPostgresResizePolicy(observation.ResizePolicy, merged, current); err != nil {
		return blockedResizeAssessment("restart_policy", err.Error())
	}
	changed, increases, decreases := compareResizeResources(current, merged)
	assessment.ChangedResources = changed
	assessment.IncreaseResources = increases
	assessment.DecreaseResources = decreases
	if len(changed) == 0 {
		assessment.State = managedPostgresResizeStateNoop
		assessment.Reason = "already_current"
		assessment.Message = "Pod resources already match the requested target"
		return assessment
	}
	if !options.AllowRequestDownscale && hasRequestDownscale(decreases) {
		return blockedResizeAssessment("request_downscale_disabled", "request downscale is disabled by policy")
	}
	if hasRequestDownscale(decreases) {
		if reason, message := ineffectiveManagedPostgresRequestDownscale(observation, current, merged, decreases); reason != "" {
			return blockedResizeAssessment(reason, message)
		}
	}
	if !options.AllowLimitChanges && hasLimitChange(changed) {
		return blockedResizeAssessment("limit_resize_disabled", "limit changes are disabled by policy")
	}
	assessment.State = managedPostgresResizeStateReady
	assessment.Reason = "safe_in_place_resize"
	assessment.Message = "Pod resize passed the no-restart safety gate"
	return assessment
}

// ineffectiveManagedPostgresRequestDownscale mirrors Kubernetes' effective Pod
// request calculation, including restartable init containers. A container-level
// request downscale can succeed through /resize while releasing no schedulable
// capacity because a non-resizable init container (or Pod-level request) remains
// the effective floor. Such a transition must fail closed instead of reporting
// capacity that the scheduler cannot actually reuse.
func ineffectiveManagedPostgresRequestDownscale(
	observation managedPostgresResizeObservation,
	current, target kubeResourceRequirements,
	decreases []string,
) (string, string) {
	if observation.PodResources != nil {
		for _, resourceName := range []string{"cpu", "memory"} {
			if _, set := parseResizeQuantity(observation.PodResources.Requests[resourceName]); set && containsResizeResource(decreases, "requests."+resourceName) {
				return "pod_level_request_floor", fmt.Sprintf("%s request downscale cannot prove schedulable capacity recovery while a Pod-level request is set", resourceName)
			}
		}
	}

	before, err := managedPostgresEffectivePodRequests(observation, current)
	if err != nil {
		return "effective_request_unknown", err.Error()
	}
	after, err := managedPostgresEffectivePodRequests(observation, target)
	if err != nil {
		return "effective_request_unknown", err.Error()
	}
	for _, resourceName := range []string{"cpu", "memory"} {
		if !containsResizeResource(decreases, "requests."+resourceName) {
			continue
		}
		beforeQuantity, beforeOK := before[resourceName]
		afterQuantity, afterOK := after[resourceName]
		if !beforeOK || !afterOK {
			return "effective_request_unknown", fmt.Sprintf("cannot calculate effective Pod %s request", resourceName)
		}
		if afterQuantity.Cmp(beforeQuantity) >= 0 {
			return "ineffective_request_downscale", fmt.Sprintf(
				"%s container request downscale would not reduce the effective Pod request (%s before and %s after); another container or init-container request remains the effective floor",
				resourceName,
				beforeQuantity.String(),
				afterQuantity.String(),
			)
		}
	}
	return "", ""
}

func managedPostgresEffectivePodRequests(
	observation managedPostgresResizeObservation,
	target kubeResourceRequirements,
) (map[string]resource.Quantity, error) {
	containers := cloneKubeResizeContainerSpecs(observation.Containers)
	foundTarget := false
	for index := range containers {
		if strings.TrimSpace(containers[index].Name) != strings.TrimSpace(observation.ContainerName) {
			continue
		}
		containers[index].Resources = cloneKubeResourceRequirements(target)
		foundTarget = true
		break
	}
	if !foundTarget {
		return nil, fmt.Errorf("effective Pod request calculation cannot find target container %s", observation.ContainerName)
	}

	total := emptyResizeQuantities()
	for _, container := range containers {
		requests, err := resizeRequestQuantities(container.Resources.Requests, container.Name)
		if err != nil {
			return nil, err
		}
		addResizeQuantities(total, requests)
	}

	restartableInitTotal := emptyResizeQuantities()
	initMaximum := emptyResizeQuantities()
	for _, container := range observation.InitContainers {
		requests, err := resizeRequestQuantities(container.Resources.Requests, container.Name)
		if err != nil {
			return nil, err
		}
		stage := emptyResizeQuantities()
		if container.RestartPolicy != nil && strings.EqualFold(strings.TrimSpace(*container.RestartPolicy), "Always") {
			addResizeQuantities(total, requests)
			addResizeQuantities(restartableInitTotal, requests)
			copyResizeQuantities(stage, restartableInitTotal)
		} else {
			copyResizeQuantities(stage, restartableInitTotal)
			addResizeQuantities(stage, requests)
		}
		maxResizeQuantities(initMaximum, stage)
	}
	maxResizeQuantities(total, initMaximum)
	return total, nil
}

func emptyResizeQuantities() map[string]resource.Quantity {
	return map[string]resource.Quantity{
		"cpu":    resource.MustParse("0"),
		"memory": resource.MustParse("0"),
	}
}

func resizeRequestQuantities(requests map[string]string, containerName string) (map[string]resource.Quantity, error) {
	out := emptyResizeQuantities()
	for _, resourceName := range []string{"cpu", "memory"} {
		raw := strings.TrimSpace(requests[resourceName])
		if raw == "" {
			continue
		}
		quantity, err := resource.ParseQuantity(raw)
		if err != nil || quantity.Sign() < 0 {
			if err == nil {
				err = fmt.Errorf("quantity must not be negative")
			}
			return nil, fmt.Errorf("parse %s request for container %s: %w", resourceName, strings.TrimSpace(containerName), err)
		}
		out[resourceName] = quantity
	}
	return out, nil
}

func addResizeQuantities(destination, source map[string]resource.Quantity) {
	for _, resourceName := range []string{"cpu", "memory"} {
		quantity := destination[resourceName]
		quantity.Add(source[resourceName])
		destination[resourceName] = quantity
	}
}

func copyResizeQuantities(destination, source map[string]resource.Quantity) {
	for _, resourceName := range []string{"cpu", "memory"} {
		destination[resourceName] = source[resourceName].DeepCopy()
	}
}

func maxResizeQuantities(destination, candidate map[string]resource.Quantity) {
	for _, resourceName := range []string{"cpu", "memory"} {
		candidateQuantity := candidate[resourceName]
		destinationQuantity := destination[resourceName]
		if candidateQuantity.Cmp(destinationQuantity) > 0 {
			destination[resourceName] = candidateQuantity.DeepCopy()
		}
	}
}

func containsResizeResource(resources []string, target string) bool {
	for _, resourceName := range resources {
		if resourceName == target {
			return true
		}
	}
	return false
}

func blockedResizeAssessment(reason, message string) managedPostgresResizeAssessment {
	return managedPostgresResizeAssessment{
		State:   managedPostgresResizeStateBlocked,
		Reason:  strings.TrimSpace(reason),
		Message: strings.TrimSpace(message),
	}
}

func resizePendingCondition(conditions []managedPostgresResizeCondition) *managedPostgresResizeCondition {
	for _, condition := range conditions {
		if !strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
			continue
		}
		if strings.TrimSpace(condition.Type) != "PodResizePending" && strings.TrimSpace(condition.Type) != "PodResizeInProgress" {
			continue
		}
		copy := condition
		copy.Type = strings.TrimSpace(copy.Type)
		copy.Reason = strings.TrimSpace(copy.Reason)
		copy.Message = strings.TrimSpace(copy.Message)
		return &copy
	}
	return nil
}

func validateManagedPostgresResizePolicy(
	policies []kubeResizePolicy,
	target, current kubeResourceRequirements,
) error {
	changed, _, _ := compareResizeResources(current, target)
	if len(changed) == 0 {
		return nil
	}
	byResource := make(map[string]string, len(policies))
	for _, policy := range policies {
		name := strings.TrimSpace(policy.ResourceName)
		if name != "cpu" && name != "memory" {
			continue
		}
		byResource[name] = strings.TrimSpace(policy.RestartPolicy)
	}
	for _, name := range changed {
		resourceName := strings.TrimPrefix(name, "requests.")
		resourceName = strings.TrimPrefix(resourceName, "limits.")
		if policy := byResource[resourceName]; policy != "" && !strings.EqualFold(policy, "NotRequired") {
			return fmt.Errorf("Pod resize policy for %s is %s; only NotRequired is allowed", resourceName, policy)
		}
	}
	return nil
}

func compareResizeResources(current, target kubeResourceRequirements) (changed, increases, decreases []string) {
	for _, scope := range []struct {
		name   string
		before map[string]string
		after  map[string]string
	}{
		{name: "requests", before: current.Requests, after: target.Requests},
		{name: "limits", before: current.Limits, after: target.Limits},
	} {
		keys := make(map[string]struct{}, len(scope.before)+len(scope.after))
		for key := range scope.before {
			keys[key] = struct{}{}
		}
		for key := range scope.after {
			keys[key] = struct{}{}
		}
		ordered := make([]string, 0, len(keys))
		for key := range keys {
			if key == "cpu" || key == "memory" {
				ordered = append(ordered, key)
			}
		}
		sort.Strings(ordered)
		for _, key := range ordered {
			before, beforeOK := parseResizeQuantity(scope.before[key])
			after, afterOK := parseResizeQuantity(scope.after[key])
			if !beforeOK || !afterOK {
				// Missing dimensions are not a valid in-place transition. The
				// caller will receive a blocked result rather than a delete.
				if beforeOK != afterOK {
					name := scope.name + "." + key
					changed = append(changed, name)
					decreases = append(decreases, name)
				}
				continue
			}
			cmp := before.Cmp(after)
			if cmp == 0 {
				continue
			}
			name := scope.name + "." + key
			changed = append(changed, name)
			if cmp < 0 {
				increases = append(increases, name)
			} else {
				decreases = append(decreases, name)
			}
		}
	}
	return changed, increases, decreases
}

func parseResizeQuantity(raw string) (resource.Quantity, bool) {
	if strings.TrimSpace(raw) == "" {
		return resource.Quantity{}, false
	}
	quantity, err := resource.ParseQuantity(strings.TrimSpace(raw))
	if err != nil || quantity.Sign() <= 0 {
		return resource.Quantity{}, false
	}
	return quantity, true
}

func hasRequestDownscale(resources []string) bool {
	for _, name := range resources {
		if strings.HasPrefix(name, "requests.") {
			return true
		}
	}
	return false
}

func hasLimitChange(resources []string) bool {
	for _, name := range resources {
		if strings.HasPrefix(name, "limits.") {
			return true
		}
	}
	return false
}

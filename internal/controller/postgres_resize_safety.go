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
	if !options.AllowLimitChanges && hasLimitChange(changed) {
		return blockedResizeAssessment("limit_resize_disabled", "limit changes are disabled by policy")
	}
	assessment.State = managedPostgresResizeStateReady
	assessment.Reason = "safe_in_place_resize"
	assessment.Message = "Pod resize passed the no-restart safety gate"
	return assessment
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

// Package resourcesizing owns measured resource recommendations and automatic resize policy.
package resourcesizing

import (
	"math"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

// UsageSamples is a read-only callback scoped to a tenant and one sample stream.
type UsageSamples func(tenantID, targetKind, targetID string, since time.Time) ([]model.ResourceUsageSample, error)

// SampleTargetKind selects the versioned per-replica stream, not aggregate usage.
func SampleTargetKind(targetKind string) string {
	switch kind := strings.TrimSpace(targetKind); kind {
	case model.ClusterNodeWorkloadKindApp:
		return "right_sizing_app_per_replica_v1"
	case model.ClusterNodeWorkloadKindBackingService:
		return "right_sizing_backing_service_per_replica_v1"
	default:
		return kind
	}
}

// RecommendApp reads usage for the app and its own managed PostgreSQL services.
func RecommendApp(app model.App, windowHours, minSamples int, since time.Time, list UsageSamples) (model.AppRightSizingRecommendation, error) {
	samples, err := list(app.TenantID, SampleTargetKind(model.ClusterNodeWorkloadKindApp), app.ID, since)
	if err != nil {
		return model.AppRightSizingRecommendation{}, err
	}
	out := model.AppRightSizingRecommendation{
		App: Recommend(model.ClusterNodeWorkloadKindApp, app.ID, app.Name, "", model.EffectiveWorkloadClass(app.Spec),
			windowHours, minSamples, app.Spec.Resources, samples),
	}
	for _, service := range app.BackingServices {
		if strings.TrimSpace(service.OwnerAppID) != strings.TrimSpace(app.ID) ||
			service.Type != model.BackingServiceTypePostgres || service.Spec.Postgres == nil {
			continue
		}
		samples, err := list(service.TenantID, SampleTargetKind(model.ClusterNodeWorkloadKindBackingService), service.ID, since)
		if err != nil {
			return model.AppRightSizingRecommendation{}, err
		}
		current := model.ManagedPostgresRuntimeResources(*service.Spec.Postgres)
		out.BackingServices = append(out.BackingServices, Recommend(model.ClusterNodeWorkloadKindBackingService, service.ID, service.Name,
			service.Type, model.WorkloadClassCritical, windowHours, minSamples, &current, samples))
	}
	return out, nil
}

// Recommend computes measured capacity and request targets from time-ordered samples.
func Recommend(
	targetKind, targetID, targetName, serviceType, workloadClass string,
	windowHours, minSamples int,
	current *model.ResourceSpec,
	samples []model.ResourceUsageSample,
) model.ResourceRightSizingRecommendation {
	workloadClass = model.NormalizeWorkloadClass(workloadClass)
	if workloadClass == "" {
		workloadClass = model.WorkloadClassService
	}
	policy := rightSizingPolicy(workloadClass, serviceType, windowHours, minSamples)
	out := model.ResourceRightSizingRecommendation{
		TargetKind:    strings.TrimSpace(targetKind),
		TargetID:      strings.TrimSpace(targetID),
		TargetName:    strings.TrimSpace(targetName),
		ServiceType:   strings.TrimSpace(serviceType),
		WorkloadClass: workloadClass,
		WindowHours:   windowHours,
		SampleCount:   len(samples),
		Current:       model.CloneResourceSpec(current),
		Policy:        policy,
		Reason:        "not enough samples",
	}
	now := time.Now().UTC()
	out.ObservedAt = &now
	if len(samples) > 0 {
		last := samples[len(samples)-1].ObservedAt
		out.LastSampleObservedAt = &last
	}
	if len(samples) < minSamples {
		return out
	}

	cpuValues := make([]int64, 0, len(samples))
	memoryValues := make([]int64, 0, len(samples))
	for _, sample := range samples {
		if sample.CPUMilliCores != nil {
			cpuValues = append(cpuValues, *sample.CPUMilliCores)
		}
		if sample.MemoryBytes != nil {
			memoryValues = append(memoryValues, *sample.MemoryBytes)
		}
	}
	if len(cpuValues) == 0 && len(memoryValues) == 0 {
		out.Reason = "samples do not include CPU or memory usage"
		return out
	}
	// Capacity is evidence-only: an unobserved dimension stays unknown instead
	// of being mislabeled with the current Kubernetes request.
	rec := &model.ResourceSpec{}
	requestTarget := &model.ResourceSpec{}
	if current != nil {
		*requestTarget = *current
	}
	if len(cpuValues) > 0 {
		sort.Slice(cpuValues, func(i, j int) bool { return cpuValues[i] < cpuValues[j] })
		peak := cpuValues[len(cpuValues)-1]
		out.PeakCPUUsageMilli = &peak
		value := int64(math.Ceil(float64(percentileInt64(cpuValues, policy.CPUPercentile)) * policy.CPUMultiplier))
		rec.CPUMilliCores = max(policy.CPUFloorMilli, roundUp(value, 5))
		rec.CPULimitMilliCores = 0

		requestValue := int64(math.Ceil(float64(percentileInt64(cpuValues, policy.CPURequestPercentile)) * policy.CPURequestMultiplier))
		requestTarget.CPUMilliCores = max(policy.CPUFloorMilli, roundUp(requestValue, 5))
		// Preserve explicit CPU limits. The runtime no longer synthesizes a
		// limit from the request, but right-sizing must not erase user intent.
		if requestTarget.CPULimitMilliCores > 0 && requestTarget.CPULimitMilliCores < requestTarget.CPUMilliCores {
			requestTarget.CPULimitMilliCores = requestTarget.CPUMilliCores
		}
	} else if requestTarget.CPUMilliCores <= 0 {
		requestTarget.CPUMilliCores = policy.CPUFloorMilli
	}
	if len(memoryValues) > 0 {
		sort.Slice(memoryValues, func(i, j int) bool { return memoryValues[i] < memoryValues[j] })
		peak := memoryValues[len(memoryValues)-1]
		out.PeakMemoryUsageBytes = &peak
		valueBytes := int64(math.Ceil(float64(percentileInt64(memoryValues, policy.MemoryPercentile)) * policy.MemoryMultiplier))
		valueMiB := int64(math.Ceil(float64(valueBytes) / 1024 / 1024))
		rec.MemoryMebibytes = max(policy.MemoryFloorMiB, roundUp(valueMiB, 16))
		if serviceType == model.BackingServiceTypePostgres {
			rec.MemoryLimitMebibytes = roundUp(rec.MemoryMebibytes+128, 16)
		} else if workloadClass == model.WorkloadClassCritical {
			rec.MemoryLimitMebibytes = rec.MemoryMebibytes
		} else {
			rec.MemoryLimitMebibytes = max(rec.MemoryMebibytes+128, rec.MemoryMebibytes*2)
		}
		requestTarget.MemoryMebibytes = rec.MemoryMebibytes
		requestTarget.MemoryLimitMebibytes = rec.MemoryLimitMebibytes
	}
	out.Recommended = rec
	out.RequestTarget = requestTarget
	out.Ready = true
	out.AlreadyCurrent = equalResources(current, requestTarget)
	out.Reason = ""
	return out
}

func rightSizingPolicy(workloadClass, serviceType string, windowHours, minSamples int) model.ResourceRightSizingPolicy {
	policy := model.ResourceRightSizingPolicy{
		WindowHours:          windowHours,
		MinSamples:           minSamples,
		CPUPercentile:        0.95,
		CPUMultiplier:        1.5,
		CPURequestPercentile: 0.50,
		CPURequestMultiplier: 1.0,
		CPUFloorMilli:        1,
		MemoryPercentile:     0.99,
		MemoryMultiplier:     1.2,
		MemoryFloorMiB:       64,
	}
	switch workloadClass {
	case model.WorkloadClassDemo:
		policy.MemoryFloorMiB = 32
	case model.WorkloadClassBatch:
		policy.MemoryFloorMiB = 64
	case model.WorkloadClassCritical:
		policy.CPURequestPercentile = 0.75
		policy.MemoryFloorMiB = 128
	}
	if serviceType == model.BackingServiceTypePostgres {
		policy.CPURequestPercentile = 0.75
		policy.MemoryFloorMiB = 256
		policy.MemoryMultiplier = 1.5
	}
	return policy
}

func percentileInt64(sorted []int64, percentile float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if percentile <= 0 {
		return sorted[0]
	}
	if percentile >= 1 {
		return sorted[len(sorted)-1]
	}
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func roundUp(value, step int64) int64 {
	if step <= 1 || value <= 0 {
		return value
	}
	remainder := value % step
	if remainder == 0 {
		return value
	}
	return value + step - remainder
}

func equalResources(left, right *model.ResourceSpec) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.CPUMilliCores == right.CPUMilliCores &&
		left.MemoryMebibytes == right.MemoryMebibytes &&
		left.CPULimitMilliCores == right.CPULimitMilliCores &&
		left.MemoryLimitMebibytes == right.MemoryLimitMebibytes
}

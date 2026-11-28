package resourcesizing

import (
	"math"
	"time"

	"fugue/internal/model"
)

// HasRecentAutomaticFailure matches failed automatic operations at the
// requested target. In-place memory operations compare only their memory request.
func HasRecentAutomaticFailure(ops []model.Operation, desired *model.ResourceSpec, since time.Time) bool {
	if desired == nil {
		return false
	}
	for _, op := range ops {
		if (op.Type != model.OperationTypeDeploy && !model.IsAppRequestResize(op.Type)) || op.Status != model.OperationStatusFailed ||
			op.RequestedByType != model.ActorTypeSystem ||
			(op.RequestedByID != model.OperationRequestedByRightSizing && op.RequestedByID != model.OperationRequestedByRightSizingDownscale) {
			continue
		}
		at := op.CompletedAt
		if at == nil {
			at = &op.UpdatedAt
		}
		if at.Before(since) || op.DesiredSpec == nil || op.DesiredSpec.Resources == nil {
			continue
		}
		if op.Type == model.OperationTypeAppMemoryResize {
			if op.DesiredSpec.Resources.MemoryMebibytes == desired.MemoryMebibytes {
				return true
			}
		} else if equalResources(op.DesiredSpec.Resources, desired) {
			return true
		}
	}
	return false
}

const (
	rightSizingAutoApplyMinCPUIncrease  = int64(5)
	rightSizingAutoApplyMinMemIncrease  = int64(0)
	rightSizingAutoApplyMinCPUIncreaseR = 0.05
	rightSizingAutoApplyMinMemIncreaseR = 0.05
	rightSizingAutoApplyMinCPUDecrease  = int64(5)
	rightSizingAutoApplyMinMemDecrease  = int64(0)
	rightSizingAutoApplyMinCPUDecreaseR = 0.05
	rightSizingAutoApplyMinMemDecreaseR = 0.05
	rightSizingAutoDownscaleStepRatio   = 0.75
)

type Decision struct {
	Allowed         bool
	Downscale       bool
	MemoryDownscale bool
	RequestedByID   string
	Resources       *model.ResourceSpec
}

func AutomaticChange(current, recommended *model.ResourceSpec) Decision {
	decision := Decision{
		RequestedByID: model.OperationRequestedByRightSizing,
	}
	if recommended == nil {
		return decision
	}
	// A nil resources block means the renderer emits no requests (zero), not
	// the historical managed default. Treat it as zero so auto right-sizing
	// cannot mistake an unset request for a downscale and inflate it silently.
	effectiveCurrent := model.ResourceSpec{}
	if current != nil {
		effectiveCurrent = *current
	}
	if equalResources(&effectiveCurrent, recommended) {
		return decision
	}
	materialCPUIncrease := materialResourceIncrease(
		effectiveCurrent.CPUMilliCores,
		recommended.CPUMilliCores,
		rightSizingAutoApplyMinCPUIncrease,
		rightSizingAutoApplyMinCPUIncreaseR,
	)
	materialMemoryIncrease := materialResourceIncrease(
		effectiveCurrent.MemoryMebibytes,
		recommended.MemoryMebibytes,
		rightSizingAutoApplyMinMemIncrease,
		rightSizingAutoApplyMinMemIncreaseR,
	)
	materialCPUDecrease := materialResourceDecrease(
		effectiveCurrent.CPUMilliCores,
		recommended.CPUMilliCores,
		rightSizingAutoApplyMinCPUDecrease,
		rightSizingAutoApplyMinCPUDecreaseR,
	)
	materialMemoryDecrease := materialResourceDecrease(
		effectiveCurrent.MemoryMebibytes,
		recommended.MemoryMebibytes,
		rightSizingAutoApplyMinMemDecrease,
		rightSizingAutoApplyMinMemDecreaseR,
	)
	if materialCPUIncrease || materialMemoryIncrease {
		decision.Allowed = true
		decision.Resources = autoRightSizingUpscaleTarget(effectiveCurrent, *recommended)
		if materialCPUDecrease {
			// A memory safety increase must not keep an inflated CPU reservation.
			decision.Resources.CPUMilliCores = recommended.CPUMilliCores
		}
		return decision
	}
	if materialCPUDecrease || materialMemoryDecrease {
		target := autoRightSizingDownscaleTarget(
			effectiveCurrent,
			*recommended,
			materialCPUDecrease,
			materialMemoryDecrease,
		)
		if equalResources(&effectiveCurrent, target) {
			return decision
		}
		decision.Allowed = true
		decision.Downscale = true
		decision.MemoryDownscale = materialMemoryDecrease
		decision.RequestedByID = model.OperationRequestedByRightSizingDownscale
		decision.Resources = target
		return decision
	}
	return decision
}

func materialResourceIncrease(current, recommended, minIncrease int64, minRatio float64) bool {
	if recommended <= current {
		return false
	}
	if current <= 0 {
		return true
	}
	increase := recommended - current
	return increase >= minIncrease && float64(increase)/float64(current) >= minRatio
}

func materialResourceDecrease(current, recommended, minDecrease int64, minRatio float64) bool {
	if recommended >= current {
		return false
	}
	if current <= 0 {
		return false
	}
	decrease := current - recommended
	return decrease >= minDecrease && float64(decrease)/float64(current) >= minRatio
}

func autoRightSizingUpscaleTarget(current, recommended model.ResourceSpec) *model.ResourceSpec {
	return &model.ResourceSpec{
		CPUMilliCores:        max(current.CPUMilliCores, recommended.CPUMilliCores),
		MemoryMebibytes:      max(current.MemoryMebibytes, recommended.MemoryMebibytes),
		CPULimitMilliCores:   max(current.CPULimitMilliCores, recommended.CPULimitMilliCores),
		MemoryLimitMebibytes: max(current.MemoryLimitMebibytes, recommended.MemoryLimitMebibytes),
	}
}

func autoRightSizingDownscaleTarget(current, recommended model.ResourceSpec, downscaleCPU, downscaleMemory bool) *model.ResourceSpec {
	target := current
	if downscaleCPU && recommended.CPUMilliCores < current.CPUMilliCores {
		// CPU is compressible. Converge directly to the measured guarantee;
		// gradual reduction is retained only for memory where OOM risk exists.
		target.CPUMilliCores = recommended.CPUMilliCores
	}
	if downscaleMemory && recommended.MemoryMebibytes < current.MemoryMebibytes {
		floor := roundUp(int64(math.Ceil(float64(current.MemoryMebibytes)*rightSizingAutoDownscaleStepRatio)), 16)
		// Creation defaults are not right-sizing floors. Keep the conservative
		// step, but never let its rounding prevent a material reduction at small
		// requests. The recommendation already carries the workload usage policy.
		floor = min(floor, current.MemoryMebibytes-1)
		target.MemoryMebibytes = max(recommended.MemoryMebibytes, floor)
		if recommended.MemoryLimitMebibytes < current.MemoryLimitMebibytes {
			target.MemoryLimitMebibytes = recommended.MemoryLimitMebibytes
		}
		if target.MemoryLimitMebibytes > 0 && target.MemoryLimitMebibytes < current.MemoryLimitMebibytes {
			target.MemoryLimitMebibytes = max(target.MemoryMebibytes+128, target.MemoryMebibytes*2)
			floor := roundUp(int64(math.Ceil(float64(current.MemoryLimitMebibytes)*rightSizingAutoDownscaleStepRatio)), 16)
			target.MemoryLimitMebibytes = max(target.MemoryLimitMebibytes, floor)
			target.MemoryLimitMebibytes = min(target.MemoryLimitMebibytes, current.MemoryLimitMebibytes)
		}
	}
	return &target
}

// Do not silently drop simultaneous memory or limit recommendations. Those
// continue through the existing deployment path with their complete target.
func CPURequestOnlyChange(current, target *model.ResourceSpec) bool {
	if current == nil || target == nil || current.CPUMilliCores == target.CPUMilliCores {
		return false
	}
	other := *target
	other.CPUMilliCores = current.CPUMilliCores
	return equalResources(current, &other)
}

func AutomaticCPURequestChange(current, target *model.ResourceSpec) Decision {
	if current == nil || target == nil {
		return AutomaticChange(current, current)
	}
	cpuOnly := model.CloneResourceSpec(current)
	cpuOnly.CPUMilliCores = target.CPUMilliCores
	return AutomaticChange(current, cpuOnly)
}

func MemoryRequestOnlyChange(current, target *model.ResourceSpec) bool {
	if current == nil || target == nil || current.MemoryMebibytes == target.MemoryMebibytes {
		return false
	}
	other := *target
	other.MemoryMebibytes = current.MemoryMebibytes
	return equalResources(current, &other)
}

func AutomaticMemoryRequestChange(current, target *model.ResourceSpec) Decision {
	if current == nil || target == nil {
		return AutomaticChange(current, current)
	}
	memoryOnly := model.CloneResourceSpec(current)
	memoryOnly.MemoryMebibytes = target.MemoryMebibytes
	decision := AutomaticChange(current, memoryOnly)
	if decision.Resources != nil {
		// A request-only resize cannot authorize a limit increase, even when
		// the recommendation needs one. Defer that transition explicitly.
		if current.MemoryLimitMebibytes > 0 && decision.Resources.MemoryMebibytes > current.MemoryLimitMebibytes {
			decision.Allowed = false
		}
	}
	return decision
}

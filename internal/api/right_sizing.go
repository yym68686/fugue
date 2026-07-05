package api

import (
	"context"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

const (
	rightSizingAutoApplyInterval  = time.Hour
	defaultRightSizingWindowHours = 168
	defaultRightSizingMinSamples  = 12
	maxRightSizingWindowHours     = 168

	rightSizingAutoApplyRequestedByID     = model.OperationRequestedByRightSizing
	rightSizingAutoDownscaleRequestedByID = model.OperationRequestedByRightSizingDownscale
	rightSizingAutoApplyMinCPUIncrease    = int64(50)
	rightSizingAutoApplyMinMemIncrease    = int64(128)
	rightSizingAutoApplyMinCPUIncreaseR   = 0.25
	rightSizingAutoApplyMinMemIncreaseR   = 0.20
	rightSizingAutoApplyMinCPUDecrease    = int64(100)
	rightSizingAutoApplyMinMemDecrease    = int64(256)
	rightSizingAutoApplyMinCPUDecreaseR   = 0.20
	rightSizingAutoApplyMinMemDecreaseR   = 0.20
	rightSizingAutoDownscaleStepRatio     = 0.75
	rightSizingAutoDownscaleOOMWindow     = 48 * time.Hour
)

func (s *Server) handleGetAppResourceRecommendation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.read scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	windowHours := readRightSizingWindowHours(r, defaultRightSizingWindowHours)
	minSamples := readRightSizingMinSamples(r, defaultRightSizingMinSamples)
	recommendation, err := s.appResourceRecommendation(app, windowHours, minSamples)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"recommendation": recommendation})
}

func (s *Server) handleApplyAppResourceRecommendation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		WindowHours int `json:"window_hours,omitempty"`
		MinSamples  int `json:"min_samples,omitempty"`
	}
	if r.ContentLength != 0 {
		if err := httpx.DecodeJSON(r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	windowHours := normalizeRightSizingWindowHours(req.WindowHours)
	minSamples := normalizeRightSizingMinSamples(req.MinSamples)
	recommendation, op, alreadyCurrent, err := s.applyAppRightSizingRecommendation(r.Context(), app, windowHours, minSamples, principal.ActorType, principal.ActorID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	status := http.StatusAccepted
	if alreadyCurrent {
		status = http.StatusOK
	}
	response := map[string]any{
		"recommendation":  recommendation,
		"already_current": alreadyCurrent,
	}
	if op != nil {
		response["operation"] = sanitizeOperationForAPI(*op)
	}
	httpx.WriteJSON(w, status, response)
}

func (s *Server) appResourceRecommendation(app model.App, windowHours, minSamples int) (model.AppRightSizingRecommendation, error) {
	windowHours = normalizeRightSizingWindowHours(windowHours)
	minSamples = normalizeRightSizingMinSamples(minSamples)
	since := time.Now().UTC().Add(-time.Duration(windowHours) * time.Hour)

	appSamples, err := s.store.ListResourceUsageSamples(app.TenantID, model.ClusterNodeWorkloadKindApp, app.ID, since)
	if err != nil {
		return model.AppRightSizingRecommendation{}, err
	}
	out := model.AppRightSizingRecommendation{
		App: buildRightSizingRecommendation(
			model.ClusterNodeWorkloadKindApp,
			app.ID,
			app.Name,
			"",
			model.EffectiveWorkloadClass(app.Spec),
			windowHours,
			minSamples,
			app.Spec.Resources,
			appSamples,
		),
	}

	for _, service := range app.BackingServices {
		if strings.TrimSpace(service.OwnerAppID) != strings.TrimSpace(app.ID) {
			continue
		}
		if service.Type != model.BackingServiceTypePostgres || service.Spec.Postgres == nil {
			continue
		}
		samples, err := s.store.ListResourceUsageSamples(service.TenantID, model.ClusterNodeWorkloadKindBackingService, service.ID, since)
		if err != nil {
			return model.AppRightSizingRecommendation{}, err
		}
		out.BackingServices = append(out.BackingServices, buildRightSizingRecommendation(
			model.ClusterNodeWorkloadKindBackingService,
			service.ID,
			service.Name,
			service.Type,
			model.WorkloadClassCritical,
			windowHours,
			minSamples,
			service.Spec.Postgres.Resources,
			samples,
		))
	}
	return out, nil
}

func (s *Server) applyAppRightSizingRecommendation(
	ctx context.Context,
	app model.App,
	windowHours, minSamples int,
	actorType, actorID string,
) (model.AppRightSizingRecommendation, *model.Operation, bool, error) {
	recommendation, err := s.appResourceRecommendation(app, windowHours, minSamples)
	if err != nil {
		return recommendation, nil, false, err
	}
	spec, source, err := s.recoverAppDeployBaseline(app)
	if err != nil {
		return recommendation, nil, false, err
	}
	changed := false
	if recommendation.App.Ready && recommendation.App.Recommended != nil && !resourceSpecsEqual(spec.Resources, recommendation.App.Recommended) {
		spec.Resources = cloneResourceSpec(recommendation.App.Recommended)
		changed = true
	}
	for _, serviceRecommendation := range recommendation.BackingServices {
		if !serviceRecommendation.Ready || serviceRecommendation.Recommended == nil {
			continue
		}
		service := findBackingServiceByID(app.BackingServices, serviceRecommendation.TargetID)
		if service == nil || service.Spec.Postgres == nil {
			continue
		}
		current := service.Spec.Postgres.Resources
		if resourceSpecsEqual(current, serviceRecommendation.Recommended) {
			continue
		}
		if spec.Postgres == nil {
			postgres := *service.Spec.Postgres
			spec.Postgres = &postgres
		}
		spec.Postgres.Resources = cloneResourceSpec(serviceRecommendation.Recommended)
		changed = true
	}
	if !changed {
		return recommendation, nil, true, nil
	}
	if hasActive, err := s.appHasActiveDeployOperation(app); err != nil {
		return recommendation, nil, false, err
	} else if hasActive {
		return recommendation, nil, true, nil
	}
	if err := s.validateAutoscalingTenantEnvelope(app, spec); err != nil {
		return recommendation, nil, false, err
	}
	op, err := s.store.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     strings.TrimSpace(actorType),
		RequestedByID:       strings.TrimSpace(actorID),
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       source,
		DesiredOriginSource: model.AppOriginSource(app),
	})
	if err != nil {
		return recommendation, nil, false, err
	}
	return recommendation, &op, false, nil
}

func (s *Server) applyAutoAppRightSizingRecommendation(
	app model.App,
	windowHours, minSamples int,
) (model.AppRightSizingRecommendation, *model.Operation, bool, error) {
	recommendation, err := s.appResourceRecommendation(app, windowHours, minSamples)
	if err != nil {
		return recommendation, nil, false, err
	}
	spec, source, err := s.recoverAppDeployBaseline(app)
	if err != nil {
		return recommendation, nil, false, err
	}
	decision := autoRightSizingAppResourceChange(spec.Resources, recommendation.App.Recommended)
	if !recommendation.App.Ready || !decision.allowed {
		if !recommendation.App.Ready {
			s.logRightSizingDecision(app, "recommendation_not_ready", "right-sizing auto apply skipped because recommendation is not ready", map[string]any{
				"sample_count":  recommendation.App.SampleCount,
				"min_samples":   recommendation.App.Policy.MinSamples,
				"window_hours":  recommendation.App.WindowHours,
				"requested_by":  rightSizingAutoApplyRequestedByID,
				"target_kind":   recommendation.App.TargetKind,
				"target_id":     recommendation.App.TargetID,
				"target_name":   recommendation.App.TargetName,
				"current":       rightSizingResourceSummary(recommendation.App.Current),
				"recommended":   rightSizingResourceSummary(recommendation.App.Recommended),
				"last_observed": recommendation.App.LastSampleObservedAt,
			})
		} else {
			s.logRightSizingDecision(app, "change_below_threshold", "right-sizing auto apply skipped because recommendation is already current or below threshold", map[string]any{
				"sample_count": recommendation.App.SampleCount,
				"min_samples":  recommendation.App.Policy.MinSamples,
				"window_hours": recommendation.App.WindowHours,
				"requested_by": rightSizingAutoApplyRequestedByID,
				"current":      rightSizingResourceSummary(spec.Resources),
				"recommended":  rightSizingResourceSummary(recommendation.App.Recommended),
			})
		}
		return recommendation, nil, true, nil
	}

	if decision.downscale {
		hasRecentOOM, err := s.appHasRecentOOMRightSizingOperation(app, time.Now().UTC().Add(-rightSizingAutoDownscaleOOMWindow))
		if err != nil {
			return recommendation, nil, false, err
		}
		if hasRecentOOM {
			s.logRightSizingDecision(app, "recent_oom_blocked", "right-sizing auto downscale skipped because a recent OOM right-sizing operation exists", map[string]any{
				"requested_by": decision.requestedByID,
				"current":      rightSizingResourceSummary(spec.Resources),
				"target":       rightSizingResourceSummary(decision.resources),
			})
			return recommendation, nil, true, nil
		}
	}
	spec.Resources = cloneResourceSpec(decision.resources)
	if err := s.validateAutoscalingTenantEnvelope(app, spec); err != nil {
		s.logRightSizingDecision(app, "billing_cap_blocked", "right-sizing auto apply skipped because tenant billing/resource envelope would be exceeded", map[string]any{
			"requested_by": decision.requestedByID,
			"target":       rightSizingResourceSummary(spec.Resources),
			"error":        err.Error(),
		})
		return recommendation, nil, false, err
	}
	op, outcome, err := s.store.CreateAutoscalingDeployOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     model.ActorTypeSystem,
		RequestedByID:       decision.requestedByID,
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       source,
		DesiredOriginSource: model.AppOriginSource(app),
	})
	if err != nil {
		s.logRightSizingDecision(app, "queue_error", "right-sizing auto apply failed while creating deploy operation", map[string]any{
			"requested_by": decision.requestedByID,
			"target":       rightSizingResourceSummary(spec.Resources),
			"error":        err.Error(),
		})
		return recommendation, nil, false, err
	}
	switch outcome.Decision {
	case store.AutoscalingDeployDecisionActiveDeployExists:
		s.logRightSizingDecision(app, string(outcome.Decision), "right-sizing auto apply skipped because an active deploy already exists", map[string]any{
			"requested_by":            decision.requestedByID,
			"existing_operation_id":   outcome.ExistingOperationID,
			"target":                  rightSizingResourceSummary(spec.Resources),
			"downscale":               decision.downscale,
			"autoscaling_benign_skip": true,
		})
		return recommendation, nil, true, nil
	case store.AutoscalingDeployDecisionAlreadyCurrent:
		s.logRightSizingDecision(app, string(outcome.Decision), "right-sizing auto apply skipped because desired resources are already current", map[string]any{
			"requested_by":            decision.requestedByID,
			"target":                  rightSizingResourceSummary(spec.Resources),
			"downscale":               decision.downscale,
			"autoscaling_benign_skip": true,
		})
		return recommendation, nil, true, nil
	default:
		s.logRightSizingDecision(app, string(outcome.Decision), "right-sizing auto apply queued deploy operation", map[string]any{
			"requested_by": decision.requestedByID,
			"operation_id": op.ID,
			"target":       rightSizingResourceSummary(spec.Resources),
			"downscale":    decision.downscale,
		})
	}
	return recommendation, &op, false, nil
}

type autoRightSizingAppResourceDecision struct {
	allowed       bool
	downscale     bool
	requestedByID string
	resources     *model.ResourceSpec
}

func autoRightSizingAppResourceChange(current, recommended *model.ResourceSpec) autoRightSizingAppResourceDecision {
	decision := autoRightSizingAppResourceDecision{
		requestedByID: rightSizingAutoApplyRequestedByID,
	}
	if recommended == nil {
		return decision
	}
	effectiveCurrent := model.DefaultManagedAppResources()
	if current != nil {
		effectiveCurrent = *current
	}
	if resourceSpecsEqual(&effectiveCurrent, recommended) {
		return decision
	}
	if resourceSpecHasDecrease(effectiveCurrent, *recommended) {
		if resourceSpecHasIncrease(effectiveCurrent, *recommended) ||
			!materialResourceDecrease(
				effectiveCurrent.CPUMilliCores,
				recommended.CPUMilliCores,
				rightSizingAutoApplyMinCPUDecrease,
				rightSizingAutoApplyMinCPUDecreaseR,
			) && !materialResourceDecrease(
				effectiveCurrent.MemoryMebibytes,
				recommended.MemoryMebibytes,
				rightSizingAutoApplyMinMemDecrease,
				rightSizingAutoApplyMinMemDecreaseR,
			) {
			return decision
		}
		decision.allowed = true
		decision.downscale = true
		decision.requestedByID = rightSizingAutoDownscaleRequestedByID
		decision.resources = autoRightSizingDownscaleTarget(effectiveCurrent, *recommended)
		return decision
	}
	if resourceSpecHasIncrease(effectiveCurrent, *recommended) &&
		(materialResourceIncrease(
			effectiveCurrent.CPUMilliCores,
			recommended.CPUMilliCores,
			rightSizingAutoApplyMinCPUIncrease,
			rightSizingAutoApplyMinCPUIncreaseR,
		) || materialResourceIncrease(
			effectiveCurrent.MemoryMebibytes,
			recommended.MemoryMebibytes,
			rightSizingAutoApplyMinMemIncrease,
			rightSizingAutoApplyMinMemIncreaseR,
		)) {
		decision.allowed = true
		decision.resources = cloneResourceSpec(recommended)
	}
	return decision
}

func resourceSpecHasDecrease(current, recommended model.ResourceSpec) bool {
	return recommended.CPUMilliCores < current.CPUMilliCores ||
		recommended.MemoryMebibytes < current.MemoryMebibytes ||
		recommended.CPULimitMilliCores < current.CPULimitMilliCores ||
		recommended.MemoryLimitMebibytes < current.MemoryLimitMebibytes
}

func resourceSpecHasIncrease(current, recommended model.ResourceSpec) bool {
	return recommended.CPUMilliCores > current.CPUMilliCores ||
		recommended.MemoryMebibytes > current.MemoryMebibytes ||
		recommended.CPULimitMilliCores > current.CPULimitMilliCores ||
		recommended.MemoryLimitMebibytes > current.MemoryLimitMebibytes
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

func autoRightSizingDownscaleTarget(current, recommended model.ResourceSpec) *model.ResourceSpec {
	defaults := model.DefaultManagedAppResources()
	target := recommended
	if recommended.CPUMilliCores < current.CPUMilliCores {
		floor := roundUpInt64(int64(math.Ceil(float64(current.CPUMilliCores)*rightSizingAutoDownscaleStepRatio)), 5)
		target.CPUMilliCores = maxInt64(recommended.CPUMilliCores, floor)
		target.CPUMilliCores = maxInt64(target.CPUMilliCores, defaults.CPUMilliCores)
	}
	if recommended.MemoryMebibytes < current.MemoryMebibytes {
		floor := roundUpInt64(int64(math.Ceil(float64(current.MemoryMebibytes)*rightSizingAutoDownscaleStepRatio)), 16)
		target.MemoryMebibytes = maxInt64(recommended.MemoryMebibytes, floor)
		target.MemoryMebibytes = maxInt64(target.MemoryMebibytes, defaults.MemoryMebibytes)
		if target.MemoryLimitMebibytes > 0 {
			target.MemoryLimitMebibytes = maxInt64(target.MemoryMebibytes+128, target.MemoryMebibytes*2)
		}
	}
	if recommended.CPULimitMilliCores < current.CPULimitMilliCores {
		floor := roundUpInt64(int64(math.Ceil(float64(current.CPULimitMilliCores)*rightSizingAutoDownscaleStepRatio)), 5)
		target.CPULimitMilliCores = maxInt64(recommended.CPULimitMilliCores, floor)
	}
	if recommended.MemoryLimitMebibytes < current.MemoryLimitMebibytes && target.MemoryLimitMebibytes > 0 {
		floor := roundUpInt64(int64(math.Ceil(float64(current.MemoryLimitMebibytes)*rightSizingAutoDownscaleStepRatio)), 16)
		target.MemoryLimitMebibytes = maxInt64(target.MemoryLimitMebibytes, floor)
	}
	return &target
}

func (s *Server) startRightSizingAutoApplyLoop(ctx context.Context) {
	if s == nil || s.store == nil || ctx == nil {
		return
	}
	go func() {
		timer := time.NewTimer(2 * time.Minute)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				s.applyAutoRightSizingOnce()
				timer.Reset(rightSizingAutoApplyInterval)
			}
		}
	}()
}

func (s *Server) applyAutoRightSizingOnce() {
	acquired, lockErr := s.store.WithAdvisoryLock(context.Background(), "background:right-sizing:auto-apply", func() error {
		return s.applyAutoRightSizingOnceLocked()
	})
	if lockErr != nil {
		if s.log != nil {
			s.log.Printf("right-sizing auto apply skipped because advisory lock failed: %v", lockErr)
		}
		return
	}
	if !acquired && s.log != nil {
		s.log.Printf("right-sizing auto apply skipped because another fugue-api replica holds the advisory lock")
	}
}

func (s *Server) applyAutoRightSizingOnceLocked() error {
	apps, err := s.store.ListApps("", true)
	if err != nil {
		if s.log != nil {
			s.log.Printf("list apps for right-sizing auto apply failed: %v", err)
		}
		return err
	}
	for _, app := range apps {
		spec := model.AppRightSizingSpec{Mode: model.AppRightSizingModeAuto}
		if app.Spec.RightSizing != nil {
			spec = model.NormalizeAppRightSizingSpec(*app.Spec.RightSizing)
		}
		if spec.Mode != model.AppRightSizingModeAuto {
			continue
		}
		runtimeObj, err := s.store.GetRuntime(app.Spec.RuntimeID)
		if err != nil || runtimeObj.Type == model.RuntimeTypeExternalOwned {
			continue
		}
		if _, _, _, err := s.applyAutoAppRightSizingRecommendation(app, spec.WindowHours, spec.MinSamples); err != nil && s.log != nil {
			s.log.Printf("right-sizing auto apply for app=%s failed: %v", app.ID, err)
		}
	}
	return nil
}

func buildRightSizingRecommendation(
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
		Current:       cloneResourceSpec(current),
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
	rec := &model.ResourceSpec{}
	if current != nil {
		*rec = *current
	}
	if len(cpuValues) > 0 {
		sort.Slice(cpuValues, func(i, j int) bool { return cpuValues[i] < cpuValues[j] })
		peak := cpuValues[len(cpuValues)-1]
		out.PeakCPUUsageMilli = &peak
		value := int64(math.Ceil(float64(percentileInt64(cpuValues, policy.CPUPercentile)) * policy.CPUMultiplier))
		rec.CPUMilliCores = maxInt64(policy.CPUFloorMilli, roundUpInt64(value, 5))
		if workloadClass == model.WorkloadClassCritical {
			rec.CPULimitMilliCores = rec.CPUMilliCores
		} else {
			rec.CPULimitMilliCores = 0
		}
	}
	if len(memoryValues) > 0 {
		sort.Slice(memoryValues, func(i, j int) bool { return memoryValues[i] < memoryValues[j] })
		peak := memoryValues[len(memoryValues)-1]
		out.PeakMemoryUsageBytes = &peak
		valueBytes := int64(math.Ceil(float64(percentileInt64(memoryValues, policy.MemoryPercentile)) * policy.MemoryMultiplier))
		valueMiB := int64(math.Ceil(float64(valueBytes) / 1024 / 1024))
		rec.MemoryMebibytes = maxInt64(policy.MemoryFloorMiB, roundUpInt64(valueMiB, 16))
		if serviceType == model.BackingServiceTypePostgres {
			rec.MemoryLimitMebibytes = roundUpInt64(rec.MemoryMebibytes+128, 16)
		} else if workloadClass == model.WorkloadClassCritical {
			rec.MemoryLimitMebibytes = rec.MemoryMebibytes
		} else {
			rec.MemoryLimitMebibytes = maxInt64(rec.MemoryMebibytes+128, rec.MemoryMebibytes*2)
		}
	}
	out.Recommended = rec
	out.Ready = true
	out.AlreadyCurrent = resourceSpecsEqual(current, rec)
	out.Reason = ""
	return out
}

func rightSizingPolicy(workloadClass, serviceType string, windowHours, minSamples int) model.ResourceRightSizingPolicy {
	policy := model.ResourceRightSizingPolicy{
		WindowHours:      windowHours,
		MinSamples:       minSamples,
		CPUPercentile:    0.95,
		CPUMultiplier:    1.5,
		CPUFloorMilli:    25,
		MemoryPercentile: 0.99,
		MemoryMultiplier: 1.2,
		MemoryFloorMiB:   64,
	}
	switch workloadClass {
	case model.WorkloadClassDemo:
		policy.CPUFloorMilli = 10
		policy.MemoryFloorMiB = 32
	case model.WorkloadClassBatch:
		policy.CPUFloorMilli = 25
		policy.MemoryFloorMiB = 64
	case model.WorkloadClassCritical:
		policy.CPUFloorMilli = 100
		policy.MemoryFloorMiB = 128
	}
	if serviceType == model.BackingServiceTypePostgres {
		policy.CPUFloorMilli = 100
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

func roundUpInt64(value, step int64) int64 {
	if step <= 1 || value <= 0 {
		return value
	}
	remainder := value % step
	if remainder == 0 {
		return value
	}
	return value + step - remainder
}

func cloneResourceSpec(in *model.ResourceSpec) *model.ResourceSpec {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func resourceSpecsEqual(left, right *model.ResourceSpec) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.CPUMilliCores == right.CPUMilliCores &&
		left.MemoryMebibytes == right.MemoryMebibytes &&
		left.CPULimitMilliCores == right.CPULimitMilliCores &&
		left.MemoryLimitMebibytes == right.MemoryLimitMebibytes
}

func findBackingServiceByID(services []model.BackingService, id string) *model.BackingService {
	for index := range services {
		if strings.TrimSpace(services[index].ID) == strings.TrimSpace(id) {
			return &services[index]
		}
	}
	return nil
}

func (s *Server) appHasActiveDeployOperation(app model.App) (bool, error) {
	ops, err := s.store.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil {
		return false, err
	}
	for _, op := range ops {
		if op.Type != model.OperationTypeDeploy {
			continue
		}
		switch op.Status {
		case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
			return true, nil
		}
	}
	return false, nil
}

func (s *Server) appHasRecentOOMRightSizingOperation(app model.App, since time.Time) (bool, error) {
	ops, err := s.store.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil {
		return false, err
	}
	for _, op := range ops {
		if !strings.HasPrefix(strings.TrimSpace(op.RequestedByID), model.OperationRequestedByOOMRightSizing+"/") &&
			strings.TrimSpace(op.RequestedByID) != model.OperationRequestedByOOMRightSizing {
			continue
		}
		if op.CreatedAt.IsZero() || op.CreatedAt.Before(since) {
			continue
		}
		return true, nil
	}
	return false, nil
}

func readRightSizingWindowHours(r *http.Request, fallback int) int {
	value, err := readIntQuery(r, "window_hours", fallback)
	if err != nil {
		return normalizeRightSizingWindowHours(fallback)
	}
	return normalizeRightSizingWindowHours(value)
}

func readRightSizingMinSamples(r *http.Request, fallback int) int {
	value, err := readIntQuery(r, "min_samples", fallback)
	if err != nil {
		return normalizeRightSizingMinSamples(fallback)
	}
	return normalizeRightSizingMinSamples(value)
}

func normalizeRightSizingWindowHours(value int) int {
	if value <= 0 {
		return defaultRightSizingWindowHours
	}
	if value > maxRightSizingWindowHours {
		return maxRightSizingWindowHours
	}
	return value
}

func normalizeRightSizingMinSamples(value int) int {
	if value <= 0 {
		return defaultRightSizingMinSamples
	}
	return value
}

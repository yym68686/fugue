package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/resourcesizing"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

const (
	rightSizingAutoFailureBackoff = 6 * time.Hour
	defaultRightSizingWindowHours = 168
	defaultRightSizingMinSamples  = 12
	maxRightSizingWindowHours     = 168

	rightSizingAutoApplyRequestedByID     = model.OperationRequestedByRightSizing
	rightSizingAutoDownscaleRequestedByID = model.OperationRequestedByRightSizingDownscale

	rightSizingAutoDownscaleOOMWindow  = 48 * time.Hour
	rightSizingRolloutCPUHeadroomRatio = 0.15
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
	return resourcesizing.RecommendApp(app, windowHours, minSamples, since, s.store.ListResourceUsageSamples)
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
	if recommendation.App.Ready && resourcesizing.CPURequestOnlyChange(app.Spec.Resources, recommendation.App.RequestTarget) && s.appCPUResizeEnabled(app) && !backingServiceRightSizingChange(app, recommendation) {
		op, err := s.queueAppCPUResize(app, recommendation.App.RequestTarget.CPUMilliCores, actorType, actorID)
		return recommendation, op, op == nil && err == nil, err
	}
	if recommendation.App.Ready && app.Spec.Replicas > 0 && app.Status.CurrentReplicas == app.Spec.Replicas && resourcesizing.MemoryRequestOnlyChange(app.Spec.Resources, recommendation.App.RequestTarget) && s.appMemoryResizeEnabled(app) && !backingServiceRightSizingChange(app, recommendation) {
		op, err := s.queueAppMemoryResize(app, recommendation.App.RequestTarget.MemoryMebibytes, actorType, actorID)
		return recommendation, op, op == nil && err == nil, err
	}
	spec, source, err := s.recoverAppDeployBaseline(app)
	if err != nil {
		return recommendation, nil, false, err
	}
	changed := false
	if recommendation.App.Ready && recommendation.App.RequestTarget != nil && !resourceSpecsEqual(spec.Resources, recommendation.App.RequestTarget) {
		spec.Resources = cloneResourceSpec(recommendation.App.RequestTarget)
		changed = true
	}
	for _, serviceRecommendation := range recommendation.BackingServices {
		if !serviceRecommendation.Ready || serviceRecommendation.RequestTarget == nil {
			continue
		}
		service := findBackingServiceByID(app.BackingServices, serviceRecommendation.TargetID)
		if service == nil || service.Spec.Postgres == nil {
			continue
		}
		current := service.Spec.Postgres.Resources
		if resourceSpecsEqual(current, serviceRecommendation.RequestTarget) {
			continue
		}
		if spec.Postgres == nil {
			spec.Postgres = model.CloneAppPostgresSpec(service.Spec.Postgres)
		}
		spec.Postgres.Resources = cloneResourceSpec(serviceRecommendation.RequestTarget)
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
	// Memory requests do not need a new Pod. Stage this dimension separately
	// from limits, CPU and database changes, preserving the existing gradual
	// downscale/OOM/billing/backoff policy but avoiding rollout surge admission.
	memoryDecision := resourcesizing.AutomaticMemoryRequestChange(app.Spec.Resources, recommendation.App.RequestTarget)
	if recommendation.App.Ready && app.Spec.Replicas > 0 && app.Status.CurrentReplicas == app.Spec.Replicas && memoryDecision.Allowed && s.appMemoryResizeEnabled(app) {
		memoryBlocked := false
		if memoryDecision.MemoryDownscale {
			oom, err := s.appHasRecentOOMRightSizingOperation(app, time.Now().UTC().Add(-rightSizingAutoDownscaleOOMWindow))
			if err != nil {
				return recommendation, nil, false, err
			}
			if oom {
				s.logRightSizingDecision(app, "recent_oom_memory_blocked", "in-place memory request downscale blocked by recent OOM; limits and serving Pods unchanged", nil)
				memoryBlocked = true
			}
		}
		if !memoryBlocked {
			op, err := s.queueAppMemoryResize(app, memoryDecision.Resources.MemoryMebibytes, model.ActorTypeSystem, memoryDecision.RequestedByID)
			if errors.Is(err, store.ErrConflict) {
				s.logRightSizingDecision(app, "memory_resize_deferred", "in-place memory resize deferred by an active operation or target failure backoff; no rolling fallback", nil)
				return recommendation, nil, true, nil
			}
			return recommendation, op, op == nil && err == nil, err
		}
	}
	// Free memory reservations before CPU admission can defer another dimension.
	cpuDecision := resourcesizing.AutomaticCPURequestChange(app.Spec.Resources, recommendation.App.RequestTarget)
	if recommendation.App.Ready && cpuDecision.Allowed && s.appCPUResizeEnabled(app) {
		op, err := s.queueAppCPUResize(app, cpuDecision.Resources.CPUMilliCores, model.ActorTypeSystem, cpuDecision.RequestedByID)
		if errors.Is(err, store.ErrConflict) {
			s.logRightSizingDecision(app, "active_operation_exists", "in-place CPU resize deferred while another app operation is active", nil)
			return recommendation, nil, true, nil
		}
		return recommendation, op, op == nil && err == nil, err
	}
	spec, source, err := s.recoverAppDeployBaseline(app)
	if err != nil {
		return recommendation, nil, false, err
	}
	if s.appCPUResizeEnabled(app) {
		spec.Resources = model.CloneResourceSpec(app.Spec.Resources)
	}
	decision := resourcesizing.AutomaticChange(spec.Resources, recommendation.App.RequestTarget)
	if !recommendation.App.Ready || !decision.Allowed {
		if !recommendation.App.Ready {
			s.logRightSizingDecision(app, "recommendation_not_ready", "right-sizing auto apply skipped because recommendation is not ready", map[string]any{
				"sample_count":   recommendation.App.SampleCount,
				"min_samples":    recommendation.App.Policy.MinSamples,
				"window_hours":   recommendation.App.WindowHours,
				"requested_by":   rightSizingAutoApplyRequestedByID,
				"target_kind":    recommendation.App.TargetKind,
				"target_id":      recommendation.App.TargetID,
				"target_name":    recommendation.App.TargetName,
				"current":        rightSizingResourceSummary(recommendation.App.Current),
				"capacity":       rightSizingResourceSummary(recommendation.App.Recommended),
				"request_target": rightSizingResourceSummary(recommendation.App.RequestTarget),
				"last_observed":  recommendation.App.LastSampleObservedAt,
			})
		} else {
			s.logRightSizingDecision(app, "change_below_threshold", "right-sizing auto apply skipped because recommendation is already current or below threshold", map[string]any{
				"sample_count":   recommendation.App.SampleCount,
				"min_samples":    recommendation.App.Policy.MinSamples,
				"window_hours":   recommendation.App.WindowHours,
				"requested_by":   rightSizingAutoApplyRequestedByID,
				"current":        rightSizingResourceSummary(spec.Resources),
				"capacity":       rightSizingResourceSummary(recommendation.App.Recommended),
				"request_target": rightSizingResourceSummary(recommendation.App.RequestTarget),
			})
		}
		return recommendation, nil, true, nil
	}

	if decision.MemoryDownscale {
		hasRecentOOM, err := s.appHasRecentOOMRightSizingOperation(app, time.Now().UTC().Add(-rightSizingAutoDownscaleOOMWindow))
		if err != nil {
			return recommendation, nil, false, err
		}
		if hasRecentOOM {
			current := model.ResourceSpec{}
			if spec.Resources != nil {
				current = *spec.Resources
			}
			decision.Resources.MemoryMebibytes = current.MemoryMebibytes
			decision.Resources.MemoryLimitMebibytes = current.MemoryLimitMebibytes
			decision.MemoryDownscale = false
			s.logRightSizingDecision(app, "recent_oom_memory_blocked", "right-sizing preserved memory after a recent OOM event", map[string]any{
				"requested_by": decision.RequestedByID,
				"current":      rightSizingResourceSummary(spec.Resources),
				"target":       rightSizingResourceSummary(decision.Resources),
			})
			if resourceSpecsEqual(spec.Resources, decision.Resources) {
				return recommendation, nil, true, nil
			}
		}
	}
	spec.Resources = cloneResourceSpec(decision.Resources)
	if reason, err := s.autoRightSizingAdmission(app, spec, recommendation.App.Recommended); err != nil {
		s.logRightSizingDecision(app, "capacity_check_failed", "right-sizing auto apply skipped because rollout capacity could not be verified", map[string]any{
			"requested_by": decision.RequestedByID,
			"target":       rightSizingResourceSummary(spec.Resources),
			"error":        err.Error(),
		})
		return recommendation, nil, true, nil
	} else if reason != "" {
		s.logRightSizingDecision(app, "rollout_policy_blocked", "right-sizing auto apply skipped because the serving rollout policy is incompatible", map[string]any{
			"requested_by": decision.RequestedByID,
			"target":       rightSizingResourceSummary(spec.Resources),
			"reason":       reason,
		})
		return recommendation, nil, true, nil
	}
	if blocked, err := s.appHasRecentAutoRightSizingFailure(app, spec, time.Now().UTC().Add(-rightSizingAutoFailureBackoff)); err != nil {
		return recommendation, nil, false, err
	} else if blocked {
		s.logRightSizingDecision(app, "failure_backoff", "right-sizing auto apply skipped because the same target failed recently", map[string]any{
			"requested_by":  decision.RequestedByID,
			"target":        rightSizingResourceSummary(spec.Resources),
			"backoff_hours": rightSizingAutoFailureBackoff.Hours(),
		})
		return recommendation, nil, true, nil
	}
	if err := s.validateAutoscalingTenantEnvelope(app, spec); err != nil {
		s.logRightSizingDecision(app, "billing_cap_blocked", "right-sizing auto apply skipped because tenant billing/resource envelope would be exceeded", map[string]any{
			"requested_by": decision.RequestedByID,
			"target":       rightSizingResourceSummary(spec.Resources),
			"error":        err.Error(),
		})
		return recommendation, nil, false, err
	}
	op, outcome, err := s.store.CreateAutoscalingDeployOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     model.ActorTypeSystem,
		RequestedByID:       decision.RequestedByID,
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       source,
		DesiredOriginSource: model.AppOriginSource(app),
	})
	if err != nil {
		s.logRightSizingDecision(app, "queue_error", "right-sizing auto apply failed while creating deploy operation", map[string]any{
			"requested_by": decision.RequestedByID,
			"target":       rightSizingResourceSummary(spec.Resources),
			"error":        err.Error(),
		})
		return recommendation, nil, false, err
	}
	switch outcome.Decision {
	case store.AutoscalingDeployDecisionActiveDeployExists:
		s.logRightSizingDecision(app, string(outcome.Decision), "right-sizing auto apply skipped because an active deploy already exists", map[string]any{
			"requested_by":            decision.RequestedByID,
			"existing_operation_id":   outcome.ExistingOperationID,
			"target":                  rightSizingResourceSummary(spec.Resources),
			"downscale":               decision.Downscale,
			"autoscaling_benign_skip": true,
		})
		return recommendation, nil, true, nil
	case store.AutoscalingDeployDecisionAlreadyCurrent:
		s.logRightSizingDecision(app, string(outcome.Decision), "right-sizing auto apply skipped because desired resources are already current", map[string]any{
			"requested_by":            decision.RequestedByID,
			"target":                  rightSizingResourceSummary(spec.Resources),
			"downscale":               decision.Downscale,
			"autoscaling_benign_skip": true,
		})
		return recommendation, nil, true, nil
	default:
		s.logRightSizingDecision(app, string(outcome.Decision), "right-sizing auto apply queued deploy operation", map[string]any{
			"requested_by": decision.RequestedByID,
			"operation_id": op.ID,
			"target":       rightSizingResourceSummary(spec.Resources),
			"downscale":    decision.Downscale,
		})
	}
	return recommendation, &op, false, nil
}

// autoRightSizingAdmission keeps known rollout incompatibilities out of the
// operation queue. The controller has the final authority, but an automatic
// resize should never enqueue an operation that cannot satisfy its serving
// contract, retain memory for a surge pod, or preserve actual CPU headroom.
// Kubernetes CPU requests remain a small contention guarantee and are not a
// capacity forecast, so scheduler-request totals are deliberately not used as
// the CPU admission signal here.
func (s *Server) autoRightSizingAdmission(app model.App, desired model.AppSpec, capacity *model.ResourceSpec) (string, error) {
	if !model.AppZeroDowntimeRequired(desired) {
		return "", nil
	}
	if desired.Workspace != nil && !model.AppWorkspaceSpecSupportsSameNodeOnlineRollout(desired.Workspace) {
		return model.AppStorageClassSameNodeOnlineMountUnsupportedSummary(desired.Workspace.StorageClassName), nil
	}
	if desired.PersistentStorage != nil && len(desired.PersistentStorage.Mounts) > 0 &&
		!model.AppPersistentStorageSpecUsesSharedProjectRWX(desired.PersistentStorage) &&
		!model.AppPersistentStorageSpecSupportsSameNodeOnlineRollout(desired.PersistentStorage) {
		return model.AppStorageClassSameNodeOnlineMountUnsupportedSummary(desired.PersistentStorage.StorageClassName), nil
	}
	if s == nil || !s.shouldWarmClusterNodeInventory() {
		return "", nil
	}
	if strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")) == "" && strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT")) == "" {
		if _, ok := s.clusterNodeInventoryCache.getEntry(clusterNodeInventoryCacheKey); !ok {
			return "", nil
		}
	}
	snapshots, err := s.loadClusterNodeInventory(context.Background())
	if err != nil {
		return "", err
	}
	resources := desired.Resources
	if resources == nil {
		resources = &model.ResourceSpec{}
	}
	targetRuntimeID := strings.TrimSpace(desired.RuntimeID)
	targetNodeName := ""
	var nodeSelector map[string]string
	if targetRuntimeID != "" && s.store != nil {
		runtimeObj, runtimeErr := s.store.GetRuntime(targetRuntimeID)
		if runtimeErr != nil {
			if !errors.Is(runtimeErr, store.ErrNotFound) {
				return "", fmt.Errorf("load target runtime %s: %w", targetRuntimeID, runtimeErr)
			}
		} else {
			targetNodeName = strings.TrimSpace(runtimeObj.ClusterNodeName)
			nodeSelector = runtimepkg.SchedulingForRuntime(runtimeObj).NodeSelector
		}
	}
	requiredCPUCapacity := maxInt64(0, resources.CPUMilliCores)
	if capacity != nil {
		requiredCPUCapacity = maxInt64(requiredCPUCapacity, capacity.CPUMilliCores)
	}
	requiredMemory := maxInt64(0, resources.MemoryMebibytes) * 1024 * 1024
	foundReadyNode := false
	foundCPUHeadroom := false
	for _, snapshot := range snapshots {
		matches := rightSizingCapacitySnapshotMatchesRuntime(snapshot, targetRuntimeID, targetNodeName)
		if len(nodeSelector) > 0 {
			matches = rightSizingSnapshotMatchesSelector(snapshot, nodeSelector)
			if targetNodeName != "" {
				matches = matches && strings.EqualFold(snapshot.node.Name, targetNodeName)
			}
		}
		if !matches {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(snapshot.node.Status), "ready") || snapshot.node.CPU == nil || snapshot.node.Memory == nil {
			continue
		}
		if snapshot.node.CPU.AllocatableMilliCores == nil || snapshot.node.CPU.UsedMilliCores == nil || snapshot.node.Memory.SchedulableFreeBytes == nil {
			continue
		}
		foundReadyNode = true
		cpuBudget := int64(math.Floor(float64(*snapshot.node.CPU.AllocatableMilliCores) * (1 - rightSizingRolloutCPUHeadroomRatio)))
		if *snapshot.node.CPU.UsedMilliCores+requiredCPUCapacity > cpuBudget {
			continue
		}
		foundCPUHeadroom = true
		if *snapshot.node.Memory.SchedulableFreeBytes >= requiredMemory {
			return "", nil
		}
	}
	if foundReadyNode && !foundCPUHeadroom {
		return "", fmt.Errorf("no ready cluster node preserves %.0f%% actual CPU headroom for the right-sizing surge capacity (%dm)", rightSizingRolloutCPUHeadroomRatio*100, requiredCPUCapacity)
	}
	if !foundReadyNode {
		return "", fmt.Errorf("no ready cluster node matches the target runtime placement constraints")
	}
	return "", fmt.Errorf("no ready cluster node has %dMi schedulable memory for the right-sizing surge pod", requiredMemory/(1024*1024))
}

func rightSizingCapacitySnapshotMatchesRuntime(snapshot clusterNodeSnapshot, targetRuntimeID, targetNodeName string) bool {
	if targetNodeName != "" {
		return strings.EqualFold(strings.TrimSpace(snapshot.node.Name), targetNodeName)
	}
	if targetRuntimeID == "" || strings.EqualFold(targetRuntimeID, tenantSharedRuntimeID) {
		return sharedClusterSnapshotCandidate(snapshot)
	}
	return strings.EqualFold(strings.TrimSpace(snapshot.runtimeID), targetRuntimeID) ||
		strings.EqualFold(strings.TrimSpace(snapshot.node.RuntimeID), targetRuntimeID)
}

func (s *Server) appHasRecentAutoRightSizingFailure(app model.App, desired model.AppSpec, since time.Time) (bool, error) {
	ops, err := s.store.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil {
		return false, err
	}
	return resourcesizing.HasRecentAutomaticFailure(ops, desired.Resources, since), nil
}

func (s *Server) enqueueSizingRequest(op model.Operation, baseline model.AppSpec) (model.Operation, error) {
	if op.RequestedByType == model.ActorTypeSystem &&
		(op.RequestedByID == model.OperationRequestedByRightSizing || op.RequestedByID == model.OperationRequestedByRightSizingDownscale) {
		return s.store.EnqueueAutomaticResourceSizingRequest(op, baseline)
	}
	return s.store.CreateAppRequestResizeOperation(op, baseline)
}

func (s *Server) startRightSizingAutoApplyLoop(ctx context.Context) <-chan struct{} {
	if s == nil || s.store == nil || ctx == nil {
		return joinWarmerTasks()
	}
	ports := resourcesizing.PeriodicPorts{
		WithLock: s.store.RunAutomaticResourceSizing,
		Apps:     func() ([]model.App, error) { return s.store.ListApps("", true) },
		Managed: func(app model.App) bool {
			rt, err := s.store.GetRuntime(app.Spec.RuntimeID)
			return err == nil && rt.Type != model.RuntimeTypeExternalOwned
		},
		Apply: func(app model.App, windowHours, minSamples int) error {
			_, _, _, err := s.applyAutoAppRightSizingRecommendation(app, windowHours, minSamples)
			return err
		},
	}
	if s.log != nil {
		ports.Logf = s.log.Printf
	}
	return startWarmerTask(ctx, func() { resourcesizing.RunPeriodic(ctx, ports) })
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

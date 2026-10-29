package api

import (
	"os"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) appCPUResizeEnabled(app model.App) bool {
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("FUGUE_APP_CPU_REQUEST_RESIZE_ENABLED")), "true") {
		return false
	}
	rt, err := s.store.GetRuntime(app.Spec.RuntimeID)
	return err == nil && (rt.Type == model.RuntimeTypeManagedShared || rt.Type == model.RuntimeTypeManagedOwned)
}

func (s *Server) queueAppCPUResize(app model.App, cpu int64, actorType, actorID string) (*model.Operation, error) {
	spec := app.Spec
	spec.Resources = model.CloneResourceSpec(app.Spec.Resources)
	spec.Resources.CPUMilliCores = cpu
	if err := s.validateAutoscalingTenantEnvelope(app, spec); err != nil {
		return nil, err
	}
	blocked, err := s.appHasRecentAutoRightSizingFailure(app, spec, time.Now().UTC().Add(-rightSizingAutoFailureBackoff))
	if err != nil {
		return nil, err
	}
	if blocked {
		s.logRightSizingDecision(app, "failure_backoff", "in-place CPU resize target failed recently; no rolling fallback", nil)
		return nil, store.ErrConflict
	}
	op, err := s.store.CreateAppCPUResizeOperation(model.Operation{
		TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppCPUResize,
		RequestedByType: actorType, RequestedByID: actorID, DesiredSpec: &spec,
	}, app.Spec)
	if err != nil {
		return nil, err
	}
	s.logRightSizingDecision(app, "queued_cpu_resize", "CPU request resize queued without a deployment", map[string]any{
		"operation_id": op.ID, "current_cpu_millicores": app.Spec.Resources.CPUMilliCores, "target_cpu_millicores": cpu,
	})
	return &op, nil
}

// Do not silently drop simultaneous memory or limit recommendations. Those
// continue through the existing deployment path with their complete target.
func appCPURequestOnlyChange(current, target *model.ResourceSpec) bool {
	if current == nil || target == nil || current.CPUMilliCores == target.CPUMilliCores {
		return false
	}
	other := *target
	other.CPUMilliCores = current.CPUMilliCores
	return resourceSpecsEqual(current, &other)
}

func autoAppCPURequestChange(current, target *model.ResourceSpec) autoRightSizingAppResourceDecision {
	if current == nil || target == nil {
		return autoRightSizingAppResourceChange(current, current)
	}
	cpuOnly := model.CloneResourceSpec(current)
	cpuOnly.CPUMilliCores = target.CPUMilliCores
	return autoRightSizingAppResourceChange(current, cpuOnly)
}

func backingServiceRightSizingChange(app model.App, recommendation model.AppRightSizingRecommendation) bool {
	for _, rec := range recommendation.BackingServices {
		if !rec.Ready || rec.RequestTarget == nil {
			continue
		}
		service := findBackingServiceByID(app.BackingServices, rec.TargetID)
		if service != nil && service.Spec.Postgres != nil && !resourceSpecsEqual(service.Spec.Postgres.Resources, rec.RequestTarget) {
			return true
		}
	}
	return false
}

func rightSizingSnapshotMatchesSelector(snapshot clusterNodeSnapshot, selector map[string]string) bool {
	for key, value := range selector {
		if snapshot.labels[key] != value {
			return false
		}
	}
	return len(selector) > 0
}

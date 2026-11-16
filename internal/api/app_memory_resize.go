package api

import (
	"os"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) appMemoryResizeEnabled(app model.App) bool {
	if value := strings.TrimSpace(os.Getenv("FUGUE_APP_MEMORY_REQUEST_RESIZE_ENABLED")); value != "" && !strings.EqualFold(value, "true") {
		return false
	}
	rt, err := s.store.GetRuntime(app.Spec.RuntimeID)
	return err == nil && (rt.Type == model.RuntimeTypeManagedShared || rt.Type == model.RuntimeTypeManagedOwned)
}

func appMemoryRequestOnlyChange(current, target *model.ResourceSpec) bool {
	if current == nil || target == nil || current.MemoryMebibytes == target.MemoryMebibytes {
		return false
	}
	other := *target
	other.MemoryMebibytes = current.MemoryMebibytes
	return resourceSpecsEqual(current, &other)
}

func autoAppMemoryRequestChange(current, target *model.ResourceSpec) autoRightSizingAppResourceDecision {
	if current == nil || target == nil {
		return autoRightSizingAppResourceChange(current, current)
	}
	memoryOnly := model.CloneResourceSpec(current)
	memoryOnly.MemoryMebibytes = target.MemoryMebibytes
	decision := autoRightSizingAppResourceChange(current, memoryOnly)
	if decision.resources != nil {
		// A request-only resize cannot authorize a limit increase, even when
		// the recommendation needs one. Defer that transition explicitly.
		if current.MemoryLimitMebibytes > 0 && decision.resources.MemoryMebibytes > current.MemoryLimitMebibytes {
			decision.allowed = false
		}
	}
	return decision
}

func (s *Server) queueAppMemoryResize(app model.App, memory int64, actorType, actorID string) (*model.Operation, error) {
	spec := app.Spec.DeepCopy()
	if spec.Resources == nil || memory <= 0 {
		return nil, store.ErrInvalidInput
	}
	spec.Resources.MemoryMebibytes = memory
	if err := s.validateAutoscalingTenantEnvelope(app, spec); err != nil {
		return nil, err
	}
	blocked, err := s.appHasRecentAutoRightSizingFailure(app, spec, time.Now().UTC().Add(-rightSizingAutoFailureBackoff))
	if err != nil {
		return nil, err
	}
	if blocked {
		s.logRightSizingDecision(app, "failure_backoff", "in-place memory resize target failed recently; no rolling fallback", nil)
		return nil, store.ErrConflict
	}
	op, err := s.store.CreateAppRequestResizeOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppMemoryResize,
		RequestedByType: actorType, RequestedByID: actorID, DesiredSpec: &spec}, app.Spec)
	if err != nil {
		return nil, err
	}
	s.logRightSizingDecision(app, "queued_memory_resize", "memory request resize queued without a deployment", map[string]any{
		"operation_id": op.ID, "current_memory_mebibytes": app.Spec.Resources.MemoryMebibytes, "target_memory_mebibytes": memory,
	})
	return &op, nil
}

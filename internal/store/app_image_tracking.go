package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) UpsertAppImageTracking(tracking model.AppImageTracking) (model.AppImageTracking, error) {
	tracking = normalizeAppImageTracking(tracking)
	if tracking.AppID == "" || tracking.ImageRef == "" {
		return model.AppImageTracking{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpsertAppImageTracking(tracking)
	}

	var out model.AppImageTracking
	err := s.withLockedState(true, func(state *model.State) error {
		appIndex := findApp(state, tracking.AppID)
		if appIndex < 0 {
			return ErrNotFound
		}
		app := state.Apps[appIndex]
		if tracking.TenantID != "" && tracking.TenantID != app.TenantID {
			return ErrNotFound
		}
		tracking.TenantID = app.TenantID

		now := time.Now().UTC()
		if index := findAppImageTrackingByApp(state, tracking.AppID); index >= 0 {
			current := state.AppImageTrackings[index]
			tracking.ID = current.ID
			tracking.CreatedAt = current.CreatedAt
			preserveAppImageTrackingObservedFields(&tracking, current)
			tracking.UpdatedAt = now
			state.AppImageTrackings[index] = tracking
			out = tracking
			return nil
		}

		tracking.ID = model.NewID("imgtrack")
		tracking.CreatedAt = now
		tracking.UpdatedAt = now
		state.AppImageTrackings = append(state.AppImageTrackings, tracking)
		out = tracking
		return nil
	})
	return out, err
}

func (s *Store) GetAppImageTracking(tenantID string, platformAdmin bool, appID string) (model.AppImageTracking, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.AppImageTracking{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetAppImageTracking(tenantID, platformAdmin, appID)
	}
	var out model.AppImageTracking
	err := s.withLockedState(false, func(state *model.State) error {
		for _, tracking := range state.AppImageTrackings {
			if !appImageTrackingMatchesFilter(tracking, model.AppImageTrackingFilter{
				TenantID:      tenantID,
				PlatformAdmin: platformAdmin,
				AppID:         appID,
			}) {
				continue
			}
			out = tracking
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

func (s *Store) ListAppImageTrackings(filter model.AppImageTrackingFilter) ([]model.AppImageTracking, error) {
	filter = normalizeAppImageTrackingFilter(filter)
	if s.usingDatabase() {
		return s.pgListAppImageTrackings(filter)
	}
	out := []model.AppImageTracking{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, tracking := range state.AppImageTrackings {
			if appImageTrackingMatchesFilter(tracking, filter) {
				out = append(out, tracking)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out, nil
}

func (s *Store) RecordAppImageTrackingCheck(id, digest, deliveryID, event, lastError string) (model.AppImageTracking, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.AppImageTracking{}, ErrInvalidInput
	}
	digest = normalizeImageDigest(digest)
	if s.usingDatabase() {
		return s.pgRecordAppImageTrackingCheck(id, digest, deliveryID, event, lastError)
	}
	var out model.AppImageTracking
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAppImageTrackingByID(state, id)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		applyAppImageTrackingCheck(&state.AppImageTrackings[index], digest, deliveryID, event, lastError, now)
		out = state.AppImageTrackings[index]
		return nil
	})
	return out, err
}

func (s *Store) RecordAppImageTrackingQueued(id, digest, operationID, deliveryID, event string) (model.AppImageTracking, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.AppImageTracking{}, ErrInvalidInput
	}
	digest = normalizeImageDigest(digest)
	operationID = strings.TrimSpace(operationID)
	if digest == "" || operationID == "" {
		return model.AppImageTracking{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRecordAppImageTrackingQueued(id, digest, operationID, deliveryID, event)
	}
	var out model.AppImageTracking
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAppImageTrackingByID(state, id)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		applyAppImageTrackingQueued(&state.AppImageTrackings[index], digest, operationID, deliveryID, event, now)
		out = state.AppImageTrackings[index]
		return nil
	})
	return out, err
}

func (s *Store) QueueAppImageTrackingImport(app model.App, tracking model.AppImageTracking, requestedByType, requestedByID string) (model.Operation, error) {
	tracking = normalizeAppImageTracking(tracking)
	if app.ID == "" || tracking.AppID == "" || tracking.ImageRef == "" || tracking.AppID != app.ID {
		return model.Operation{}, ErrInvalidInput
	}
	if app.TenantID == "" || tracking.TenantID != app.TenantID {
		return model.Operation{}, ErrNotFound
	}

	spec := cloneAppSpec(&app.Spec)
	if spec == nil {
		return model.Operation{}, ErrInvalidInput
	}
	if spec.Replicas < 1 {
		spec.Replicas = 1
	}
	if strings.TrimSpace(spec.RuntimeID) == "" {
		spec.RuntimeID = "runtime_managed_shared"
	}
	if spec.Workspace != nil {
		spec.Workspace.ResetToken = model.NewID("workspace-reset")
	}

	source := model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: tracking.ImageRef,
	}
	for _, candidate := range []*model.AppSource{model.AppOriginSource(app), model.AppBuildSource(app), app.Source} {
		if candidate == nil || strings.TrimSpace(candidate.Type) != model.AppSourceTypeDockerImage {
			continue
		}
		if strings.TrimSpace(candidate.ImageRef) != tracking.ImageRef {
			continue
		}
		source.ImageNameSuffix = strings.TrimSpace(candidate.ImageNameSuffix)
		source.ComposeService = strings.TrimSpace(candidate.ComposeService)
		source.ComposeDependsOn = append([]string(nil), candidate.ComposeDependsOn...)
		break
	}

	return s.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeImport,
		RequestedByType:     strings.TrimSpace(requestedByType),
		RequestedByID:       strings.TrimSpace(requestedByID),
		AppID:               app.ID,
		DesiredSpec:         spec,
		DesiredSource:       &source,
		DesiredOriginSource: model.CloneAppSource(&source),
	})
}

func normalizeAppImageTracking(tracking model.AppImageTracking) model.AppImageTracking {
	tracking.ID = strings.TrimSpace(tracking.ID)
	tracking.TenantID = strings.TrimSpace(tracking.TenantID)
	tracking.AppID = strings.TrimSpace(tracking.AppID)
	tracking.ImageRef = strings.TrimSpace(tracking.ImageRef)
	tracking.LastSeenDigest = normalizeImageDigest(tracking.LastSeenDigest)
	tracking.LastQueuedDigest = normalizeImageDigest(tracking.LastQueuedDigest)
	tracking.LastDeployedDigest = normalizeImageDigest(tracking.LastDeployedDigest)
	tracking.LastOperationID = strings.TrimSpace(tracking.LastOperationID)
	tracking.LastDeliveryID = strings.TrimSpace(tracking.LastDeliveryID)
	tracking.LastEvent = strings.TrimSpace(tracking.LastEvent)
	tracking.LastError = strings.TrimSpace(tracking.LastError)
	return tracking
}

func normalizeAppImageTrackingFilter(filter model.AppImageTrackingFilter) model.AppImageTrackingFilter {
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.AppID = strings.TrimSpace(filter.AppID)
	filter.ImageRef = strings.TrimSpace(filter.ImageRef)
	return filter
}

func preserveAppImageTrackingObservedFields(next *model.AppImageTracking, current model.AppImageTracking) {
	if next.LastSeenDigest == "" {
		next.LastSeenDigest = current.LastSeenDigest
	}
	if next.LastQueuedDigest == "" {
		next.LastQueuedDigest = current.LastQueuedDigest
	}
	if next.LastDeployedDigest == "" {
		next.LastDeployedDigest = current.LastDeployedDigest
	}
	if next.LastOperationID == "" {
		next.LastOperationID = current.LastOperationID
	}
	if next.LastDeliveryID == "" {
		next.LastDeliveryID = current.LastDeliveryID
	}
	if next.LastEvent == "" {
		next.LastEvent = current.LastEvent
	}
	if next.LastError == "" {
		next.LastError = current.LastError
	}
	if next.LastCheckedAt == nil {
		next.LastCheckedAt = current.LastCheckedAt
	}
	if next.LastTriggeredAt == nil {
		next.LastTriggeredAt = current.LastTriggeredAt
	}
}

func findAppImageTrackingByApp(state *model.State, appID string) int {
	for idx, existing := range state.AppImageTrackings {
		if strings.TrimSpace(existing.AppID) == appID {
			return idx
		}
	}
	return -1
}

func findAppImageTrackingByID(state *model.State, id string) int {
	for idx, existing := range state.AppImageTrackings {
		if strings.TrimSpace(existing.ID) == id {
			return idx
		}
	}
	return -1
}

func appImageTrackingMatchesFilter(tracking model.AppImageTracking, filter model.AppImageTrackingFilter) bool {
	if !filter.PlatformAdmin && strings.TrimSpace(tracking.TenantID) != filter.TenantID {
		return false
	}
	if filter.AppID != "" && strings.TrimSpace(tracking.AppID) != filter.AppID {
		return false
	}
	if filter.ImageRef != "" && strings.TrimSpace(tracking.ImageRef) != filter.ImageRef {
		return false
	}
	if filter.Enabled != nil && tracking.Enabled != *filter.Enabled {
		return false
	}
	return true
}

func applyAppImageTrackingCheck(tracking *model.AppImageTracking, digest, deliveryID, event, lastError string, now time.Time) {
	if digest != "" {
		tracking.LastSeenDigest = digest
	}
	tracking.LastDeliveryID = strings.TrimSpace(deliveryID)
	tracking.LastEvent = strings.TrimSpace(event)
	tracking.LastError = strings.TrimSpace(lastError)
	tracking.LastCheckedAt = &now
	tracking.UpdatedAt = now
}

func applyAppImageTrackingQueued(tracking *model.AppImageTracking, digest, operationID, deliveryID, event string, now time.Time) {
	tracking.LastSeenDigest = digest
	tracking.LastQueuedDigest = digest
	tracking.LastOperationID = operationID
	tracking.LastDeliveryID = strings.TrimSpace(deliveryID)
	tracking.LastEvent = strings.TrimSpace(event)
	tracking.LastError = ""
	tracking.LastCheckedAt = &now
	tracking.LastTriggeredAt = &now
	tracking.UpdatedAt = now
}

func updateAppImageTrackingDeployedInState(state *model.State, op model.Operation, now time.Time) {
	if op.Type != model.OperationTypeDeploy || op.DesiredSource == nil || strings.TrimSpace(op.DesiredSource.Type) != model.AppSourceTypeDockerImage {
		return
	}
	imageRef := strings.TrimSpace(op.DesiredSource.ImageRef)
	if imageRef == "" {
		return
	}
	digest := digestFromImageReference(op.DesiredSource.ResolvedImageRef)
	if digest == "" && op.DesiredSpec != nil {
		digest = digestFromImageReference(op.DesiredSpec.Image)
	}
	if digest == "" {
		return
	}
	for idx := range state.AppImageTrackings {
		tracking := &state.AppImageTrackings[idx]
		if strings.TrimSpace(tracking.AppID) != op.AppID || strings.TrimSpace(tracking.ImageRef) != imageRef {
			continue
		}
		tracking.LastDeployedDigest = digest
		tracking.LastOperationID = op.ID
		tracking.LastError = ""
		tracking.UpdatedAt = now
	}
}

func digestFromImageReference(imageRef string) string {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return ""
	}
	if strings.HasPrefix(imageRef, "sha256:") {
		return imageRef
	}
	if idx := strings.Index(imageRef, "@sha256:"); idx >= 0 {
		return imageRef[idx+1:]
	}
	return ""
}

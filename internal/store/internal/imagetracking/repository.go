package imagetracking

import (
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	appImageTrackingCheckRetentionLimit  = 200
	appImageTrackingCheckRetentionWindow = 7 * 24 * time.Hour
)

func (s *Repository) UpsertAppImageTracking(tracking model.AppImageTracking) (model.AppImageTracking, error) {
	tracking = normalizeAppImageTracking(tracking)
	if tracking.AppID == "" || tracking.ImageRef == "" {
		return model.AppImageTracking{}, s.invalidInput
	}
	return s.pgUpsertAppImageTracking(tracking)
}

func (s *Repository) GetAppImageTracking(tenantID string, platformAdmin bool, appID string) (model.AppImageTracking, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.AppImageTracking{}, s.invalidInput
	}
	return s.pgGetAppImageTracking(tenantID, platformAdmin, appID)
}

func (s *Repository) ListAppImageTrackings(filter model.AppImageTrackingFilter) ([]model.AppImageTracking, error) {
	filter = normalizeAppImageTrackingFilter(filter)
	return s.pgListAppImageTrackings(filter)
}

func (s *Repository) RecordAppImageTrackingCheck(id, digest, deliveryID, event, lastError string) (model.AppImageTracking, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.AppImageTracking{}, s.invalidInput
	}
	digest = normalizeImageDigest(digest)
	return s.pgRecordAppImageTrackingCheck(id, digest, deliveryID, event, lastError)
}

func (s *Repository) RecordAppImageTrackingQueued(id, digest, operationID, deliveryID, event string) (model.AppImageTracking, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.AppImageTracking{}, s.invalidInput
	}
	digest = normalizeImageDigest(digest)
	operationID = strings.TrimSpace(operationID)
	if digest == "" || operationID == "" {
		return model.AppImageTracking{}, s.invalidInput
	}
	return s.pgRecordAppImageTrackingQueued(id, digest, operationID, deliveryID, event)
}

func (s *Repository) CreateAppImageTrackingCheck(check model.AppImageTrackingCheck) (model.AppImageTrackingCheck, error) {
	check = normalizeAppImageTrackingCheck(check)
	if check.AppID == "" || check.TrackingID == "" || check.ImageRef == "" || check.Decision == "" {
		return model.AppImageTrackingCheck{}, s.invalidInput
	}
	return s.pgCreateAppImageTrackingCheck(check)
}

func (s *Repository) ListAppImageTrackingChecks(filter model.AppImageTrackingCheckFilter) ([]model.AppImageTrackingCheck, error) {
	filter = normalizeAppImageTrackingCheckFilter(filter)
	return s.pgListAppImageTrackingChecks(filter)
}

func (s *Repository) QueueAppImageTrackingImport(app model.App, tracking model.AppImageTracking, requestedByType, requestedByID string) (model.Operation, error) {
	tracking = normalizeAppImageTracking(tracking)
	if app.ID == "" || tracking.AppID == "" || tracking.ImageRef == "" || tracking.AppID != app.ID {
		return model.Operation{}, s.invalidInput
	}
	if app.TenantID == "" || tracking.TenantID != app.TenantID {
		return model.Operation{}, s.notFound
	}

	spec := s.cloneAppSpec(&app.Spec)
	if spec == nil {
		return model.Operation{}, s.invalidInput
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
	inheritImageTrackingSourceMetadata(&source, app, tracking.ImageRef)

	return s.createOperation(model.Operation{
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

func inheritImageTrackingSourceMetadata(source *model.AppSource, app model.App, imageRef string) {
	if source == nil {
		return
	}
	for _, candidate := range imageTrackingSourceMetadataCandidates(app, imageRef) {
		if candidate == nil {
			continue
		}
		if strings.TrimSpace(source.ImageNameSuffix) == "" {
			source.ImageNameSuffix = strings.TrimSpace(candidate.ImageNameSuffix)
		}
		if strings.TrimSpace(source.ComposeService) == "" {
			source.ComposeService = strings.TrimSpace(candidate.ComposeService)
		}
		if len(source.ComposeDependsOn) == 0 && len(candidate.ComposeDependsOn) > 0 {
			source.ComposeDependsOn = append([]string(nil), candidate.ComposeDependsOn...)
		}
		if strings.TrimSpace(source.DetectedProvider) == "" {
			source.DetectedProvider = strings.TrimSpace(candidate.DetectedProvider)
		}
		if strings.TrimSpace(source.DetectedStack) == "" {
			source.DetectedStack = strings.TrimSpace(candidate.DetectedStack)
		}
		if imageTrackingSourceMetadataComplete(source) {
			return
		}
	}
}

func imageTrackingSourceMetadataCandidates(app model.App, imageRef string) []*model.AppSource {
	base := []*model.AppSource{model.AppOriginSource(app), model.AppBuildSource(app), app.Source}
	out := make([]*model.AppSource, 0, len(base)*2)
	seen := make(map[*model.AppSource]struct{}, len(base))
	for _, candidate := range base {
		if candidate == nil {
			continue
		}
		if strings.TrimSpace(candidate.Type) != model.AppSourceTypeDockerImage {
			continue
		}
		if strings.TrimSpace(candidate.ImageRef) != strings.TrimSpace(imageRef) {
			continue
		}
		out = append(out, candidate)
		seen[candidate] = struct{}{}
	}
	for _, candidate := range base {
		if candidate == nil {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		out = append(out, candidate)
	}
	return out
}

func imageTrackingSourceMetadataComplete(source *model.AppSource) bool {
	return strings.TrimSpace(source.ImageNameSuffix) != "" &&
		strings.TrimSpace(source.ComposeService) != "" &&
		strings.TrimSpace(source.DetectedProvider) != "" &&
		strings.TrimSpace(source.DetectedStack) != ""
}

func normalizeAppImageTracking(tracking model.AppImageTracking) model.AppImageTracking {
	tracking.ID = stripImageNUL(tracking.ID)
	tracking.TenantID = stripImageNUL(tracking.TenantID)
	tracking.AppID = stripImageNUL(tracking.AppID)
	tracking.ImageRef = stripImageNUL(tracking.ImageRef)
	tracking.LastOperationID = stripImageNUL(tracking.LastOperationID)
	tracking.LastDeliveryID = stripImageNUL(tracking.LastDeliveryID)
	tracking.LastEvent = stripImageNUL(tracking.LastEvent)
	tracking.LastError = stripImageNUL(tracking.LastError)
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

func normalizeAppImageTrackingCheck(check model.AppImageTrackingCheck) model.AppImageTrackingCheck {
	check.ID = stripImageNUL(check.ID)
	check.TenantID = stripImageNUL(check.TenantID)
	check.AppID = stripImageNUL(check.AppID)
	check.TrackingID = stripImageNUL(check.TrackingID)
	check.ImageRef = stripImageNUL(check.ImageRef)
	check.ObservedDigest = stripImageNUL(check.ObservedDigest)
	check.CurrentAppDigest = stripImageNUL(check.CurrentAppDigest)
	check.LastQueuedDigest = stripImageNUL(check.LastQueuedDigest)
	check.LastDeployedDigest = stripImageNUL(check.LastDeployedDigest)
	check.Decision = stripImageNUL(check.Decision)
	check.SkipReason = stripImageNUL(check.SkipReason)
	check.OperationID = stripImageNUL(check.OperationID)
	check.ActiveOperationID = stripImageNUL(check.ActiveOperationID)
	check.ResolverError = stripImageNUL(check.ResolverError)
	check.DeliveryID = stripImageNUL(check.DeliveryID)
	check.Event = stripImageNUL(check.Event)
	check.ControllerPod = stripImageNUL(check.ControllerPod)
	check.ControllerLeaderIdentity = stripImageNUL(check.ControllerLeaderIdentity)
	check.ID = strings.TrimSpace(check.ID)
	check.TenantID = strings.TrimSpace(check.TenantID)
	check.AppID = strings.TrimSpace(check.AppID)
	check.TrackingID = strings.TrimSpace(check.TrackingID)
	check.ImageRef = strings.TrimSpace(check.ImageRef)
	check.ObservedDigest = normalizeImageDigest(check.ObservedDigest)
	check.CurrentAppDigest = normalizeImageDigest(check.CurrentAppDigest)
	check.LastQueuedDigest = normalizeImageDigest(check.LastQueuedDigest)
	check.LastDeployedDigest = normalizeImageDigest(check.LastDeployedDigest)
	check.Decision = strings.TrimSpace(check.Decision)
	check.SkipReason = strings.TrimSpace(check.SkipReason)
	check.OperationID = strings.TrimSpace(check.OperationID)
	check.ActiveOperationID = strings.TrimSpace(check.ActiveOperationID)
	check.ResolverError = strings.TrimSpace(check.ResolverError)
	check.DeliveryID = strings.TrimSpace(check.DeliveryID)
	check.Event = strings.TrimSpace(check.Event)
	check.ControllerPod = strings.TrimSpace(check.ControllerPod)
	check.ControllerLeaderIdentity = strings.TrimSpace(check.ControllerLeaderIdentity)
	if check.DurationMilliseconds < 0 {
		check.DurationMilliseconds = 0
	}
	if !check.CheckedAt.IsZero() {
		check.CheckedAt = check.CheckedAt.UTC()
	}
	return check
}

func normalizeAppImageTrackingCheckFilter(filter model.AppImageTrackingCheckFilter) model.AppImageTrackingCheckFilter {
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.AppID = strings.TrimSpace(filter.AppID)
	filter.TrackingID = strings.TrimSpace(filter.TrackingID)
	if filter.Limit < 0 {
		filter.Limit = 0
	}
	if filter.Limit > appImageTrackingCheckRetentionLimit {
		filter.Limit = appImageTrackingCheckRetentionLimit
	}
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

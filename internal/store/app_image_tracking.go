package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	appImageTrackingCheckRetentionLimit  = 200
	appImageTrackingCheckRetentionWindow = 7 * 24 * time.Hour
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

func (s *Store) CreateAppImageTrackingCheck(check model.AppImageTrackingCheck) (model.AppImageTrackingCheck, error) {
	check = normalizeAppImageTrackingCheck(check)
	if check.AppID == "" || check.TrackingID == "" || check.ImageRef == "" || check.Decision == "" {
		return model.AppImageTrackingCheck{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateAppImageTrackingCheck(check)
	}
	var out model.AppImageTrackingCheck
	err := s.withLockedState(true, func(state *model.State) error {
		trackingIndex := findAppImageTrackingByID(state, check.TrackingID)
		if trackingIndex < 0 {
			return ErrNotFound
		}
		tracking := state.AppImageTrackings[trackingIndex]
		if check.TenantID != "" && check.TenantID != tracking.TenantID {
			return ErrNotFound
		}
		if check.AppID != tracking.AppID {
			return ErrNotFound
		}
		now := time.Now().UTC()
		check.TenantID = tracking.TenantID
		if check.ImageRef == "" {
			check.ImageRef = tracking.ImageRef
		}
		if check.ID == "" {
			check.ID = model.NewID("imgtrackchk")
		}
		if check.CheckedAt.IsZero() {
			check.CheckedAt = now
		}
		state.AppImageTrackingChecks = append(state.AppImageTrackingChecks, check)
		state.AppImageTrackingChecks = retainAppImageTrackingChecks(state.AppImageTrackingChecks, check.AppID, now)
		out = check
		return nil
	})
	return out, err
}

func (s *Store) ListAppImageTrackingChecks(filter model.AppImageTrackingCheckFilter) ([]model.AppImageTrackingCheck, error) {
	filter = normalizeAppImageTrackingCheckFilter(filter)
	if s.usingDatabase() {
		return s.pgListAppImageTrackingChecks(filter)
	}
	out := []model.AppImageTrackingCheck{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, check := range state.AppImageTrackingChecks {
			if appImageTrackingCheckMatchesFilter(check, filter) {
				out = append(out, check)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortAppImageTrackingChecksNewestFirst(out)
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
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
	inheritImageTrackingSourceMetadata(&source, app, tracking.ImageRef)

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

func appImageTrackingCheckMatchesFilter(check model.AppImageTrackingCheck, filter model.AppImageTrackingCheckFilter) bool {
	if !filter.PlatformAdmin && strings.TrimSpace(check.TenantID) != filter.TenantID {
		return false
	}
	if filter.AppID != "" && strings.TrimSpace(check.AppID) != filter.AppID {
		return false
	}
	if filter.TrackingID != "" && strings.TrimSpace(check.TrackingID) != filter.TrackingID {
		return false
	}
	return true
}

func sortAppImageTrackingChecksNewestFirst(checks []model.AppImageTrackingCheck) {
	sort.Slice(checks, func(i, j int) bool {
		left := checks[i].CheckedAt
		right := checks[j].CheckedAt
		if !left.Equal(right) {
			return left.After(right)
		}
		return strings.Compare(checks[i].ID, checks[j].ID) > 0
	})
}

func retainAppImageTrackingChecks(checks []model.AppImageTrackingCheck, appID string, now time.Time) []model.AppImageTrackingCheck {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return checks
	}
	cutoff := now.UTC().Add(-appImageTrackingCheckRetentionWindow)
	out := checks[:0]
	appChecks := []model.AppImageTrackingCheck{}
	for _, check := range checks {
		if strings.TrimSpace(check.AppID) != appID {
			out = append(out, check)
			continue
		}
		if !check.CheckedAt.IsZero() && check.CheckedAt.Before(cutoff) {
			continue
		}
		appChecks = append(appChecks, check)
	}
	sortAppImageTrackingChecksNewestFirst(appChecks)
	if len(appChecks) > appImageTrackingCheckRetentionLimit {
		appChecks = appChecks[:appImageTrackingCheckRetentionLimit]
	}
	out = append(out, appChecks...)
	return out
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
	if op.Type != model.OperationTypeDeploy || op.DesiredSource == nil {
		return
	}
	imageRef := strings.TrimSpace(op.DesiredSource.ImageRef)
	digest := model.ImageDigestFromReference(op.DesiredSource.ResolvedImageRef)
	if digest == "" && op.DesiredSpec != nil {
		digest = model.ImageDigestFromReference(op.DesiredSpec.Image)
	}
	for idx := range state.AppImageTrackings {
		tracking := &state.AppImageTrackings[idx]
		if strings.TrimSpace(tracking.AppID) != op.AppID {
			continue
		}
		if strings.TrimSpace(op.DesiredSource.Type) != model.AppSourceTypeDockerImage ||
			strings.TrimSpace(tracking.ImageRef) != imageRef ||
			imageRef == "" ||
			digest == "" {
			tracking.LastDeployedDigest = ""
		} else {
			tracking.LastDeployedDigest = digest
		}
		tracking.LastOperationID = op.ID
		tracking.LastError = ""
		tracking.UpdatedAt = now
	}
}

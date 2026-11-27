package store

import (
	"errors"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) UpsertImage(image model.Image) (model.Image, error) {
	image = normalizeImage(image)
	if image.ImageRef == "" && image.CanonicalDigest == "" {
		return model.Image{}, ErrInvalidInput
	}
	if image.LifecycleState == "" {
		return model.Image{}, ErrInvalidInput
	}
	if err := validateAvailableImage(image); err != nil {
		return model.Image{}, err
	}
	return s.pgUpsertImage(image)
}

func (s *Store) DemoteLegacyImageIfAvailableCanonicalPeer(
	legacyID,
	tenantID,
	imageRef,
	canonicalDigest string,
) (model.Image, error) {
	legacyID = strings.TrimSpace(legacyID)
	tenantID = strings.TrimSpace(tenantID)
	imageRef = strings.TrimSpace(imageRef)
	canonicalDigest = CanonicalImageDigest(canonicalDigest)
	if legacyID == "" || imageRef == "" || canonicalDigest == "" {
		return model.Image{}, ErrInvalidInput
	}
	return s.pgDemoteLegacyImageIfAvailableCanonicalPeer(legacyID, tenantID, imageRef, canonicalDigest)
}

func (s *Store) GetImage(id, tenantID string, platformAdmin bool) (model.Image, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.Image{}, ErrInvalidInput
	}
	return s.pgGetImage(id, tenantID, platformAdmin)
}

func (s *Store) ListImages(filter model.ImageFilter) ([]model.Image, error) {
	filter = normalizeImageFilter(filter)
	return s.pgListImages(filter)
}

func (s *Store) UpsertImageAlias(alias model.ImageAlias) (model.ImageAlias, error) {
	alias = normalizeImageAlias(alias)
	if alias.ImageID == "" || alias.AliasRef == "" {
		return model.ImageAlias{}, ErrInvalidInput
	}
	return s.pgUpsertImageAlias(alias)
}

func (s *Store) ListImageAliases(filter model.ImageAliasFilter) ([]model.ImageAlias, error) {
	filter = normalizeImageAliasFilter(filter)
	return s.pgListImageAliases(filter)
}

func (s *Store) UpsertImageReplica(replica model.ImageReplica) (model.ImageReplica, error) {
	rawReplicaDigest := strings.TrimSpace(replica.Digest)
	replica = normalizeImageReplica(replica)
	if replica.ImageID == "" || replica.Status == "" {
		return model.ImageReplica{}, ErrInvalidInput
	}
	if replica.Status == model.ImageReplicaStatusPresent {
		image, err := s.GetImage(replica.ImageID, "", true)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return model.ImageReplica{}, err
		}
		if err == nil {
			wantDigest := CanonicalImageDigest(image.CanonicalDigest)
			gotDigest := CanonicalImageDigest(replica.Digest)
			if wantDigest == "" {
				return model.ImageReplica{}, errors.Join(ErrInvalidInput, ErrCanonicalDigestRequired)
			}
			if rawReplicaDigest != "" && gotDigest == "" {
				return model.ImageReplica{}, errors.Join(ErrInvalidInput, ErrCanonicalDigestRequired)
			}
			if gotDigest == "" {
				replica.Digest = wantDigest
			} else if wantDigest != "" && gotDigest != wantDigest {
				return model.ImageReplica{}, errors.Join(ErrInvalidInput, ErrImageDigestMismatch)
			}
		}
	}
	if err := validatePresentImageReplica(replica); err != nil {
		return model.ImageReplica{}, err
	}
	return s.pgUpsertImageReplica(replica)
}

func (s *Store) ListImageReplicas(filter model.ImageReplicaFilter) ([]model.ImageReplica, error) {
	filter = normalizeImageReplicaFilter(filter)
	return s.pgListImageReplicas(filter)
}

// MarkStaleImageReplicas expires leased replicas only after their lease ends.
// The cutoff remains a compatibility fallback for historical replicas that do
// not have lease metadata.
func (s *Store) MarkStaleImageReplicas(now, unleasedCutoff time.Time) (int, error) {
	return s.pgMarkStaleImageReplicas(now, unleasedCutoff)
}

func (s *Store) UpsertImagePin(pin model.ImagePin) (model.ImagePin, error) {
	pin = normalizeImagePin(pin)
	if pin.ImageID == "" || pin.Reason == "" {
		return model.ImagePin{}, ErrInvalidInput
	}
	return s.pgUpsertImagePin(pin)
}

func (s *Store) DeleteImagePin(id, tenantID string, platformAdmin bool) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrInvalidInput
	}
	return s.pgDeleteImagePin(id, tenantID, platformAdmin)
}

func (s *Store) ListImagePins(filter model.ImagePinFilter) ([]model.ImagePin, error) {
	filter = normalizeImagePinFilter(filter)
	return s.pgListImagePins(filter)
}

func (s *Store) UpsertImageReplicationTask(task model.ImageReplicationTask) (model.ImageReplicationTask, error) {
	task = normalizeImageReplicationTask(task)
	if task.ImageID == "" || task.Status == "" || task.Priority == "" {
		return model.ImageReplicationTask{}, ErrInvalidInput
	}
	return s.pgUpsertImageReplicationTask(task)
}

func (s *Store) ListImageReplicationTasks(filter model.ImageReplicationTaskFilter) ([]model.ImageReplicationTask, error) {
	filter = normalizeImageReplicationTaskFilter(filter)
	return s.pgListImageReplicationTasks(filter)
}

func normalizeImage(image model.Image) model.Image {
	sanitizeImageMetadata(&image)
	rawLifecycle := strings.TrimSpace(image.LifecycleState)
	image.TenantID = strings.TrimSpace(image.TenantID)
	image.AppID = strings.TrimSpace(image.AppID)
	image.ImageRef = strings.TrimSpace(image.ImageRef)
	image.CanonicalDigest = normalizeImageDigest(image.CanonicalDigest)
	if image.CanonicalDigest == "" {
		image.CanonicalDigest = imageDigestFromReference(image.ImageRef)
	}
	image.MediaType = strings.TrimSpace(image.MediaType)
	image.ManifestJSON = strings.TrimSpace(image.ManifestJSON)
	image.SourceOperationID = strings.TrimSpace(image.SourceOperationID)
	image.LifecycleState = normalizeImageLifecycleState(image.LifecycleState)
	if rawLifecycle == "" {
		// An image without a verified content identity is still being imported
		// or repaired.  It must never silently enter Available merely because a
		// caller omitted the lifecycle field.
		if image.CanonicalDigest == "" {
			image.LifecycleState = model.ImageLifecycleImporting
		} else {
			image.LifecycleState = model.ImageLifecycleAvailable
		}
	}
	if image.ManifestSizeBytes < 0 {
		image.ManifestSizeBytes = 0
	}
	if image.BlobBytes < 0 {
		image.BlobBytes = 0
	}
	if image.RequiredReplicaCount < 0 {
		image.RequiredReplicaCount = 0
	}
	if image.MinAvailableReplicaCount < 0 {
		image.MinAvailableReplicaCount = 0
	}
	return image
}

func sanitizeImageMetadata(image *model.Image) {
	if image == nil {
		return
	}
	image.ID = stripImageNUL(image.ID)
	image.TenantID = stripImageNUL(image.TenantID)
	image.AppID = stripImageNUL(image.AppID)
	image.ImageRef = stripImageNUL(image.ImageRef)
	image.CanonicalDigest = stripImageNUL(image.CanonicalDigest)
	image.MediaType = stripImageNUL(image.MediaType)
	image.ManifestJSON = stripImageNUL(image.ManifestJSON)
	image.SourceOperationID = stripImageNUL(image.SourceOperationID)
	image.LifecycleState = stripImageNUL(image.LifecycleState)
}

func stripImageNUL(value string) string {
	return strings.ReplaceAll(value, "\x00", "")
}

func normalizeImageFilter(filter model.ImageFilter) model.ImageFilter {
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.AppID = strings.TrimSpace(filter.AppID)
	filter.ImageRef = strings.TrimSpace(filter.ImageRef)
	filter.CanonicalDigest = normalizeImageDigest(filter.CanonicalDigest)
	filter.LifecycleState = normalizeImageLifecycleStateFilter(filter.LifecycleState)
	filter.AppIDs = sortedTrimmedStringKeys(trimmedStringSet(filter.AppIDs))
	return filter
}

func normalizeImageLifecycleState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", model.ImageLifecycleAvailable:
		return model.ImageLifecycleAvailable
	case model.ImageLifecycleImporting:
		return model.ImageLifecycleImporting
	case model.ImageLifecycleDeleting:
		return model.ImageLifecycleDeleting
	case model.ImageLifecycleDeleted:
		return model.ImageLifecycleDeleted
	case model.ImageLifecycleLost:
		return model.ImageLifecycleLost
	default:
		return ""
	}
}

func normalizeImageLifecycleStateFilter(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	return normalizeImageLifecycleState(raw)
}

func normalizeImageAlias(alias model.ImageAlias) model.ImageAlias {
	alias.ID = stripImageNUL(alias.ID)
	alias.ImageID = stripImageNUL(alias.ImageID)
	alias.TenantID = stripImageNUL(alias.TenantID)
	alias.AliasRef = stripImageNUL(alias.AliasRef)
	alias.Digest = stripImageNUL(alias.Digest)
	alias.ImageID = strings.TrimSpace(alias.ImageID)
	alias.TenantID = strings.TrimSpace(alias.TenantID)
	alias.AliasRef = strings.TrimSpace(alias.AliasRef)
	alias.Digest = normalizeImageDigest(alias.Digest)
	return alias
}

func normalizeImageAliasFilter(filter model.ImageAliasFilter) model.ImageAliasFilter {
	filter.ImageID = strings.TrimSpace(filter.ImageID)
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.AliasRef = strings.TrimSpace(filter.AliasRef)
	filter.Digest = normalizeImageDigest(filter.Digest)
	return filter
}

func normalizeImageReplica(replica model.ImageReplica) model.ImageReplica {
	replica.ID = stripImageNUL(replica.ID)
	replica.ImageID = stripImageNUL(replica.ImageID)
	replica.TenantID = stripImageNUL(replica.TenantID)
	replica.AppID = stripImageNUL(replica.AppID)
	replica.Digest = stripImageNUL(replica.Digest)
	replica.NodeID = stripImageNUL(replica.NodeID)
	replica.RuntimeID = stripImageNUL(replica.RuntimeID)
	replica.ClusterNodeName = stripImageNUL(replica.ClusterNodeName)
	replica.CacheEndpoint = stripImageNUL(replica.CacheEndpoint)
	replica.FailureDomain = stripImageNUL(replica.FailureDomain)
	replica.Status = stripImageNUL(replica.Status)
	replica.SourceReplicaID = stripImageNUL(replica.SourceReplicaID)
	replica.LastError = stripImageNUL(replica.LastError)
	replica.ImageID = strings.TrimSpace(replica.ImageID)
	replica.TenantID = strings.TrimSpace(replica.TenantID)
	replica.AppID = strings.TrimSpace(replica.AppID)
	replica.Digest = normalizeImageDigest(replica.Digest)
	replica.NodeID = strings.TrimSpace(replica.NodeID)
	replica.RuntimeID = strings.TrimSpace(replica.RuntimeID)
	replica.ClusterNodeName = strings.TrimSpace(replica.ClusterNodeName)
	replica.CacheEndpoint = strings.TrimRight(strings.TrimSpace(replica.CacheEndpoint), "/")
	replica.FailureDomain = strings.TrimSpace(replica.FailureDomain)
	replica.Status = normalizeImageReplicaStatus(replica.Status)
	if replica.Status == "" {
		replica.Status = model.ImageReplicaStatusPresent
	}
	replica.SourceReplicaID = strings.TrimSpace(replica.SourceReplicaID)
	replica.LastError = strings.TrimSpace(replica.LastError)
	if replica.SizeBytes < 0 {
		replica.SizeBytes = 0
	}
	return replica
}

func normalizeImageReplicaFilter(filter model.ImageReplicaFilter) model.ImageReplicaFilter {
	filter.ImageID = strings.TrimSpace(filter.ImageID)
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.AppID = strings.TrimSpace(filter.AppID)
	filter.Digest = normalizeImageDigest(filter.Digest)
	filter.NodeID = strings.TrimSpace(filter.NodeID)
	filter.RuntimeID = strings.TrimSpace(filter.RuntimeID)
	filter.ClusterNodeName = strings.TrimSpace(filter.ClusterNodeName)
	filter.Status = normalizeImageReplicaStatusFilter(filter.Status)
	return filter
}

func normalizeImageReplicaStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", model.ImageReplicaStatusPresent:
		return model.ImageReplicaStatusPresent
	case model.ImageReplicaStatusPlanned:
		return model.ImageReplicaStatusPlanned
	case model.ImageReplicaStatusCopying:
		return model.ImageReplicaStatusCopying
	case model.ImageReplicaStatusVerifying:
		return model.ImageReplicaStatusVerifying
	case model.ImageReplicaStatusStale:
		return model.ImageReplicaStatusStale
	case model.ImageReplicaStatusDraining:
		return model.ImageReplicaStatusDraining
	case model.ImageReplicaStatusDeleting:
		return model.ImageReplicaStatusDeleting
	case model.ImageReplicaStatusMissing:
		return model.ImageReplicaStatusMissing
	case model.ImageReplicaStatusFailed:
		return model.ImageReplicaStatusFailed
	default:
		return ""
	}
}

func normalizeImageReplicaStatusFilter(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	return normalizeImageReplicaStatus(raw)
}

func normalizeImagePin(pin model.ImagePin) model.ImagePin {
	pin.ID = stripImageNUL(pin.ID)
	pin.ImageID = stripImageNUL(pin.ImageID)
	pin.TenantID = stripImageNUL(pin.TenantID)
	pin.AppID = stripImageNUL(pin.AppID)
	pin.OperationID = stripImageNUL(pin.OperationID)
	pin.Reason = stripImageNUL(pin.Reason)
	pin.ImageID = strings.TrimSpace(pin.ImageID)
	pin.TenantID = strings.TrimSpace(pin.TenantID)
	pin.AppID = strings.TrimSpace(pin.AppID)
	pin.OperationID = strings.TrimSpace(pin.OperationID)
	pin.Reason = normalizeImagePinReason(pin.Reason)
	if pin.Reason == "" {
		pin.Reason = model.ImagePinReasonUserPin
	}
	if pin.MinReplicas < 0 {
		pin.MinReplicas = 0
	}
	return pin
}

func normalizeImagePinFilter(filter model.ImagePinFilter) model.ImagePinFilter {
	filter.ImageID = strings.TrimSpace(filter.ImageID)
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.AppID = strings.TrimSpace(filter.AppID)
	filter.OperationID = strings.TrimSpace(filter.OperationID)
	filter.Reason = normalizeImagePinReasonFilter(filter.Reason)
	return filter
}

func normalizeImagePinReason(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", model.ImagePinReasonUserPin:
		return model.ImagePinReasonUserPin
	case model.ImagePinReasonCurrentDeploy:
		return model.ImagePinReasonCurrentDeploy
	case model.ImagePinReasonRollbackWindow:
		return model.ImagePinReasonRollbackWindow
	case model.ImagePinReasonImportResult:
		return model.ImagePinReasonImportResult
	case model.ImagePinReasonRetention:
		return model.ImagePinReasonRetention
	case model.ImagePinReasonReplicationSeed:
		return model.ImagePinReasonReplicationSeed
	default:
		return ""
	}
}

func normalizeImagePinReasonFilter(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	return normalizeImagePinReason(raw)
}

func normalizeImageReplicationTask(task model.ImageReplicationTask) model.ImageReplicationTask {
	task.ID = stripImageNUL(task.ID)
	task.ImageID = stripImageNUL(task.ImageID)
	task.TenantID = stripImageNUL(task.TenantID)
	task.AppID = stripImageNUL(task.AppID)
	task.SourceReplicaID = stripImageNUL(task.SourceReplicaID)
	task.SourceCacheEndpoint = stripImageNUL(task.SourceCacheEndpoint)
	task.TargetNodeID = stripImageNUL(task.TargetNodeID)
	task.TargetRuntimeID = stripImageNUL(task.TargetRuntimeID)
	task.TargetClusterNodeName = stripImageNUL(task.TargetClusterNodeName)
	task.Priority = stripImageNUL(task.Priority)
	task.Status = stripImageNUL(task.Status)
	task.LastError = stripImageNUL(task.LastError)
	task.ImageID = strings.TrimSpace(task.ImageID)
	task.TenantID = strings.TrimSpace(task.TenantID)
	task.AppID = strings.TrimSpace(task.AppID)
	task.SourceReplicaID = strings.TrimSpace(task.SourceReplicaID)
	task.SourceCacheEndpoint = strings.TrimRight(strings.TrimSpace(task.SourceCacheEndpoint), "/")
	task.TargetNodeID = strings.TrimSpace(task.TargetNodeID)
	task.TargetRuntimeID = strings.TrimSpace(task.TargetRuntimeID)
	task.TargetClusterNodeName = strings.TrimSpace(task.TargetClusterNodeName)
	task.Priority = normalizeImageReplicationPriority(task.Priority)
	if task.Priority == "" {
		task.Priority = model.ImageReplicationPriorityRepair
	}
	task.Status = normalizeImageReplicationTaskStatus(task.Status)
	if task.Status == "" {
		task.Status = model.ImageReplicationTaskStatusPending
	}
	task.LastError = strings.TrimSpace(task.LastError)
	if task.Attempts < 0 {
		task.Attempts = 0
	}
	return task
}

func normalizeImageReplicationTaskFilter(filter model.ImageReplicationTaskFilter) model.ImageReplicationTaskFilter {
	filter.ImageID = strings.TrimSpace(filter.ImageID)
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.AppID = strings.TrimSpace(filter.AppID)
	filter.SourceReplicaID = strings.TrimSpace(filter.SourceReplicaID)
	filter.TargetNodeID = strings.TrimSpace(filter.TargetNodeID)
	filter.TargetRuntimeID = strings.TrimSpace(filter.TargetRuntimeID)
	filter.TargetClusterNodeName = strings.TrimSpace(filter.TargetClusterNodeName)
	filter.Priority = normalizeImageReplicationPriorityFilter(filter.Priority)
	filter.Status = normalizeImageReplicationTaskStatusFilter(filter.Status)
	return filter
}

func normalizeImageReplicationTaskStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", model.ImageReplicationTaskStatusPending:
		return model.ImageReplicationTaskStatusPending
	case model.ImageReplicationTaskStatusRunning:
		return model.ImageReplicationTaskStatusRunning
	case model.ImageReplicationTaskStatusCompleted:
		return model.ImageReplicationTaskStatusCompleted
	case model.ImageReplicationTaskStatusFailed:
		return model.ImageReplicationTaskStatusFailed
	case model.ImageReplicationTaskStatusCanceled:
		return model.ImageReplicationTaskStatusCanceled
	default:
		return ""
	}
}

func normalizeImageReplicationTaskStatusFilter(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	return normalizeImageReplicationTaskStatus(raw)
}

func normalizeImageReplicationPriority(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", model.ImageReplicationPriorityRepair:
		return model.ImageReplicationPriorityRepair
	case model.ImageReplicationPriorityDeployBlocking:
		return model.ImageReplicationPriorityDeployBlocking
	case model.ImageReplicationPriorityWarmup:
		return model.ImageReplicationPriorityWarmup
	case model.ImageReplicationPriorityRebalance:
		return model.ImageReplicationPriorityRebalance
	default:
		return ""
	}
}

func normalizeImageReplicationPriorityFilter(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	return normalizeImageReplicationPriority(raw)
}

func firstNonEmptyImageString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

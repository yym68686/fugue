package store

import (
	"sort"
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
	if s.usingDatabase() {
		return s.pgUpsertImage(image)
	}
	var out model.Image
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findImage(state.Images, image)
		if index >= 0 {
			current := state.Images[index]
			image.ID = current.ID
			image.CreatedAt = current.CreatedAt
			if image.TenantID == "" {
				image.TenantID = current.TenantID
			}
			if image.AppID == "" {
				image.AppID = current.AppID
			}
			if image.SourceOperationID == "" {
				image.SourceOperationID = current.SourceOperationID
			}
			image.UpdatedAt = now
			state.Images[index] = image
			out = image
			return nil
		}
		image.ID = firstNonEmptyImageString(image.ID, model.NewID("img"))
		image.CreatedAt = now
		image.UpdatedAt = now
		state.Images = append(state.Images, image)
		out = image
		return nil
	})
	return out, err
}

func (s *Store) GetImage(id, tenantID string, platformAdmin bool) (model.Image, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.Image{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetImage(id, tenantID, platformAdmin)
	}
	var out model.Image
	err := s.withLockedState(false, func(state *model.State) error {
		for _, image := range state.Images {
			if image.ID != id {
				continue
			}
			if !platformAdmin && strings.TrimSpace(image.TenantID) != strings.TrimSpace(tenantID) {
				return ErrNotFound
			}
			out = image
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

func (s *Store) ListImages(filter model.ImageFilter) ([]model.Image, error) {
	filter = normalizeImageFilter(filter)
	if s.usingDatabase() {
		return s.pgListImages(filter)
	}
	out := []model.Image{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, image := range state.Images {
			if imageMatchesFilter(image, filter) {
				out = append(out, image)
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

func (s *Store) UpsertImageAlias(alias model.ImageAlias) (model.ImageAlias, error) {
	alias = normalizeImageAlias(alias)
	if alias.ImageID == "" || alias.AliasRef == "" {
		return model.ImageAlias{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpsertImageAlias(alias)
	}
	var out model.ImageAlias
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findImageAlias(state.ImageAliases, alias)
		if index >= 0 {
			current := state.ImageAliases[index]
			alias.ID = current.ID
			alias.CreatedAt = current.CreatedAt
			if alias.TenantID == "" {
				alias.TenantID = current.TenantID
			}
			alias.UpdatedAt = now
			state.ImageAliases[index] = alias
			out = alias
			return nil
		}
		alias.ID = firstNonEmptyImageString(alias.ID, model.NewID("imgalias"))
		alias.CreatedAt = now
		alias.UpdatedAt = now
		state.ImageAliases = append(state.ImageAliases, alias)
		out = alias
		return nil
	})
	return out, err
}

func (s *Store) ListImageAliases(filter model.ImageAliasFilter) ([]model.ImageAlias, error) {
	filter = normalizeImageAliasFilter(filter)
	if s.usingDatabase() {
		return s.pgListImageAliases(filter)
	}
	out := []model.ImageAlias{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, alias := range state.ImageAliases {
			if imageAliasMatchesFilter(alias, filter) {
				out = append(out, alias)
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

func (s *Store) UpsertImageReplica(replica model.ImageReplica) (model.ImageReplica, error) {
	replica = normalizeImageReplica(replica)
	if replica.ImageID == "" || replica.Status == "" {
		return model.ImageReplica{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpsertImageReplica(replica)
	}
	var out model.ImageReplica
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findImageReplica(state.ImageReplicas, replica)
		if index >= 0 {
			current := state.ImageReplicas[index]
			replica.ID = current.ID
			replica.CreatedAt = current.CreatedAt
			if replica.TenantID == "" {
				replica.TenantID = current.TenantID
			}
			if replica.AppID == "" {
				replica.AppID = current.AppID
			}
			if replica.Digest == "" {
				replica.Digest = current.Digest
			}
			if replica.CacheEndpoint == "" {
				replica.CacheEndpoint = current.CacheEndpoint
			}
			replica.UpdatedAt = now
			state.ImageReplicas[index] = replica
			out = replica
			return nil
		}
		replica.ID = firstNonEmptyImageString(replica.ID, model.NewID("imgrep"))
		replica.CreatedAt = now
		replica.UpdatedAt = now
		state.ImageReplicas = append(state.ImageReplicas, replica)
		out = replica
		return nil
	})
	return out, err
}

func (s *Store) ListImageReplicas(filter model.ImageReplicaFilter) ([]model.ImageReplica, error) {
	filter = normalizeImageReplicaFilter(filter)
	if s.usingDatabase() {
		return s.pgListImageReplicas(filter)
	}
	out := []model.ImageReplica{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, replica := range state.ImageReplicas {
			if imageReplicaMatchesFilter(replica, filter) {
				out = append(out, replica)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		left := imageReplicaSeenAt(out[i])
		right := imageReplicaSeenAt(out[j])
		if !left.Equal(right) {
			return left.After(right)
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out, nil
}

func (s *Store) MarkStaleImageReplicas(cutoff time.Time) (int, error) {
	if s.usingDatabase() {
		return s.pgMarkStaleImageReplicas(cutoff)
	}
	count := 0
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		for idx := range state.ImageReplicas {
			replica := &state.ImageReplicas[idx]
			if replica.Status != model.ImageReplicaStatusPresent {
				continue
			}
			seen := imageReplicaSeenAt(*replica)
			if seen.IsZero() || !seen.Before(cutoff) {
				continue
			}
			replica.Status = model.ImageReplicaStatusStale
			replica.UpdatedAt = now
			count++
		}
		return nil
	})
	return count, err
}

func (s *Store) UpsertImagePin(pin model.ImagePin) (model.ImagePin, error) {
	pin = normalizeImagePin(pin)
	if pin.ImageID == "" || pin.Reason == "" {
		return model.ImagePin{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpsertImagePin(pin)
	}
	var out model.ImagePin
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findImagePin(state.ImagePins, pin)
		if index >= 0 {
			current := state.ImagePins[index]
			pin.ID = current.ID
			pin.CreatedAt = current.CreatedAt
			if pin.TenantID == "" {
				pin.TenantID = current.TenantID
			}
			if pin.AppID == "" {
				pin.AppID = current.AppID
			}
			pin.UpdatedAt = now
			state.ImagePins[index] = pin
			out = pin
			return nil
		}
		pin.ID = firstNonEmptyImageString(pin.ID, model.NewID("imgpin"))
		pin.CreatedAt = now
		pin.UpdatedAt = now
		state.ImagePins = append(state.ImagePins, pin)
		out = pin
		return nil
	})
	return out, err
}

func (s *Store) DeleteImagePin(id, tenantID string, platformAdmin bool) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteImagePin(id, tenantID, platformAdmin)
	}
	return s.withLockedState(true, func(state *model.State) error {
		for idx, pin := range state.ImagePins {
			if pin.ID != id {
				continue
			}
			if !platformAdmin && strings.TrimSpace(pin.TenantID) != strings.TrimSpace(tenantID) {
				return ErrNotFound
			}
			state.ImagePins = append(state.ImagePins[:idx], state.ImagePins[idx+1:]...)
			return nil
		}
		return ErrNotFound
	})
}

func (s *Store) ListImagePins(filter model.ImagePinFilter) ([]model.ImagePin, error) {
	filter = normalizeImagePinFilter(filter)
	if s.usingDatabase() {
		return s.pgListImagePins(filter)
	}
	out := []model.ImagePin{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, pin := range state.ImagePins {
			if imagePinMatchesFilter(pin, filter) {
				out = append(out, pin)
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

func (s *Store) UpsertImageReplicationTask(task model.ImageReplicationTask) (model.ImageReplicationTask, error) {
	task = normalizeImageReplicationTask(task)
	if task.ImageID == "" || task.Status == "" || task.Priority == "" {
		return model.ImageReplicationTask{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpsertImageReplicationTask(task)
	}
	var out model.ImageReplicationTask
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findImageReplicationTask(state.ImageReplicationTasks, task)
		if index >= 0 {
			current := state.ImageReplicationTasks[index]
			task.ID = current.ID
			task.CreatedAt = current.CreatedAt
			if task.TenantID == "" {
				task.TenantID = current.TenantID
			}
			if task.AppID == "" {
				task.AppID = current.AppID
			}
			task.UpdatedAt = now
			state.ImageReplicationTasks[index] = task
			out = task
			return nil
		}
		task.ID = firstNonEmptyImageString(task.ID, model.NewID("imgtask"))
		task.CreatedAt = now
		task.UpdatedAt = now
		state.ImageReplicationTasks = append(state.ImageReplicationTasks, task)
		out = task
		return nil
	})
	return out, err
}

func (s *Store) ListImageReplicationTasks(filter model.ImageReplicationTaskFilter) ([]model.ImageReplicationTask, error) {
	filter = normalizeImageReplicationTaskFilter(filter)
	if s.usingDatabase() {
		return s.pgListImageReplicationTasks(filter)
	}
	out := []model.ImageReplicationTask{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, task := range state.ImageReplicationTasks {
			if imageReplicationTaskMatchesFilter(task, filter) {
				out = append(out, task)
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

func normalizeImage(image model.Image) model.Image {
	image.TenantID = strings.TrimSpace(image.TenantID)
	image.AppID = strings.TrimSpace(image.AppID)
	image.ImageRef = strings.TrimSpace(image.ImageRef)
	image.CanonicalDigest = normalizeImageDigest(image.CanonicalDigest)
	image.MediaType = strings.TrimSpace(image.MediaType)
	image.ManifestJSON = strings.TrimSpace(image.ManifestJSON)
	image.SourceOperationID = strings.TrimSpace(image.SourceOperationID)
	image.LifecycleState = normalizeImageLifecycleState(image.LifecycleState)
	if image.LifecycleState == "" {
		image.LifecycleState = model.ImageLifecycleAvailable
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

func normalizeImageFilter(filter model.ImageFilter) model.ImageFilter {
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.AppID = strings.TrimSpace(filter.AppID)
	filter.ImageRef = strings.TrimSpace(filter.ImageRef)
	filter.CanonicalDigest = normalizeImageDigest(filter.CanonicalDigest)
	filter.LifecycleState = normalizeImageLifecycleStateFilter(filter.LifecycleState)
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

func findImage(images []model.Image, image model.Image) int {
	if image.ID != "" {
		for idx, existing := range images {
			if existing.ID == image.ID {
				return idx
			}
		}
	}
	for idx, existing := range images {
		if strings.TrimSpace(existing.TenantID) != image.TenantID {
			continue
		}
		if strings.TrimSpace(existing.ImageRef) == image.ImageRef && normalizeImageDigest(existing.CanonicalDigest) == image.CanonicalDigest {
			return idx
		}
	}
	return -1
}

func findImageAlias(aliases []model.ImageAlias, alias model.ImageAlias) int {
	if alias.ID != "" {
		for idx, existing := range aliases {
			if existing.ID == alias.ID {
				return idx
			}
		}
	}
	for idx, existing := range aliases {
		if existing.ImageID == alias.ImageID && existing.AliasRef == alias.AliasRef {
			return idx
		}
	}
	return -1
}

func findImageReplica(replicas []model.ImageReplica, replica model.ImageReplica) int {
	if replica.ID != "" {
		for idx, existing := range replicas {
			if existing.ID == replica.ID {
				return idx
			}
		}
	}
	for idx, existing := range replicas {
		if existing.ImageID != replica.ImageID {
			continue
		}
		if strings.TrimSpace(existing.NodeID) != replica.NodeID {
			continue
		}
		if strings.TrimSpace(existing.RuntimeID) != replica.RuntimeID {
			continue
		}
		if strings.TrimSpace(existing.ClusterNodeName) != replica.ClusterNodeName {
			continue
		}
		return idx
	}
	return -1
}

func findImagePin(pins []model.ImagePin, pin model.ImagePin) int {
	if pin.ID != "" {
		for idx, existing := range pins {
			if existing.ID == pin.ID {
				return idx
			}
		}
	}
	for idx, existing := range pins {
		if existing.ImageID == pin.ImageID &&
			strings.TrimSpace(existing.AppID) == pin.AppID &&
			strings.TrimSpace(existing.OperationID) == pin.OperationID &&
			strings.TrimSpace(existing.Reason) == pin.Reason {
			return idx
		}
	}
	return -1
}

func findImageReplicationTask(tasks []model.ImageReplicationTask, task model.ImageReplicationTask) int {
	if task.ID != "" {
		for idx, existing := range tasks {
			if existing.ID == task.ID {
				return idx
			}
		}
	}
	for idx, existing := range tasks {
		if existing.ImageID == task.ImageID &&
			strings.TrimSpace(existing.SourceReplicaID) == task.SourceReplicaID &&
			strings.TrimSpace(existing.TargetNodeID) == task.TargetNodeID &&
			strings.TrimSpace(existing.TargetRuntimeID) == task.TargetRuntimeID &&
			strings.TrimSpace(existing.TargetClusterNodeName) == task.TargetClusterNodeName &&
			strings.TrimSpace(existing.Priority) == task.Priority &&
			(existing.Status == model.ImageReplicationTaskStatusPending || existing.Status == model.ImageReplicationTaskStatusRunning) {
			return idx
		}
	}
	return -1
}

func imageMatchesFilter(image model.Image, filter model.ImageFilter) bool {
	if !filter.PlatformAdmin && strings.TrimSpace(image.TenantID) != filter.TenantID {
		return false
	}
	if filter.AppID != "" && strings.TrimSpace(image.AppID) != filter.AppID {
		return false
	}
	if filter.ImageRef != "" && strings.TrimSpace(image.ImageRef) != filter.ImageRef {
		return false
	}
	if filter.CanonicalDigest != "" && normalizeImageDigest(image.CanonicalDigest) != filter.CanonicalDigest {
		return false
	}
	if filter.LifecycleState != "" && strings.TrimSpace(image.LifecycleState) != filter.LifecycleState {
		return false
	}
	return true
}

func imageAliasMatchesFilter(alias model.ImageAlias, filter model.ImageAliasFilter) bool {
	if !filter.PlatformAdmin && strings.TrimSpace(alias.TenantID) != filter.TenantID {
		return false
	}
	if filter.ImageID != "" && strings.TrimSpace(alias.ImageID) != filter.ImageID {
		return false
	}
	if filter.AliasRef != "" && strings.TrimSpace(alias.AliasRef) != filter.AliasRef {
		return false
	}
	if filter.Digest != "" && normalizeImageDigest(alias.Digest) != filter.Digest {
		return false
	}
	return true
}

func imageReplicaMatchesFilter(replica model.ImageReplica, filter model.ImageReplicaFilter) bool {
	if !filter.PlatformAdmin && strings.TrimSpace(replica.TenantID) != filter.TenantID {
		return false
	}
	if filter.ImageID != "" && strings.TrimSpace(replica.ImageID) != filter.ImageID {
		return false
	}
	if filter.AppID != "" && strings.TrimSpace(replica.AppID) != filter.AppID {
		return false
	}
	if filter.Digest != "" && normalizeImageDigest(replica.Digest) != filter.Digest {
		return false
	}
	if filter.NodeID != "" && strings.TrimSpace(replica.NodeID) != filter.NodeID {
		return false
	}
	if filter.RuntimeID != "" && strings.TrimSpace(replica.RuntimeID) != filter.RuntimeID {
		return false
	}
	if filter.ClusterNodeName != "" && strings.TrimSpace(replica.ClusterNodeName) != filter.ClusterNodeName {
		return false
	}
	if filter.Status != "" && strings.TrimSpace(replica.Status) != filter.Status {
		return false
	}
	return true
}

func imagePinMatchesFilter(pin model.ImagePin, filter model.ImagePinFilter) bool {
	if !filter.PlatformAdmin && strings.TrimSpace(pin.TenantID) != filter.TenantID {
		return false
	}
	if filter.ImageID != "" && strings.TrimSpace(pin.ImageID) != filter.ImageID {
		return false
	}
	if filter.AppID != "" && strings.TrimSpace(pin.AppID) != filter.AppID {
		return false
	}
	if filter.OperationID != "" && strings.TrimSpace(pin.OperationID) != filter.OperationID {
		return false
	}
	if filter.Reason != "" && strings.TrimSpace(pin.Reason) != filter.Reason {
		return false
	}
	return true
}

func imageReplicationTaskMatchesFilter(task model.ImageReplicationTask, filter model.ImageReplicationTaskFilter) bool {
	if !filter.PlatformAdmin && strings.TrimSpace(task.TenantID) != filter.TenantID {
		return false
	}
	if filter.ImageID != "" && strings.TrimSpace(task.ImageID) != filter.ImageID {
		return false
	}
	if filter.AppID != "" && strings.TrimSpace(task.AppID) != filter.AppID {
		return false
	}
	if filter.SourceReplicaID != "" && strings.TrimSpace(task.SourceReplicaID) != filter.SourceReplicaID {
		return false
	}
	if filter.TargetNodeID != "" && strings.TrimSpace(task.TargetNodeID) != filter.TargetNodeID {
		return false
	}
	if filter.TargetRuntimeID != "" && strings.TrimSpace(task.TargetRuntimeID) != filter.TargetRuntimeID {
		return false
	}
	if filter.TargetClusterNodeName != "" && strings.TrimSpace(task.TargetClusterNodeName) != filter.TargetClusterNodeName {
		return false
	}
	if filter.Priority != "" && strings.TrimSpace(task.Priority) != filter.Priority {
		return false
	}
	if filter.Status != "" && strings.TrimSpace(task.Status) != filter.Status {
		return false
	}
	return true
}

func imageReplicaSeenAt(replica model.ImageReplica) time.Time {
	if replica.LastVerifiedAt != nil {
		return *replica.LastVerifiedAt
	}
	return replica.UpdatedAt
}

func firstNonEmptyImageString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

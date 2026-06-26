package controller

import (
	"context"
	"errors"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Service) imageStoreDistributedMode() bool {
	if s == nil {
		return false
	}
	switch strings.TrimSpace(s.Config.ImageStoreMode) {
	case "distributed", "distributed-with-registry-fallback":
		return true
	default:
		return false
	}
}

func (s *Service) imageStoreStrictDistributedMode() bool {
	if s == nil {
		return false
	}
	return strings.TrimSpace(s.Config.ImageStoreMode) == "distributed"
}

func (s *Service) imageStoreRegistryFallbackEnabled() bool {
	if s == nil {
		return true
	}
	switch strings.TrimSpace(s.Config.ImageStoreMode) {
	case "distributed":
		return false
	default:
		return true
	}
}

func (s *Service) reconcileImageReplication(ctx context.Context) error {
	if !s.imageStoreDistributedMode() || s.Store == nil {
		return nil
	}
	now := time.Now().UTC()
	verifyInterval := s.Config.ImageStoreVerifyInterval
	if verifyInterval <= 0 {
		verifyInterval = 10 * time.Minute
	}
	if _, err := s.Store.MarkStaleImageReplicas(now.Add(-verifyInterval)); err != nil {
		return err
	}
	if err := s.completeSatisfiedImageReplicationTasks(ctx); err != nil {
		return err
	}
	images, err := s.Store.ListImages(model.ImageFilter{PlatformAdmin: true})
	if err != nil {
		return err
	}
	for _, image := range images {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch strings.TrimSpace(image.LifecycleState) {
		case "", model.ImageLifecycleAvailable, model.ImageLifecycleImporting:
		default:
			continue
		}
		if err := s.ensureImageReplicaPolicy(ctx, image); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ensureImageReplicaPolicy(ctx context.Context, image model.Image) error {
	target := s.imageTargetReplicaCount(image)
	if target <= 0 {
		return nil
	}
	replicas, err := s.Store.ListImageReplicas(model.ImageReplicaFilter{
		ImageID:       image.ID,
		TenantID:      image.TenantID,
		Status:        model.ImageReplicaStatusPresent,
		PlatformAdmin: true,
	})
	if err != nil {
		return err
	}
	healthy := healthyImageReplicas(replicas, time.Now().UTC())
	if len(healthy) >= target {
		return nil
	}
	if len(healthy) == 0 && s.imageStoreStrictDistributedMode() && strings.TrimSpace(image.LifecycleState) == model.ImageLifecycleAvailable {
		image.LifecycleState = model.ImageLifecycleLost
		if _, err := s.Store.UpsertImage(image); err != nil && s.Logger != nil {
			s.Logger.Printf("mark lost distributed image failed image=%s ref=%s: %v", image.ID, image.ImageRef, err)
		}
		return nil
	}
	updaters, err := s.Store.ListNodeUpdaters("", true)
	if err != nil {
		return err
	}
	source := model.ImageReplica{}
	if len(healthy) > 0 {
		source = healthy[0]
	}
	for _, updater := range updaters {
		if len(healthy) >= target {
			return nil
		}
		if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
			continue
		}
		if imageReplicaExistsOnTarget(healthy, updater) {
			continue
		}
		supported, err := s.Store.NodeUpdaterTargetSupportsTask(updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeReplicateAppImage)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidInput) {
				continue
			}
			return err
		}
		if !supported {
			continue
		}
		if _, err := s.scheduleImageReplicationTask(ctx, image, source, updater, model.ImageReplicationPriorityRepair); err != nil {
			return err
		}
		healthy = append(healthy, model.ImageReplica{
			ImageID:         image.ID,
			TenantID:        image.TenantID,
			AppID:           image.AppID,
			Digest:          image.CanonicalDigest,
			NodeID:          updater.MachineID,
			RuntimeID:       updater.RuntimeID,
			ClusterNodeName: updater.ClusterNodeName,
			Status:          model.ImageReplicaStatusPlanned,
		})
	}
	return nil
}

func (s *Service) scheduleDeployTargetImageReplica(ctx context.Context, image model.Image, source model.ImageReplica, target deployImageTarget) (bool, error) {
	if s == nil || s.Store == nil {
		return false, nil
	}
	updaters, err := s.Store.ListNodeUpdaters("", true)
	if err != nil {
		return false, err
	}
	for _, updater := range updaters {
		if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
			continue
		}
		if strings.TrimSpace(target.ClusterNodeName) != "" && strings.TrimSpace(updater.ClusterNodeName) != strings.TrimSpace(target.ClusterNodeName) {
			continue
		}
		if strings.TrimSpace(target.RuntimeID) != "" && strings.TrimSpace(updater.RuntimeID) != strings.TrimSpace(target.RuntimeID) {
			continue
		}
		supported, err := s.Store.NodeUpdaterTargetSupportsTask(updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeReplicateAppImage)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidInput) {
				continue
			}
			return false, err
		}
		if !supported {
			continue
		}
		if _, err := s.scheduleImageReplicationTask(ctx, image, source, updater, model.ImageReplicationPriorityDeployBlocking); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (s *Service) scheduleImageReplicationTask(ctx context.Context, image model.Image, source model.ImageReplica, updater model.NodeUpdater, priority string) (model.ImageReplicationTask, error) {
	task, err := s.Store.UpsertImageReplicationTask(model.ImageReplicationTask{
		ImageID:               image.ID,
		TenantID:              image.TenantID,
		AppID:                 image.AppID,
		SourceReplicaID:       source.ID,
		SourceCacheEndpoint:   source.CacheEndpoint,
		TargetNodeID:          updater.MachineID,
		TargetRuntimeID:       updater.RuntimeID,
		TargetClusterNodeName: updater.ClusterNodeName,
		Priority:              priority,
		Status:                model.ImageReplicationTaskStatusPending,
	})
	if err != nil {
		return model.ImageReplicationTask{}, err
	}
	_, err = s.Store.CreateNodeUpdateTask(model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   "fugue-controller/image-replication",
		TenantID:  strings.TrimSpace(image.TenantID),
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}, updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeReplicateAppImage, map[string]string{
		"image_id":              image.ID,
		"image_ref":             image.ImageRef,
		"digest":                image.CanonicalDigest,
		"app_id":                image.AppID,
		"source_replica_id":     source.ID,
		"source_cache_endpoint": source.CacheEndpoint,
		"replication_task_id":   task.ID,
		"priority":              priority,
	})
	if err != nil && !errors.Is(err, store.ErrInvalidInput) {
		return model.ImageReplicationTask{}, err
	}
	_ = ctx
	return task, nil
}

func (s *Service) completeSatisfiedImageReplicationTasks(ctx context.Context) error {
	tasks, err := s.Store.ListImageReplicationTasks(model.ImageReplicationTaskFilter{
		Status:        model.ImageReplicationTaskStatusPending,
		PlatformAdmin: true,
	})
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, task := range tasks {
		if err := ctx.Err(); err != nil {
			return err
		}
		replicas, err := s.Store.ListImageReplicas(model.ImageReplicaFilter{
			ImageID:         task.ImageID,
			NodeID:          task.TargetNodeID,
			RuntimeID:       task.TargetRuntimeID,
			ClusterNodeName: task.TargetClusterNodeName,
			Status:          model.ImageReplicaStatusPresent,
			PlatformAdmin:   true,
		})
		if err != nil {
			return err
		}
		if len(healthyImageReplicas(replicas, now)) == 0 {
			continue
		}
		task.Status = model.ImageReplicationTaskStatusCompleted
		task.CompletedAt = &now
		if _, err := s.Store.UpsertImageReplicationTask(task); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) imageTargetReplicaCount(image model.Image) int {
	target := s.Config.ImageStoreTargetReplicas
	if target <= 0 {
		target = 3
	}
	if image.RequiredReplicaCount > target {
		target = image.RequiredReplicaCount
	}
	if image.MinAvailableReplicaCount > target {
		target = image.MinAvailableReplicaCount
	}
	min := s.Config.ImageStoreMinReplicas
	if min <= 0 {
		min = 2
	}
	if target < min {
		target = min
	}
	return target
}

func (s *Service) imageMinReplicaCount() int {
	if s == nil || s.Config.ImageStoreMinReplicas <= 0 {
		return 2
	}
	return s.Config.ImageStoreMinReplicas
}

func (s *Service) imageReplicaLeaseTTL() time.Duration {
	if s == nil || s.Config.ImageStoreReplicaLeaseTTL <= 0 {
		return 30 * time.Minute
	}
	return s.Config.ImageStoreReplicaLeaseTTL
}

func healthyImageReplicas(replicas []model.ImageReplica, now time.Time) []model.ImageReplica {
	out := make([]model.ImageReplica, 0, len(replicas))
	for _, replica := range replicas {
		if replica.Status != model.ImageReplicaStatusPresent {
			continue
		}
		if replica.LeaseExpiresAt != nil && replica.LeaseExpiresAt.Before(now) {
			continue
		}
		out = append(out, replica)
	}
	return out
}

func imageReplicaExistsOnTarget(replicas []model.ImageReplica, updater model.NodeUpdater) bool {
	for _, replica := range replicas {
		if updater.MachineID != "" && strings.TrimSpace(replica.NodeID) == updater.MachineID {
			return true
		}
		if updater.RuntimeID != "" && strings.TrimSpace(replica.RuntimeID) == updater.RuntimeID {
			return true
		}
		if updater.ClusterNodeName != "" && strings.TrimSpace(replica.ClusterNodeName) == updater.ClusterNodeName {
			return true
		}
	}
	return false
}

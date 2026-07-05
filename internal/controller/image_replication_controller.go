package controller

import (
	"context"
	"errors"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
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
	eligibility, err := s.loadImageReplicationEligibility()
	if err != nil {
		return err
	}
	if err := s.cancelObsoletePendingImageReplicationTasks(ctx, eligibility); err != nil {
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
		if err := s.ensureImageReplicaPolicyWithEligibility(ctx, image, eligibility); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ensureImageReplicaPolicy(ctx context.Context, image model.Image) error {
	eligibility, err := s.loadImageReplicationEligibility()
	if err != nil {
		return err
	}
	return s.ensureImageReplicaPolicyWithEligibility(ctx, image, eligibility)
}

func (s *Service) ensureImageReplicaPolicyWithEligibility(ctx context.Context, image model.Image, eligibility imageReplicationEligibility) error {
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
	if len(healthy) == 0 {
		locations, err := s.presentImageLocations(model.App{ID: image.AppID, TenantID: image.TenantID}, image.ImageRef, image.CanonicalDigest)
		if err != nil {
			return err
		}
		if len(locations) > 0 {
			s.restoreLostDistributedImageFromLocations(image, locations)
			return nil
		}
		if s.imageStoreStrictDistributedMode() && strings.TrimSpace(image.LifecycleState) == model.ImageLifecycleAvailable {
			image.LifecycleState = model.ImageLifecycleLost
			if _, err := s.Store.UpsertImage(image); err != nil && s.Logger != nil {
				s.Logger.Printf("mark lost distributed image failed image=%s ref=%s: %v", image.ID, image.ImageRef, err)
			}
			return nil
		}
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
		if ok, reason := s.nodeEligibleForAppImageReplication(updater, model.ImageReplicationPriorityRepair, deployImageTarget{}, eligibility); !ok {
			if s.Logger != nil && reason != "" {
				s.Logger.Printf("skip image repair replication target image=%s node=%s reason=%s", image.ID, updater.ClusterNodeName, reason)
			}
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
	eligibility, err := s.loadImageReplicationEligibility()
	if err != nil {
		return false, err
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
		if ok, _ := s.nodeEligibleForAppImageReplication(updater, model.ImageReplicationPriorityDeployBlocking, target, eligibility); !ok {
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
		target = 1
	}
	if image.RequiredReplicaCount > target && !legacyDefaultReplicaCount(image.RequiredReplicaCount, target) {
		target = image.RequiredReplicaCount
	}
	if image.MinAvailableReplicaCount > target && !legacyDefaultReplicaCount(image.MinAvailableReplicaCount, target) {
		target = image.MinAvailableReplicaCount
	}
	min := s.Config.ImageStoreMinReplicas
	if min <= 0 {
		min = 1
	}
	if target < min {
		target = min
	}
	return target
}

func (s *Service) imageMinReplicaCount() int {
	if s == nil || s.Config.ImageStoreMinReplicas <= 0 {
		return 1
	}
	return s.Config.ImageStoreMinReplicas
}

func legacyDefaultReplicaCount(count, fallback int) bool {
	return count == 2 && fallback <= 1
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

type imageReplicationEligibility struct {
	runtimeByID      map[string]model.Runtime
	machineByID      map[string]model.Machine
	machineByNode    map[string]model.Machine
	pressureByTarget map[string]struct{}
}

func (s *Service) loadImageReplicationEligibility() (imageReplicationEligibility, error) {
	out := imageReplicationEligibility{
		runtimeByID:      map[string]model.Runtime{},
		machineByID:      map[string]model.Machine{},
		machineByNode:    map[string]model.Machine{},
		pressureByTarget: map[string]struct{}{},
	}
	if s == nil || s.Store == nil {
		return out, nil
	}
	if runtimes, err := s.Store.ListRuntimes("", true); err == nil {
		for _, runtimeObj := range runtimes {
			if strings.TrimSpace(runtimeObj.ID) != "" {
				out.runtimeByID[strings.TrimSpace(runtimeObj.ID)] = runtimeObj
			}
		}
	} else {
		return out, err
	}
	if machines, err := s.Store.ListMachines("", true); err == nil {
		for _, machine := range machines {
			if strings.TrimSpace(machine.ID) != "" {
				out.machineByID[strings.TrimSpace(machine.ID)] = machine
			}
			if strings.TrimSpace(machine.ClusterNodeName) != "" {
				out.machineByNode[strings.TrimSpace(machine.ClusterNodeName)] = machine
			}
		}
	} else {
		return out, err
	}
	if nodes, err := s.Store.ListImageCacheNodeInventories(model.ImageCacheNodeInventoryFilter{}); err == nil {
		for _, node := range nodes {
			if !controllerImageCacheNodePressure(node) {
				continue
			}
			for _, key := range imageReplicationTargetKeys(node.NodeID, node.RuntimeID, node.ClusterNodeName) {
				out.pressureByTarget[key] = struct{}{}
			}
		}
	} else {
		return out, err
	}
	return out, nil
}

func (s *Service) nodeEligibleForAppImageReplication(updater model.NodeUpdater, priority string, target deployImageTarget, eligibility imageReplicationEligibility) (bool, string) {
	if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
		return false, "node_updater_inactive"
	}
	if strings.TrimSpace(priority) == model.ImageReplicationPriorityDeployBlocking {
		if strings.TrimSpace(target.ClusterNodeName) != "" && strings.TrimSpace(updater.ClusterNodeName) != strings.TrimSpace(target.ClusterNodeName) {
			return false, "not_deploy_target_node"
		}
		if strings.TrimSpace(target.RuntimeID) != "" && strings.TrimSpace(updater.RuntimeID) != strings.TrimSpace(target.RuntimeID) {
			return false, "not_deploy_target_runtime"
		}
	}
	if runtimeID := strings.TrimSpace(updater.RuntimeID); runtimeID != "" {
		if runtimeObj, ok := eligibility.runtimeByID[runtimeID]; ok {
			if strings.TrimSpace(runtimeObj.Status) != model.RuntimeStatusActive {
				return false, "runtime_not_active"
			}
			if !labelsAllowAppReplication(runtimeObj.Labels) && labelsEdgeOrDNSOnly(runtimeObj.Labels) {
				return false, "runtime_edge_or_dns_only"
			}
		}
	}
	if strings.TrimSpace(priority) != model.ImageReplicationPriorityDeployBlocking {
		for _, key := range imageReplicationTargetKeys(updater.MachineID, updater.RuntimeID, updater.ClusterNodeName) {
			if _, ok := eligibility.pressureByTarget[key]; ok {
				return false, "filesystem_pressure"
			}
		}
	}
	machine, hasMachine := eligibility.machineByID[strings.TrimSpace(updater.MachineID)]
	if !hasMachine && strings.TrimSpace(updater.ClusterNodeName) != "" {
		machine, hasMachine = eligibility.machineByNode[strings.TrimSpace(updater.ClusterNodeName)]
	}
	if hasMachine {
		if !machine.Policy.AllowAppRuntime && !machine.Policy.AllowSharedPool {
			if machine.Policy.AllowEdge || machine.Policy.AllowDNS {
				return false, "edge_or_dns_only"
			}
			if role := model.NormalizeMachineControlPlaneRole(machine.Policy.DesiredControlPlaneRole); role == model.MachineControlPlaneRoleMember || role == model.MachineControlPlaneRoleCandidate {
				return false, "control_plane_only"
			}
			return false, "app_runtime_not_allowed"
		}
	}
	if !labelsAllowAppReplication(updater.Labels) && labelsEdgeOrDNSOnly(updater.Labels) {
		return false, "updater_edge_or_dns_only"
	}
	if !labelsAllowAppReplication(updater.Labels) {
		if role := model.NormalizeMachineControlPlaneRole(updater.Labels[runtimepkg.ControlPlaneDesiredRoleKey]); role == model.MachineControlPlaneRoleMember || role == model.MachineControlPlaneRoleCandidate {
			return false, "control_plane_only"
		}
	}
	return true, ""
}

func labelsAllowAppReplication(labels map[string]string) bool {
	if len(labels) == 0 {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(labels[runtimepkg.AppRuntimeRoleLabelKey]), runtimepkg.NodeRoleLabelValue) ||
		strings.EqualFold(strings.TrimSpace(labels[runtimepkg.SharedPoolLabelKey]), runtimepkg.SharedPoolLabelValue)
}

func labelsEdgeOrDNSOnly(labels map[string]string) bool {
	if len(labels) == 0 {
		return false
	}
	edge := strings.EqualFold(strings.TrimSpace(labels[runtimepkg.EdgeRoleLabelKey]), runtimepkg.NodeRoleLabelValue)
	dns := strings.EqualFold(strings.TrimSpace(labels[runtimepkg.DNSRoleLabelKey]), runtimepkg.NodeRoleLabelValue)
	return (edge || dns) && !labelsAllowAppReplication(labels)
}

func imageReplicationTargetKeys(nodeID, runtimeID, clusterNodeName string) []string {
	values := []string{
		"node:" + strings.TrimSpace(nodeID),
		"runtime:" + strings.TrimSpace(runtimeID),
		"cluster:" + strings.TrimSpace(clusterNodeName),
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if strings.HasSuffix(value, ":") {
			continue
		}
		out = append(out, value)
	}
	return out
}

func (s *Service) cancelObsoletePendingImageReplicationTasks(ctx context.Context, eligibility imageReplicationEligibility) error {
	if s == nil || s.Store == nil {
		return nil
	}
	tasks, err := s.Store.ListImageReplicationTasks(model.ImageReplicationTaskFilter{Status: model.ImageReplicationTaskStatusPending, PlatformAdmin: true})
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if err := ctx.Err(); err != nil {
			return err
		}
		image, err := s.Store.GetImage(task.ImageID, "", true)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				return err
			}
			task.Status = model.ImageReplicationTaskStatusCanceled
			task.LastError = "missing_image"
		} else if imageCacheReplicationTaskObsoleteForController(task, image) {
			task.Status = model.ImageReplicationTaskStatusCanceled
			task.LastError = "obsolete_image_generation"
		} else if strings.TrimSpace(task.Priority) != model.ImageReplicationPriorityDeployBlocking {
			updater := model.NodeUpdater{
				MachineID:       task.TargetNodeID,
				RuntimeID:       task.TargetRuntimeID,
				ClusterNodeName: task.TargetClusterNodeName,
				Status:          model.NodeUpdaterStatusActive,
			}
			if ok, reason := s.nodeEligibleForAppImageReplication(updater, task.Priority, deployImageTarget{}, eligibility); !ok {
				task.Status = model.ImageReplicationTaskStatusCanceled
				task.LastError = reason
			}
		}
		if task.Status != model.ImageReplicationTaskStatusCanceled {
			continue
		}
		now := time.Now().UTC()
		task.CompletedAt = &now
		if _, err := s.Store.UpsertImageReplicationTask(task); err != nil {
			return err
		}
	}
	if err := s.cancelObsoletePendingNodeImageUpdateTasks(ctx, eligibility); err != nil {
		return err
	}
	return nil
}

func (s *Service) cancelObsoletePendingNodeImageUpdateTasks(ctx context.Context, eligibility imageReplicationEligibility) error {
	tasks, err := s.Store.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if err := ctx.Err(); err != nil {
			return err
		}
		reason, err := s.obsoletePendingNodeImageUpdateTaskReason(task, eligibility)
		if err != nil {
			return err
		}
		if reason == "" {
			continue
		}
		if _, err := s.Store.CancelNodeUpdateTask(task.ID, task.NodeUpdaterID, "obsolete image update task canceled: "+reason); err != nil && !errors.Is(err, store.ErrConflict) && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return nil
}

func (s *Service) obsoletePendingNodeImageUpdateTaskReason(task model.NodeUpdateTask, eligibility imageReplicationEligibility) (string, error) {
	switch task.Type {
	case model.NodeUpdateTaskTypeReplicateAppImage, model.NodeUpdateTaskTypePrepullAppImages:
	default:
		return "", nil
	}
	priority := strings.TrimSpace(task.Payload["priority"])
	if priority == "" {
		priority = model.ImageReplicationPriorityRepair
	}
	if task.Type == model.NodeUpdateTaskTypeReplicateAppImage {
		imageID := strings.TrimSpace(task.Payload["image_id"])
		if imageID == "" {
			return "missing_image_id", nil
		}
		image, err := s.Store.GetImage(imageID, "", true)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return "missing_image", nil
			}
			return "", err
		}
		if imageCacheReplicationTaskObsoleteForController(model.ImageReplicationTask{Priority: priority}, image) {
			return "obsolete_image_generation", nil
		}
	}
	if priority == model.ImageReplicationPriorityDeployBlocking {
		return "", nil
	}
	updater := model.NodeUpdater{
		MachineID:       task.MachineID,
		RuntimeID:       task.RuntimeID,
		ClusterNodeName: task.ClusterNodeName,
		Status:          model.NodeUpdaterStatusActive,
	}
	if ok, reason := s.nodeEligibleForAppImageReplication(updater, priority, deployImageTarget{}, eligibility); !ok {
		return reason, nil
	}
	return "", nil
}

func imageCacheReplicationTaskObsoleteForController(task model.ImageReplicationTask, image model.Image) bool {
	switch strings.TrimSpace(image.LifecycleState) {
	case model.ImageLifecycleDeleting, model.ImageLifecycleDeleted, model.ImageLifecycleLost:
		return strings.TrimSpace(task.Priority) != model.ImageReplicationPriorityDeployBlocking
	default:
		return false
	}
}

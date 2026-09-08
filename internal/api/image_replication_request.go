package api

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

type createImageReplicationTaskRequest struct {
	ImageID               string `json:"image_id"`
	AppID                 string `json:"app_id"`
	SourceReplicaID       string `json:"source_replica_id"`
	SourceCacheEndpoint   string `json:"source_cache_endpoint"`
	TargetNodeID          string `json:"target_node_id"`
	TargetRuntimeID       string `json:"target_runtime_id"`
	TargetClusterNodeName string `json:"target_cluster_node_name"`
	Priority              string `json:"priority"`
}

func (s *Server) enqueueRequestedImageReplication(principal model.Principal, image model.Image, req createImageReplicationTaskRequest) (model.ImageReplicationTask, error) {
	if req.AppID = strings.TrimSpace(req.AppID); req.AppID != "" && req.AppID != image.AppID {
		return model.ImageReplicationTask{}, fmt.Errorf("%w: app_id must match the image owner", store.ErrInvalidInput)
	}
	req.TargetNodeID = strings.TrimSpace(req.TargetNodeID)
	req.TargetRuntimeID = strings.TrimSpace(req.TargetRuntimeID)
	req.TargetClusterNodeName = strings.TrimSpace(req.TargetClusterNodeName)
	if req.TargetNodeID == "" && req.TargetRuntimeID == "" && req.TargetClusterNodeName == "" {
		return model.ImageReplicationTask{}, fmt.Errorf("%w: an image replication target is required", store.ErrInvalidInput)
	}
	updaters, err := s.store.ListNodeUpdaters(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return model.ImageReplicationTask{}, err
	}
	var target *model.NodeUpdater
	for _, updater := range updaters {
		if (req.TargetNodeID != "" && req.TargetNodeID != updater.MachineID) ||
			(req.TargetRuntimeID != "" && req.TargetRuntimeID != updater.RuntimeID) ||
			(req.TargetClusterNodeName != "" && req.TargetClusterNodeName != updater.ClusterNodeName) {
			continue
		}
		if target != nil {
			return model.ImageReplicationTask{}, fmt.Errorf("%w: replication target is ambiguous; specify a node", store.ErrConflict)
		}
		copy := updater
		target = &copy
	}
	if target == nil {
		return model.ImageReplicationTask{}, store.ErrNotFound
	}
	supported, err := s.store.NodeUpdaterTargetSupportsTask(target.ID, target.ClusterNodeName, target.RuntimeID, model.NodeUpdateTaskTypeReplicateAppImage)
	if err != nil {
		return model.ImageReplicationTask{}, err
	}
	if !supported {
		return model.ImageReplicationTask{}, fmt.Errorf("%w: target does not support image replication", store.ErrInvalidInput)
	}
	replicas, err := s.store.ListImageReplicas(model.ImageReplicaFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		return model.ImageReplicationTask{}, err
	}
	source, err := requestedImageReplicationSource(image, replicas, req, time.Now().UTC())
	if err != nil {
		return model.ImageReplicationTask{}, err
	}
	priority := strings.TrimSpace(req.Priority)
	if priority == "" {
		priority = model.ImageReplicationPriorityWarmup
	}
	task, err := s.store.UpsertImageReplicationTask(model.ImageReplicationTask{
		ImageID: image.ID, TenantID: image.TenantID, AppID: image.AppID,
		SourceReplicaID: source.ID, SourceCacheEndpoint: source.CacheEndpoint,
		TargetNodeID: target.MachineID, TargetRuntimeID: target.RuntimeID,
		TargetClusterNodeName: target.ClusterNodeName, Priority: priority,
		Status: model.ImageReplicationTaskStatusPending,
	})
	if err != nil {
		return model.ImageReplicationTask{}, err
	}
	// Both stores deduplicate active tasks. A dispatch failure is returned to the
	// caller, whose retry can repair dispatch without creating another transfer.
	_, err = s.store.CreateNodeUpdateTask(principal, target.ID, target.ClusterNodeName, target.RuntimeID, model.NodeUpdateTaskTypeReplicateAppImage, map[string]string{
		"image_id": image.ID, "image_ref": image.ImageRef, "digest": image.CanonicalDigest,
		"app_id": image.AppID, "source_replica_id": source.ID,
		"source_cache_endpoint": source.CacheEndpoint, "replication_task_id": task.ID,
		"priority": priority,
	})
	if err != nil {
		return model.ImageReplicationTask{}, fmt.Errorf("dispatch image replication: %w", err)
	}
	return task, nil
}

func requestedImageReplicationSource(image model.Image, replicas []model.ImageReplica, req createImageReplicationTaskRequest, now time.Time) (model.ImageReplica, error) {
	digest := store.CanonicalImageDigest(image.CanonicalDigest)
	if digest == "" {
		return model.ImageReplica{}, fmt.Errorf("%w: image has no immutable digest", store.ErrConflict)
	}
	var source model.ImageReplica
	for _, replica := range replicas {
		if replica.ImageID != image.ID || replica.Status != model.ImageReplicaStatusPresent ||
			store.CanonicalImageDigest(replica.Digest) != digest ||
			(replica.LeaseExpiresAt != nil && !now.Before(*replica.LeaseExpiresAt)) {
			continue
		}
		if id := strings.TrimSpace(req.SourceReplicaID); id != "" && id != replica.ID {
			continue
		}
		endpoint := strings.TrimRight(strings.TrimSpace(replica.CacheEndpoint), "/")
		if requested := strings.TrimRight(strings.TrimSpace(req.SourceCacheEndpoint), "/"); requested != "" && requested != endpoint {
			continue
		}
		if _, err := normalizeReportedImageCacheEndpoint(endpoint, "", true); err != nil {
			continue
		}
		if source.ID == "" || (replica.LastVerifiedAt != nil && (source.LastVerifiedAt == nil || replica.LastVerifiedAt.After(*source.LastVerifiedAt))) {
			source = replica
			source.CacheEndpoint = endpoint
		}
	}
	if source.ID == "" {
		return model.ImageReplica{}, fmt.Errorf("%w: no eligible replica source matches the image digest", store.ErrConflict)
	}
	return source, nil
}

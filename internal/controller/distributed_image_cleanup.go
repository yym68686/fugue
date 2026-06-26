package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Service) cleanupDeletedAppDistributedImages(ctx context.Context, app model.App) error {
	if s == nil || s.Store == nil {
		return nil
	}
	pins, err := s.Store.ListImagePins(model.ImagePinFilter{
		TenantID: strings.TrimSpace(app.TenantID),
		AppID:    strings.TrimSpace(app.ID),
	})
	if err != nil {
		return err
	}
	for _, pin := range pins {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.Store.DeleteImagePin(pin.ID, pin.TenantID, true); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return s.scheduleDistributedImagePruneForApp(ctx, app)
}

func (s *Service) sweepExpiredDistributedImagePins(ctx context.Context) error {
	if s == nil || s.Store == nil || !s.imageStoreDistributedMode() {
		return nil
	}
	pins, err := s.Store.ListImagePins(model.ImagePinFilter{PlatformAdmin: true})
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, pin := range pins {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pin.ExpiresAt == nil || pin.ExpiresAt.After(now) {
			continue
		}
		if err := s.Store.DeleteImagePin(pin.ID, pin.TenantID, true); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return nil
}

func (s *Service) scheduleDistributedImagePruneForApp(ctx context.Context, app model.App) error {
	if s == nil || s.Store == nil || !s.imageStoreDistributedMode() {
		return nil
	}
	images, err := s.Store.ListImages(model.ImageFilter{
		TenantID: strings.TrimSpace(app.TenantID),
		AppID:    strings.TrimSpace(app.ID),
	})
	if err != nil {
		return err
	}
	for _, image := range images {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.scheduleDistributedImagePrune(ctx, image); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) scheduleDistributedImagePrune(ctx context.Context, image model.Image) error {
	if s == nil || s.Store == nil || strings.TrimSpace(image.ID) == "" {
		return nil
	}
	pins, err := s.Store.ListImagePins(model.ImagePinFilter{
		ImageID:       image.ID,
		PlatformAdmin: true,
	})
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, pin := range pins {
		if pin.ExpiresAt == nil || pin.ExpiresAt.After(now) {
			return nil
		}
	}
	replicas, err := s.Store.ListImageReplicas(model.ImageReplicaFilter{
		ImageID:       image.ID,
		Status:        model.ImageReplicaStatusPresent,
		PlatformAdmin: true,
	})
	if err != nil {
		return err
	}
	healthy := healthyImageReplicas(replicas, now)
	if len(healthy) <= s.imageMinReplicaCount() {
		return nil
	}
	updaters, err := s.Store.ListNodeUpdaters("", true)
	if err != nil {
		return err
	}
	updatersByTarget := make(map[string]model.NodeUpdater, len(updaters))
	for _, updater := range updaters {
		updatersByTarget[distributedImageReplicaTargetKey(updater.MachineID, updater.RuntimeID, updater.ClusterNodeName)] = updater
	}
	for idx := s.imageMinReplicaCount(); idx < len(healthy); idx++ {
		replica := healthy[idx]
		updater, ok := updatersByTarget[distributedImageReplicaTargetKey(replica.NodeID, replica.RuntimeID, replica.ClusterNodeName)]
		if !ok || !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
			continue
		}
		supported, err := s.Store.NodeUpdaterTargetSupportsTask(updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypePruneImageCache)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidInput) {
				continue
			}
			return err
		}
		if !supported {
			continue
		}
		_, err = s.Store.CreateNodeUpdateTask(model.Principal{
			ActorType: model.ActorTypeSystem,
			ActorID:   "fugue-controller/image-prune",
			TenantID:  strings.TrimSpace(image.TenantID),
			Scopes:    map[string]struct{}{"platform.admin": {}},
		}, updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypePruneImageCache, map[string]string{
			"image_id":     image.ID,
			"image_ref":    image.ImageRef,
			"digest":       image.CanonicalDigest,
			"app_id":       image.AppID,
			"dry_run":      "true",
			"allow_delete": "false",
			"replica_id":   replica.ID,
			"prune_reason": "unpinned-excess-replica",
			"min_replicas": fmt.Sprint(s.imageMinReplicaCount()),
		})
		if err != nil && !errors.Is(err, store.ErrInvalidInput) {
			return err
		}
	}
	return nil
}

func distributedImageReplicaTargetKey(nodeID, runtimeID, clusterNodeName string) string {
	return strings.TrimSpace(nodeID) + "\x00" + strings.TrimSpace(runtimeID) + "\x00" + strings.TrimSpace(clusterNodeName)
}

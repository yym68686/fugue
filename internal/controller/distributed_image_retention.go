package controller

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"fugue/internal/imageretention"
	"fugue/internal/model"
	"fugue/internal/store"
)

const distributedImageRollbackWindow = 7 * 24 * time.Hour

type DistributedImageRetentionPlan = model.DistributedImageRetentionPlan
type ImageRetentionDecision = model.ImageRetentionDecision

func planDistributedImageRetention(app model.App, images []model.Image, ops []model.Operation, pins []model.ImagePin, liveRefs map[string]struct{}, now time.Time) DistributedImageRetentionPlan {
	return imageretention.Plan(app, images, ops, pins, liveRefs, now)
}

func (s *Service) reconcileDistributedImageRetentionForApp(ctx context.Context, app model.App, ops []model.Operation, liveRefs map[string]struct{}) (DistributedImageRetentionPlan, error) {
	if s == nil || s.Store == nil || !s.imageStoreDistributedMode() {
		return DistributedImageRetentionPlan{AppID: strings.TrimSpace(app.ID)}, nil
	}
	images, err := s.Store.ListImages(model.ImageFilter{TenantID: app.TenantID, AppID: app.ID, PlatformAdmin: true})
	if err != nil {
		return DistributedImageRetentionPlan{}, err
	}
	pins, err := s.Store.ListImagePins(model.ImagePinFilter{TenantID: app.TenantID, AppID: app.ID, PlatformAdmin: true})
	if err != nil {
		return DistributedImageRetentionPlan{}, err
	}
	now := time.Now().UTC()
	if s.now != nil {
		now = s.now().UTC()
	}
	plan := planDistributedImageRetention(app, images, ops, pins, liveRefs, now)
	if err := s.reconcileDistributedImagePinsForApp(ctx, app, images, pins, plan, now); err != nil {
		return plan, err
	}
	if err := s.applyDistributedImageRetentionPlan(ctx, app, images, plan, now); err != nil {
		return plan, err
	}
	return plan, nil
}

func (s *Service) reconcileDistributedImagePinsForApp(ctx context.Context, app model.App, images []model.Image, pins []model.ImagePin, plan DistributedImageRetentionPlan, now time.Time) error {
	imageByID := map[string]model.Image{}
	for _, image := range images {
		imageByID[image.ID] = image
	}
	keep := stringSet(plan.KeepImageIDs)
	drop := stringSet(plan.DropImageIDs)
	current := map[string]struct{}{}
	for _, decision := range plan.ImageDecisions {
		if decision.CurrentWorkload {
			current[decision.ImageID] = struct{}{}
		}
	}
	if len(current) == 0 {
		for _, decision := range plan.ImageDecisions {
			if decision.Keep && decision.Reason == "retention_keep_latest_n" {
				current[decision.ImageID] = struct{}{}
				break
			}
		}
	}
	if len(current) == 0 {
		for _, decision := range plan.ImageDecisions {
			if decision.Keep {
				current[decision.ImageID] = struct{}{}
				break
			}
		}
	}
	for imageID := range current {
		image := imageByID[imageID]
		if strings.TrimSpace(image.ID) == "" {
			continue
		}
		if _, err := s.Store.UpsertImagePin(model.ImagePin{
			ImageID:     image.ID,
			TenantID:    image.TenantID,
			AppID:       app.ID,
			OperationID: "",
			Reason:      model.ImagePinReasonCurrentDeploy,
			MinReplicas: 1,
		}); err != nil {
			return err
		}
	}
	for imageID := range keep {
		if _, ok := current[imageID]; ok {
			continue
		}
		image := imageByID[imageID]
		if strings.TrimSpace(image.ID) == "" {
			continue
		}
		if _, err := s.Store.UpsertImagePin(model.ImagePin{
			ImageID:     image.ID,
			TenantID:    image.TenantID,
			AppID:       app.ID,
			OperationID: "",
			Reason:      model.ImagePinReasonRollbackWindow,
			MinReplicas: 1,
			ExpiresAt:   timePointer(now.Add(distributedImageRollbackWindow)),
		}); err != nil {
			return err
		}
	}
	for _, pin := range pins {
		if err := ctx.Err(); err != nil {
			return err
		}
		imageID := strings.TrimSpace(pin.ImageID)
		switch strings.TrimSpace(pin.Reason) {
		case model.ImagePinReasonUserPin, model.ImagePinReasonRetention:
			continue
		case model.ImagePinReasonCurrentDeploy:
			if _, ok := current[imageID]; ok && strings.TrimSpace(pin.OperationID) == "" {
				continue
			}
		case model.ImagePinReasonRollbackWindow:
			if _, ok := keep[imageID]; ok {
				if _, isCurrent := current[imageID]; !isCurrent && strings.TrimSpace(pin.OperationID) == "" {
					continue
				}
			}
		}
		if _, ok := drop[imageID]; ok || pin.Reason == model.ImagePinReasonCurrentDeploy || pin.Reason == model.ImagePinReasonRollbackWindow || pin.Reason == model.ImagePinReasonImportResult || pin.Reason == model.ImagePinReasonReplicationSeed {
			if err := s.Store.DeleteImagePin(pin.ID, pin.TenantID, true); err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
		}
	}
	return nil
}

func (s *Service) applyDistributedImageRetentionPlan(ctx context.Context, app model.App, images []model.Image, plan DistributedImageRetentionPlan, now time.Time) error {
	keep := stringSet(plan.KeepImageIDs)
	drop := stringSet(plan.DropImageIDs)
	for _, image := range images {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := keep[image.ID]; ok {
			if s.normalizeHistoricalImageReplicaPolicy(&image) {
				if _, err := s.Store.UpsertImage(image); err != nil {
					return err
				}
			}
			continue
		}
		if _, ok := drop[image.ID]; !ok {
			continue
		}
		if err := s.cancelObsoleteDistributedImageReplicationTasks(ctx, image, "retention_excess"); err != nil {
			return err
		}
		if err := s.scheduleDistributedImagePrune(ctx, image); err != nil {
			return err
		}
		if strings.TrimSpace(image.LifecycleState) == model.ImageLifecycleAvailable || strings.TrimSpace(image.LifecycleState) == model.ImageLifecycleImporting || strings.TrimSpace(image.LifecycleState) == model.ImageLifecycleLost || strings.TrimSpace(image.LifecycleState) == "" {
			image.LifecycleState = model.ImageLifecycleDeleting
			image.RequiredReplicaCount = 1
			image.MinAvailableReplicaCount = 1
			if _, err := s.Store.UpsertImage(image); err != nil {
				return err
			}
		}
		if err := s.markDistributedImageReplicasRetentionExcess(ctx, image, now); err != nil {
			return err
		}
	}
	_ = app
	return nil
}

func (s *Service) normalizeHistoricalImageReplicaPolicy(image *model.Image) bool {
	if image == nil || strings.TrimSpace(image.ID) == "" {
		return false
	}
	changed := false
	if image.RequiredReplicaCount <= 0 || legacyDefaultReplicaCount(image.RequiredReplicaCount, s.imageTargetReplicaCount(model.Image{})) {
		if image.RequiredReplicaCount != 1 {
			image.RequiredReplicaCount = 1
			changed = true
		}
	}
	if image.MinAvailableReplicaCount <= 0 || legacyDefaultReplicaCount(image.MinAvailableReplicaCount, s.imageMinReplicaCount()) {
		if image.MinAvailableReplicaCount != 1 {
			image.MinAvailableReplicaCount = 1
			changed = true
		}
	}
	return changed
}

func (s *Service) cancelObsoleteDistributedImageReplicationTasks(ctx context.Context, image model.Image, reason string) error {
	if s == nil || s.Store == nil || strings.TrimSpace(image.ID) == "" {
		return nil
	}
	for _, status := range []string{model.ImageReplicationTaskStatusPending, model.ImageReplicationTaskStatusRunning} {
		tasks, err := s.Store.ListImageReplicationTasks(model.ImageReplicationTaskFilter{ImageID: image.ID, Status: status, PlatformAdmin: true})
		if err != nil {
			return err
		}
		for _, task := range tasks {
			if err := ctx.Err(); err != nil {
				return err
			}
			if strings.TrimSpace(task.Priority) == model.ImageReplicationPriorityDeployBlocking {
				continue
			}
			task.Status = model.ImageReplicationTaskStatusCanceled
			task.LastError = strings.TrimSpace(reason)
			completed := time.Now().UTC()
			task.CompletedAt = &completed
			if _, err := s.Store.UpsertImageReplicationTask(task); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Service) markDistributedImageReplicasRetentionExcess(ctx context.Context, image model.Image, now time.Time) error {
	replicas, err := s.Store.ListImageReplicas(model.ImageReplicaFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		return err
	}
	for _, replica := range replicas {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch strings.TrimSpace(replica.Status) {
		case model.ImageReplicaStatusPresent:
			replica.Status = model.ImageReplicaStatusStale
		case model.ImageReplicaStatusPlanned, model.ImageReplicaStatusCopying, model.ImageReplicaStatusVerifying:
			replica.Status = model.ImageReplicaStatusMissing
		default:
			continue
		}
		replica.LastError = "retention_excess"
		replica.LeaseExpiresAt = &now
		if _, err := s.Store.UpsertImageReplica(replica); err != nil {
			return err
		}
	}
	return nil
}

func stringSet(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func distributedImageRetentionPlanSummary(plan DistributedImageRetentionPlan) string {
	return "app=" + strings.TrimSpace(plan.AppID) +
		" keep=" + strconv.Itoa(len(plan.KeepImageIDs)) +
		" drop=" + strconv.Itoa(len(plan.DropImageIDs)) +
		" limit=" + strconv.Itoa(plan.EffectiveLimit)
}

package api

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/imageretention"
	"fugue/internal/model"
)

func (s *Server) handleAdminGetImageRetentionPlan(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	all := parseImageCacheBoolQuery(r.URL.Query().Get("all"))
	appSelector := strings.TrimSpace(r.URL.Query().Get("app"))
	apps, err := s.store.ListAppsMetadata("", true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	selected, err := selectAppsForImageRetentionPlan(apps, appSelector, all)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	liveRefs := s.liveManagedImageRefSet(r.Context(), apps)
	plans := make([]model.DistributedImageRetentionPlan, 0, len(selected))
	now := time.Now().UTC()
	for _, app := range selected {
		images, err := s.store.ListImages(model.ImageFilter{TenantID: app.TenantID, AppID: app.ID, PlatformAdmin: true})
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		ops, err := s.store.ListOperationsByApp(app.TenantID, true, app.ID)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		pins, err := s.store.ListImagePins(model.ImagePinFilter{TenantID: app.TenantID, AppID: app.ID, PlatformAdmin: true})
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		plan := imageretention.Plan(app, images, ops, pins, liveRefs, now)
		if err := s.annotateImageRetentionDryRunStats(&plan, images, pins); err != nil {
			s.writeStoreError(w, err)
			return
		}
		plans = append(plans, plan)
	}
	sort.SliceStable(plans, func(i, j int) bool {
		if plans[i].AppName != plans[j].AppName {
			return plans[i].AppName < plans[j].AppName
		}
		return plans[i].AppID < plans[j].AppID
	})
	response := map[string]any{"plans": plans}
	if len(plans) == 1 {
		response["plan"] = plans[0]
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

func selectAppsForImageRetentionPlan(apps []model.App, selector string, all bool) ([]model.App, error) {
	if all {
		return apps, nil
	}
	if selector == "" {
		return nil, errBadImageRetentionPlanSelector()
	}
	matches := make([]model.App, 0, 1)
	for _, app := range apps {
		if strings.TrimSpace(app.ID) == selector || strings.EqualFold(strings.TrimSpace(app.Name), selector) {
			matches = append(matches, app)
		}
	}
	if len(matches) == 0 {
		return nil, errImageRetentionAppNotFound(selector)
	}
	if len(matches) > 1 {
		return nil, errImageRetentionAppAmbiguous(selector)
	}
	return matches, nil
}

func errBadImageRetentionPlanSelector() error {
	return imageRetentionPlanError("--app or --all is required")
}

func errImageRetentionAppNotFound(selector string) error {
	return imageRetentionPlanError("app not found for selector " + selector)
}

func errImageRetentionAppAmbiguous(selector string) error {
	return imageRetentionPlanError("app selector is ambiguous: " + selector)
}

type imageRetentionPlanError string

func (e imageRetentionPlanError) Error() string { return string(e) }

func (s *Server) annotateImageRetentionDryRunStats(plan *model.DistributedImageRetentionPlan, images []model.Image, pins []model.ImagePin) error {
	if plan == nil {
		return nil
	}
	keep := stringSetAPI(plan.KeepImageIDs)
	drop := stringSetAPI(plan.DropImageIDs)
	current := imageRetentionCurrentSet(*plan)
	for _, pin := range pins {
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
			plan.WouldDeletePins++
		}
	}
	for _, image := range images {
		if (image.RequiredReplicaCount <= 0 || image.RequiredReplicaCount == 2 || image.MinAvailableReplicaCount <= 0 || image.MinAvailableReplicaCount == 2) && (image.RequiredReplicaCount != 1 || image.MinAvailableReplicaCount != 1) {
			plan.WouldNormalizeImages++
		}
		if _, ok := drop[image.ID]; !ok {
			continue
		}
		for _, status := range []string{model.ImageReplicationTaskStatusPending, model.ImageReplicationTaskStatusRunning} {
			tasks, err := s.store.ListImageReplicationTasks(model.ImageReplicationTaskFilter{ImageID: image.ID, Status: status, PlatformAdmin: true})
			if err != nil {
				return err
			}
			for _, task := range tasks {
				if strings.TrimSpace(task.Priority) == model.ImageReplicationPriorityDeployBlocking {
					continue
				}
				plan.WouldCancelTasks++
			}
		}
	}
	return nil
}

func imageRetentionCurrentSet(plan model.DistributedImageRetentionPlan) map[string]struct{} {
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
	return current
}

func stringSetAPI(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

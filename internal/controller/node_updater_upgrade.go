package controller

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Service) reconcileNodeUpdaterVersions(ctx context.Context) error {
	if s == nil || s.Store == nil {
		return nil
	}
	targetVersion := strings.TrimSpace(model.NodeUpdaterCurrentVersion)
	if targetVersion == "" {
		return nil
	}
	updaters, err := s.Store.ListNodeUpdaters("", true)
	if err != nil {
		return err
	}
	principal := model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   "fugue-controller/node-updater-upgrade",
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}
	for _, updater := range updaters {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
			continue
		}
		if !controllerNodeUpdaterNeedsUpgrade(updater.UpdaterVersion, targetVersion) {
			continue
		}
		supported, err := s.Store.NodeUpdaterTargetSupportsTask(updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeUpgradeUpdater)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidInput) {
				continue
			}
			return err
		}
		if !supported {
			continue
		}
		active, err := s.controllerNodeUpdaterHasActiveTask(updater.ID, model.NodeUpdateTaskTypeUpgradeUpdater)
		if err != nil {
			return err
		}
		if active {
			continue
		}
		payload := map[string]string{
			"target_version": targetVersion,
			"reason":         "controller-version-reconcile",
		}
		if _, err := s.Store.CreateNodeUpdateTask(principal, updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeUpgradeUpdater, payload); err != nil {
			if !errors.Is(err, store.ErrInvalidInput) && !errors.Is(err, store.ErrNotFound) {
				return err
			}
		}
	}
	return nil
}

func (s *Service) controllerNodeUpdaterHasActiveTask(updaterID, taskType string) (bool, error) {
	if s == nil || s.Store == nil {
		return false, nil
	}
	updaterID = strings.TrimSpace(updaterID)
	taskType = strings.TrimSpace(taskType)
	if updaterID == "" || taskType == "" {
		return false, nil
	}
	for _, status := range []string{model.NodeUpdateTaskStatusPending, model.NodeUpdateTaskStatusRunning} {
		tasks, err := s.Store.ListNodeUpdateTasks("", true, updaterID, status)
		if err != nil {
			return false, err
		}
		for _, task := range tasks {
			if task.Type == taskType {
				return true, nil
			}
		}
	}
	return false, nil
}

func controllerNodeUpdaterNeedsUpgrade(currentVersion, targetVersion string) bool {
	currentVersion = strings.TrimSpace(currentVersion)
	targetVersion = strings.TrimSpace(targetVersion)
	if targetVersion == "" || currentVersion == targetVersion {
		return false
	}
	current, currentOK := parseControllerNodeUpdaterVersion(currentVersion)
	target, targetOK := parseControllerNodeUpdaterVersion(targetVersion)
	if currentOK && targetOK {
		return current < target
	}
	return true
}

func parseControllerNodeUpdaterVersion(version string) (int, bool) {
	version = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(version), "v"))
	if version == "" {
		return 0, false
	}
	value, err := strconv.Atoi(version)
	if err != nil {
		return 0, false
	}
	return value, true
}

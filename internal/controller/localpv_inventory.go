package controller

import (
	"context"
	"errors"

	localpvinventory "fugue/internal/localpvsafety/inventory"
	"fugue/internal/model"
	"fugue/internal/storagerecovery"
	"fugue/internal/store"
)

func (s *Service) scheduleLocalPVInventoryReports(ctx context.Context) error {
	if s == nil || s.Store == nil || !s.Config.LocalPVInventoryEnabled {
		return nil
	}
	return localpvinventory.ScheduleReports(ctx, localPVInventoryQueue{s}, storagerecovery.NodeUpdaterVersion, controllerNodeUpdaterNeedsUpgrade)
}

// Only the Controller adapter can translate scheduling into Store writes.
type localPVInventoryQueue struct{ service *Service }

func (q localPVInventoryQueue) ListUpdaters() ([]model.NodeUpdater, error) {
	return q.service.Store.ListNodeUpdaters("", true)
}

func (q localPVInventoryQueue) SupportsReport(updater model.NodeUpdater) (bool, error) {
	ok, err := q.supports(updater, model.NodeUpdateTaskTypeReportLocalPV)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	return ok, err
}

func (q localPVInventoryQueue) SupportsUpgrade(updater model.NodeUpdater) (bool, error) {
	return q.supports(updater, model.NodeUpdateTaskTypeUpgradeUpdater)
}

func (q localPVInventoryQueue) supports(updater model.NodeUpdater, kind string) (bool, error) {
	return q.service.Store.NodeUpdaterTargetSupportsTask(updater.ID, updater.ClusterNodeName, updater.RuntimeID, kind)
}

func (q localPVInventoryQueue) UpgradeActive(updaterID string) (bool, error) {
	return q.service.controllerNodeUpdaterHasActiveTask(updaterID, model.NodeUpdateTaskTypeUpgradeUpdater)
}

func (q localPVInventoryQueue) QueueUpgrade(updater model.NodeUpdater) error {
	return q.service.Store.EnqueueLocalPVInventoryProtocolUpgrade(updater)
}

func (q localPVInventoryQueue) QueueReport(updater model.NodeUpdater) error {
	err := q.service.Store.EnqueueLocalPVInventoryReport(updater)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	return err
}

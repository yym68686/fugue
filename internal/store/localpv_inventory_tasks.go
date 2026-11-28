package store

import (
	"fugue/internal/model"
	"fugue/internal/storagerecovery"
)

const localPVInventorySchedulerActor = "fugue-controller/localpv-inventory"

// EnqueueLocalPVInventoryReport schedules only the inventory command for one updater.
func (s *Store) EnqueueLocalPVInventoryReport(updater model.NodeUpdater) error {
	return s.enqueueLocalPVInventoryTask(updater, model.NodeUpdateTaskTypeReportLocalPV, map[string]string{
		"reason": "scheduled-localpv-inventory",
	})
}

// EnqueueLocalPVInventoryProtocolUpgrade installs only the inventory protocol target.
func (s *Store) EnqueueLocalPVInventoryProtocolUpgrade(updater model.NodeUpdater) error {
	return s.enqueueLocalPVInventoryTask(updater, model.NodeUpdateTaskTypeUpgradeUpdater, map[string]string{
		"target_version": storagerecovery.NodeUpdaterVersion,
		"reason":         "localpv-ownership-evidence",
	})
}

func (s *Store) enqueueLocalPVInventoryTask(updater model.NodeUpdater, kind string, payload map[string]string) error {
	principal := model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   localPVInventorySchedulerActor,
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}
	_, err := s.pgCreateNodeUpdateTask(principal, updater.ID, updater.ClusterNodeName, updater.RuntimeID, kind, payload)
	return err
}

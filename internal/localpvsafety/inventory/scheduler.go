package inventory

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/model"
)

// ReportQueue can schedule inventory and its prerequisite protocol upgrade.
// It cannot update an inventory or enqueue unrelated maintenance.
type ReportQueue interface {
	ListUpdaters() ([]model.NodeUpdater, error)
	SupportsReport(model.NodeUpdater) (bool, error)
	SupportsUpgrade(model.NodeUpdater) (bool, error)
	UpgradeActive(updaterID string) (bool, error)
	QueueUpgrade(model.NodeUpdater) error
	QueueReport(model.NodeUpdater) error
}

// ScheduleReports schedules one pass through the available updaters. The
// caller supplies the leader lifecycle; the queue owns durable deduplication.
func ScheduleReports(ctx context.Context, queue ReportQueue, targetVersion string, needsUpgrade func(current, target string) bool) error {
	updaters, err := queue.ListUpdaters()
	if err != nil {
		return err
	}
	for _, updater := range updaters {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
			continue
		}
		supported, err := queue.SupportsReport(updater)
		if err != nil {
			return err
		}
		if !supported {
			continue
		}

		if needsUpgrade(updater.UpdaterVersion, targetVersion) {
			upgradeOK, err := queue.SupportsUpgrade(updater)
			if err != nil {
				return err
			}
			if upgradeOK {
				active, err := queue.UpgradeActive(updater.ID)
				if err != nil {
					return err
				}
				if !active {
					if err := queue.QueueUpgrade(updater); err != nil {
						return err
					}
				}
			}
		}

		if err := queue.QueueReport(updater); err != nil {
			return fmt.Errorf("schedule LocalPV inventory for node updater %s: %w", updater.ID, err)
		}
	}
	return nil
}

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Service) scheduleLocalPVInventoryReports(ctx context.Context) error {
	if s == nil || s.Store == nil || !s.Config.LocalPVInventoryEnabled {
		return nil
	}
	updaters, err := s.Store.ListNodeUpdaters("", true)
	if err != nil {
		return err
	}
	principal := model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   "fugue-controller/localpv-inventory",
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}
	for _, updater := range updaters {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
			continue
		}
		supported, err := s.Store.NodeUpdaterTargetSupportsTask(
			updater.ID,
			updater.ClusterNodeName,
			updater.RuntimeID,
			model.NodeUpdateTaskTypeReportLocalPV,
		)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			return err
		}
		if !supported {
			continue
		}
		if _, err := s.Store.CreateNodeUpdateTask(
			principal,
			updater.ID,
			updater.ClusterNodeName,
			updater.RuntimeID,
			model.NodeUpdateTaskTypeReportLocalPV,
			map[string]string{"reason": "scheduled-localpv-inventory"},
		); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			return fmt.Errorf("schedule LocalPV inventory for node updater %s: %w", updater.ID, err)
		}
	}
	return nil
}

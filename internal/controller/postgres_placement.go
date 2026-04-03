package controller

import (
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func (s *Service) managedPostgresPlacements(app model.App) (map[string][]runtime.SchedulingConstraints, error) {
	placements := make(map[string][]runtime.SchedulingConstraints)

	buildPlacements := func(serviceName, appRuntimeID string, spec model.AppPostgresSpec) error {
		serviceName = strings.TrimSpace(serviceName)
		if serviceName == "" {
			return nil
		}

		runtimeIDs := []string{}
		primaryRuntimeID := strings.TrimSpace(spec.RuntimeID)
		if primaryRuntimeID == "" {
			primaryRuntimeID = strings.TrimSpace(appRuntimeID)
		}
		if primaryRuntimeID != "" {
			runtimeIDs = append(runtimeIDs, primaryRuntimeID)
		}
		if targetRuntimeID := strings.TrimSpace(spec.FailoverTargetRuntimeID); targetRuntimeID != "" && targetRuntimeID != primaryRuntimeID {
			runtimeIDs = append(runtimeIDs, targetRuntimeID)
		}
		if len(runtimeIDs) == 0 {
			return nil
		}

		scheduling := make([]runtime.SchedulingConstraints, 0, len(runtimeIDs))
		seen := make(map[string]struct{}, len(runtimeIDs))
		for _, runtimeID := range runtimeIDs {
			if _, ok := seen[runtimeID]; ok {
				continue
			}
			seen[runtimeID] = struct{}{}
			constraints, err := s.managedSchedulingConstraints(runtimeID)
			if err != nil {
				return err
			}
			scheduling = append(scheduling, constraints)
		}
		if len(scheduling) > 0 {
			placements[serviceName] = scheduling
		}
		return nil
	}

	for _, service := range app.BackingServices {
		if service.Type != model.BackingServiceTypePostgres || service.Spec.Postgres == nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(service.Provisioner), model.BackingServiceProvisionerManaged) {
			continue
		}
		if err := buildPlacements(service.Spec.Postgres.ServiceName, app.Spec.RuntimeID, *service.Spec.Postgres); err != nil {
			return nil, err
		}
	}

	if app.Spec.Postgres != nil {
		if err := buildPlacements(app.Spec.Postgres.ServiceName, app.Spec.RuntimeID, *app.Spec.Postgres); err != nil {
			return nil, err
		}
	}

	if len(placements) == 0 {
		return nil, nil
	}
	return placements, nil
}

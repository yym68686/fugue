package store

import (
	"strings"

	"fugue/internal/model"
)

func (s *Store) ListProjectRuntimeReservations(projectID string) ([]model.ProjectRuntimeReservation, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil, ErrInvalidInput
	}
	return s.pgListProjectRuntimeReservations(projectID)
}

func (s *Store) ReserveProjectRuntime(projectID, runtimeID string) (model.ProjectRuntimeReservation, error) {
	projectID = strings.TrimSpace(projectID)
	runtimeID = strings.TrimSpace(runtimeID)
	if projectID == "" || runtimeID == "" {
		return model.ProjectRuntimeReservation{}, ErrInvalidInput
	}
	return s.pgReserveProjectRuntime(projectID, runtimeID)
}

func (s *Store) ValidateAppSpecRuntimeReservations(projectID string, spec model.AppSpec) error {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return ErrInvalidInput
	}
	return s.pgValidateAppSpecRuntimeReservations(projectID, spec)
}

func (s *Store) DeleteProjectRuntimeReservation(projectID, runtimeID string) (model.ProjectRuntimeReservation, error) {
	projectID = strings.TrimSpace(projectID)
	runtimeID = strings.TrimSpace(runtimeID)
	if projectID == "" || runtimeID == "" {
		return model.ProjectRuntimeReservation{}, ErrInvalidInput
	}
	return s.pgDeleteProjectRuntimeReservation(projectID, runtimeID)
}

func normalizeProjectRuntimeReservation(reservation model.ProjectRuntimeReservation) model.ProjectRuntimeReservation {
	reservation.TenantID = strings.TrimSpace(reservation.TenantID)
	reservation.ProjectID = strings.TrimSpace(reservation.ProjectID)
	reservation.RuntimeID = strings.TrimSpace(reservation.RuntimeID)
	if strings.TrimSpace(reservation.Mode) == "" {
		reservation.Mode = model.ProjectRuntimeReservationModeExclusive
	}
	return reservation
}

func validateProjectRuntimeReservationTarget(project model.Project, runtimeObj model.Runtime, visibleToProjectTenant bool) error {
	if strings.TrimSpace(project.ID) == "" || strings.TrimSpace(runtimeObj.ID) == "" {
		return ErrInvalidInput
	}
	switch strings.TrimSpace(runtimeObj.Type) {
	case model.RuntimeTypeManagedOwned, model.RuntimeTypeExternalOwned:
	default:
		return ErrInvalidInput
	}
	if strings.TrimSpace(project.TenantID) == "" {
		return ErrNotFound
	}
	if runtimeObj.TenantID != project.TenantID {
		if !visibleToProjectTenant {
			return ErrNotFound
		}
	}
	return nil
}

func runtimeHasProjectReservationBlockersState(state *storeProjection, projectID, runtimeID string) bool {
	projectID = strings.TrimSpace(projectID)
	runtimeID = strings.TrimSpace(runtimeID)
	if state == nil || projectID == "" || runtimeID == "" {
		return true
	}

	for _, project := range state.Projects {
		if project.ID == projectID || projectDeleteRequested(state, project.ID) {
			continue
		}
		if strings.TrimSpace(project.DefaultRuntimeID) == runtimeID {
			return true
		}
	}

	for _, app := range state.Apps {
		if app.ProjectID == projectID || isDeletedApp(app) {
			continue
		}
		if appReferencesRuntime(app, runtimeID) {
			return true
		}
	}

	for _, service := range state.BackingServices {
		if service.ProjectID == projectID || isDeletedBackingService(service) || service.Spec.Postgres == nil {
			continue
		}
		for _, postgresRuntimeID := range managedPostgresReferencedRuntimeIDs("", *service.Spec.Postgres) {
			if postgresRuntimeID == runtimeID {
				return true
			}
		}
	}

	for _, op := range state.Operations {
		if !isActiveOperationStatus(op.Status) || !operationReferencesRuntime(op, runtimeID) {
			continue
		}
		if operationProjectIDState(state, op) != projectID {
			return true
		}
	}
	return false
}

func appReferencesRuntime(app model.App, runtimeID string) bool {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return false
	}
	if strings.TrimSpace(app.Spec.RuntimeID) == runtimeID {
		return true
	}
	if app.Spec.Failover != nil && strings.TrimSpace(app.Spec.Failover.TargetRuntimeID) == runtimeID {
		return true
	}
	if app.Spec.Postgres != nil {
		for _, postgresRuntimeID := range managedPostgresReferencedRuntimeIDs(app.Spec.RuntimeID, *app.Spec.Postgres) {
			if postgresRuntimeID == runtimeID {
				return true
			}
		}
	}
	for _, service := range app.BackingServices {
		if service.Spec.Postgres == nil {
			continue
		}
		for _, postgresRuntimeID := range managedPostgresReferencedRuntimeIDs(app.Spec.RuntimeID, *service.Spec.Postgres) {
			if postgresRuntimeID == runtimeID {
				return true
			}
		}
	}
	return strings.TrimSpace(app.Status.CurrentRuntimeID) == runtimeID
}

func operationReferencesRuntime(op model.Operation, runtimeID string) bool {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return false
	}
	if strings.TrimSpace(op.SourceRuntimeID) == runtimeID ||
		strings.TrimSpace(op.TargetRuntimeID) == runtimeID ||
		strings.TrimSpace(op.AssignedRuntimeID) == runtimeID {
		return true
	}
	if op.DesiredSpec == nil {
		return false
	}
	if strings.TrimSpace(op.DesiredSpec.RuntimeID) == runtimeID {
		return true
	}
	if op.DesiredSpec.Failover != nil && strings.TrimSpace(op.DesiredSpec.Failover.TargetRuntimeID) == runtimeID {
		return true
	}
	if op.DesiredSpec.Postgres != nil {
		for _, postgresRuntimeID := range managedPostgresReferencedRuntimeIDs(op.DesiredSpec.RuntimeID, *op.DesiredSpec.Postgres) {
			if postgresRuntimeID == runtimeID {
				return true
			}
		}
	}
	return false
}

func operationProjectIDState(state *storeProjection, op model.Operation) string {
	index := findApp(state, op.AppID)
	if index < 0 {
		return ""
	}
	return strings.TrimSpace(state.Apps[index].ProjectID)
}

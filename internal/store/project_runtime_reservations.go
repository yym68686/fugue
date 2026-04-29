package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) ListProjectRuntimeReservations(projectID string) ([]model.ProjectRuntimeReservation, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListProjectRuntimeReservations(projectID)
	}

	var reservations []model.ProjectRuntimeReservation
	err := s.withLockedState(false, func(state *model.State) error {
		if findProject(state, projectID) < 0 {
			return ErrNotFound
		}
		for _, reservation := range state.ProjectRuntimeReservations {
			if reservation.ProjectID != projectID {
				continue
			}
			reservations = append(reservations, normalizeProjectRuntimeReservation(reservation))
		}
		sortProjectRuntimeReservations(reservations)
		return nil
	})
	return reservations, err
}

func (s *Store) ReserveProjectRuntime(projectID, runtimeID string) (model.ProjectRuntimeReservation, error) {
	projectID = strings.TrimSpace(projectID)
	runtimeID = strings.TrimSpace(runtimeID)
	if projectID == "" || runtimeID == "" {
		return model.ProjectRuntimeReservation{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgReserveProjectRuntime(projectID, runtimeID)
	}

	var reservation model.ProjectRuntimeReservation
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		projectIndex := findProject(state, projectID)
		if projectIndex < 0 {
			return ErrNotFound
		}
		if projectDeleteRequested(state, projectID) {
			return ErrConflict
		}
		runtimeIndex := findRuntime(state, runtimeID)
		if runtimeIndex < 0 {
			return ErrNotFound
		}
		project := state.Projects[projectIndex]
		runtimeObj := state.Runtimes[runtimeIndex]
		if err := validateProjectRuntimeReservationTarget(project, runtimeObj); err != nil {
			return err
		}

		if existingIndex := findProjectRuntimeReservationByRuntime(state, runtimeID); existingIndex >= 0 {
			existing := normalizeProjectRuntimeReservation(state.ProjectRuntimeReservations[existingIndex])
			if existing.ProjectID != projectID {
				return ErrConflict
			}
			if state.ProjectRuntimeReservations[existingIndex].Mode != existing.Mode {
				state.ProjectRuntimeReservations[existingIndex].Mode = existing.Mode
				state.ProjectRuntimeReservations[existingIndex].UpdatedAt = time.Now().UTC()
				existing = state.ProjectRuntimeReservations[existingIndex]
			}
			reservation = existing
			return nil
		}

		if runtimeHasProjectReservationBlockersState(state, projectID, runtimeID) {
			return ErrConflict
		}

		now := time.Now().UTC()
		reservation = model.ProjectRuntimeReservation{
			TenantID:  project.TenantID,
			ProjectID: project.ID,
			RuntimeID: runtimeObj.ID,
			Mode:      model.ProjectRuntimeReservationModeExclusive,
			CreatedAt: now,
			UpdatedAt: now,
		}
		state.ProjectRuntimeReservations = append(state.ProjectRuntimeReservations, reservation)
		return nil
	})
	return reservation, err
}

func (s *Store) DeleteProjectRuntimeReservation(projectID, runtimeID string) (model.ProjectRuntimeReservation, error) {
	projectID = strings.TrimSpace(projectID)
	runtimeID = strings.TrimSpace(runtimeID)
	if projectID == "" || runtimeID == "" {
		return model.ProjectRuntimeReservation{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteProjectRuntimeReservation(projectID, runtimeID)
	}

	var reservation model.ProjectRuntimeReservation
	err := s.withLockedState(true, func(state *model.State) error {
		index := findProjectRuntimeReservation(state, projectID, runtimeID)
		if index < 0 {
			return ErrNotFound
		}
		reservation = normalizeProjectRuntimeReservation(state.ProjectRuntimeReservations[index])
		state.ProjectRuntimeReservations = append(state.ProjectRuntimeReservations[:index], state.ProjectRuntimeReservations[index+1:]...)
		return nil
	})
	return reservation, err
}

func sortProjectRuntimeReservations(reservations []model.ProjectRuntimeReservation) {
	sort.Slice(reservations, func(i, j int) bool {
		if reservations[i].CreatedAt.Equal(reservations[j].CreatedAt) {
			return reservations[i].RuntimeID < reservations[j].RuntimeID
		}
		return reservations[i].CreatedAt.Before(reservations[j].CreatedAt)
	})
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

func validateProjectRuntimeReservationTarget(project model.Project, runtimeObj model.Runtime) error {
	if strings.TrimSpace(project.ID) == "" || strings.TrimSpace(runtimeObj.ID) == "" {
		return ErrInvalidInput
	}
	switch strings.TrimSpace(runtimeObj.Type) {
	case model.RuntimeTypeManagedOwned, model.RuntimeTypeExternalOwned:
	default:
		return ErrInvalidInput
	}
	if strings.TrimSpace(project.TenantID) == "" || runtimeObj.TenantID != project.TenantID {
		return ErrNotFound
	}
	return nil
}

func findProjectRuntimeReservation(state *model.State, projectID, runtimeID string) int {
	projectID = strings.TrimSpace(projectID)
	runtimeID = strings.TrimSpace(runtimeID)
	for idx, reservation := range state.ProjectRuntimeReservations {
		if strings.TrimSpace(reservation.ProjectID) == projectID && strings.TrimSpace(reservation.RuntimeID) == runtimeID {
			return idx
		}
	}
	return -1
}

func findProjectRuntimeReservationByRuntime(state *model.State, runtimeID string) int {
	runtimeID = strings.TrimSpace(runtimeID)
	for idx, reservation := range state.ProjectRuntimeReservations {
		if strings.TrimSpace(reservation.RuntimeID) == runtimeID {
			return idx
		}
	}
	return -1
}

func validateRuntimeReservedForProjectState(state *model.State, projectID, runtimeID string) error {
	projectID = strings.TrimSpace(projectID)
	runtimeID = strings.TrimSpace(runtimeID)
	if state == nil || runtimeID == "" {
		return nil
	}
	index := findProjectRuntimeReservationByRuntime(state, runtimeID)
	if index < 0 {
		return nil
	}
	reservation := normalizeProjectRuntimeReservation(state.ProjectRuntimeReservations[index])
	if reservation.ProjectID != projectID {
		return ErrConflict
	}
	return nil
}

func validateAppSpecRuntimeReservationsState(state *model.State, projectID string, spec model.AppSpec) error {
	if err := validateRuntimeReservedForProjectState(state, projectID, spec.RuntimeID); err != nil {
		return err
	}
	if spec.Failover != nil {
		if err := validateRuntimeReservedForProjectState(state, projectID, spec.Failover.TargetRuntimeID); err != nil {
			return err
		}
	}
	if spec.Postgres != nil {
		for _, runtimeID := range managedPostgresReferencedRuntimeIDs(spec.RuntimeID, *spec.Postgres) {
			if err := validateRuntimeReservedForProjectState(state, projectID, runtimeID); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateBackingServiceSpecRuntimeReservationsState(state *model.State, projectID string, spec model.BackingServiceSpec) error {
	if spec.Postgres == nil {
		return nil
	}
	for _, runtimeID := range managedPostgresReferencedRuntimeIDs("", *spec.Postgres) {
		if err := validateRuntimeReservedForProjectState(state, projectID, runtimeID); err != nil {
			return err
		}
	}
	return nil
}

func validateOperationRuntimeReservationsState(state *model.State, projectID string, op model.Operation) error {
	switch op.Type {
	case model.OperationTypeImport, model.OperationTypeDeploy, model.OperationTypeMigrate:
		if op.DesiredSpec != nil {
			return validateAppSpecRuntimeReservationsState(state, projectID, *op.DesiredSpec)
		}
		return validateRuntimeReservedForProjectState(state, projectID, op.TargetRuntimeID)
	case model.OperationTypeFailover, model.OperationTypeDatabaseSwitchover, model.OperationTypeDatabaseLocalize:
		if err := validateRuntimeReservedForProjectState(state, projectID, op.TargetRuntimeID); err != nil {
			return err
		}
		if op.Type == model.OperationTypeDatabaseLocalize && op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
			return validateRuntimeReservedForProjectState(state, projectID, op.DesiredSpec.Postgres.RuntimeID)
		}
	}
	return nil
}

func runtimeHasProjectReservationBlockersState(state *model.State, projectID, runtimeID string) bool {
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

func operationProjectIDState(state *model.State, op model.Operation) string {
	index := findApp(state, op.AppID)
	if index < 0 {
		return ""
	}
	return strings.TrimSpace(state.Apps[index].ProjectID)
}

func deleteProjectRuntimeReservationsByTenant(reservations []model.ProjectRuntimeReservation, tenantID string) []model.ProjectRuntimeReservation {
	filtered := reservations[:0]
	for _, reservation := range reservations {
		if reservation.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, reservation)
	}
	return filtered
}

func deleteProjectRuntimeReservationsByProject(reservations []model.ProjectRuntimeReservation, projectID string) []model.ProjectRuntimeReservation {
	filtered := reservations[:0]
	for _, reservation := range reservations {
		if reservation.ProjectID == projectID {
			continue
		}
		filtered = append(filtered, reservation)
	}
	return filtered
}

func deleteProjectRuntimeReservationsByRuntime(reservations []model.ProjectRuntimeReservation, runtimeID string) []model.ProjectRuntimeReservation {
	filtered := reservations[:0]
	for _, reservation := range reservations {
		if reservation.RuntimeID == runtimeID {
			continue
		}
		filtered = append(filtered, reservation)
	}
	return filtered
}

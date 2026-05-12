package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgListProjectRuntimeReservations(projectID string) ([]model.ProjectRuntimeReservation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fugue_projects WHERE id = $1)`, projectID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check project %s exists: %w", projectID, err)
	}
	if !exists {
		return nil, ErrNotFound
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT tenant_id, project_id, runtime_id, mode, created_at, updated_at
FROM fugue_project_runtime_reservations
WHERE project_id = $1
ORDER BY created_at ASC, runtime_id ASC
`, projectID)
	if err != nil {
		return nil, fmt.Errorf("list project runtime reservations: %w", err)
	}
	defer rows.Close()

	reservations := make([]model.ProjectRuntimeReservation, 0)
	for rows.Next() {
		reservation, err := scanProjectRuntimeReservation(rows)
		if err != nil {
			return nil, err
		}
		reservations = append(reservations, reservation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate project runtime reservations: %w", err)
	}
	return reservations, nil
}

func (s *Store) pgReserveProjectRuntime(projectID, runtimeID string) (model.ProjectRuntimeReservation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ProjectRuntimeReservation{}, fmt.Errorf("begin reserve project runtime transaction: %w", err)
	}
	defer tx.Rollback()

	if err := pgLockRuntimeReservationTx(ctx, tx, runtimeID); err != nil {
		return model.ProjectRuntimeReservation{}, err
	}

	project, err := scanProject(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, name, slug, description, default_runtime_id, created_at, updated_at
FROM fugue_projects
WHERE id = $1
FOR UPDATE
`, projectID))
	if err != nil {
		return model.ProjectRuntimeReservation{}, mapDBErr(err)
	}
	deleteRequested, err := s.pgProjectDeleteRequestedTx(ctx, tx, project.ID)
	if err != nil {
		return model.ProjectRuntimeReservation{}, err
	}
	if deleteRequested {
		return model.ProjectRuntimeReservation{}, ErrConflict
	}

	runtimeObj, err := scanRuntime(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, name, machine_name, type, access_mode, public_offer_json, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE id = $1
FOR UPDATE
`, runtimeID))
	if err != nil {
		return model.ProjectRuntimeReservation{}, mapDBErr(err)
	}
	if err := validateProjectRuntimeReservationTarget(project, runtimeObj); err != nil {
		return model.ProjectRuntimeReservation{}, err
	}

	existing, found, err := pgGetProjectRuntimeReservationByRuntimeTx(ctx, tx, runtimeID)
	if err != nil {
		return model.ProjectRuntimeReservation{}, err
	}
	if found {
		if existing.ProjectID != project.ID {
			return model.ProjectRuntimeReservation{}, ErrConflict
		}
		if err := tx.Commit(); err != nil {
			return model.ProjectRuntimeReservation{}, fmt.Errorf("commit reserve project runtime transaction: %w", err)
		}
		return existing, nil
	}

	blocked, err := s.pgRuntimeHasProjectReservationBlockersTx(ctx, tx, project.ID, runtimeID)
	if err != nil {
		return model.ProjectRuntimeReservation{}, err
	}
	if blocked {
		return model.ProjectRuntimeReservation{}, ErrConflict
	}

	now := time.Now().UTC()
	reservation := model.ProjectRuntimeReservation{
		TenantID:  project.TenantID,
		ProjectID: project.ID,
		RuntimeID: runtimeObj.ID,
		Mode:      model.ProjectRuntimeReservationModeExclusive,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_project_runtime_reservations (tenant_id, project_id, runtime_id, mode, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)
`, reservation.TenantID, reservation.ProjectID, reservation.RuntimeID, reservation.Mode, reservation.CreatedAt, reservation.UpdatedAt); err != nil {
		return model.ProjectRuntimeReservation{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.ProjectRuntimeReservation{}, fmt.Errorf("commit reserve project runtime transaction: %w", err)
	}
	return reservation, nil
}

func (s *Store) pgDeleteProjectRuntimeReservation(projectID, runtimeID string) (model.ProjectRuntimeReservation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ProjectRuntimeReservation{}, fmt.Errorf("begin delete project runtime reservation transaction: %w", err)
	}
	defer tx.Rollback()

	if err := pgLockRuntimeReservationTx(ctx, tx, runtimeID); err != nil {
		return model.ProjectRuntimeReservation{}, err
	}

	reservation, err := scanProjectRuntimeReservation(tx.QueryRowContext(ctx, `
DELETE FROM fugue_project_runtime_reservations
WHERE project_id = $1 AND runtime_id = $2
RETURNING tenant_id, project_id, runtime_id, mode, created_at, updated_at
`, projectID, runtimeID))
	if err != nil {
		return model.ProjectRuntimeReservation{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.ProjectRuntimeReservation{}, fmt.Errorf("commit delete project runtime reservation transaction: %w", err)
	}
	return reservation, nil
}

func pgLockRuntimeReservationTx(ctx context.Context, tx *sql.Tx, runtimeID string) error {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return ErrInvalidInput
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1)::bigint)`, runtimeID); err != nil {
		return fmt.Errorf("lock runtime reservation %s: %w", runtimeID, err)
	}
	return nil
}

func pgGetProjectRuntimeReservationByRuntimeTx(ctx context.Context, tx *sql.Tx, runtimeID string) (model.ProjectRuntimeReservation, bool, error) {
	reservation, err := scanProjectRuntimeReservation(tx.QueryRowContext(ctx, `
SELECT tenant_id, project_id, runtime_id, mode, created_at, updated_at
FROM fugue_project_runtime_reservations
WHERE runtime_id = $1
FOR UPDATE
`, runtimeID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.ProjectRuntimeReservation{}, false, nil
		}
		return model.ProjectRuntimeReservation{}, false, err
	}
	return reservation, true, nil
}

func pgValidateRuntimeReservedForProjectTx(ctx context.Context, tx *sql.Tx, projectID, runtimeID string) error {
	return pgValidateRuntimesReservedForProjectTx(ctx, tx, projectID, []string{runtimeID})
}

func pgValidateRuntimesReservedForProjectTx(ctx context.Context, tx *sql.Tx, projectID string, runtimeIDs []string) error {
	ids := make([]string, 0, len(runtimeIDs))
	seen := make(map[string]struct{}, len(runtimeIDs))
	for _, runtimeID := range runtimeIDs {
		runtimeID = strings.TrimSpace(runtimeID)
		if runtimeID == "" {
			continue
		}
		if _, ok := seen[runtimeID]; ok {
			continue
		}
		seen[runtimeID] = struct{}{}
		ids = append(ids, runtimeID)
	}
	sort.Strings(ids)
	for _, runtimeID := range ids {
		if err := pgLockRuntimeReservationTx(ctx, tx, runtimeID); err != nil {
			return err
		}
		reservation, found, err := pgGetProjectRuntimeReservationByRuntimeTx(ctx, tx, runtimeID)
		if err != nil {
			return err
		}
		if found && reservation.ProjectID != strings.TrimSpace(projectID) {
			return ErrConflict
		}
	}
	return nil
}

func pgValidateAppSpecRuntimeReservationsTx(ctx context.Context, tx *sql.Tx, projectID string, spec model.AppSpec) error {
	runtimeIDs := []string{spec.RuntimeID}
	if spec.Failover != nil {
		runtimeIDs = append(runtimeIDs, spec.Failover.TargetRuntimeID)
	}
	if spec.Postgres != nil {
		runtimeIDs = append(runtimeIDs, managedPostgresReferencedRuntimeIDs(spec.RuntimeID, *spec.Postgres)...)
	}
	return pgValidateRuntimesReservedForProjectTx(ctx, tx, projectID, runtimeIDs)
}

func pgValidateBackingServiceSpecRuntimeReservationsTx(ctx context.Context, tx *sql.Tx, projectID string, spec model.BackingServiceSpec) error {
	if spec.Postgres == nil {
		return nil
	}
	return pgValidateRuntimesReservedForProjectTx(ctx, tx, projectID, managedPostgresReferencedRuntimeIDs("", *spec.Postgres))
}

func pgValidateOperationRuntimeReservationsTx(ctx context.Context, tx *sql.Tx, projectID string, op model.Operation) error {
	switch op.Type {
	case model.OperationTypeImport, model.OperationTypeDeploy, model.OperationTypeMigrate:
		if op.DesiredSpec != nil {
			return pgValidateAppSpecRuntimeReservationsTx(ctx, tx, projectID, *op.DesiredSpec)
		}
		return pgValidateRuntimeReservedForProjectTx(ctx, tx, projectID, op.TargetRuntimeID)
	case model.OperationTypeFailover, model.OperationTypeDatabaseSwitchover, model.OperationTypeDatabaseLocalize:
		runtimeIDs := []string{op.TargetRuntimeID}
		if op.Type == model.OperationTypeDatabaseLocalize && op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
			runtimeIDs = append(runtimeIDs, op.DesiredSpec.Postgres.RuntimeID)
		}
		return pgValidateRuntimesReservedForProjectTx(ctx, tx, projectID, runtimeIDs)
	}
	return nil
}

func (s *Store) pgRuntimeHasProjectReservationBlockersTx(ctx context.Context, tx *sql.Tx, projectID, runtimeID string) (bool, error) {
	state := model.State{}

	projectRows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, name, slug, description, default_runtime_id, created_at, updated_at
FROM fugue_projects
`)
	if err != nil {
		return false, fmt.Errorf("list projects for runtime reservation blockers: %w", err)
	}
	for projectRows.Next() {
		project, err := scanProject(projectRows)
		if err != nil {
			projectRows.Close()
			return false, err
		}
		state.Projects = append(state.Projects, project)
	}
	if err := projectRows.Close(); err != nil {
		return false, err
	}
	if err := projectRows.Err(); err != nil {
		return false, fmt.Errorf("iterate projects for runtime reservation blockers: %w", err)
	}

	appRows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
FROM fugue_apps
`)
	if err != nil {
		return false, fmt.Errorf("list apps for runtime reservation blockers: %w", err)
	}
	for appRows.Next() {
		app, err := scanApp(appRows)
		if err != nil {
			appRows.Close()
			return false, err
		}
		state.Apps = append(state.Apps, app)
	}
	if err := appRows.Close(); err != nil {
		return false, err
	}
	if err := appRows.Err(); err != nil {
		return false, fmt.Errorf("iterate apps for runtime reservation blockers: %w", err)
	}

	serviceRows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, project_id, owner_app_id, name, description, type, provisioner, status, spec_json, current_runtime_started_at, current_runtime_ready_at, created_at, updated_at
FROM fugue_backing_services
`)
	if err != nil {
		return false, fmt.Errorf("list backing services for runtime reservation blockers: %w", err)
	}
	for serviceRows.Next() {
		service, err := scanBackingService(serviceRows)
		if err != nil {
			serviceRows.Close()
			return false, err
		}
		state.BackingServices = append(state.BackingServices, service)
	}
	if err := serviceRows.Close(); err != nil {
		return false, err
	}
	if err := serviceRows.Err(); err != nil {
		return false, fmt.Errorf("iterate backing services for runtime reservation blockers: %w", err)
	}

	operationRows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, service_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at
FROM fugue_operations
`)
	if err != nil {
		return false, fmt.Errorf("list operations for runtime reservation blockers: %w", err)
	}
	for operationRows.Next() {
		op, err := scanOperation(operationRows)
		if err != nil {
			operationRows.Close()
			return false, err
		}
		state.Operations = append(state.Operations, op)
	}
	if err := operationRows.Close(); err != nil {
		return false, err
	}
	if err := operationRows.Err(); err != nil {
		return false, fmt.Errorf("iterate operations for runtime reservation blockers: %w", err)
	}

	return runtimeHasProjectReservationBlockersState(&state, projectID, runtimeID), nil
}

func scanProjectRuntimeReservation(scanner sqlScanner) (model.ProjectRuntimeReservation, error) {
	var reservation model.ProjectRuntimeReservation
	if err := scanner.Scan(&reservation.TenantID, &reservation.ProjectID, &reservation.RuntimeID, &reservation.Mode, &reservation.CreatedAt, &reservation.UpdatedAt); err != nil {
		return model.ProjectRuntimeReservation{}, err
	}
	return normalizeProjectRuntimeReservation(reservation), nil
}

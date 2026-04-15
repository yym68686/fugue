package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"fugue/internal/model"
	"fugue/internal/store"
)

func isDeletingProjectAppPhase(app model.App) bool {
	phase := strings.TrimSpace(strings.ToLower(app.Status.Phase))
	return phase != "" && strings.Contains(phase, "deleting")
}

func (s *Server) deleteProjectCascade(
	principal model.Principal,
	project model.Project,
) (int, map[string]any, map[string]string, error) {
	apps, err := s.store.ListAppsMetadataByProjectIDs([]string{project.ID})
	if err != nil {
		return 0, nil, nil, err
	}

	allServices, err := s.store.ListBackingServices(project.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return 0, nil, nil, err
	}

	projectServices := make([]model.BackingService, 0)
	for _, service := range allServices {
		if service.ProjectID == project.ID {
			projectServices = append(projectServices, service)
		}
	}

	if _, _, err := s.store.MarkProjectDeleteRequested(project.ID); err != nil {
		return 0, nil, nil, err
	}

	queuedOperations := make([]model.Operation, 0, len(apps))
	alreadyDeletingApps := 0
	for _, app := range apps {
		if isDeletingProjectAppPhase(app) {
			alreadyDeletingApps++
			continue
		}
		op, err := s.store.CreateOperation(model.Operation{
			TenantID:        app.TenantID,
			Type:            model.OperationTypeDelete,
			RequestedByType: principal.ActorType,
			RequestedByID:   principal.ActorID,
			AppID:           app.ID,
		})
		if err != nil {
			if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) {
				alreadyDeletingApps++
				continue
			}
			return 0, nil, nil, err
		}
		queuedOperations = append(queuedOperations, op)
	}

	deletedBackingServices := 0
	for _, service := range projectServices {
		if _, err := s.store.DeleteBackingService(service.ID); err != nil {
			if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) {
				continue
			}
			return 0, nil, nil, err
		}
		deletedBackingServices++
	}

	deletedProject, err := s.store.DeleteProject(project.ID)
	switch {
	case err == nil:
		return http.StatusOK, map[string]any{
				"delete_requested": false,
				"deleted":          true,
				"project":          deletedProject,
				"operations":       sanitizeOperationsForAPI(queuedOperations),
			}, map[string]string{
				"already_deleting_apps":    strconv.Itoa(alreadyDeletingApps),
				"deleted_backing_services": strconv.Itoa(deletedBackingServices),
				"name":                     deletedProject.Name,
				"queued_operations":        strconv.Itoa(len(queuedOperations)),
			}, nil
	case errors.Is(err, store.ErrNotFound):
		return http.StatusOK, map[string]any{
				"delete_requested": false,
				"deleted":          true,
				"project":          project,
				"operations":       sanitizeOperationsForAPI(queuedOperations),
			}, map[string]string{
				"already_deleting_apps":    strconv.Itoa(alreadyDeletingApps),
				"deleted_backing_services": strconv.Itoa(deletedBackingServices),
				"name":                     project.Name,
				"queued_operations":        strconv.Itoa(len(queuedOperations)),
			}, nil
	case errors.Is(err, store.ErrConflict):
		return http.StatusAccepted, map[string]any{
				"delete_requested": true,
				"deleted":          false,
				"project":          project,
				"operations":       sanitizeOperationsForAPI(queuedOperations),
			}, map[string]string{
				"already_deleting_apps":    strconv.Itoa(alreadyDeletingApps),
				"deleted_backing_services": strconv.Itoa(deletedBackingServices),
				"name":                     project.Name,
				"queued_operations":        strconv.Itoa(len(queuedOperations)),
			}, nil
	default:
		return 0, nil, nil, err
	}
}

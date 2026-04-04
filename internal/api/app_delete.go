package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func isActiveAppOperationStatus(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
		return true
	default:
		return false
	}
}

func forceDeleteAbortMessage(op model.Operation) string {
	switch op.Type {
	case model.OperationTypeImport:
		return "build canceled so the app can be force deleted"
	case model.OperationTypeDeploy:
		return "deploy canceled so the app can be force deleted"
	default:
		return "operation canceled so the app can be force deleted"
	}
}

func activeDeleteOperationForApp(operations []model.Operation) *model.Operation {
	for _, operation := range operations {
		if operation.Type != model.OperationTypeDelete || !isActiveAppOperationStatus(operation.Status) {
			continue
		}
		operationCopy := operation
		return &operationCopy
	}

	return nil
}

func (s *Server) handleForceDeleteApp(
	w http.ResponseWriter,
	r *http.Request,
	principal model.Principal,
	app model.App,
) {
	operations, err := s.store.ListOperationsByApp(
		principal.TenantID,
		principal.IsPlatformAdmin(),
		app.ID,
	)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	abortedCount := 0
	for _, operation := range operations {
		if operation.Type == model.OperationTypeDelete || !isActiveAppOperationStatus(operation.Status) {
			continue
		}

		if _, err := s.store.FailOperation(operation.ID, forceDeleteAbortMessage(operation)); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			s.writeStoreError(w, err)
			return
		}
		abortedCount++
	}

	if deleteOp := activeDeleteOperationForApp(operations); deleteOp != nil {
		s.appendAudit(principal, "app.force_delete", "operation", deleteOp.ID, app.TenantID, map[string]string{
			"aborted_operations": strconv.Itoa(abortedCount),
			"app_id":             app.ID,
			"mode":               "already-deleting",
		})
		httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
			"already_deleting": true,
			"operation":        sanitizeOperationForAPI(*deleteOp),
		})
		return
	}

	reloadedApp, err := s.store.GetApp(app.ID)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		s.appendAudit(principal, "app.force_delete", "app", app.ID, app.TenantID, map[string]string{
			"aborted_operations": strconv.Itoa(abortedCount),
			"app_id":             app.ID,
			"mode":               "already-deleted",
		})
		httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
			"deleted": true,
		})
		return
	default:
		s.writeStoreError(w, err)
		return
	}

	if strings.EqualFold(strings.TrimSpace(reloadedApp.Status.Phase), "deleting") {
		s.appendAudit(principal, "app.force_delete", "app", app.ID, app.TenantID, map[string]string{
			"aborted_operations": strconv.Itoa(abortedCount),
			"app_id":             app.ID,
			"mode":               "deleting",
		})
		httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
			"already_deleting": true,
		})
		return
	}

	if _, err := s.store.PurgeApp(app.ID); err == nil {
		s.appendAudit(principal, "app.force_delete", "app", app.ID, app.TenantID, map[string]string{
			"aborted_operations": strconv.Itoa(abortedCount),
			"app_id":             app.ID,
			"mode":               "purged",
		})
		httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
			"deleted": true,
		})
		return
	} else if !errors.Is(err, store.ErrConflict) {
		if errors.Is(err, store.ErrNotFound) {
			s.appendAudit(principal, "app.force_delete", "app", app.ID, app.TenantID, map[string]string{
				"aborted_operations": strconv.Itoa(abortedCount),
				"app_id":             app.ID,
				"mode":               "already-deleted",
			})
			httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
				"deleted": true,
			})
			return
		}
		s.writeStoreError(w, err)
		return
	}

	deleteOp, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDelete,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.appendAudit(principal, "app.force_delete", "app", app.ID, app.TenantID, map[string]string{
				"aborted_operations": strconv.Itoa(abortedCount),
				"app_id":             app.ID,
				"mode":               "already-deleted",
			})
			httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
				"deleted": true,
			})
			return
		}
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(principal, "app.force_delete", "operation", deleteOp.ID, app.TenantID, map[string]string{
		"aborted_operations": strconv.Itoa(abortedCount),
		"app_id":             app.ID,
		"mode":               "queued-delete",
	})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"operation": sanitizeOperationForAPI(deleteOp),
	})
}

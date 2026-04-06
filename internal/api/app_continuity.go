package api

import (
	"net/http"
	"reflect"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handlePatchAppContinuity(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	var req struct {
		AppFailover *struct {
			Enabled         bool   `json:"enabled"`
			TargetRuntimeID string `json:"target_runtime_id"`
		} `json:"app_failover"`
		DatabaseFailover *struct {
			Enabled         bool   `json:"enabled"`
			TargetRuntimeID string `json:"target_runtime_id"`
		} `json:"database_failover"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.AppFailover == nil && req.DatabaseFailover == nil {
		httpx.WriteError(w, http.StatusBadRequest, "app_failover or database_failover is required")
		return
	}

	spec := cloneAppSpec(app.Spec)
	currentAppFailover := cloneAppFailoverSpec(app.Spec.Failover)
	currentDatabase := normalizedOwnedManagedPostgresSpec(app)
	nextAppFailover := cloneAppFailoverSpec(currentAppFailover)
	nextDatabase := cloneAppPostgresSpec(currentDatabase)
	changed := false

	if req.AppFailover != nil {
		if req.AppFailover.Enabled {
			targetRuntimeID := strings.TrimSpace(req.AppFailover.TargetRuntimeID)
			if targetRuntimeID == "" {
				httpx.WriteError(w, http.StatusBadRequest, "app_failover.target_runtime_id is required when enabled")
				return
			}
			nextAppFailover = &model.AppFailoverSpec{
				TargetRuntimeID: targetRuntimeID,
				Auto:            true,
			}
		} else {
			nextAppFailover = nil
		}
		if !reflect.DeepEqual(currentAppFailover, nextAppFailover) {
			spec.Failover = cloneAppFailoverSpec(nextAppFailover)
			changed = true
		}
	}

	if req.DatabaseFailover != nil {
		if currentDatabase == nil {
			httpx.WriteError(w, http.StatusBadRequest, "managed postgres is not configured for this app")
			return
		}
		if nextDatabase == nil {
			nextDatabase = cloneAppPostgresSpec(currentDatabase)
		}
		if req.DatabaseFailover.Enabled {
			targetRuntimeID := strings.TrimSpace(req.DatabaseFailover.TargetRuntimeID)
			if targetRuntimeID == "" {
				httpx.WriteError(w, http.StatusBadRequest, "database_failover.target_runtime_id is required when enabled")
				return
			}
			nextDatabase.FailoverTargetRuntimeID = targetRuntimeID
			if nextDatabase.Instances < 2 {
				nextDatabase.Instances = 2
			}
			if nextDatabase.SynchronousReplicas < 1 {
				nextDatabase.SynchronousReplicas = 1
			}
		} else {
			nextDatabase.FailoverTargetRuntimeID = ""
			nextDatabase.Instances = 1
			nextDatabase.SynchronousReplicas = 0
		}
		if !reflect.DeepEqual(currentDatabase, nextDatabase) {
			spec.Postgres = cloneAppPostgresSpec(nextDatabase)
			changed = true
		}
	}

	response := map[string]any{
		"app_failover": cloneAppFailoverSpec(nextAppFailover),
		"database":     sanitizePostgresSpecForContinuityResponse(nextDatabase),
	}
	if !changed {
		response["already_current"] = true
		httpx.WriteJSON(w, http.StatusOK, response)
		return
	}

	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		DesiredSpec:     &spec,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	response["operation"] = sanitizeOperationForAPI(op)
	s.appendAudit(principal, "app.continuity.patch", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusAccepted, response)
}

func normalizedOwnedManagedPostgresSpec(app model.App) *model.AppPostgresSpec {
	return store.OwnedManagedPostgresSpec(app)
}

func sanitizePostgresSpecForContinuityResponse(spec *model.AppPostgresSpec) *model.AppPostgresSpec {
	if spec == nil {
		return nil
	}
	out := cloneAppPostgresSpec(spec)
	if out != nil {
		out.Password = ""
	}
	return out
}

func cloneAppFailoverSpec(spec *model.AppFailoverSpec) *model.AppFailoverSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	return &out
}

func cloneAppPostgresSpec(spec *model.AppPostgresSpec) *model.AppPostgresSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	if spec.Resources != nil {
		resources := *spec.Resources
		out.Resources = &resources
	}
	return &out
}

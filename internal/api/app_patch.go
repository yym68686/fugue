package api

import (
	"net/http"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handlePatchApp(w http.ResponseWriter, r *http.Request) {
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
		ImageMirrorLimit  *int                            `json:"image_mirror_limit"`
		StartupCommand    *string                         `json:"startup_command,omitempty"`
		PersistentStorage *model.AppPersistentStorageSpec `json:"persistent_storage,omitempty"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.ImageMirrorLimit == nil && req.StartupCommand == nil && req.PersistentStorage == nil {
		httpx.WriteError(w, http.StatusBadRequest, "image_mirror_limit, startup_command, or persistent_storage is required")
		return
	}
	if req.ImageMirrorLimit != nil && *req.ImageMirrorLimit < 0 {
		httpx.WriteError(w, http.StatusBadRequest, "image_mirror_limit must be greater than or equal to 0")
		return
	}

	currentApp := app
	responseApp := app
	changed := false
	auditMetadata := map[string]string{}

	if req.ImageMirrorLimit != nil {
		currentLimit := model.EffectiveAppImageMirrorLimit(app.Spec.ImageMirrorLimit)
		nextLimit := model.EffectiveAppImageMirrorLimit(*req.ImageMirrorLimit)
		if currentLimit != nextLimit {
			updatedApp, err := s.store.UpdateAppImageMirrorLimit(app.ID, nextLimit)
			if err != nil {
				s.writeStoreError(w, err)
				return
			}
			currentApp = updatedApp
			responseApp = updatedApp
			changed = true
			auditMetadata["image_mirror_limit"] = httpxValue(nextLimit)

			if nextLimit < currentLimit {
				if err := s.pruneExcessManagedAppImages(r.Context(), updatedApp); err != nil && s.log != nil {
					s.log.Printf("prune excess managed app images for app=%s failed: %v", updatedApp.ID, err)
				}
				s.scheduleTenantBillingImageStorageRefresh(updatedApp.TenantID)
			}
		}
	}

	var operation *model.Operation
	if req.StartupCommand != nil || req.PersistentStorage != nil {
		spec, source, err := s.recoverAppDeployBaseline(currentApp)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}

		currentCommand := append([]string(nil), spec.Command...)
		currentPersistentStorage := cloneAppSpec(spec).PersistentStorage
		spec.ImageMirrorLimit = model.EffectiveAppImageMirrorLimit(currentApp.Spec.ImageMirrorLimit)

		deployChanged := false
		if req.StartupCommand != nil {
			applyStartupCommand(&spec, req.StartupCommand)
			if !startupCommandsEqual(currentCommand, spec.Command) {
				deployChanged = true
				if len(spec.Command) == 0 {
					auditMetadata["startup_command"] = "cleared"
				} else {
					auditMetadata["startup_command"] = "set"
				}
			}
		}

		if req.PersistentStorage != nil {
			normalizedPersistentStorage, err := normalizeImportedPersistentStorage(req.PersistentStorage, spec.Files)
			if err != nil {
				httpx.WriteError(w, http.StatusBadRequest, err.Error())
				return
			}
			spec.PersistentStorage = normalizedPersistentStorage
			if !appPersistentStorageEqual(currentPersistentStorage, spec.PersistentStorage) {
				deployChanged = true
				if spec.PersistentStorage == nil {
					auditMetadata["persistent_storage"] = "cleared"
				} else {
					auditMetadata["persistent_storage"] = "set"
				}
			}
		}

		if deployChanged {
			op, err := s.store.CreateOperation(model.Operation{
				TenantID:        currentApp.TenantID,
				Type:            model.OperationTypeDeploy,
				RequestedByType: principal.ActorType,
				RequestedByID:   principal.ActorID,
				AppID:           currentApp.ID,
				DesiredSpec:     &spec,
				DesiredSource:   source,
			})
			if err != nil {
				s.writeStoreError(w, err)
				return
			}
			operation = &op
			changed = true
		}
	}

	if !changed {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"app":             sanitizeAppForAPI(app),
			"already_current": true,
		})
		return
	}

	s.appendAudit(principal, "app.patch", "app", currentApp.ID, currentApp.TenantID, auditMetadata)
	response := map[string]any{
		"app":             sanitizeAppForAPI(responseApp),
		"already_current": false,
	}
	if operation != nil {
		response["operation"] = sanitizeOperationForAPI(*operation)
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

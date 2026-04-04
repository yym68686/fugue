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
		ImageMirrorLimit *int `json:"image_mirror_limit"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.ImageMirrorLimit == nil {
		httpx.WriteError(w, http.StatusBadRequest, "image_mirror_limit is required")
		return
	}
	if *req.ImageMirrorLimit < 0 {
		httpx.WriteError(w, http.StatusBadRequest, "image_mirror_limit must be greater than or equal to 0")
		return
	}

	currentLimit := model.EffectiveAppImageMirrorLimit(app.Spec.ImageMirrorLimit)
	nextLimit := model.EffectiveAppImageMirrorLimit(*req.ImageMirrorLimit)
	if currentLimit == nextLimit {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"app":             sanitizeAppForAPI(app),
			"already_current": true,
		})
		return
	}

	updatedApp, err := s.store.UpdateAppImageMirrorLimit(app.ID, nextLimit)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	if nextLimit < currentLimit {
		if err := s.pruneExcessManagedAppImages(r.Context(), updatedApp); err != nil && s.log != nil {
			s.log.Printf("prune excess managed app images for app=%s failed: %v", updatedApp.ID, err)
		}
		s.refreshTenantBillingImageStorage(r.Context(), updatedApp.TenantID, principal.IsPlatformAdmin())
	}

	s.appendAudit(principal, "app.patch", "app", updatedApp.ID, updatedApp.TenantID, map[string]string{
		"image_mirror_limit": httpxValue(nextLimit),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"app":             sanitizeAppForAPI(updatedApp),
		"already_current": false,
	})
}

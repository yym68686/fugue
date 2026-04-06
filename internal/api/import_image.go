package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type importImageRequest struct {
	TenantID          string                          `json:"tenant_id"`
	ProjectID         string                          `json:"project_id"`
	Project           *importProjectRequest           `json:"project,omitempty"`
	ImageRef          string                          `json:"image_ref"`
	Name              string                          `json:"name"`
	Description       string                          `json:"description"`
	RuntimeID         string                          `json:"runtime_id"`
	Replicas          int                             `json:"replicas"`
	ServicePort       int                             `json:"service_port"`
	Env               map[string]string               `json:"env"`
	ConfigContent     string                          `json:"config_content"`
	Files             []model.AppFile                 `json:"files"`
	StartupCommand    *string                         `json:"startup_command,omitempty"`
	PersistentStorage *model.AppPersistentStorageSpec `json:"persistent_storage,omitempty"`
	Postgres          *model.AppPostgresSpec          `json:"postgres"`
}

func (s *Server) handleImportImageApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && (!principal.HasScope("app.write") || !principal.HasScope("app.deploy")) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}

	var req importImageRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.ProjectID) != "" && req.Project != nil {
		httpx.WriteError(w, http.StatusBadRequest, "project_id and project are mutually exclusive")
		return
	}

	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create app for another tenant")
		return
	}
	if strings.TrimSpace(s.registryPushBase) == "" {
		httpx.WriteError(w, http.StatusInternalServerError, "internal registry is not configured")
		return
	}
	if strings.TrimSpace(s.appBaseDomain) == "" {
		httpx.WriteError(w, http.StatusInternalServerError, "app base domain is not configured")
		return
	}

	source, err := buildQueuedImageSource(req.ImageRef, "", "")
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	replicas := req.Replicas
	if replicas <= 0 {
		replicas = 1
	}
	runtimeID := strings.TrimSpace(req.RuntimeID)
	if runtimeID == "" {
		runtimeID = "runtime_managed_shared"
	}

	description := strings.TrimSpace(req.Description)
	if description == "" {
		description = fmt.Sprintf("Imported from image %s", strings.TrimSpace(req.ImageRef))
	}

	baseName := strings.TrimSpace(req.Name)
	if baseName == "" {
		baseName = imageImportBaseName(req.ImageRef)
	}
	baseName = normalizeImportBaseName(baseName)
	if baseName == "" {
		baseName = "app"
	}

	project, _, err := s.resolveImportProjectFields(tenantID, req.ProjectID, req.Project)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	var cleanupProject *model.Project
	if req.Project != nil && strings.TrimSpace(req.ProjectID) == "" {
		projectCopy := project
		cleanupProject = &projectCopy
	}
	cleanupApps := make([]model.App, 0, 1)
	cleanupEnabled := true
	defer func() {
		if !cleanupEnabled {
			return
		}
		if err := s.cleanupImportArtifacts(cleanupProject, cleanupApps); err != nil {
			s.log.Printf("cleanup failed after image import error for tenant=%s image=%s: %v", tenantID, strings.TrimSpace(req.ImageRef), err)
		}
	}()

	var app model.App
	for attempt := 0; attempt < 8; attempt++ {
		candidateName, candidateHost := buildImportIdentity(baseName, s.appBaseDomain, attempt)
		if s.isReservedAppHostname(candidateHost) {
			continue
		}

		spec, err := s.buildImportedAppSpec(
			"",
			candidateName,
			"",
			runtimeID,
			replicas,
			effectiveImportServicePort(req.ServicePort, 0),
			req.ConfigContent,
			req.Files,
			req.PersistentStorage,
			req.Postgres,
			req.Env,
		)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		applyStartupCommand(&spec, req.StartupCommand)
		route := model.AppRoute{
			Hostname:    candidateHost,
			BaseDomain:  s.appBaseDomain,
			PublicURL:   "https://" + candidateHost,
			ServicePort: firstServicePort(spec),
		}
		app, err = s.store.CreateImportedApp(tenantID, project.ID, candidateName, description, spec, source, route)
		if err == nil {
			cleanupApps = append(cleanupApps, app)
			break
		}
		if !errors.Is(err, store.ErrConflict) {
			s.writeStoreError(w, err)
			return
		}
	}
	if app.ID == "" {
		httpx.WriteError(w, http.StatusConflict, "could not allocate a unique app name")
		return
	}

	spec := cloneAppSpec(app.Spec)
	sourceCopy := *app.Source
	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        tenantID,
		Type:            model.OperationTypeImport,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		DesiredSpec:     &spec,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(principal, "app.import_image", "app", app.ID, app.TenantID, map[string]string{
		"image_ref": source.ImageRef,
	})
	cleanupEnabled = false
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"app":       sanitizeAppForAPI(app),
		"operation": sanitizeOperationForAPI(op),
	})
}

func buildQueuedImageSource(imageRef, imageNameSuffix, composeService string) (model.AppSource, error) {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return model.AppSource{}, fmt.Errorf("image_ref is required")
	}
	return model.AppSource{
		Type:            model.AppSourceTypeDockerImage,
		ImageRef:        imageRef,
		ImageNameSuffix: strings.TrimSpace(imageNameSuffix),
		ComposeService:  strings.TrimSpace(composeService),
	}, nil
}

func imageImportBaseName(imageRef string) string {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return ""
	}
	if idx := strings.Index(imageRef, "@"); idx >= 0 {
		imageRef = imageRef[:idx]
	}
	lastSegment := imageRef
	if idx := strings.LastIndex(lastSegment, "/"); idx >= 0 {
		lastSegment = lastSegment[idx+1:]
	}
	if idx := strings.Index(lastSegment, ":"); idx >= 0 {
		lastSegment = lastSegment[:idx]
	}
	return lastSegment
}

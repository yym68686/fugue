package api

import (
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

type rebuildAppRequest struct {
	Branch          *string `json:"branch"`
	SourceDir       *string `json:"source_dir"`
	DockerfilePath  *string `json:"dockerfile_path"`
	BuildContextDir *string `json:"build_context_dir"`
}

func (s *Server) handleRebuildApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}

	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	if app.Source == nil {
		httpx.WriteError(w, http.StatusBadRequest, "app does not have an import source")
		return
	}

	var req rebuildAppRequest
	if r.ContentLength != 0 {
		if err := httpx.DecodeJSON(r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	branch := strings.TrimSpace(app.Source.RepoBranch)
	if req.Branch != nil {
		branch = strings.TrimSpace(*req.Branch)
	}

	buildStrategy := strings.TrimSpace(app.Source.BuildStrategy)
	if buildStrategy == "" {
		buildStrategy = model.AppBuildStrategyStaticSite
	}

	sourceDir := strings.TrimSpace(app.Source.SourceDir)
	dockerfilePath := strings.TrimSpace(app.Source.DockerfilePath)
	buildContextDir := strings.TrimSpace(app.Source.BuildContextDir)
	switch buildStrategy {
	case model.AppBuildStrategyStaticSite, model.AppBuildStrategyBuildpacks, model.AppBuildStrategyNixpacks:
		if req.SourceDir != nil {
			sourceDir = strings.TrimSpace(*req.SourceDir)
		}
	case model.AppBuildStrategyDockerfile:
		if req.DockerfilePath != nil {
			dockerfilePath = strings.TrimSpace(*req.DockerfilePath)
		}
		if req.BuildContextDir != nil {
			buildContextDir = strings.TrimSpace(*req.BuildContextDir)
		}
	default:
		if req.SourceDir != nil {
			sourceDir = strings.TrimSpace(*req.SourceDir)
		}
		if req.DockerfilePath != nil {
			dockerfilePath = strings.TrimSpace(*req.DockerfilePath)
		}
		if req.BuildContextDir != nil {
			buildContextDir = strings.TrimSpace(*req.BuildContextDir)
		}
	}

	var (
		source model.AppSource
		err    error
	)
	switch strings.TrimSpace(app.Source.Type) {
	case model.AppSourceTypeGitHubPublic:
		if strings.TrimSpace(app.Source.RepoURL) == "" {
			httpx.WriteError(w, http.StatusBadRequest, "app source repo_url is missing")
			return
		}
		source, err = buildQueuedGitHubSource(
			app.Source.RepoURL,
			branch,
			sourceDir,
			dockerfilePath,
			buildContextDir,
			buildStrategy,
			strings.TrimSpace(app.Source.ImageNameSuffix),
			strings.TrimSpace(app.Source.ComposeService),
		)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	case model.AppSourceTypeUpload:
		uploadID := strings.TrimSpace(app.Source.UploadID)
		if uploadID == "" {
			httpx.WriteError(w, http.StatusBadRequest, "app source upload_id is missing")
			return
		}
		upload, err := s.store.GetSourceUpload(uploadID)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if upload.TenantID != app.TenantID {
			httpx.WriteError(w, http.StatusBadRequest, "app source upload is not visible to this tenant")
			return
		}
		source, err = buildQueuedUploadSource(
			upload,
			sourceDir,
			dockerfilePath,
			buildContextDir,
			buildStrategy,
			strings.TrimSpace(app.Source.ImageNameSuffix),
			strings.TrimSpace(app.Source.ComposeService),
		)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	default:
		httpx.WriteError(w, http.StatusBadRequest, "only github-public or upload apps can be rebuilt")
		return
	}

	spec := cloneAppSpec(app.Spec)
	if spec.Replicas < 1 {
		spec.Replicas = 1
	}
	if strings.TrimSpace(spec.RuntimeID) == "" {
		spec.RuntimeID = "runtime_managed_shared"
	}

	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeImport,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		DesiredSpec:     &spec,
		DesiredSource:   &source,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	auditMetadata := map[string]string{
		"app_id":         app.ID,
		"source_type":    source.Type,
		"build_strategy": source.BuildStrategy,
	}
	if strings.TrimSpace(source.RepoURL) != "" {
		auditMetadata["repo_url"] = source.RepoURL
	}
	if strings.TrimSpace(source.RepoBranch) != "" {
		auditMetadata["repo_branch"] = source.RepoBranch
	}
	if strings.TrimSpace(source.UploadID) != "" {
		auditMetadata["upload_id"] = source.UploadID
	}
	s.appendAudit(principal, "app.rebuild", "operation", op.ID, app.TenantID, auditMetadata)
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"operation": sanitizeOperationForAPI(op),
		"build": map[string]any{
			"source_type":       source.Type,
			"branch":            source.RepoBranch,
			"upload_id":         source.UploadID,
			"source_dir":        source.SourceDir,
			"dockerfile_path":   source.DockerfilePath,
			"build_context_dir": source.BuildContextDir,
			"build_strategy":    source.BuildStrategy,
			"compose_service":   source.ComposeService,
		},
	})
}

package api

import (
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

type rebuildAppRequest struct {
	Branch          *string `json:"branch"`
	ImageRef        *string `json:"image_ref"`
	SourceDir       *string `json:"source_dir"`
	DockerfilePath  *string `json:"dockerfile_path"`
	BuildContextDir *string `json:"build_context_dir"`
	RepoAuthToken   *string `json:"repo_auth_token"`
	ClearFiles      bool    `json:"clear_files,omitempty"`
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
	spec, _, err := s.recoverAppDeployBaseline(app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	baselineSource, err := s.recoverAppOriginSource(app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if baselineSource == nil {
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

	branch := strings.TrimSpace(baselineSource.RepoBranch)
	if req.Branch != nil {
		branch = strings.TrimSpace(*req.Branch)
	}

	buildStrategy := strings.TrimSpace(baselineSource.BuildStrategy)
	if buildStrategy == "" {
		buildStrategy = model.AppBuildStrategyStaticSite
	}

	sourceDir := strings.TrimSpace(baselineSource.SourceDir)
	dockerfilePath := strings.TrimSpace(baselineSource.DockerfilePath)
	buildContextDir := strings.TrimSpace(baselineSource.BuildContextDir)
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
		source   model.AppSource
		buildErr error
	)
	switch strings.TrimSpace(baselineSource.Type) {
	case model.AppSourceTypeDockerImage:
		imageRef := strings.TrimSpace(baselineSource.ImageRef)
		if req.ImageRef != nil {
			imageRef = strings.TrimSpace(*req.ImageRef)
		}
		if imageRef == "" {
			httpx.WriteError(w, http.StatusBadRequest, "app source image_ref is missing")
			return
		}
		source, buildErr = buildQueuedImageSource(
			imageRef,
			strings.TrimSpace(baselineSource.ImageNameSuffix),
			strings.TrimSpace(baselineSource.ComposeService),
		)
		if buildErr != nil {
			httpx.WriteError(w, http.StatusBadRequest, buildErr.Error())
			return
		}
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate:
		if strings.TrimSpace(baselineSource.RepoURL) == "" {
			httpx.WriteError(w, http.StatusBadRequest, "app source repo_url is missing")
			return
		}
		repoAuthToken := strings.TrimSpace(baselineSource.RepoAuthToken)
		if req.RepoAuthToken != nil {
			repoAuthToken = strings.TrimSpace(*req.RepoAuthToken)
		}
		source, buildErr = buildQueuedGitHubSource(
			baselineSource.RepoURL,
			baselineSource.Type,
			repoAuthToken,
			branch,
			sourceDir,
			dockerfilePath,
			buildContextDir,
			buildStrategy,
			strings.TrimSpace(baselineSource.ImageNameSuffix),
			strings.TrimSpace(baselineSource.ComposeService),
		)
		if buildErr != nil {
			httpx.WriteError(w, http.StatusBadRequest, buildErr.Error())
			return
		}
	case model.AppSourceTypeUpload:
		uploadID := strings.TrimSpace(baselineSource.UploadID)
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
		source, buildErr = buildQueuedUploadSource(
			upload,
			sourceDir,
			dockerfilePath,
			buildContextDir,
			buildStrategy,
			strings.TrimSpace(baselineSource.ImageNameSuffix),
			strings.TrimSpace(baselineSource.ComposeService),
		)
		if buildErr != nil {
			httpx.WriteError(w, http.StatusBadRequest, buildErr.Error())
			return
		}
	default:
		httpx.WriteError(w, http.StatusBadRequest, "only github-backed, image-backed, or upload apps can be rebuilt")
		return
	}
	if len(baselineSource.ComposeDependsOn) > 0 {
		source.ComposeDependsOn = append([]string(nil), baselineSource.ComposeDependsOn...)
	}
	if spec.Replicas < 1 {
		spec.Replicas = 1
	}
	if strings.TrimSpace(spec.RuntimeID) == "" {
		spec.RuntimeID = "runtime_managed_shared"
	}
	if req.ClearFiles {
		spec.Files = nil
	}
	if spec.Workspace != nil {
		spec.Workspace.ResetToken = model.NewID("workspace-reset")
	}

	op, err := s.store.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeImport,
		RequestedByType:     principal.ActorType,
		RequestedByID:       principal.ActorID,
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       &source,
		DesiredOriginSource: model.CloneAppSource(&source),
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
	if strings.TrimSpace(source.ImageRef) != "" {
		auditMetadata["image_ref"] = source.ImageRef
	}
	if req.ClearFiles {
		auditMetadata["clear_files"] = "true"
	}
	s.appendAudit(principal, "app.rebuild", "operation", op.ID, app.TenantID, auditMetadata)
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"operation": sanitizeOperationForAPI(op),
		"build": map[string]any{
			"source_type":        source.Type,
			"image_ref":          source.ImageRef,
			"resolved_image_ref": source.ResolvedImageRef,
			"branch":             source.RepoBranch,
			"upload_id":          source.UploadID,
			"source_dir":         source.SourceDir,
			"dockerfile_path":    source.DockerfilePath,
			"build_context_dir":  source.BuildContextDir,
			"build_strategy":     source.BuildStrategy,
			"compose_service":    source.ComposeService,
			"clear_files":        req.ClearFiles,
		},
	})
}

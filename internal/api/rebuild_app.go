package api

import (
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

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
	if app.Source.Type != model.AppSourceTypeGitHubPublic {
		httpx.WriteError(w, http.StatusBadRequest, "only github-public apps can be rebuilt")
		return
	}
	if strings.TrimSpace(app.Source.RepoURL) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "app source repo_url is missing")
		return
	}

	var req struct {
		Branch          *string `json:"branch"`
		SourceDir       *string `json:"source_dir"`
		DockerfilePath  *string `json:"dockerfile_path"`
		BuildContextDir *string `json:"build_context_dir"`
	}
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
	case model.AppBuildStrategyStaticSite, model.AppBuildStrategyNixpacks:
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

	source, err := buildQueuedGitHubSource(
		app.Source.RepoURL,
		branch,
		sourceDir,
		dockerfilePath,
		buildContextDir,
		buildStrategy,
		strings.TrimSpace(app.Source.ImportProfile),
	)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
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

	s.appendAudit(principal, "app.rebuild", "operation", op.ID, app.TenantID, map[string]string{
		"app_id":         app.ID,
		"repo_url":       source.RepoURL,
		"repo_branch":    source.RepoBranch,
		"build_strategy": source.BuildStrategy,
	})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"operation": sanitizeOperationForAPI(op),
		"build": map[string]any{
			"branch":            source.RepoBranch,
			"source_dir":        source.SourceDir,
			"dockerfile_path":   source.DockerfilePath,
			"build_context_dir": source.BuildContextDir,
			"build_strategy":    source.BuildStrategy,
		},
	})
}

package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
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

	buildStrategy := strings.TrimSpace(app.Source.BuildStrategy)
	if buildStrategy != "" && buildStrategy != model.AppBuildStrategyStaticSite {
		httpx.WriteError(w, http.StatusBadRequest, "only static-site github imports can be rebuilt in this MVP")
		return
	}
	if strings.TrimSpace(app.Source.RepoURL) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "app source repo_url is missing")
		return
	}

	var req struct {
		Branch    *string `json:"branch"`
		SourceDir *string `json:"source_dir"`
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
	sourceDir := strings.TrimSpace(app.Source.SourceDir)
	if req.SourceDir != nil {
		sourceDir = strings.TrimSpace(*req.SourceDir)
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	importResult, err := s.importer.ImportPublicGitHubStaticSite(ctx, sourceimport.GitHubImportRequest{
		RepoURL:          app.Source.RepoURL,
		Branch:           branch,
		SourceDir:        sourceDir,
		RegistryPushBase: s.registryPushBase,
		ImageRepository:  "fugue-apps",
	})
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	source := model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       strings.TrimSpace(app.Source.RepoURL),
		RepoBranch:    importResult.Branch,
		SourceDir:     importResult.SourceDir,
		BuildStrategy: importResult.BuildStrategy,
		CommitSHA:     importResult.CommitSHA,
	}
	spec := app.Spec
	spec.Image = importResult.ImageRef
	if spec.Replicas < 1 {
		spec.Replicas = 1
	}
	if strings.TrimSpace(spec.RuntimeID) == "" {
		spec.RuntimeID = "runtime_managed_shared"
	}

	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
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
		"app_id":     app.ID,
		"repo_url":   source.RepoURL,
		"commit_sha": source.CommitSHA,
		"image_ref":  spec.Image,
	})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"operation": op,
		"build": map[string]any{
			"branch":     source.RepoBranch,
			"source_dir": source.SourceDir,
			"commit_sha": source.CommitSHA,
			"image_ref":  spec.Image,
		},
	})
}

package api

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

var hostnameWords = []string{
	"amber",
	"cedar",
	"comet",
	"ember",
	"falcon",
	"forest",
	"harbor",
	"maple",
	"meadow",
	"nova",
	"ocean",
	"river",
	"solar",
	"stone",
	"timber",
	"violet",
}

func (s *Server) handleImportGitHubApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && (!principal.HasScope("app.write") || !principal.HasScope("app.deploy")) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}

	var req struct {
		TenantID    string `json:"tenant_id"`
		ProjectID   string `json:"project_id"`
		RepoURL     string `json:"repo_url"`
		Branch      string `json:"branch"`
		SourceDir   string `json:"source_dir"`
		Name        string `json:"name"`
		Description string `json:"description"`
		RuntimeID   string `json:"runtime_id"`
		Replicas    int    `json:"replicas"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
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

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	importResult, err := s.importer.ImportPublicGitHubStaticSite(ctx, sourceimport.GitHubImportRequest{
		RepoURL:          req.RepoURL,
		Branch:           req.Branch,
		SourceDir:        req.SourceDir,
		RegistryPushBase: s.registryPushBase,
		ImageRepository:  "fugue-apps",
	})
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

	source := model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       strings.TrimSpace(req.RepoURL),
		RepoBranch:    importResult.Branch,
		SourceDir:     importResult.SourceDir,
		BuildStrategy: importResult.BuildStrategy,
		CommitSHA:     importResult.CommitSHA,
	}

	description := strings.TrimSpace(req.Description)
	if description == "" {
		description = fmt.Sprintf("Imported from %s", strings.TrimSpace(req.RepoURL))
	}

	baseName := strings.TrimSpace(req.Name)
	if baseName == "" {
		baseName = importResult.DefaultAppName
	}
	baseName = normalizeImportBaseName(baseName)

	var app model.App
	for attempt := 0; attempt < 8; attempt++ {
		candidateName, candidateHost := buildImportIdentity(baseName, s.appBaseDomain, attempt)
		if s.isReservedAppHostname(candidateHost) {
			continue
		}
		route := model.AppRoute{
			Hostname:    candidateHost,
			BaseDomain:  s.appBaseDomain,
			PublicURL:   "https://" + candidateHost,
			ServicePort: 80,
		}
		spec := model.AppSpec{
			Image:     importResult.ImageRef,
			Ports:     []int{80},
			Replicas:  replicas,
			RuntimeID: runtimeID,
		}
		app, err = s.store.CreateImportedApp(tenantID, req.ProjectID, candidateName, description, spec, source, route)
		if err == nil {
			break
		}
		if !errors.Is(err, store.ErrConflict) {
			s.writeStoreError(w, err)
			return
		}
	}
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, "failed to allocate unique app name or hostname")
		return
	}

	spec := app.Spec
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

	s.appendAudit(principal, "app.import_github", "app", app.ID, app.TenantID, map[string]string{
		"repo_url":  source.RepoURL,
		"hostname":  app.Route.Hostname,
		"image_ref": app.Spec.Image,
	})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"app":       app,
		"operation": op,
	})
}

func buildImportIdentity(baseName, baseDomain string, attempt int) (string, string) {
	name := baseName
	if attempt > 0 {
		suffix := randomHostnameWord()
		maxBaseLen := 50 - len(suffix) - 1
		if maxBaseLen < 8 {
			maxBaseLen = 8
		}
		name = truncateSlug(baseName, maxBaseLen) + "-" + suffix
	}
	return name, name + "." + baseDomain
}

func normalizeImportBaseName(raw string) string {
	return truncateSlug(model.Slugify(raw), 50)
}

func truncateSlug(value string, maxLen int) string {
	value = model.Slugify(value)
	if maxLen <= 0 || len(value) <= maxLen {
		return value
	}
	value = strings.Trim(value[:maxLen], "-")
	if value == "" {
		return "app"
	}
	return value
}

func randomHostnameWord() string {
	if len(hostnameWords) == 0 {
		return "node"
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(hostnameWords))))
	if err != nil {
		return hostnameWords[0]
	}
	return hostnameWords[n.Int64()]
}

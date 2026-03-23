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

	var req importGitHubRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create app for another tenant")
		return
	}
	if strings.TrimSpace(req.ProjectID) == "" {
		project, err := s.store.EnsureDefaultProject(tenantID)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		req.ProjectID = project.ID
	}

	if strings.TrimSpace(s.registryPushBase) == "" {
		httpx.WriteError(w, http.StatusInternalServerError, "internal registry is not configured")
		return
	}
	if strings.TrimSpace(s.appBaseDomain) == "" {
		httpx.WriteError(w, http.StatusInternalServerError, "app base domain is not configured")
		return
	}
	idempotencyKey, err := resolveIdempotencyKey(r, req.IdempotencyKey)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	replicas := req.Replicas
	if replicas <= 0 {
		replicas = 1
	}
	servicePort := req.ServicePort
	runtimeID := strings.TrimSpace(req.RuntimeID)
	if runtimeID == "" {
		runtimeID = "runtime_managed_shared"
	}
	buildStrategy := normalizeBuildStrategy(req.BuildStrategy)

	profile := resolveImportProfile(req.Profile, req.RepoURL, strings.TrimSpace(req.ConfigContent) != "" || len(req.Files) > 0 || req.Postgres != nil || strings.TrimSpace(req.DockerfilePath) != "")
	var releaseIdempotency bool
	if idempotencyKey != "" {
		requestHash, err := hashImportGitHubRequest(tenantID, req, runtimeID, replicas, profile)
		if err != nil {
			httpx.WriteError(w, http.StatusInternalServerError, err.Error())
			return
		}
		record, fresh, err := s.store.ReserveIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenantID, idempotencyKey, requestHash)
		if err != nil {
			if errors.Is(err, store.ErrIdempotencyMismatch) {
				httpx.WriteError(w, http.StatusConflict, "idempotency key has already been used with a different import request")
				return
			}
			s.writeStoreError(w, err)
			return
		}
		if !fresh {
			if record.AppID != "" && record.OperationID != "" {
				app, appErr := s.store.GetApp(record.AppID)
				op, opErr := s.store.GetOperation(record.OperationID)
				if appErr == nil && opErr == nil {
					httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
						"app":       sanitizeAppForAPI(app),
						"operation": sanitizeOperationForAPI(op),
						"idempotency": map[string]any{
							"key":      idempotencyKey,
							"status":   record.Status,
							"replayed": true,
						},
					})
					return
				}
				httpx.WriteError(w, http.StatusConflict, "idempotency key points to an import result that is no longer available")
				return
			}
			httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
				"idempotency": map[string]any{
					"key":      idempotencyKey,
					"status":   record.Status,
					"replayed": true,
				},
				"request_in_progress": true,
			})
			return
		}
		releaseIdempotency = true
		defer func() {
			if !releaseIdempotency {
				return
			}
			if err := s.store.ReleaseIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenantID, idempotencyKey); err != nil {
				s.log.Printf("release idempotency record failed for tenant=%s key=%s: %v", tenantID, idempotencyKey, err)
			}
		}()
	}
	importResult, source, err := s.importGitHubSource(r.Context(), req.RepoURL, req.Branch, req.SourceDir, req.DockerfilePath, req.BuildContextDir, buildStrategy, profile)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
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

		spec, err := s.buildImportedAppSpec(profile, importResult.BuildStrategy, candidateName, importResult.ImageRef, runtimeID, replicas, effectiveImportServicePort(servicePort, importResult.DetectedPort), req.ConfigContent, req.Files, req.Postgres, importResult.SuggestedEnv)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}

		route := model.AppRoute{
			Hostname:    candidateHost,
			BaseDomain:  s.appBaseDomain,
			PublicURL:   "https://" + candidateHost,
			ServicePort: firstServicePort(spec),
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
	if idempotencyKey != "" {
		releaseIdempotency = false
		if _, err := s.store.CompleteIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenantID, idempotencyKey, app.ID, op.ID); err != nil {
			s.log.Printf("complete idempotency record failed for tenant=%s key=%s app=%s op=%s: %v", tenantID, idempotencyKey, app.ID, op.ID, err)
		}
	}

	s.appendAudit(principal, "app.import_github", "app", app.ID, app.TenantID, map[string]string{
		"repo_url":  source.RepoURL,
		"hostname":  app.Route.Hostname,
		"image_ref": app.Spec.Image,
	})
	response := map[string]any{
		"app":       sanitizeAppForAPI(app),
		"operation": sanitizeOperationForAPI(op),
	}
	if idempotencyKey != "" {
		response["idempotency"] = map[string]any{
			"key":    idempotencyKey,
			"status": model.IdempotencyStatusCompleted,
		}
	}
	httpx.WriteJSON(w, http.StatusAccepted, response)
}

func (s *Server) importGitHubSource(parent context.Context, repoURL, branch, sourceDir, dockerfilePath, buildContextDir, buildStrategy, profile string) (sourceimport.GitHubImportResult, model.AppSource, error) {
	timeout := 10 * time.Minute
	if profile == model.AppImportProfileUniAPI {
		timeout = 25 * time.Minute
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	buildStrategy = normalizeBuildStrategy(buildStrategy)
	if buildStrategy == "" {
		buildStrategy = model.AppBuildStrategyAuto
	}
	if profile == model.AppImportProfileUniAPI {
		switch buildStrategy {
		case model.AppBuildStrategyAuto, model.AppBuildStrategyDockerfile:
			buildStrategy = model.AppBuildStrategyDockerfile
		default:
			return sourceimport.GitHubImportResult{}, model.AppSource{}, fmt.Errorf("profile %q currently requires dockerfile build strategy", profile)
		}
	}

	switch buildStrategy {
	case model.AppBuildStrategyAuto:
		importResult, err := s.importer.ImportPublicGitHubAuto(ctx, sourceimport.GitHubAutoImportRequest{
			RepoURL:          repoURL,
			Branch:           branch,
			SourceDir:        sourceDir,
			DockerfilePath:   dockerfilePath,
			BuildContextDir:  buildContextDir,
			RegistryPushBase: s.registryPushBase,
			ImageRepository:  "fugue-apps",
		})
		if err != nil {
			return sourceimport.GitHubImportResult{}, model.AppSource{}, err
		}
		return importResult, model.AppSource{
			Type:            model.AppSourceTypeGitHubPublic,
			RepoURL:         strings.TrimSpace(repoURL),
			RepoBranch:      importResult.Branch,
			SourceDir:       importResult.SourceDir,
			BuildStrategy:   importResult.BuildStrategy,
			CommitSHA:       importResult.CommitSHA,
			DockerfilePath:  importResult.DockerfilePath,
			BuildContextDir: importResult.BuildContextDir,
			ImportProfile:   profile,
		}, nil
	case model.AppBuildStrategyStaticSite:
		importResult, err := s.importer.ImportPublicGitHubStaticSite(ctx, sourceimport.GitHubImportRequest{
			RepoURL:          repoURL,
			Branch:           branch,
			SourceDir:        sourceDir,
			RegistryPushBase: s.registryPushBase,
			ImageRepository:  "fugue-apps",
		})
		if err != nil {
			return sourceimport.GitHubImportResult{}, model.AppSource{}, err
		}
		return importResult, model.AppSource{
			Type:          model.AppSourceTypeGitHubPublic,
			RepoURL:       strings.TrimSpace(repoURL),
			RepoBranch:    importResult.Branch,
			SourceDir:     importResult.SourceDir,
			BuildStrategy: importResult.BuildStrategy,
			CommitSHA:     importResult.CommitSHA,
			ImportProfile: profile,
		}, nil
	case model.AppBuildStrategyDockerfile:
		importResult, err := s.importer.ImportPublicGitHubDockerfileImage(ctx, sourceimport.GitHubDockerImportRequest{
			RepoURL:          repoURL,
			Branch:           branch,
			DockerfilePath:   dockerfilePath,
			BuildContextDir:  buildContextDir,
			RegistryPushBase: s.registryPushBase,
			ImageRepository:  "fugue-apps",
		})
		if err != nil {
			return sourceimport.GitHubImportResult{}, model.AppSource{}, err
		}
		return importResult, model.AppSource{
			Type:            model.AppSourceTypeGitHubPublic,
			RepoURL:         strings.TrimSpace(repoURL),
			RepoBranch:      importResult.Branch,
			BuildStrategy:   importResult.BuildStrategy,
			CommitSHA:       importResult.CommitSHA,
			DockerfilePath:  importResult.DockerfilePath,
			BuildContextDir: importResult.BuildContextDir,
			ImportProfile:   profile,
		}, nil
	case model.AppBuildStrategyNixpacks:
		importResult, err := s.importer.ImportPublicGitHubNixpacks(ctx, sourceimport.GitHubNixpacksImportRequest{
			RepoURL:          repoURL,
			Branch:           branch,
			SourceDir:        sourceDir,
			RegistryPushBase: s.registryPushBase,
			ImageRepository:  "fugue-apps",
		})
		if err != nil {
			return sourceimport.GitHubImportResult{}, model.AppSource{}, err
		}
		return importResult, model.AppSource{
			Type:          model.AppSourceTypeGitHubPublic,
			RepoURL:       strings.TrimSpace(repoURL),
			RepoBranch:    importResult.Branch,
			SourceDir:     importResult.SourceDir,
			BuildStrategy: importResult.BuildStrategy,
			CommitSHA:     importResult.CommitSHA,
			ImportProfile: profile,
		}, nil
	default:
		return sourceimport.GitHubImportResult{}, model.AppSource{}, fmt.Errorf("unsupported build strategy %q", buildStrategy)
	}
}

func effectiveImportServicePort(requested, detected int) int {
	if requested > 0 {
		return requested
	}
	if detected > 0 {
		return detected
	}
	return 0
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

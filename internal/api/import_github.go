package api

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"

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
	if strings.TrimSpace(req.ProjectID) != "" && req.Project != nil {
		httpx.WriteError(w, http.StatusBadRequest, "project_id and project are mutually exclusive")
		return
	}
	if strings.TrimSpace(principal.ProjectID) != "" {
		if req.Project != nil {
			httpx.WriteError(w, http.StatusForbidden, "workload credentials cannot create projects")
			return
		}
		req.ProjectID = projectIDForPrincipal(principal, req.ProjectID)
		if req.ProjectID != principal.ProjectID {
			httpx.WriteError(w, http.StatusForbidden, "cannot create app for another project")
			return
		}
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
	if req.DeleteMissing && !req.UpdateExisting {
		httpx.WriteError(w, http.StatusBadRequest, "delete_missing requires update_existing")
		return
	}
	networkMode, err := resolveImportNetworkMode(req.NetworkMode)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(s.appBaseDomain) == "" && networkMode == "" {
		httpx.WriteError(w, http.StatusInternalServerError, "app base domain is not configured")
		return
	}
	idempotencyKey, err := resolveIdempotencyKey(r, req.IdempotencyKey)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.DryRun {
		idempotencyKey = ""
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
	var releaseIdempotency bool
	if idempotencyKey != "" {
		requestHash, err := hashImportGitHubRequest(tenantID, req, runtimeID, replicas)
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

	description := strings.TrimSpace(req.Description)
	if description == "" {
		description = fmt.Sprintf("Imported from %s", strings.TrimSpace(req.RepoURL))
	}

	baseName := strings.TrimSpace(req.Name)
	if baseName == "" {
		baseName = repoNameFromGitHubURL(req.RepoURL)
	}
	baseName = normalizeImportBaseName(baseName)
	if baseName == "" {
		baseName = "app"
	}

	var cleanupProject *model.Project
	cleanupApps := make([]model.App, 0, 1)
	cleanupEnabled := true
	defer func() {
		if !cleanupEnabled {
			return
		}
		if err := s.cleanupImportArtifacts(cleanupProject, cleanupApps); err != nil {
			s.log.Printf("cleanup failed after github import error for tenant=%s repo=%s: %v", tenantID, strings.TrimSpace(req.RepoURL), err)
		}
	}()

	var resolvedProject model.Project
	var projectResolved bool
	ensureImportProject := func() (model.Project, error) {
		if projectResolved {
			return resolvedProject, nil
		}

		var (
			project model.Project
			created bool
			err     error
		)
		if req.DryRun {
			project, err = s.previewImportProjectFields(tenantID, req.ProjectID, req.Project)
		} else {
			project, created, err = s.resolveImportProject(tenantID, req)
		}
		if err != nil {
			return model.Project{}, err
		}
		resolvedProject = project
		req.ProjectID = project.ID
		projectResolved = true
		if created {
			projectCopy := project
			cleanupProject = &projectCopy
			s.appendAudit(principal, "project.create", "project", project.ID, project.TenantID, map[string]string{"name": project.Name})
		}
		return resolvedProject, nil
	}

	if shouldInspectFugueManifestImport(req, buildStrategy) {
		manifest, inspectErr := s.importer.InspectGitHubFugueManifest(r.Context(), sourceimport.GitHubFugueManifestInspectRequest{
			RepoURL:       strings.TrimSpace(req.RepoURL),
			Branch:        strings.TrimSpace(req.Branch),
			RepoAuthToken: strings.TrimSpace(req.RepoAuthToken),
		})
		switch {
		case inspectErr == nil:
			if networkMode == model.AppNetworkModeBackground {
				httpx.WriteError(w, http.StatusBadRequest, "network_mode is only supported for single-app imports")
				return
			}
			if hasStartupCommand(req.StartupCommand) {
				httpx.WriteError(w, http.StatusBadRequest, "startup_command is only supported for single-app imports")
				return
			}
			if hasImportedPersistentStorage(req.PersistentStorage) {
				httpx.WriteError(w, http.StatusBadRequest, "persistent_storage is only supported for single-app imports")
				return
			}
			if _, err := ensureImportProject(); err != nil {
				s.writeStoreError(w, err)
				return
			}
			response, primaryApp, primaryOp, err := s.importFugueManifestGitHubStack(principal, tenantID, req, runtimeID, replicas, description, baseName, manifest)
			if err != nil {
				if errors.Is(err, store.ErrConflict) {
					httpx.WriteError(w, http.StatusConflict, err.Error())
					return
				}
				if errors.Is(err, errInvalidComposeImport) {
					httpx.WriteError(w, http.StatusBadRequest, err.Error())
					return
				}
				s.writeStoreError(w, err)
				return
			}
			if idempotencyKey != "" {
				releaseIdempotency = false
				if _, err := s.store.CompleteIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenantID, idempotencyKey, primaryApp.ID, primaryOp.ID); err != nil {
					s.log.Printf("complete idempotency record failed for tenant=%s key=%s app=%s op=%s: %v", tenantID, idempotencyKey, primaryApp.ID, primaryOp.ID, err)
				}
				response["idempotency"] = map[string]any{
					"key":    idempotencyKey,
					"status": model.IdempotencyStatusCompleted,
				}
			}
			cleanupEnabled = false
			httpx.WriteJSON(w, http.StatusAccepted, response)
			return
		case inspectErr != nil && !errors.Is(inspectErr, sourceimport.ErrFugueManifestNotFound):
			httpx.WriteError(w, http.StatusBadRequest, inspectErr.Error())
			return
		}
	}

	if shouldInspectComposeImport(req, buildStrategy) {
		stack, inspectErr := s.importer.InspectGitHubCompose(r.Context(), sourceimport.GitHubComposeInspectRequest{
			RepoURL:       strings.TrimSpace(req.RepoURL),
			Branch:        strings.TrimSpace(req.Branch),
			RepoAuthToken: strings.TrimSpace(req.RepoAuthToken),
		})
		switch {
		case inspectErr == nil:
			if networkMode == model.AppNetworkModeBackground {
				httpx.WriteError(w, http.StatusBadRequest, "network_mode is only supported for single-app imports")
				return
			}
			if hasStartupCommand(req.StartupCommand) {
				httpx.WriteError(w, http.StatusBadRequest, "startup_command is only supported for single-app imports")
				return
			}
			if hasImportedPersistentStorage(req.PersistentStorage) {
				httpx.WriteError(w, http.StatusBadRequest, "persistent_storage is only supported for single-app imports")
				return
			}
			if _, err := ensureImportProject(); err != nil {
				s.writeStoreError(w, err)
				return
			}
			response, primaryApp, primaryOp, err := s.importComposeGitHubStack(principal, tenantID, req, runtimeID, replicas, description, baseName, stack)
			if err != nil {
				if errors.Is(err, store.ErrConflict) {
					httpx.WriteError(w, http.StatusConflict, err.Error())
					return
				}
				if errors.Is(err, errInvalidComposeImport) {
					httpx.WriteError(w, http.StatusBadRequest, err.Error())
					return
				}
				s.writeStoreError(w, err)
				return
			}
			if idempotencyKey != "" {
				releaseIdempotency = false
				if _, err := s.store.CompleteIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenantID, idempotencyKey, primaryApp.ID, primaryOp.ID); err != nil {
					s.log.Printf("complete idempotency record failed for tenant=%s key=%s app=%s op=%s: %v", tenantID, idempotencyKey, primaryApp.ID, primaryOp.ID, err)
				}
				response["idempotency"] = map[string]any{
					"key":    idempotencyKey,
					"status": model.IdempotencyStatusCompleted,
				}
			}
			cleanupEnabled = false
			httpx.WriteJSON(w, http.StatusAccepted, response)
			return
		case inspectErr != nil && !errors.Is(inspectErr, sourceimport.ErrComposeNotFound):
			httpx.WriteError(w, http.StatusBadRequest, inspectErr.Error())
			return
		}
	}

	source, err := buildQueuedGitHubSource(
		req.RepoURL,
		req.RepoVisibility,
		req.RepoAuthToken,
		req.Branch,
		req.SourceDir,
		req.DockerfilePath,
		req.BuildContextDir,
		buildStrategy,
		"",
		"",
	)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.UpdateExisting || req.DeleteMissing || req.DryRun {
		httpx.WriteError(w, http.StatusBadRequest, "update_existing, delete_missing, and dry_run are only supported for topology imports")
		return
	}
	if _, err := ensureImportProject(); err != nil {
		s.writeStoreError(w, err)
		return
	}

	var app model.App
	for attempt := 0; attempt < 8; attempt++ {
		candidateName, candidateHost := buildImportIdentity(baseName, s.appBaseDomain, attempt)
		if s.isReservedAppHostname(candidateHost) {
			continue
		}

		spec, err := s.buildImportedAppSpec(source.BuildStrategy, candidateName, "", runtimeID, replicas, effectiveImportServicePort(servicePort, 0), req.ConfigContent, req.Files, req.PersistentStorage, req.Postgres, req.Env)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		applyStartupCommand(&spec, req.StartupCommand)
		applyImportedNetworkMode(&spec, networkMode)

		route := model.AppRoute{}
		if model.AppManagedRouteEnabled(spec) {
			route = model.AppRoute{
				Hostname:    candidateHost,
				BaseDomain:  s.appBaseDomain,
				PublicURL:   "https://" + candidateHost,
				ServicePort: firstServicePort(spec),
			}
		}
		app, err = s.store.CreateImportedApp(tenantID, req.ProjectID, candidateName, description, spec, source, route)
		if err == nil {
			cleanupApps = append(cleanupApps, app)
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

	spec := cloneAppSpec(app.Spec)
	desiredSource := source
	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeImport,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		DesiredSpec:     &spec,
		DesiredSource:   &desiredSource,
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
		"repo_url":       source.RepoURL,
		"hostname":       app.Route.Hostname,
		"build_strategy": source.BuildStrategy,
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
	cleanupEnabled = false
	httpx.WriteJSON(w, http.StatusAccepted, response)
}

func (s *Server) resolveImportProject(tenantID string, req importGitHubRequest) (model.Project, bool, error) {
	return s.resolveImportProjectFields(tenantID, req.ProjectID, req.Project)
}

func (s *Server) resolveImportProjectFields(tenantID, projectID string, project *importProjectRequest) (model.Project, bool, error) {
	projectID = strings.TrimSpace(projectID)
	switch {
	case projectID != "":
		project, err := s.store.GetProject(projectID)
		if err != nil {
			return model.Project{}, false, err
		}
		if project.TenantID != tenantID {
			return model.Project{}, false, store.ErrNotFound
		}
		return project, false, nil
	case project != nil:
		project, err := s.store.CreateProject(tenantID, project.Name, project.Description)
		return project, err == nil, err
	default:
		return s.store.EnsureDefaultProjectWithStatus(tenantID)
	}
}

func (s *Server) previewImportProjectFields(tenantID, projectID string, project *importProjectRequest) (model.Project, error) {
	projectID = strings.TrimSpace(projectID)
	switch {
	case projectID != "":
		existing, err := s.store.GetProject(projectID)
		if err != nil {
			return model.Project{}, err
		}
		if existing.TenantID != tenantID {
			return model.Project{}, store.ErrNotFound
		}
		return existing, nil
	case project != nil:
		existing, ok, err := s.findProjectPreviewByName(tenantID, project.Name)
		if err != nil {
			return model.Project{}, err
		}
		if ok {
			return existing, nil
		}
		return model.Project{
			TenantID:    tenantID,
			Name:        strings.TrimSpace(project.Name),
			Slug:        model.Slugify(project.Name),
			Description: strings.TrimSpace(project.Description),
		}, nil
	default:
		existing, ok, err := s.findProjectPreviewByName(tenantID, "default")
		if err != nil {
			return model.Project{}, err
		}
		if ok {
			return existing, nil
		}
		return model.Project{TenantID: tenantID, Name: "default", Slug: "default"}, nil
	}
}

func (s *Server) findProjectPreviewByName(tenantID, raw string) (model.Project, bool, error) {
	projects, err := s.store.ListProjects(tenantID)
	if err != nil {
		return model.Project{}, false, err
	}
	want := model.Slugify(strings.TrimSpace(raw))
	for _, project := range projects {
		if strings.EqualFold(strings.TrimSpace(project.Name), strings.TrimSpace(raw)) || strings.EqualFold(strings.TrimSpace(project.Slug), want) {
			return project, true, nil
		}
	}
	return model.Project{}, false, nil
}

func (s *Server) cleanupImportArtifacts(project *model.Project, apps []model.App) error {
	var errs []error
	if err := s.rollbackImportedApps(apps); err != nil {
		errs = append(errs, err)
	}
	if project != nil {
		if _, err := s.store.DeleteProject(project.ID); err != nil && !errors.Is(err, store.ErrNotFound) {
			errs = append(errs, fmt.Errorf("delete project %s: %w", project.ID, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Server) rollbackImportedApps(apps []model.App) error {
	var errs []error
	seen := make(map[string]struct{}, len(apps))
	for index := len(apps) - 1; index >= 0; index-- {
		appID := strings.TrimSpace(apps[index].ID)
		if appID == "" {
			continue
		}
		if _, ok := seen[appID]; ok {
			continue
		}
		seen[appID] = struct{}{}
		if _, err := s.store.PurgeApp(appID); err != nil && !errors.Is(err, store.ErrNotFound) {
			errs = append(errs, fmt.Errorf("purge app %s: %w", appID, err))
		}
	}
	return errors.Join(errs...)
}

func normalizeGitHubRepoVisibility(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "public", model.AppSourceTypeGitHubPublic:
		return "public"
	case "private", model.AppSourceTypeGitHubPrivate:
		return "private"
	default:
		return ""
	}
}

func buildQueuedGitHubSource(repoURL, repoVisibility, repoAuthToken, branch, sourceDir, dockerfilePath, buildContextDir, buildStrategy, imageNameSuffix, composeService string) (model.AppSource, error) {
	buildStrategy = normalizeBuildStrategy(buildStrategy)
	switch buildStrategy {
	case model.AppBuildStrategyAuto, model.AppBuildStrategyStaticSite, model.AppBuildStrategyDockerfile, model.AppBuildStrategyBuildpacks, model.AppBuildStrategyNixpacks:
	default:
		return model.AppSource{}, fmt.Errorf("unsupported build strategy %q", buildStrategy)
	}
	repoURL = strings.TrimSpace(repoURL)
	if repoURL == "" {
		return model.AppSource{}, fmt.Errorf("repo_url is required")
	}
	repoAuthToken = strings.TrimSpace(repoAuthToken)
	normalizedVisibility := normalizeGitHubRepoVisibility(repoVisibility)
	if normalizedVisibility == "" {
		if repoAuthToken != "" {
			normalizedVisibility = "private"
		} else {
			normalizedVisibility = "public"
		}
	}
	if normalizedVisibility == "private" && repoAuthToken == "" {
		return model.AppSource{}, fmt.Errorf("repo_auth_token is required for private GitHub repositories")
	}

	source := model.AppSource{
		Type:            model.ResolveGitHubAppSourceType("", repoAuthToken != ""),
		RepoURL:         repoURL,
		RepoBranch:      strings.TrimSpace(branch),
		RepoAuthToken:   repoAuthToken,
		SourceDir:       strings.TrimSpace(sourceDir),
		BuildStrategy:   buildStrategy,
		DockerfilePath:  strings.TrimSpace(dockerfilePath),
		BuildContextDir: strings.TrimSpace(buildContextDir),
		ImageNameSuffix: strings.TrimSpace(imageNameSuffix),
		ComposeService:  strings.TrimSpace(composeService),
	}
	if normalizedVisibility == "public" {
		source.Type = model.AppSourceTypeGitHubPublic
		source.RepoAuthToken = ""
	}
	switch buildStrategy {
	case model.AppBuildStrategyStaticSite, model.AppBuildStrategyBuildpacks, model.AppBuildStrategyNixpacks:
		source.DockerfilePath = ""
		source.BuildContextDir = ""
	case model.AppBuildStrategyDockerfile:
		source.SourceDir = ""
	}
	return source, nil
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

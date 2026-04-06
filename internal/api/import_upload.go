package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

const (
	maxSourceUploadArchiveBytes = 128 << 20
	multipartFormMemoryBytes    = 32 << 20
)

type importUploadRequest struct {
	AppID             string                          `json:"app_id"`
	TenantID          string                          `json:"tenant_id"`
	ProjectID         string                          `json:"project_id"`
	Project           *importProjectRequest           `json:"project,omitempty"`
	SourceDir         string                          `json:"source_dir"`
	Name              string                          `json:"name"`
	Description       string                          `json:"description"`
	BuildStrategy     string                          `json:"build_strategy"`
	RuntimeID         string                          `json:"runtime_id"`
	Replicas          int                             `json:"replicas"`
	ServicePort       int                             `json:"service_port"`
	DockerfilePath    string                          `json:"dockerfile_path"`
	BuildContextDir   string                          `json:"build_context_dir"`
	Env               map[string]string               `json:"env"`
	ConfigContent     string                          `json:"config_content"`
	Files             []model.AppFile                 `json:"files"`
	StartupCommand    *string                         `json:"startup_command,omitempty"`
	PersistentStorage *model.AppPersistentStorageSpec `json:"persistent_storage,omitempty"`
	Postgres          *model.AppPostgresSpec          `json:"postgres"`
}

func (s *Server) handleImportUploadApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)

	r.Body = http.MaxBytesReader(w, r.Body, maxSourceUploadArchiveBytes+multipartFormMemoryBytes)
	if err := r.ParseMultipartForm(multipartFormMemoryBytes); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, fmt.Sprintf("parse multipart form: %v", err))
		return
	}
	defer func() {
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
	}()

	req, archiveHeader, archiveBytes, err := decodeImportUploadMultipart(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.ProjectID) != "" && req.Project != nil {
		httpx.WriteError(w, http.StatusBadRequest, "project_id and project are mutually exclusive")
		return
	}

	buildStrategy := normalizeBuildStrategy(req.BuildStrategy)
	sourceFileName := archiveHeader.Filename
	if strings.TrimSpace(sourceFileName) == "" {
		sourceFileName = "source.tgz"
	}

	if strings.TrimSpace(req.AppID) != "" {
		if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
			httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
			return
		}

		app, err := s.store.GetApp(strings.TrimSpace(req.AppID))
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if !principal.IsPlatformAdmin() && app.TenantID != principal.TenantID {
			httpx.WriteError(w, http.StatusForbidden, "cannot deploy app for another tenant")
			return
		}

		upload, err := s.store.CreateSourceUpload(app.TenantID, sourceFileName, archiveHeader.Header.Get("Content-Type"), archiveBytes)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		source, err := buildQueuedUploadSource(upload, req.SourceDir, req.DockerfilePath, req.BuildContextDir, buildStrategy, "", "")
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}

		spec := cloneAppSpec(app.Spec)
		if runtimeID := strings.TrimSpace(req.RuntimeID); runtimeID != "" {
			spec.RuntimeID = runtimeID
		}
		if req.Replicas > 0 {
			spec.Replicas = req.Replicas
		}
		if req.ServicePort > 0 {
			spec.Ports = []int{req.ServicePort}
		}
		if req.Env != nil {
			env, err := normalizeImportedEnv(req.Env)
			if err != nil {
				httpx.WriteError(w, http.StatusBadRequest, err.Error())
				return
			}
			spec.Env = env
		}
		if req.PersistentStorage != nil {
			normalizedPersistentStorage, err := normalizeImportedPersistentStorage(req.PersistentStorage, spec.Files)
			if err != nil {
				httpx.WriteError(w, http.StatusBadRequest, err.Error())
				return
			}
			spec.PersistentStorage = normalizedPersistentStorage
		}
		applyStartupCommand(&spec, req.StartupCommand)

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

		s.appendAudit(principal, "app.import_upload", "app", app.ID, app.TenantID, map[string]string{
			"upload_id":      upload.ID,
			"archive_sha256": upload.SHA256,
			"build_strategy": source.BuildStrategy,
		})
		httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
			"app":       sanitizeAppForAPI(app),
			"operation": sanitizeOperationForAPI(op),
		})
		return
	}

	if !principal.IsPlatformAdmin() && (!principal.HasScope("app.write") || !principal.HasScope("app.deploy")) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}

	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create app for another tenant")
		return
	}
	if strings.TrimSpace(s.appBaseDomain) == "" {
		httpx.WriteError(w, http.StatusInternalServerError, "app base domain is not configured")
		return
	}

	upload, err := s.store.CreateSourceUpload(tenantID, sourceFileName, archiveHeader.Header.Get("Content-Type"), archiveBytes)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	description := strings.TrimSpace(req.Description)
	if description == "" {
		description = fmt.Sprintf("Uploaded from %s", sourceFileName)
	}
	baseName := normalizeImportBaseName(strings.TrimSpace(req.Name))
	if baseName == "" {
		baseName = normalizeImportBaseName(uploadSourceBaseName(sourceFileName))
	}
	if baseName == "" {
		baseName = "app"
	}

	replicas := req.Replicas
	if replicas <= 0 {
		replicas = 1
	}
	runtimeID := strings.TrimSpace(req.RuntimeID)
	if runtimeID == "" {
		runtimeID = "runtime_managed_shared"
	}

	if shouldInspectUploadTopologyImport(req, buildStrategy) {
		topology, inspectErr := s.importer.InspectUploadedImportableTopology(r.Context(), sourceimport.UploadTopologyInspectRequest{
			ArchiveFilename:  sourceFileName,
			ArchiveSHA256:    upload.SHA256,
			ArchiveSizeBytes: upload.SizeBytes,
			ArchiveData:      archiveBytes,
			AppName:          baseName,
		})
		switch {
		case inspectErr == nil:
			if hasStartupCommand(req.StartupCommand) {
				httpx.WriteError(w, http.StatusBadRequest, "startup_command is only supported for single-app imports")
				return
			}
			if hasImportedPersistentStorage(req.PersistentStorage) {
				httpx.WriteError(w, http.StatusBadRequest, "persistent_storage is only supported for single-app imports")
				return
			}
			project, created, err := s.resolveImportProjectFields(tenantID, req.ProjectID, req.Project)
			if err != nil {
				s.writeStoreError(w, err)
				return
			}
			req.ProjectID = project.ID
			var cleanupProject *model.Project
			if created {
				projectCopy := project
				cleanupProject = &projectCopy
				s.appendAudit(principal, "project.create", "project", project.ID, project.TenantID, map[string]string{"name": project.Name})
			}
			cleanupEnabled := true
			defer func() {
				if !cleanupEnabled {
					return
				}
				if err := s.cleanupImportArtifacts(cleanupProject, nil); err != nil {
					s.log.Printf("cleanup failed after upload topology import error for tenant=%s upload=%s: %v", tenantID, upload.ID, err)
				}
			}()

			result, err := s.importResolvedUploadTopology(principal, tenantID, req, upload, runtimeID, replicas, description, baseName, topology)
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
			cleanupEnabled = false
			httpx.WriteJSON(w, http.StatusAccepted, uploadTopologyImportResponse(topology, result))
			return
		case inspectErr != nil && !errors.Is(inspectErr, sourceimport.ErrSourceTopologyNotFound):
			httpx.WriteError(w, http.StatusBadRequest, inspectErr.Error())
			return
		}
	}

	source, err := buildQueuedUploadSource(upload, req.SourceDir, req.DockerfilePath, req.BuildContextDir, buildStrategy, "", "")
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	project, _, err := s.resolveImportProjectFields(tenantID, req.ProjectID, req.Project)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	var app model.App
	for attempt := 0; attempt < 8; attempt++ {
		candidateName, candidateHost := buildImportIdentity(baseName, s.appBaseDomain, attempt)
		if s.isReservedAppHostname(candidateHost) {
			continue
		}

		spec, err := s.buildImportedAppSpec(source.BuildStrategy, candidateName, "", runtimeID, replicas, effectiveImportServicePort(req.ServicePort, 0), req.ConfigContent, req.Files, req.PersistentStorage, req.Postgres, req.Env)
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
			break
		}
		if !errors.Is(err, store.ErrConflict) {
			s.writeStoreError(w, err)
			return
		}
	}
	if strings.TrimSpace(app.ID) == "" {
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

	s.appendAudit(principal, "app.import_upload", "app", app.ID, app.TenantID, map[string]string{
		"upload_id":      upload.ID,
		"archive_sha256": upload.SHA256,
		"build_strategy": source.BuildStrategy,
		"hostname":       app.Route.Hostname,
	})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"app":       sanitizeAppForAPI(app),
		"operation": sanitizeOperationForAPI(op),
	})
}

func shouldInspectUploadTopologyImport(req importUploadRequest, buildStrategy string) bool {
	if strings.TrimSpace(req.AppID) != "" {
		return false
	}
	if normalizeBuildStrategy(buildStrategy) != model.AppBuildStrategyAuto {
		return false
	}
	if strings.TrimSpace(req.SourceDir) != "" || strings.TrimSpace(req.DockerfilePath) != "" || strings.TrimSpace(req.BuildContextDir) != "" {
		return false
	}
	if strings.TrimSpace(req.ConfigContent) != "" || len(req.Files) > 0 || req.Postgres != nil {
		return false
	}
	return true
}

func uploadTopologyImportResponse(topology sourceimport.NormalizedTopology, result importedGitHubTopology) map[string]any {
	response := map[string]any{
		"app":        sanitizeAppForAPI(result.PrimaryApp),
		"operation":  sanitizeOperationForAPI(result.PrimaryOp),
		"apps":       sanitizeAppsForAPI(result.Apps),
		"operations": sanitizeOperationsForAPI(result.Operations),
	}
	switch strings.TrimSpace(topology.SourceKind) {
	case sourceimport.TopologySourceKindFugue:
		response["fugue_manifest"] = map[string]any{
			"manifest_path":    topology.SourcePath,
			"primary_service":  result.PrimaryService,
			"services":         result.ServiceDetails,
			"warnings":         result.Warnings,
			"inference_report": result.InferenceReport,
		}
	default:
		response["compose_stack"] = map[string]any{
			"compose_path":     topology.SourcePath,
			"primary_service":  result.PrimaryService,
			"services":         result.ServiceDetails,
			"warnings":         result.Warnings,
			"inference_report": result.InferenceReport,
		}
	}
	return response
}

func (s *Server) handleGetSourceUploadArchive(w http.ResponseWriter, r *http.Request) {
	upload, archiveBytes, err := s.store.GetSourceUploadArchiveByToken(r.PathValue("id"), strings.TrimSpace(r.URL.Query().Get("download_token")))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	contentType := strings.TrimSpace(upload.ContentType)
	if contentType == "" {
		contentType = "application/gzip"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(archiveBytes)))
	if strings.TrimSpace(upload.Filename) != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", upload.Filename))
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(archiveBytes)
}

func decodeImportUploadMultipart(r *http.Request) (importUploadRequest, *multipart.FileHeader, []byte, error) {
	var req importUploadRequest
	rawRequest := strings.TrimSpace(r.FormValue("request"))
	if rawRequest == "" {
		return importUploadRequest{}, nil, nil, fmt.Errorf("multipart field request is required")
	}
	decoder := json.NewDecoder(strings.NewReader(rawRequest))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return importUploadRequest{}, nil, nil, fmt.Errorf("decode request field: %w", err)
	}
	if decoder.More() {
		return importUploadRequest{}, nil, nil, fmt.Errorf("request field must contain a single JSON object")
	}

	archiveFile, archiveHeader, err := r.FormFile("archive")
	if err != nil {
		return importUploadRequest{}, nil, nil, fmt.Errorf("multipart file archive is required")
	}
	defer archiveFile.Close()

	if err := validateUploadArchiveName(archiveHeader.Filename); err != nil {
		return importUploadRequest{}, nil, nil, err
	}
	archiveBytes, err := io.ReadAll(io.LimitReader(archiveFile, maxSourceUploadArchiveBytes+1))
	if err != nil {
		return importUploadRequest{}, nil, nil, fmt.Errorf("read archive: %w", err)
	}
	if len(archiveBytes) == 0 {
		return importUploadRequest{}, nil, nil, fmt.Errorf("archive is empty")
	}
	if len(archiveBytes) > maxSourceUploadArchiveBytes {
		return importUploadRequest{}, nil, nil, fmt.Errorf("archive exceeds %d bytes", maxSourceUploadArchiveBytes)
	}
	if !bytes.HasPrefix(archiveBytes, []byte{0x1f, 0x8b}) {
		return importUploadRequest{}, nil, nil, fmt.Errorf("archive must be a .tgz or .tar.gz file")
	}
	return req, archiveHeader, archiveBytes, nil
}

func validateUploadArchiveName(name string) error {
	lower := strings.ToLower(strings.TrimSpace(name))
	switch {
	case strings.HasSuffix(lower, ".tgz"), strings.HasSuffix(lower, ".tar.gz"):
		return nil
	default:
		return fmt.Errorf("archive filename must end with .tgz or .tar.gz")
	}
}

func buildQueuedUploadSource(upload model.SourceUpload, sourceDir, dockerfilePath, buildContextDir, buildStrategy, imageNameSuffix, composeService string) (model.AppSource, error) {
	buildStrategy = normalizeBuildStrategy(buildStrategy)
	switch buildStrategy {
	case model.AppBuildStrategyAuto, model.AppBuildStrategyStaticSite, model.AppBuildStrategyDockerfile, model.AppBuildStrategyBuildpacks, model.AppBuildStrategyNixpacks:
	default:
		return model.AppSource{}, fmt.Errorf("unsupported build strategy %q", buildStrategy)
	}
	if strings.TrimSpace(upload.ID) == "" {
		return model.AppSource{}, fmt.Errorf("upload_id is required")
	}
	source := model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         strings.TrimSpace(upload.ID),
		UploadFilename:   strings.TrimSpace(upload.Filename),
		ArchiveSHA256:    strings.TrimSpace(upload.SHA256),
		ArchiveSizeBytes: upload.SizeBytes,
		SourceDir:        strings.TrimSpace(sourceDir),
		BuildStrategy:    buildStrategy,
		CommitSHA:        strings.TrimSpace(upload.SHA256),
		DockerfilePath:   strings.TrimSpace(dockerfilePath),
		BuildContextDir:  strings.TrimSpace(buildContextDir),
		ImageNameSuffix:  strings.TrimSpace(imageNameSuffix),
		ComposeService:   strings.TrimSpace(composeService),
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

func uploadSourceBaseName(filename string) string {
	name := strings.TrimSpace(filename)
	name = strings.TrimSuffix(name, ".tar.gz")
	name = strings.TrimSuffix(name, ".tgz")
	name = strings.TrimSuffix(name, filepath.Ext(name))
	return name
}

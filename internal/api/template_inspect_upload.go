package api

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/sourceimport"
)

func (s *Server) handleInspectUploadTemplate(w http.ResponseWriter, r *http.Request) {
	_ = mustPrincipal(r)

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

	baseName := resolveUploadImportBaseName(req.Name, archiveHeader.Filename)

	archiveSHA256 := sha256.Sum256(archiveBytes)
	response := map[string]any{
		"upload": map[string]any{
			"archive_filename":   archiveHeader.Filename,
			"archive_sha256":     hex.EncodeToString(archiveSHA256[:]),
			"archive_size_bytes": int64(len(archiveBytes)),
			"default_app_name":   baseName,
			"source_kind":        "",
			"source_path":        "",
		},
		"fugue_manifest": nil,
		"compose_stack":  nil,
	}

	topology, err := s.importer.InspectUploadedImportableTopology(r.Context(), sourceimport.UploadTopologyInspectRequest{
		ArchiveFilename:  archiveHeader.Filename,
		ArchiveSHA256:    hex.EncodeToString(archiveSHA256[:]),
		ArchiveSizeBytes: int64(len(archiveBytes)),
		ArchiveData:      archiveBytes,
		AppName:          baseName,
	})
	switch {
	case err == nil:
	case errors.Is(err, sourceimport.ErrSourceTopologyNotFound):
		httpx.WriteJSON(w, http.StatusOK, response)
		return
	default:
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	uploadInfo := response["upload"].(map[string]any)
	if value := strings.TrimSpace(topology.DefaultAppName); value != "" {
		uploadInfo["default_app_name"] = value
	}
	uploadInfo["source_kind"] = strings.TrimSpace(topology.SourceKind)
	uploadInfo["source_path"] = strings.TrimSpace(topology.SourcePath)

	switch strings.TrimSpace(topology.SourceKind) {
	case sourceimport.TopologySourceKindFugue:
		response["fugue_manifest"] = sanitizeUploadTemplateManifest(topology)
	case sourceimport.TopologySourceKindCompose:
		response["compose_stack"] = sanitizeUploadTemplateComposeStack(topology)
	}

	httpx.WriteJSON(w, http.StatusOK, response)
}

func sanitizeUploadTemplateManifest(topology sourceimport.NormalizedTopology) map[string]any {
	return map[string]any{
		"manifest_path":    topology.SourcePath,
		"primary_service":  resolveUploadTopologyPrimaryService(topology),
		"services":         sanitizeUploadTemplateServices(topology.Services),
		"warnings":         append([]string(nil), topology.Warnings...),
		"inference_report": append([]sourceimport.TopologyInference(nil), topology.InferenceReport...),
	}
}

func sanitizeUploadTemplateComposeStack(topology sourceimport.NormalizedTopology) map[string]any {
	return map[string]any{
		"compose_path":     topology.SourcePath,
		"primary_service":  resolveUploadTopologyPrimaryService(topology),
		"services":         sanitizeUploadTemplateServices(topology.Services),
		"warnings":         append([]string(nil), topology.Warnings...),
		"inference_report": append([]sourceimport.TopologyInference(nil), topology.InferenceReport...),
	}
}

func sanitizeUploadTemplateServices(services []sourceimport.ComposeService) []map[string]any {
	items := make([]map[string]any, 0, len(services))
	for _, service := range services {
		items = append(items, sanitizeGitHubTemplateService(service))
	}
	return items
}

func resolveUploadTopologyPrimaryService(topology sourceimport.NormalizedTopology) string {
	if primary := strings.TrimSpace(topology.PrimaryService); primary != "" {
		return primary
	}
	service, err := sourceimport.SelectPrimaryTopologyService(topology.Services, "")
	if err != nil {
		return ""
	}
	return service.Name
}

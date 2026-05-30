package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type dataTransferPlanBlob = model.DataTransferPlanBlob

const (
	dataBackendMigrationConcurrency = 16
	dataTransferPlanConcurrency     = 16
	dataTransferPartPlanConcurrency = 16
)

type dataUploadPlanRequest struct {
	Version  string             `json:"version"`
	Message  string             `json:"message"`
	Manifest model.DataManifest `json:"manifest"`
}

type dataUploadPlanResponse struct {
	Workspace       model.DataWorkspace    `json:"workspace"`
	Transfer        dataTransferSummary    `json:"transfer"`
	Blobs           []dataTransferPlanBlob `json:"blobs"`
	BlobsTotal      int                    `json:"blobs_total,omitempty"`
	BlobsOffset     int                    `json:"blobs_offset,omitempty"`
	BlobsLimit      int                    `json:"blobs_limit,omitempty"`
	BlobsNextOffset *int                   `json:"blobs_next_offset,omitempty"`
}

type dataDownloadPlanRequest struct {
	Version string   `json:"version"`
	Assets  []string `json:"assets,omitempty"`
}

type dataDownloadPlanResponse struct {
	Workspace       model.DataWorkspace    `json:"workspace"`
	Snapshot        dataSnapshotSummary    `json:"snapshot"`
	Transfer        dataTransferSummary    `json:"transfer"`
	Manifest        model.DataManifest     `json:"manifest"`
	Blobs           []dataTransferPlanBlob `json:"blobs"`
	BlobsTotal      int                    `json:"blobs_total,omitempty"`
	BlobsOffset     int                    `json:"blobs_offset,omitempty"`
	BlobsLimit      int                    `json:"blobs_limit,omitempty"`
	BlobsNextOffset *int                   `json:"blobs_next_offset,omitempty"`
}

type dataTransferSummary struct {
	ID           string     `json:"id"`
	TenantID     string     `json:"tenant_id,omitempty"`
	WorkspaceID  string     `json:"workspace_id"`
	SnapshotID   string     `json:"snapshot_id,omitempty"`
	Version      string     `json:"version,omitempty"`
	Message      string     `json:"message,omitempty"`
	Direction    string     `json:"direction"`
	Status       string     `json:"status"`
	Source       string     `json:"source,omitempty"`
	Target       string     `json:"target,omitempty"`
	PartSize     int64      `json:"part_size,omitempty"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
	BytesTotal   int64      `json:"bytes_total"`
	BytesDone    int64      `json:"bytes_done"`
	FilesTotal   int        `json:"files_total"`
	FilesDone    int        `json:"files_done"`
	ErrorCode    string     `json:"error_code,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
}

type dataSnapshotSummary struct {
	ID             string     `json:"id"`
	TenantID       string     `json:"tenant_id,omitempty"`
	ProjectID      string     `json:"project_id,omitempty"`
	WorkspaceID    string     `json:"workspace_id"`
	Version        string     `json:"version"`
	Message        string     `json:"message,omitempty"`
	ManifestDigest string     `json:"manifest_digest"`
	AssetCount     int        `json:"asset_count"`
	FileCount      int        `json:"file_count"`
	TotalBytes     int64      `json:"total_bytes"`
	CreatedBy      string     `json:"created_by,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	DeletedAt      *time.Time `json:"deleted_at,omitempty"`
}

type dataTransferBlobPage struct {
	Offset     int
	Limit      int
	Total      int
	NextOffset *int
}

const dataTransferMaxBlobPageLimit = 5000

func summarizeDataTransfer(transfer model.DataTransfer) dataTransferSummary {
	return dataTransferSummary{
		ID:           transfer.ID,
		TenantID:     transfer.TenantID,
		WorkspaceID:  transfer.WorkspaceID,
		SnapshotID:   transfer.SnapshotID,
		Version:      transfer.Version,
		Message:      transfer.Message,
		Direction:    transfer.Direction,
		Status:       transfer.Status,
		Source:       transfer.Source,
		Target:       transfer.Target,
		PartSize:     transfer.PartSize,
		ExpiresAt:    transfer.ExpiresAt,
		BytesTotal:   transfer.BytesTotal,
		BytesDone:    transfer.BytesDone,
		FilesTotal:   transfer.FilesTotal,
		FilesDone:    transfer.FilesDone,
		ErrorCode:    transfer.ErrorCode,
		ErrorMessage: transfer.ErrorMessage,
		CreatedAt:    transfer.CreatedAt,
		UpdatedAt:    transfer.UpdatedAt,
		StartedAt:    transfer.StartedAt,
		FinishedAt:   transfer.FinishedAt,
	}
}

func summarizeDataTransfers(transfers []model.DataTransfer) []dataTransferSummary {
	out := make([]dataTransferSummary, 0, len(transfers))
	for _, transfer := range transfers {
		out = append(out, summarizeDataTransfer(transfer))
	}
	return out
}

func parseDataTransferBlobPage(r *http.Request, total int) (dataTransferBlobPage, error) {
	page := dataTransferBlobPage{Total: total, Limit: total}
	rawLimit := strings.TrimSpace(r.URL.Query().Get("blob_limit"))
	rawOffset := strings.TrimSpace(r.URL.Query().Get("blob_offset"))
	if rawLimit == "" && rawOffset == "" {
		return finalizeDataTransferBlobPage(page), nil
	}
	if rawLimit != "" {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil || limit <= 0 {
			return dataTransferBlobPage{}, fmt.Errorf("blob_limit must be a positive integer")
		}
		if limit > dataTransferMaxBlobPageLimit {
			limit = dataTransferMaxBlobPageLimit
		}
		page.Limit = limit
	}
	if rawOffset != "" {
		offset, err := strconv.Atoi(rawOffset)
		if err != nil || offset < 0 {
			return dataTransferBlobPage{}, fmt.Errorf("blob_offset must be a non-negative integer")
		}
		page.Offset = offset
	}
	return finalizeDataTransferBlobPage(page), nil
}

func dataTransferBlobTotal(transfer model.DataTransfer) int {
	if len(transfer.Manifest.Entries) == 0 {
		return len(transfer.PlanBlobs)
	}
	return countDataManifestBlobs(transfer.Manifest)
}

func countDataManifestBlobs(manifest model.DataManifest) int {
	seen := map[string]struct{}{}
	for _, entry := range manifest.Entries {
		if entry.Kind != model.DataManifestEntryKindFile {
			continue
		}
		digest := strings.TrimSpace(strings.ToLower(entry.SHA256))
		if digest == "" {
			continue
		}
		seen[digest] = struct{}{}
	}
	return len(seen)
}

func finalizeDataTransferBlobPage(page dataTransferBlobPage) dataTransferBlobPage {
	if page.Total < 0 {
		page.Total = 0
	}
	if page.Limit < 0 {
		page.Limit = 0
	}
	if page.Offset < 0 {
		page.Offset = 0
	}
	if page.Offset > page.Total {
		page.Offset = page.Total
	}
	end := page.Offset + page.Limit
	if page.Limit == 0 || end > page.Total {
		end = page.Total
	}
	if end < page.Total {
		next := end
		page.NextOffset = &next
	}
	return page
}

func sliceDataTransferBlobs(blobs []dataTransferPlanBlob, page dataTransferBlobPage) []dataTransferPlanBlob {
	start := page.Offset
	if start > len(blobs) {
		start = len(blobs)
	}
	end := start + page.Limit
	if page.Limit == 0 || end > len(blobs) {
		end = len(blobs)
	}
	return append([]dataTransferPlanBlob(nil), blobs[start:end]...)
}

func summarizeDataSnapshot(snapshot model.DataSnapshot) dataSnapshotSummary {
	return dataSnapshotSummary{
		ID:             snapshot.ID,
		TenantID:       snapshot.TenantID,
		ProjectID:      snapshot.ProjectID,
		WorkspaceID:    snapshot.WorkspaceID,
		Version:        snapshot.Version,
		Message:        snapshot.Message,
		ManifestDigest: snapshot.ManifestDigest,
		AssetCount:     snapshot.AssetCount,
		FileCount:      snapshot.FileCount,
		TotalBytes:     snapshot.TotalBytes,
		CreatedBy:      snapshot.CreatedBy,
		CreatedAt:      snapshot.CreatedAt,
		DeletedAt:      snapshot.DeletedAt,
	}
}

func (s *Server) handleListDataBackends(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	backends, err := s.store.ListDataBackends(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"backends": backends})
}

func (s *Server) handleCreateDataBackend(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.admin") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.admin scope")
		return
	}
	var req struct {
		TenantID        string                       `json:"tenant_id"`
		Name            string                       `json:"name"`
		Provider        string                       `json:"provider"`
		Bucket          string                       `json:"bucket"`
		Region          string                       `json:"region"`
		Endpoint        string                       `json:"endpoint"`
		BaseURL         string                       `json:"base_url"`
		Prefix          string                       `json:"prefix"`
		Credentials     model.DataBackendCredentials `json:"credentials"`
		UseForTenantKey bool                         `json:"use_for_tenant_key"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create data backend for another tenant")
		return
	}
	backend, err := s.store.CreateDataBackend(model.DataBackend{
		TenantID:     tenantID,
		Name:         req.Name,
		Provider:     req.Provider,
		Bucket:       req.Bucket,
		Region:       req.Region,
		Endpoint:     req.Endpoint,
		BaseURL:      req.BaseURL,
		Prefix:       req.Prefix,
		Credentials:  req.Credentials,
		Capabilities: model.DataBackendCapabilitiesForProvider(req.Provider),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.backend.create", "data_backend", backend.ID, tenantID, map[string]string{"provider": backend.Provider, "name": backend.Name})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"backend": backend})
}

func (s *Server) handleGetDataBackend(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	backend, err := s.store.GetDataBackend(r.PathValue("backend_id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"backend": backend})
}

func (s *Server) handleDeleteDataBackend(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.admin") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.admin scope")
		return
	}
	backend, err := s.store.DeleteDataBackend(r.PathValue("backend_id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.backend.delete", "data_backend", backend.ID, backend.TenantID, map[string]string{"name": backend.Name})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"backend": backend, "deleted": true})
}

func (s *Server) handleRotateDataBackendCredentials(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.admin") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.admin scope")
		return
	}
	var req struct {
		Credentials model.DataBackendCredentials `json:"credentials"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	backend, err := s.store.RotateDataBackendCredentials(r.PathValue("backend_id"), principal.TenantID, principal.IsPlatformAdmin(), req.Credentials)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.backend.credentials.rotate", "data_backend", backend.ID, backend.TenantID, map[string]string{"name": backend.Name})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"backend": backend, "rotated": true})
}

func (s *Server) handleListDataWorkspaces(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	tenantID, ok := s.resolveTenantID(principal, r.URL.Query().Get("tenant_id"))
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot list data workspaces for another tenant")
		return
	}
	projectID := projectIDForPrincipal(principal, r.URL.Query().Get("project_id"))
	workspaces, err := s.store.ListDataWorkspaces(tenantID, projectID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"workspaces": workspaces})
}

func (s *Server) handleCreateDataWorkspace(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.write scope")
		return
	}
	var req struct {
		TenantID         string            `json:"tenant_id"`
		ProjectID        string            `json:"project_id"`
		Name             string            `json:"name"`
		DefaultRegion    string            `json:"default_region"`
		StorageBackendID string            `json:"storage_backend_id"`
		QuotaBytes       int64             `json:"quota_bytes"`
		Assets           []model.DataAsset `json:"assets"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create data workspace for another tenant")
		return
	}
	workspace, err := s.store.CreateDataWorkspace(model.DataWorkspace{
		TenantID:         tenantID,
		ProjectID:        projectIDForPrincipal(principal, req.ProjectID),
		Name:             req.Name,
		DefaultRegion:    req.DefaultRegion,
		StorageBackendID: req.StorageBackendID,
		QuotaBytes:       req.QuotaBytes,
		Assets:           req.Assets,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.workspace.create", "data_workspace", workspace.ID, workspace.TenantID, map[string]string{"name": workspace.Name})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"workspace": workspace})
}

func (s *Server) handleGetDataWorkspace(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	latest, _ := s.store.LatestDataSnapshot(workspace.ID)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"workspace": workspace, "latest_snapshot": latest})
}

func (s *Server) handlePatchDataWorkspace(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.write scope")
		return
	}
	var req struct {
		Name             *string            `json:"name"`
		ProjectID        *string            `json:"project_id"`
		DefaultRegion    *string            `json:"default_region"`
		StorageBackendID *string            `json:"storage_backend_id"`
		QuotaBytes       *int64             `json:"quota_bytes"`
		Assets           *[]model.DataAsset `json:"assets"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	updated, err := s.store.UpdateDataWorkspace(workspace.ID, principal.TenantID, principal.IsPlatformAdmin(), store.DataWorkspaceUpdate{
		Name:             req.Name,
		ProjectID:        req.ProjectID,
		DefaultRegion:    req.DefaultRegion,
		StorageBackendID: req.StorageBackendID,
		QuotaBytes:       req.QuotaBytes,
		Assets:           req.Assets,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.workspace.update", "data_workspace", updated.ID, updated.TenantID, map[string]string{"name": updated.Name})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"workspace": updated})
}

func (s *Server) handleDeleteDataWorkspace(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.delete") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.delete scope")
		return
	}
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	deleted, err := s.store.DeleteDataWorkspace(workspace.ID, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.workspace.delete", "data_workspace", deleted.ID, deleted.TenantID, map[string]string{"name": deleted.Name})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"workspace": deleted, "deleted": true})
}

func (s *Server) handleListDataSnapshots(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	snapshots, err := s.store.ListDataSnapshots(workspace.ID, false)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"workspace": workspace, "snapshots": snapshots})
}

func (s *Server) handleCreateDataSnapshot(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.write scope")
		return
	}
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		Version  string             `json:"version"`
		Message  string             `json:"message"`
		Manifest model.DataManifest `json:"manifest"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	snapshot, err := s.store.CreateDataSnapshot(model.DataSnapshot{
		WorkspaceID: workspace.ID,
		Version:     strings.TrimSpace(req.Version),
		Message:     strings.TrimSpace(req.Message),
		Manifest:    req.Manifest,
		CreatedBy:   principal.ActorID,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.snapshot.create", "data_snapshot", snapshot.ID, workspace.TenantID, map[string]string{"workspace": workspace.Name, "version": snapshot.Version})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"workspace": workspace, "snapshot": snapshot})
}

func (s *Server) handleGetDataSnapshot(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	snapshot, err := s.store.GetDataSnapshot(workspace.ID, r.PathValue("snapshot_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"workspace": workspace, "snapshot": snapshot})
}

func (s *Server) handleDeleteDataSnapshot(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.delete") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.delete scope")
		return
	}
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	snapshot, err := s.store.DeleteDataSnapshot(workspace.ID, r.PathValue("snapshot_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.snapshot.delete", "data_snapshot", snapshot.ID, workspace.TenantID, map[string]string{"workspace": workspace.Name, "version": snapshot.Version})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"snapshot": snapshot, "deleted": true})
}

func (s *Server) handlePlanDataUpload(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.write scope")
		return
	}
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	var req dataUploadPlanRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	manifest := model.NormalizeDataManifest(req.Manifest)
	expiresAt := time.Now().UTC().Add(dataPresignTTL())
	transfer, err := s.store.CreateDataTransfer(model.DataTransfer{
		WorkspaceID: workspace.ID,
		Version:     strings.TrimSpace(req.Version),
		Message:     strings.TrimSpace(req.Message),
		Direction:   model.DataTransferDirectionUpload,
		Status:      model.DataTransferStatusPlanned,
		BytesTotal:  manifest.TotalBytes,
		FilesTotal:  manifest.FileCount,
		Source:      "cli",
		Target:      workspace.StorageBackendID,
		Manifest:    manifest,
		PartSize:    dataMultipartPartSize,
		ExpiresAt:   &expiresAt,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	blobs, err := s.dataPlanBlobs(r.Context(), r, workspace, transfer, manifest, true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	transfer.PlanBlobs = s.storedDataTransferPlanBlobs(workspace, transfer.Direction, blobs)
	if transfer, err = s.store.UpdateDataTransfer(transfer); err != nil {
		s.writeStoreError(w, err)
		return
	}
	page, err := parseDataTransferBlobPage(r, len(blobs))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	blobs, responseBlobs, err := s.refreshDataPlanBlobs(r.Context(), r, workspace, transfer, true, page)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	expiresAt = time.Now().UTC().Add(dataPresignTTL())
	transfer.PlanBlobs = s.storedDataTransferPlanBlobs(workspace, transfer.Direction, blobs)
	transfer.ExpiresAt = &expiresAt
	if transfer, err = s.store.UpdateDataTransfer(transfer); err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, dataUploadPlanResponse{
		Workspace:       workspace,
		Transfer:        summarizeDataTransfer(transfer),
		Blobs:           responseBlobs,
		BlobsTotal:      page.Total,
		BlobsOffset:     page.Offset,
		BlobsLimit:      page.Limit,
		BlobsNextOffset: page.NextOffset,
	})
}

func (s *Server) handlePlanDataDownload(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.read scope")
		return
	}
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	var req dataDownloadPlanRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	version := strings.TrimSpace(req.Version)
	if version == "" {
		version = "latest"
	}
	snapshot, err := s.store.GetDataSnapshot(workspace.ID, version)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	manifest := filterDataManifestAssets(snapshot.Manifest, req.Assets)
	expiresAt := time.Now().UTC().Add(dataPresignTTL())
	transfer, err := s.store.CreateDataTransfer(model.DataTransfer{
		WorkspaceID: workspace.ID,
		SnapshotID:  snapshot.ID,
		Version:     snapshot.Version,
		Message:     snapshot.Message,
		Direction:   model.DataTransferDirectionDownload,
		Status:      model.DataTransferStatusPlanned,
		BytesTotal:  manifest.TotalBytes,
		FilesTotal:  manifest.FileCount,
		Source:      workspace.StorageBackendID,
		Target:      "cli",
		Manifest:    manifest,
		PartSize:    dataMultipartPartSize,
		ExpiresAt:   &expiresAt,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	blobs, err := s.dataPlanBlobs(r.Context(), r, workspace, transfer, manifest, false)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	transfer.PlanBlobs = s.storedDataTransferPlanBlobs(workspace, transfer.Direction, blobs)
	if transfer, err = s.store.UpdateDataTransfer(transfer); err != nil {
		s.writeStoreError(w, err)
		return
	}
	page, err := parseDataTransferBlobPage(r, len(blobs))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, dataDownloadPlanResponse{
		Workspace:       workspace,
		Snapshot:        summarizeDataSnapshot(snapshot),
		Transfer:        summarizeDataTransfer(transfer),
		Manifest:        manifest,
		Blobs:           sliceDataTransferBlobs(blobs, page),
		BlobsTotal:      page.Total,
		BlobsOffset:     page.Offset,
		BlobsLimit:      page.Limit,
		BlobsNextOffset: page.NextOffset,
	})
}

func (s *Server) handleCreateDataPrewarm(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.write scope")
		return
	}
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		Version   string   `json:"version"`
		RuntimeID string   `json:"runtime_id"`
		Assets    []string `json:"assets"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	version := strings.TrimSpace(req.Version)
	if version == "" {
		version = "latest"
	}
	snapshot, err := s.store.GetDataSnapshot(workspace.ID, version)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	manifest := filterDataManifestAssets(snapshot.Manifest, req.Assets)
	transfer, err := s.store.CreateDataTransfer(model.DataTransfer{
		WorkspaceID: workspace.ID,
		SnapshotID:  snapshot.ID,
		Version:     snapshot.Version,
		Direction:   model.DataTransferDirectionPrewarm,
		Status:      model.DataTransferStatusPlanned,
		Source:      workspace.StorageBackendID,
		Target:      strings.TrimSpace(req.RuntimeID),
		Manifest:    manifest,
		BytesTotal:  manifest.TotalBytes,
		FilesTotal:  manifest.FileCount,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.prewarm.create", "data_workspace", workspace.ID, workspace.TenantID, map[string]string{"workspace": workspace.Name, "version": snapshot.Version, "runtime_id": strings.TrimSpace(req.RuntimeID)})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"workspace": workspace, "snapshot": summarizeDataSnapshot(snapshot), "transfer": summarizeDataTransfer(transfer)})
}

func (s *Server) handleCompleteDataTransfer(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	var req struct {
		SnapshotID   string             `json:"snapshot_id"`
		Version      string             `json:"version"`
		Message      string             `json:"message"`
		Manifest     model.DataManifest `json:"manifest"`
		BytesDone    int64              `json:"bytes_done"`
		FilesDone    int                `json:"files_done"`
		ErrorCode    string             `json:"error_code"`
		ErrorMessage string             `json:"error_message"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	transfer, err := s.store.GetDataTransfer(r.PathValue("transfer_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	workspace, err := s.store.GetDataWorkspace(transfer.WorkspaceID, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principalAllowsDataWorkspace(principal, workspace) {
		httpx.WriteError(w, http.StatusForbidden, "data workspace is not visible to this tenant")
		return
	}
	now := time.Now().UTC()
	var snapshot model.DataSnapshot
	if req.ErrorMessage == "" && transfer.Direction == model.DataTransferDirectionUpload {
		version := strings.TrimSpace(req.Version)
		snapshot, err = s.store.CreateDataSnapshot(model.DataSnapshot{
			WorkspaceID: workspace.ID,
			Version:     version,
			Message:     req.Message,
			Manifest:    req.Manifest,
			CreatedBy:   principal.ActorID,
		})
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		transfer.SnapshotID = snapshot.ID
	} else if req.ErrorMessage == "" && req.SnapshotID != "" {
		snapshot, _ = s.store.GetDataSnapshot(workspace.ID, req.SnapshotID)
	}
	transfer.BytesDone = req.BytesDone
	transfer.FilesDone = req.FilesDone
	transfer.ErrorCode = req.ErrorCode
	transfer.ErrorMessage = req.ErrorMessage
	transfer.FinishedAt = &now
	if req.ErrorMessage != "" {
		transfer.Status = model.DataTransferStatusFailed
		transfer.PlanBlobs = s.storedDataTransferPlanBlobs(workspace, transfer.Direction, transfer.PlanBlobs)
	} else {
		transfer.Status = model.DataTransferStatusCompleted
		transfer.PlanBlobs = nil
	}
	updated, err := s.store.UpdateDataTransfer(transfer)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.transfer.complete", "data_transfer", updated.ID, workspace.TenantID, map[string]string{"direction": updated.Direction, "status": updated.Status})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"workspace": workspace, "transfer": summarizeDataTransfer(updated), "snapshot": summarizeDataSnapshot(snapshot)})
}

func (s *Server) handleListDataTransfers(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	transfers, err := s.store.ListDataTransfers(principal.TenantID, r.URL.Query().Get("workspace_id"), principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"transfers": summarizeDataTransfers(transfers)})
}

func (s *Server) handleGetDataTransfer(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	transfer, err := s.store.GetDataTransfer(r.PathValue("transfer_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && transfer.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "data transfer is not visible to this tenant")
		return
	}
	summary, err := parseBoolQuery(r.URL.Query().Get("summary"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if summary {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"transfer": summarizeDataTransfer(transfer)})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"transfer": transfer})
}

func (s *Server) handleCancelDataTransfer(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	transfer, err := s.store.GetDataTransfer(r.PathValue("transfer_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && transfer.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "data transfer is not visible to this tenant")
		return
	}
	transfer, err = s.store.CancelDataTransfer(transfer.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"transfer": summarizeDataTransfer(transfer)})
}

func (s *Server) handleSweepDataWorkspaceGC(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.delete") && !principal.HasScope("data.admin") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.delete scope")
		return
	}
	workspace, ok := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !ok {
		return
	}
	var req struct {
		DryRun        bool `json:"dry_run"`
		Confirm       bool `json:"confirm"`
		RetentionDays int  `json:"retention_days"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.RetentionDays <= 0 {
		req.RetentionDays = 7
	}
	dryRun := req.DryRun || !req.Confirm
	result, err := s.sweepDataWorkspaceGC(r.Context(), workspace, req.RetentionDays, dryRun)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.gc.sweep", "data_workspace", workspace.ID, workspace.TenantID, map[string]string{"workspace": workspace.Name, "dry_run": strconv.FormatBool(dryRun)})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"workspace": workspace, "gc": result})
}

func (s *Server) handleCreateDataBackendMigration(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.admin") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.admin scope")
		return
	}
	workspace, ok := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !ok {
		return
	}
	var req struct {
		TargetBackendID string `json:"target_backend_id"`
		DryRun          bool   `json:"dry_run"`
		Cutover         bool   `json:"cutover"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.TargetBackendID) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "target_backend_id is required")
		return
	}
	transfer, err := s.migrateDataWorkspaceBackend(r.Context(), workspace, strings.TrimSpace(req.TargetBackendID), req.DryRun, req.Cutover)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	updatedWorkspace := workspace
	if req.Cutover && !req.DryRun && transfer.Status == model.DataTransferStatusCompleted {
		if refreshed, err := s.store.GetDataWorkspace(workspace.ID, workspace.TenantID, true); err == nil {
			updatedWorkspace = refreshed
		}
	}
	s.appendAudit(principal, "data.backend.migrate", "data_workspace", workspace.ID, workspace.TenantID, map[string]string{"workspace": workspace.Name, "target_backend_id": req.TargetBackendID, "dry_run": strconv.FormatBool(req.DryRun), "cutover": strconv.FormatBool(req.Cutover)})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"workspace": updatedWorkspace, "transfer": summarizeDataTransfer(transfer)})
}

func (s *Server) handleRollbackDataBackendMigration(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.admin") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.admin scope")
		return
	}
	workspace, ok := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !ok {
		return
	}
	updatedWorkspace, rollbackTransfer, err := s.rollbackDataWorkspaceBackendMigration(r.Context(), workspace, strings.TrimSpace(r.PathValue("transfer_id")))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.backend.migration.rollback", "data_workspace", workspace.ID, workspace.TenantID, map[string]string{"workspace": workspace.Name, "migration_transfer_id": r.PathValue("transfer_id"), "target_backend_id": updatedWorkspace.StorageBackendID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"workspace": updatedWorkspace, "transfer": summarizeDataTransfer(rollbackTransfer)})
}

func (s *Server) handleRefreshDataTransferAuthorization(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	transfer, workspace, ok := s.loadAuthorizedDataTransfer(w, r, principal)
	if !ok {
		return
	}
	upload := transfer.Direction == model.DataTransferDirectionUpload
	page, err := parseDataTransferBlobPage(r, dataTransferBlobTotal(transfer))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	blobs, responseBlobs, err := s.refreshDataPlanBlobs(r.Context(), r, workspace, transfer, upload, page)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	expiresAt := time.Now().UTC().Add(dataPresignTTL())
	transfer.PlanBlobs = s.storedDataTransferPlanBlobs(workspace, transfer.Direction, blobs)
	transfer.ExpiresAt = &expiresAt
	transfer.Status = model.DataTransferStatusRunning
	transfer, err = s.store.UpdateDataTransfer(transfer)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	resp := map[string]any{
		"workspace":    workspace,
		"transfer":     summarizeDataTransfer(transfer),
		"blobs":        responseBlobs,
		"blobs_total":  page.Total,
		"blobs_offset": page.Offset,
		"blobs_limit":  page.Limit,
	}
	if page.NextOffset != nil {
		resp["blobs_next_offset"] = *page.NextOffset
	}
	if transfer.Direction == model.DataTransferDirectionDownload {
		resp["manifest"] = transfer.Manifest
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) handleCheckpointDataTransfer(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	transfer, workspace, ok := s.loadAuthorizedDataTransfer(w, r, principal)
	if !ok {
		return
	}
	switch transfer.Status {
	case model.DataTransferStatusPlanned, model.DataTransferStatusRunning:
	default:
		httpx.WriteError(w, http.StatusConflict, "data transfer is not active")
		return
	}
	var req struct {
		BytesDone *int64                       `json:"bytes_done"`
		FilesDone *int                         `json:"files_done"`
		Blobs     []model.DataTransferPlanBlob `json:"blobs"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.BytesDone != nil {
		if *req.BytesDone < 0 {
			httpx.WriteError(w, http.StatusBadRequest, "bytes_done must be non-negative")
			return
		}
		transfer.BytesDone = *req.BytesDone
	}
	if req.FilesDone != nil {
		if *req.FilesDone < 0 {
			httpx.WriteError(w, http.StatusBadRequest, "files_done must be non-negative")
			return
		}
		transfer.FilesDone = *req.FilesDone
	}
	if len(req.Blobs) > 0 {
		transfer.PlanBlobs = s.storedDataTransferPlanBlobs(workspace, transfer.Direction, mergeTransferPlanBlobCheckpoints(transfer.PlanBlobs, req.Blobs))
	}
	transfer.Status = model.DataTransferStatusRunning
	if transfer.StartedAt == nil {
		now := time.Now().UTC()
		transfer.StartedAt = &now
	}
	updated, err := s.store.UpdateDataTransfer(transfer)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"workspace": workspace, "transfer": summarizeDataTransfer(updated), "checkpointed": true})
}

func (s *Server) handleListDataMultipartParts(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	transfer, workspace, ok := s.loadAuthorizedDataTransfer(w, r, principal)
	if !ok {
		return
	}
	if transfer.Direction != model.DataTransferDirectionUpload {
		httpx.WriteError(w, http.StatusConflict, "multipart parts can only be listed for upload transfers")
		return
	}
	blob, ok := transferPlanBlobBySHA(transfer.PlanBlobs, r.URL.Query().Get("sha256"))
	if !ok || blob.UploadID == "" {
		httpx.WriteError(w, http.StatusNotFound, "multipart upload not found for sha256")
		return
	}
	backend, err := s.store.GetDataBackendForUse(workspace.StorageBackendID, workspace.TenantID, true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	objectBackend, err := newDataObjectBackend(backend)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	parts, err := objectBackend.listMultipartParts(r.Context(), blob.ObjectKey, blob.UploadID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"transfer": summarizeDataTransfer(transfer), "parts": parts})
}

func (s *Server) handleCompleteDataMultipartUpload(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	transfer, workspace, ok := s.loadAuthorizedDataTransfer(w, r, principal)
	if !ok {
		return
	}
	if transfer.Direction != model.DataTransferDirectionUpload {
		httpx.WriteError(w, http.StatusConflict, "multipart completion only applies to upload transfers")
		return
	}
	var req struct {
		SHA256    string                   `json:"sha256"`
		ObjectKey string                   `json:"object_key"`
		UploadID  string                   `json:"upload_id"`
		Parts     []model.DataTransferPart `json:"parts"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	blob, exists := transferPlanBlobBySHA(transfer.PlanBlobs, req.SHA256)
	if !exists {
		httpx.WriteError(w, http.StatusNotFound, "multipart upload not found for sha256")
		return
	}
	if req.UploadID == "" {
		req.UploadID = blob.UploadID
	}
	if req.ObjectKey == "" {
		req.ObjectKey = blob.ObjectKey
	}
	if req.UploadID == "" || req.ObjectKey == "" {
		httpx.WriteError(w, http.StatusBadRequest, "upload_id and object_key are required")
		return
	}
	backend, err := s.store.GetDataBackendForUse(workspace.StorageBackendID, workspace.TenantID, true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	objectBackend, err := newDataObjectBackend(backend)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if err := objectBackend.completeMultipartUpload(r.Context(), req.ObjectKey, req.UploadID, req.Parts); err != nil {
		s.writeStoreError(w, err)
		return
	}
	transfer.PlanBlobs = s.storedDataTransferPlanBlobs(workspace, transfer.Direction, markTransferPlanBlobComplete(transfer.PlanBlobs, req.SHA256, req.Parts))
	if transfer, err = s.store.UpdateDataTransfer(transfer); err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"transfer": summarizeDataTransfer(transfer), "sha256": req.SHA256, "completed": true})
}

func (s *Server) handleAbortDataMultipartUpload(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	transfer, workspace, ok := s.loadAuthorizedDataTransfer(w, r, principal)
	if !ok {
		return
	}
	var req struct {
		SHA256    string `json:"sha256"`
		ObjectKey string `json:"object_key"`
		UploadID  string `json:"upload_id"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	blob, exists := transferPlanBlobBySHA(transfer.PlanBlobs, req.SHA256)
	if exists {
		if req.UploadID == "" {
			req.UploadID = blob.UploadID
		}
		if req.ObjectKey == "" {
			req.ObjectKey = blob.ObjectKey
		}
	}
	if req.UploadID == "" || req.ObjectKey == "" {
		httpx.WriteError(w, http.StatusBadRequest, "upload_id and object_key are required")
		return
	}
	backend, err := s.store.GetDataBackendForUse(workspace.StorageBackendID, workspace.TenantID, true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	objectBackend, err := newDataObjectBackend(backend)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if err := objectBackend.abortMultipartUpload(r.Context(), req.ObjectKey, req.UploadID); err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"transfer": summarizeDataTransfer(transfer), "sha256": req.SHA256, "aborted": true})
}

func (s *Server) handleCreateDataGrant(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.grant") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.grant scope")
		return
	}
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		SnapshotID       string `json:"snapshot_id"`
		AssetName        string `json:"asset_name"`
		Mode             string `json:"mode"`
		ExpiresInMinutes int    `json:"expires_in_minutes"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = "read-only"
	}
	var expiresAt *time.Time
	if req.ExpiresInMinutes > 0 {
		value := time.Now().UTC().Add(time.Duration(req.ExpiresInMinutes) * time.Minute)
		expiresAt = &value
	}
	grant, secret, err := s.store.CreateDataGrant(model.DataGrant{
		WorkspaceID: workspace.ID,
		SnapshotID:  req.SnapshotID,
		AssetName:   req.AssetName,
		Mode:        mode,
		ExpiresAt:   expiresAt,
		CreatedBy:   principal.ActorID,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.grant.create", "data_grant", grant.ID, workspace.TenantID, map[string]string{"workspace": workspace.Name, "mode": grant.Mode})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"grant": grant, "secret": secret})
}

func (s *Server) handleListDataGrants(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	workspace, allowed := s.loadAuthorizedDataWorkspace(w, r, principal)
	if !allowed {
		return
	}
	grants, err := s.store.ListDataGrants(workspace.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"workspace": workspace, "grants": grants})
}

func (s *Server) handleRevokeDataGrant(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.grant") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.grant scope")
		return
	}
	grant, err := s.store.GetDataGrant(r.PathValue("grant_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && grant.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "data grant is not visible to this tenant")
		return
	}
	grant, err = s.store.RevokeDataGrant(grant.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "data.grant.revoke", "data_grant", grant.ID, grant.TenantID, map[string]string{"workspace_id": grant.WorkspaceID})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"grant": grant, "revoked": true})
}

func (s *Server) handlePutDataBlob(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.write scope")
		return
	}
	if !s.authorizeDataBlobTransfer(w, r, principal, model.DataTransferDirectionUpload) {
		return
	}
	digest := strings.TrimSpace(strings.ToLower(r.PathValue("sha256")))
	written, err := s.store.WriteDataBlob(digest, r.Body)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"sha256": digest, "size": written})
}

func (s *Server) handleGetDataBlob(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("data.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing data.read scope")
		return
	}
	if !s.authorizeDataBlobTransfer(w, r, principal, model.DataTransferDirectionDownload) {
		return
	}
	digest := strings.TrimSpace(strings.ToLower(r.PathValue("sha256")))
	file, info, err := s.store.OpenDataBlob(digest)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	defer file.Close()
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("X-Fugue-Data-SHA256", digest)
	http.ServeContent(w, r, digest, info.ModTime(), file)
}

func (s *Server) dataPlanBlobs(ctx context.Context, r *http.Request, workspace model.DataWorkspace, transfer model.DataTransfer, manifest model.DataManifest, upload bool) ([]dataTransferPlanBlob, error) {
	seen := map[string]struct{}{}
	blobs := []dataTransferPlanBlob{}
	entries := []model.DataManifestEntry{}
	storedByDigest := map[string]dataTransferPlanBlob{}
	for _, stored := range transfer.PlanBlobs {
		digest := strings.TrimSpace(strings.ToLower(stored.SHA256))
		if digest == "" {
			continue
		}
		storedByDigest[digest] = sanitizeDataTransferPlanBlob(stored)
	}
	var objectBackend *dataObjectBackend
	backend, err := s.store.GetDataBackendForUse(workspace.StorageBackendID, workspace.TenantID, true)
	if err == nil && dataBackendSupportsDirectObjectStorage(backend) {
		objectBackend, err = newDataObjectBackend(backend)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	knownObjectDigests := map[string]struct{}{}
	if upload && objectBackend != nil {
		knownObjectDigests, err = s.dataWorkspaceSnapshotBlobDigests(workspace.ID)
		if err != nil {
			return nil, err
		}
	}
	for _, entry := range manifest.Entries {
		if entry.Kind != model.DataManifestEntryKindFile || strings.TrimSpace(entry.SHA256) == "" {
			continue
		}
		if _, exists := seen[entry.SHA256]; exists {
			continue
		}
		seen[entry.SHA256] = struct{}{}
		blob := dataTransferPlanBlob{
			SHA256:    entry.SHA256,
			Size:      entry.Size,
			ObjectKey: entry.ObjectKey,
		}
		if stored, ok := storedByDigest[strings.ToLower(strings.TrimSpace(entry.SHA256))]; ok {
			blob = mergeTransferPlanBlobCheckpoint(blob, stored)
		}
		if objectBackend == nil {
			blob.Exists = s.store.DataBlobExists(entry.SHA256)
			if upload && !blob.Exists {
				blob.UploadURL = s.dataBlobURL(r, transfer.ID, entry.SHA256)
			}
			if !upload {
				blob.DownloadURL = s.dataBlobURL(r, transfer.ID, entry.SHA256)
			}
		}
		blobs = append(blobs, blob)
		entries = append(entries, entry)
	}
	if objectBackend != nil {
		if err := s.enrichDirectDataPlanBlobs(ctx, objectBackend, blobs, entries, knownObjectDigests, upload); err != nil {
			return nil, err
		}
	}
	return blobs, nil
}

func (s *Server) enrichDirectDataPlanBlobs(ctx context.Context, objectBackend *dataObjectBackend, blobs []dataTransferPlanBlob, entries []model.DataManifestEntry, knownObjectDigests map[string]struct{}, upload bool) error {
	if len(blobs) == 0 {
		return nil
	}
	planCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerCount := dataPlanWorkerCount(len(blobs))
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for idx := range jobs {
			entry := entries[idx]
			blob := blobs[idx]
			if upload {
				_, snapshotExists := knownObjectDigests[entry.SHA256]
				blob.Exists = blob.Exists || snapshotExists
				if !blob.Exists {
					if blob.UploadMode == model.DataBlobUploadModeMultipart && blob.UploadID != "" {
						if len(blob.Parts) == 0 {
							blob.Parts = planDataUploadParts(entry.Size, dataMultipartPartSize)
						}
						if blob.PartSize <= 0 {
							blob.PartSize = dataMultipartPartSize
						}
					} else if entry.Size > dataMultipartPartSize {
						uploadID, err := objectBackend.createMultipartUpload(planCtx, entry.ObjectKey)
						if err != nil {
							select {
							case errCh <- err:
								cancel()
							default:
							}
							return
						}
						blob.UploadMode = model.DataBlobUploadModeMultipart
						blob.UploadID = uploadID
						blob.PartSize = dataMultipartPartSize
						blob.Parts = planDataUploadParts(entry.Size, dataMultipartPartSize)
					} else {
						blob.UploadMode = model.DataBlobUploadModeSingle
					}
				}
				blobs[idx] = blob
				continue
			}
			exists, err := objectBackend.headObject(planCtx, entry.ObjectKey)
			if err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
				return
			}
			if !exists {
				select {
				case errCh <- fmt.Errorf("remote object missing for %s", entry.ObjectKey):
					cancel()
				default:
				}
				return
			}
			downloadURL, expiresAt, err := objectBackend.presignGet(planCtx, entry.ObjectKey, dataPresignTTL())
			if err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
				return
			}
			blob.Exists = true
			blob.DownloadURL = downloadURL
			blob.ExpiresAt = expiresAt
			blobs[idx] = blob
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	for idx := range blobs {
		select {
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return err
		case jobs <- idx:
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func dataPlanWorkerCount(jobCount int) int {
	if jobCount <= 1 {
		return 1
	}
	workerCount := dataTransferPlanConcurrency
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > jobCount {
		workerCount = jobCount
	}
	return workerCount
}

func (s *Server) dataWorkspaceSnapshotBlobDigests(workspaceID string) (map[string]struct{}, error) {
	snapshots, err := s.store.ListDataSnapshots(workspaceID, false)
	if err != nil {
		return nil, err
	}
	digests := map[string]struct{}{}
	for _, snapshot := range snapshots {
		for _, entry := range snapshot.Manifest.Entries {
			if entry.Kind != model.DataManifestEntryKindFile {
				continue
			}
			digest := strings.TrimSpace(strings.ToLower(entry.SHA256))
			if digest == "" {
				continue
			}
			digests[digest] = struct{}{}
		}
	}
	return digests, nil
}

func (s *Server) storedDataTransferPlanBlobs(workspace model.DataWorkspace, direction string, blobs []dataTransferPlanBlob) []dataTransferPlanBlob {
	directObjectBackend := false
	if backend, err := s.store.GetDataBackendForUse(workspace.StorageBackendID, workspace.TenantID, true); err == nil {
		directObjectBackend = dataBackendSupportsDirectObjectStorage(backend)
	}
	out := make([]dataTransferPlanBlob, 0, len(blobs))
	for _, blob := range blobs {
		blob = sanitizeDataTransferPlanBlob(blob)
		if directObjectBackend {
			switch direction {
			case model.DataTransferDirectionUpload:
				if blob.UploadMode != model.DataBlobUploadModeMultipart && blob.UploadID == "" && len(blob.Parts) == 0 {
					continue
				}
			case model.DataTransferDirectionDownload:
				continue
			}
		}
		out = append(out, blob)
	}
	return out
}

func sanitizeDataTransferPlanBlob(blob dataTransferPlanBlob) dataTransferPlanBlob {
	blob.UploadURL = ""
	blob.DownloadURL = ""
	blob.ExpiresAt = time.Time{}
	blob.Parts = append([]model.DataTransferPart(nil), blob.Parts...)
	for idx := range blob.Parts {
		blob.Parts[idx].UploadURL = ""
		blob.Parts[idx].DownloadURL = ""
		blob.Parts[idx].ExpiresAt = time.Time{}
	}
	return blob
}

func (s *Server) sweepDataWorkspaceGC(ctx context.Context, workspace model.DataWorkspace, retentionDays int, dryRun bool) (model.DataGCSweepResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if retentionDays <= 0 {
		retentionDays = 7
	}
	cutoff := time.Now().UTC().Add(-time.Duration(retentionDays) * 24 * time.Hour)
	referenceCounts := map[string]int{}
	snapshots, err := s.store.ListDataSnapshots(workspace.ID, false)
	if err != nil {
		return model.DataGCSweepResult{}, err
	}
	backend, err := s.store.GetDataBackendForUse(workspace.StorageBackendID, workspace.TenantID, true)
	if err != nil {
		return model.DataGCSweepResult{}, err
	}
	var objectBackend *dataObjectBackend
	if dataBackendSupportsDirectObjectStorage(backend) {
		objectBackend, err = newDataObjectBackend(backend)
		if err != nil {
			return model.DataGCSweepResult{}, err
		}
	}
	for _, snapshot := range snapshots {
		for _, entry := range snapshot.Manifest.Entries {
			if entry.Kind != model.DataManifestEntryKindFile || strings.TrimSpace(entry.SHA256) == "" {
				continue
			}
			key := entry.ObjectKey
			if key == "" {
				key = model.DataObjectKey(entry.SHA256)
			}
			if objectBackend != nil {
				key = objectBackend.objectKey(key)
			} else {
				key = strings.TrimSpace(strings.ToLower(entry.SHA256))
			}
			referenceCounts[key]++
		}
	}
	result := model.DataGCSweepResult{
		WorkspaceID:   workspace.ID,
		BackendID:     backend.ID,
		DryRun:        dryRun,
		RetentionDays: retentionDays,
		Cutoff:        cutoff,
		Candidates:    []model.DataGCSweepCandidate{},
	}
	if objectBackend != nil {
		objects, err := objectBackend.listObjects(ctx, "blobs/sha256/")
		if err != nil {
			return model.DataGCSweepResult{}, err
		}
		var deleteKeys []string
		for _, object := range objects {
			result.Scanned++
			if referenceCounts[object.Key] > 0 {
				continue
			}
			if object.LastModified.IsZero() || object.LastModified.After(cutoff) {
				continue
			}
			candidate := model.DataGCSweepCandidate{Key: object.Key, Size: object.Size, References: referenceCounts[object.Key], LastModified: object.LastModified, Reason: "unreferenced"}
			result.Candidates = append(result.Candidates, candidate)
			result.DeletedBytes += object.Size
			deleteKeys = append(deleteKeys, object.Key)
		}
		if !dryRun && len(deleteKeys) > 0 {
			if err := objectBackend.deleteObjects(ctx, deleteKeys); err != nil {
				return model.DataGCSweepResult{}, err
			}
			result.Deleted = len(deleteKeys)
		}
		if err := s.sweepOldMigrationBackendObjects(ctx, workspace, backend.ID, snapshots, cutoff, dryRun, &result); err != nil {
			return model.DataGCSweepResult{}, err
		}
		return result, nil
	}
	local, err := s.store.ListDataBlobDigests()
	if err != nil {
		return model.DataGCSweepResult{}, err
	}
	for _, candidate := range local {
		result.Scanned++
		candidate.References = referenceCounts[candidate.Key]
		if candidate.References > 0 {
			continue
		}
		if candidate.LastModified.IsZero() || candidate.LastModified.After(cutoff) {
			continue
		}
		candidate.Reason = "unreferenced"
		result.Candidates = append(result.Candidates, candidate)
		result.DeletedBytes += candidate.Size
		if !dryRun {
			if err := s.store.DeleteDataBlobDigest(candidate.Key); err != nil {
				return model.DataGCSweepResult{}, err
			}
			result.Deleted++
		}
	}
	return result, nil
}

func (s *Server) sweepOldMigrationBackendObjects(ctx context.Context, workspace model.DataWorkspace, currentBackendID string, snapshots []model.DataSnapshot, cutoff time.Time, dryRun bool, result *model.DataGCSweepResult) error {
	transfers, err := s.store.ListDataTransfers(workspace.TenantID, workspace.ID, true)
	if err != nil {
		return err
	}
	seen := map[string]struct{}{}
	for _, transfer := range transfers {
		if transfer.Direction != model.DataTransferDirectionMigrate || transfer.Status != model.DataTransferStatusCompleted {
			continue
		}
		sourceID := strings.TrimSpace(transfer.Source)
		if sourceID == "" || sourceID == currentBackendID {
			continue
		}
		if _, ok := seen[sourceID]; ok {
			continue
		}
		seen[sourceID] = struct{}{}
		backend, err := s.store.GetDataBackendForUse(sourceID, workspace.TenantID, true)
		if err != nil {
			return err
		}
		if !dataBackendSupportsDirectObjectStorage(backend) {
			continue
		}
		objectBackend, err := newDataObjectBackend(backend)
		if err != nil {
			return err
		}
		referenceCounts := map[string]int{}
		for _, snapshot := range snapshots {
			for _, entry := range snapshot.Manifest.Entries {
				if entry.Kind != model.DataManifestEntryKindFile || strings.TrimSpace(entry.SHA256) == "" {
					continue
				}
				key := entry.ObjectKey
				if key == "" {
					key = model.DataObjectKey(entry.SHA256)
				}
				referenceCounts[objectBackend.objectKey(key)]++
			}
		}
		objects, err := objectBackend.listObjects(ctx, "blobs/sha256/")
		if err != nil {
			return err
		}
		var deleteKeys []string
		for _, object := range objects {
			result.Scanned++
			if referenceCounts[object.Key] > 0 {
				continue
			}
			if object.LastModified.IsZero() || object.LastModified.After(cutoff) {
				continue
			}
			result.Candidates = append(result.Candidates, model.DataGCSweepCandidate{Key: object.Key, Size: object.Size, References: 0, LastModified: object.LastModified, Reason: "old-backend-unreferenced"})
			result.DeletedBytes += object.Size
			deleteKeys = append(deleteKeys, object.Key)
		}
		if !dryRun && len(deleteKeys) > 0 {
			if err := objectBackend.deleteObjects(ctx, deleteKeys); err != nil {
				return err
			}
			result.Deleted += len(deleteKeys)
		}
	}
	return nil
}

func (s *Server) migrateDataWorkspaceBackend(ctx context.Context, workspace model.DataWorkspace, targetBackendID string, dryRun, cutover bool) (model.DataTransfer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sourceBackend, err := s.store.GetDataBackendForUse(workspace.StorageBackendID, workspace.TenantID, true)
	if err != nil {
		return model.DataTransfer{}, err
	}
	targetBackend, err := s.store.GetDataBackendForUse(targetBackendID, workspace.TenantID, true)
	if err != nil {
		return model.DataTransfer{}, err
	}
	if sourceBackend.ID == targetBackend.ID {
		return model.DataTransfer{}, store.ErrInvalidInput
	}
	sourceObjectBackend, err := newDataObjectBackend(sourceBackend)
	if err != nil {
		return model.DataTransfer{}, err
	}
	targetObjectBackend, err := newDataObjectBackend(targetBackend)
	if err != nil {
		return model.DataTransfer{}, err
	}
	snapshots, err := s.store.ListDataSnapshots(workspace.ID, false)
	if err != nil {
		return model.DataTransfer{}, err
	}
	manifest := model.DataManifest{}
	seen := map[string]model.DataManifestEntry{}
	for _, snapshot := range snapshots {
		for _, entry := range snapshot.Manifest.Entries {
			if entry.Kind != model.DataManifestEntryKindFile || strings.TrimSpace(entry.SHA256) == "" {
				continue
			}
			if _, ok := seen[entry.SHA256]; ok {
				continue
			}
			seen[entry.SHA256] = entry
			manifest.Entries = append(manifest.Entries, entry)
		}
	}
	manifest = model.NormalizeDataManifest(manifest)
	transfer, err := s.store.CreateDataTransfer(model.DataTransfer{
		WorkspaceID: workspace.ID,
		Direction:   model.DataTransferDirectionMigrate,
		Status:      model.DataTransferStatusRunning,
		Source:      sourceBackend.ID,
		Target:      targetBackend.ID,
		Manifest:    manifest,
		BytesTotal:  manifest.TotalBytes,
		FilesTotal:  manifest.FileCount,
	})
	if err != nil {
		return model.DataTransfer{}, err
	}
	now := time.Now().UTC()
	transfer.StartedAt = &now
	var copiedBytes int64
	var copiedFiles int
	if !dryRun {
		migrationCtx, cancel := context.WithCancel(ctx)
		workerCount := dataBackendMigrationConcurrency
		if workerCount > len(manifest.Entries) {
			workerCount = len(manifest.Entries)
		}
		if workerCount <= 0 {
			workerCount = 1
		}
		jobs := make(chan model.DataManifestEntry)
		errCh := make(chan error, 1)
		var mu sync.Mutex
		var wg sync.WaitGroup
		recordProgress := func(size int64) error {
			mu.Lock()
			defer mu.Unlock()
			copiedBytes += size
			copiedFiles++
			transfer.BytesDone = copiedBytes
			transfer.FilesDone = copiedFiles
			updated, err := s.store.UpdateDataTransfer(transfer)
			if err != nil {
				return err
			}
			transfer = updated
			return nil
		}
		worker := func() {
			defer wg.Done()
			for entry := range jobs {
				if err := copyDataWorkspaceObject(migrationCtx, sourceObjectBackend, targetObjectBackend, entry); err != nil {
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
				if err := recordProgress(entry.Size); err != nil {
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
			}
		}
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go worker()
		}
		for _, entry := range manifest.Entries {
			select {
			case err := <-errCh:
				close(jobs)
				wg.Wait()
				cancel()
				return failDataMigrationTransfer(s.store, transfer, err)
			case jobs <- entry:
			}
		}
		close(jobs)
		wg.Wait()
		cancel()
		select {
		case err := <-errCh:
			return failDataMigrationTransfer(s.store, transfer, err)
		default:
		}
		if cutover {
			backendID := targetBackend.ID
			if _, err := s.store.UpdateDataWorkspace(workspace.ID, workspace.TenantID, true, store.DataWorkspaceUpdate{StorageBackendID: &backendID}); err != nil {
				return failDataMigrationTransfer(s.store, transfer, err)
			}
		}
	} else {
		transfer.BytesDone = 0
		transfer.FilesDone = 0
	}
	finished := time.Now().UTC()
	transfer.Status = model.DataTransferStatusCompleted
	transfer.FinishedAt = &finished
	if dryRun {
		transfer.Message = "dry-run"
	}
	return s.store.UpdateDataTransfer(transfer)
}

func copyDataWorkspaceObject(ctx context.Context, sourceObjectBackend, targetObjectBackend *dataObjectBackend, entry model.DataManifestEntry) error {
	objectKey := strings.TrimSpace(entry.ObjectKey)
	if objectKey == "" {
		objectKey = model.DataObjectKey(entry.SHA256)
	}
	exists, err := targetObjectBackend.headObject(ctx, objectKey)
	if err != nil {
		return err
	}
	if !exists {
		body, size, err := sourceObjectBackend.getObject(ctx, objectKey)
		if err != nil {
			return err
		}
		if err := targetObjectBackend.putObject(ctx, objectKey, body, size); err != nil {
			body.Close()
			return err
		}
		if err := body.Close(); err != nil {
			return err
		}
	}
	info, verified, err := targetObjectBackend.headObjectInfo(ctx, objectKey)
	if err != nil {
		return err
	}
	if !verified {
		return fmt.Errorf("target object missing after migration: %s", objectKey)
	}
	if entry.Size >= 0 && info.Size != entry.Size {
		return fmt.Errorf("target object size mismatch after migration for %s: got %d expected %d", objectKey, info.Size, entry.Size)
	}
	return nil
}

func (s *Server) rollbackDataWorkspaceBackendMigration(ctx context.Context, workspace model.DataWorkspace, migrationTransferID string) (model.DataWorkspace, model.DataTransfer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(migrationTransferID) == "" {
		return model.DataWorkspace{}, model.DataTransfer{}, store.ErrInvalidInput
	}
	migration, err := s.store.GetDataTransfer(migrationTransferID)
	if err != nil {
		return model.DataWorkspace{}, model.DataTransfer{}, err
	}
	if migration.WorkspaceID != workspace.ID || migration.Direction != model.DataTransferDirectionMigrate {
		return model.DataWorkspace{}, model.DataTransfer{}, store.ErrInvalidInput
	}
	if migration.Status != model.DataTransferStatusCompleted || strings.TrimSpace(migration.Source) == "" {
		return model.DataWorkspace{}, model.DataTransfer{}, store.ErrInvalidInput
	}
	if _, err := s.store.GetDataBackendForUse(migration.Source, workspace.TenantID, true); err != nil {
		return model.DataWorkspace{}, model.DataTransfer{}, err
	}
	now := time.Now().UTC()
	previousBackendID := workspace.StorageBackendID
	sourceBackendID := migration.Source
	updatedWorkspace, err := s.store.UpdateDataWorkspace(workspace.ID, workspace.TenantID, true, store.DataWorkspaceUpdate{StorageBackendID: &sourceBackendID})
	if err != nil {
		return model.DataWorkspace{}, model.DataTransfer{}, err
	}
	rollbackTransfer, err := s.store.CreateDataTransfer(model.DataTransfer{
		WorkspaceID: workspace.ID,
		Direction:   model.DataTransferDirectionMigrate,
		Status:      model.DataTransferStatusCompleted,
		Source:      previousBackendID,
		Target:      sourceBackendID,
		Message:     "rollback of " + migration.ID,
		Manifest:    migration.Manifest,
		BytesTotal:  migration.BytesTotal,
		BytesDone:   migration.BytesTotal,
		FilesTotal:  migration.FilesTotal,
		FilesDone:   migration.FilesTotal,
		StartedAt:   &now,
		FinishedAt:  &now,
	})
	if err != nil {
		return model.DataWorkspace{}, model.DataTransfer{}, err
	}
	return updatedWorkspace, rollbackTransfer, nil
}

func failDataMigrationTransfer(dataStore *store.Store, transfer model.DataTransfer, cause error) (model.DataTransfer, error) {
	finished := time.Now().UTC()
	transfer.Status = model.DataTransferStatusFailed
	transfer.ErrorCode = "backend_migration_failed"
	transfer.ErrorMessage = cause.Error()
	transfer.FinishedAt = &finished
	updated, err := dataStore.UpdateDataTransfer(transfer)
	if err != nil {
		return model.DataTransfer{}, err
	}
	return updated, cause
}

func (s *Server) presignDataUploadParts(ctx context.Context, objectBackend *dataObjectBackend, objectKey, uploadID string, size, partSize int64) ([]model.DataTransferPart, error) {
	if partSize <= 0 {
		partSize = dataMultipartPartSize
	}
	parts := planDataUploadParts(size, partSize)
	if _, err := presignDataUploadPartURLs(ctx, objectBackend, objectKey, uploadID, parts, dataPresignTTL()); err != nil {
		return nil, err
	}
	return parts, nil
}

func presignDataUploadPartURLs(ctx context.Context, objectBackend *dataObjectBackend, objectKey, uploadID string, parts []model.DataTransferPart, ttl time.Duration) (time.Time, error) {
	if len(parts) == 0 {
		return time.Time{}, nil
	}
	planCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerCount := dataTransferPartPlanConcurrency
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(parts) {
		workerCount = len(parts)
	}
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var expiresAt time.Time
	var mu sync.Mutex
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for idx := range jobs {
			if parts[idx].Completed && strings.TrimSpace(parts[idx].ETag) != "" {
				continue
			}
			uploadURL, partExpiresAt, err := objectBackend.presignUploadPart(planCtx, objectKey, uploadID, parts[idx].PartNumber, ttl)
			if err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
				return
			}
			parts[idx].UploadURL = uploadURL
			parts[idx].ExpiresAt = partExpiresAt
			mu.Lock()
			if partExpiresAt.After(expiresAt) {
				expiresAt = partExpiresAt
			}
			mu.Unlock()
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	for idx := range parts {
		select {
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return time.Time{}, err
		case jobs <- idx:
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return time.Time{}, err
	default:
	}
	return expiresAt, nil
}

func planDataUploadParts(size, partSize int64) []model.DataTransferPart {
	if partSize <= 0 {
		partSize = dataMultipartPartSize
	}
	var parts []model.DataTransferPart
	var offset int64
	var number int32 = 1
	for offset < size {
		partBytes := partSize
		if remaining := size - offset; remaining < partBytes {
			partBytes = remaining
		}
		parts = append(parts, model.DataTransferPart{PartNumber: number, Offset: offset, Size: partBytes})
		offset += partBytes
		number++
	}
	return parts
}

func (s *Server) refreshDataPlanBlobs(ctx context.Context, r *http.Request, workspace model.DataWorkspace, transfer model.DataTransfer, upload bool, page dataTransferBlobPage) ([]dataTransferPlanBlob, []dataTransferPlanBlob, error) {
	backend, err := s.store.GetDataBackendForUse(workspace.StorageBackendID, workspace.TenantID, true)
	if err != nil {
		return nil, nil, err
	}
	if !dataBackendSupportsDirectObjectStorage(backend) {
		blobs, err := s.dataPlanBlobs(ctx, r, workspace, transfer, transfer.Manifest, upload)
		if err != nil {
			return nil, nil, err
		}
		return blobs, sliceDataTransferBlobs(blobs, page), nil
	}
	objectBackend, err := newDataObjectBackend(backend)
	if err != nil {
		return nil, nil, err
	}
	blobs, err := s.dataPlanBlobs(ctx, r, workspace, transfer, transfer.Manifest, upload)
	if err != nil {
		return nil, nil, err
	}
	start := page.Offset
	if start > len(blobs) {
		start = len(blobs)
	}
	end := start + page.Limit
	if page.Limit == 0 || end > len(blobs) {
		end = len(blobs)
	}
	if err := s.refreshDirectDataPlanBlobPage(ctx, objectBackend, blobs, upload, start, end); err != nil {
		return nil, nil, err
	}
	return blobs, append([]dataTransferPlanBlob(nil), blobs[start:end]...), nil
}

func (s *Server) refreshDirectDataPlanBlobPage(ctx context.Context, objectBackend *dataObjectBackend, blobs []dataTransferPlanBlob, upload bool, start, end int) error {
	if start >= end {
		return nil
	}
	planCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerCount := dataPlanWorkerCount(end - start)
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for idx := range jobs {
			blob := blobs[idx]
			if upload {
				if blob.UploadMode == model.DataBlobUploadModeMultipart && blob.UploadID != "" {
					completed, err := objectBackend.listMultipartParts(planCtx, blob.ObjectKey, blob.UploadID)
					if err != nil {
						select {
						case errCh <- err:
							cancel()
						default:
						}
						return
					}
					completedByNumber := map[int32]model.DataTransferPart{}
					for _, part := range completed {
						completedByNumber[part.PartNumber] = part
					}
					for partIdx := range blob.Parts {
						if completedPart, ok := completedByNumber[blob.Parts[partIdx].PartNumber]; ok {
							blob.Parts[partIdx].Completed = true
							blob.Parts[partIdx].ETag = completedPart.ETag
						}
					}
					expiresAt, err := presignDataUploadPartURLs(planCtx, objectBackend, blob.ObjectKey, blob.UploadID, blob.Parts, dataPresignTTL())
					if err != nil {
						select {
						case errCh <- err:
							cancel()
						default:
						}
						return
					}
					if !expiresAt.IsZero() {
						blob.ExpiresAt = expiresAt
					}
					blobs[idx] = blob
					continue
				}
				if !blob.Exists && blob.UploadMode != model.DataBlobUploadModeMultipart {
					uploadURL, expiresAt, err := objectBackend.presignPut(planCtx, blob.ObjectKey, dataPresignTTL())
					if err != nil {
						select {
						case errCh <- err:
							cancel()
						default:
						}
						return
					}
					blob.UploadURL = uploadURL
					blob.ExpiresAt = expiresAt
					blobs[idx] = blob
				}
				continue
			}
			downloadURL, expiresAt, err := objectBackend.presignGet(planCtx, blob.ObjectKey, dataPresignTTL())
			if err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
				return
			}
			blob.DownloadURL = downloadURL
			blob.ExpiresAt = expiresAt
			blobs[idx] = blob
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	for idx := start; idx < end; idx++ {
		select {
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return err
		case jobs <- idx:
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func transferPlanBlobBySHA(blobs []dataTransferPlanBlob, sha256 string) (dataTransferPlanBlob, bool) {
	sha256 = strings.TrimSpace(strings.ToLower(sha256))
	for _, blob := range blobs {
		if strings.EqualFold(blob.SHA256, sha256) {
			return blob, true
		}
	}
	return dataTransferPlanBlob{}, false
}

func markTransferPlanBlobComplete(blobs []dataTransferPlanBlob, sha256 string, parts []model.DataTransferPart) []dataTransferPlanBlob {
	sha256 = strings.TrimSpace(strings.ToLower(sha256))
	out := append([]dataTransferPlanBlob(nil), blobs...)
	for idx := range out {
		if !strings.EqualFold(out[idx].SHA256, sha256) {
			continue
		}
		out[idx].Exists = true
		partsByNumber := map[int32]model.DataTransferPart{}
		for _, part := range parts {
			partsByNumber[part.PartNumber] = part
		}
		for partIdx := range out[idx].Parts {
			if part, ok := partsByNumber[out[idx].Parts[partIdx].PartNumber]; ok {
				out[idx].Parts[partIdx].Completed = true
				out[idx].Parts[partIdx].ETag = part.ETag
			}
		}
	}
	return out
}

func mergeTransferPlanBlobCheckpoints(current, checkpoints []dataTransferPlanBlob) []dataTransferPlanBlob {
	out := append([]dataTransferPlanBlob(nil), current...)
	indexByDigest := map[string]int{}
	for idx := range out {
		indexByDigest[strings.ToLower(strings.TrimSpace(out[idx].SHA256))] = idx
	}
	for _, checkpoint := range checkpoints {
		digest := strings.ToLower(strings.TrimSpace(checkpoint.SHA256))
		if digest == "" {
			continue
		}
		checkpoint.UploadURL = ""
		checkpoint.DownloadURL = ""
		checkpoint.ExpiresAt = time.Time{}
		for idx := range checkpoint.Parts {
			checkpoint.Parts[idx].UploadURL = ""
			checkpoint.Parts[idx].DownloadURL = ""
			checkpoint.Parts[idx].ExpiresAt = time.Time{}
		}
		if idx, ok := indexByDigest[digest]; ok {
			out[idx] = mergeTransferPlanBlobCheckpoint(out[idx], checkpoint)
			continue
		}
		indexByDigest[digest] = len(out)
		out = append(out, checkpoint)
	}
	return out
}

func mergeTransferPlanBlobCheckpoint(current, checkpoint dataTransferPlanBlob) dataTransferPlanBlob {
	if checkpoint.Exists {
		current.Exists = true
	}
	if checkpoint.UploadID != "" {
		current.UploadID = checkpoint.UploadID
	}
	if checkpoint.UploadMode != "" {
		current.UploadMode = checkpoint.UploadMode
	}
	if checkpoint.PartSize > 0 {
		current.PartSize = checkpoint.PartSize
	}
	if checkpoint.ObjectKey != "" {
		current.ObjectKey = checkpoint.ObjectKey
	}
	if checkpoint.Size > 0 {
		current.Size = checkpoint.Size
	}
	if len(checkpoint.Parts) == 0 {
		return current
	}
	partsByNumber := map[int32]int{}
	for idx := range current.Parts {
		partsByNumber[current.Parts[idx].PartNumber] = idx
	}
	for _, part := range checkpoint.Parts {
		if part.PartNumber <= 0 {
			continue
		}
		part.UploadURL = ""
		part.DownloadURL = ""
		part.ExpiresAt = time.Time{}
		if idx, ok := partsByNumber[part.PartNumber]; ok {
			if part.Completed {
				current.Parts[idx].Completed = true
			}
			if strings.TrimSpace(part.ETag) != "" {
				current.Parts[idx].ETag = part.ETag
			}
			if part.Size > 0 {
				current.Parts[idx].Size = part.Size
			}
			if part.Offset > 0 {
				current.Parts[idx].Offset = part.Offset
			}
			continue
		}
		partsByNumber[part.PartNumber] = len(current.Parts)
		current.Parts = append(current.Parts, part)
	}
	sort.Slice(current.Parts, func(i, j int) bool { return current.Parts[i].PartNumber < current.Parts[j].PartNumber })
	return current
}

func (s *Server) dataBlobURL(r *http.Request, transferID, digest string) string {
	base := strings.TrimRight(s.apiPublicDomain, "/")
	if base != "" && !strings.Contains(base, "://") {
		base = "https://" + base
	}
	if base == "" {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		host := r.Host
		base = scheme + "://" + host
	}
	return base + "/v1/data/blobs/" + url.PathEscape(digest) + "?transfer_id=" + url.QueryEscape(transferID)
}

func (s *Server) authorizeDataBlobTransfer(w http.ResponseWriter, r *http.Request, principal model.Principal, direction string) bool {
	transferID := strings.TrimSpace(r.URL.Query().Get("transfer_id"))
	if transferID == "" {
		httpx.WriteError(w, http.StatusForbidden, "data blob transfer_id is required")
		return false
	}
	transfer, err := s.store.GetDataTransfer(transferID)
	if err != nil {
		s.writeStoreError(w, err)
		return false
	}
	if !principal.IsPlatformAdmin() && transfer.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "data transfer is not visible to this tenant")
		return false
	}
	if transfer.Direction != direction {
		httpx.WriteError(w, http.StatusForbidden, "data transfer direction does not allow this blob operation")
		return false
	}
	switch transfer.Status {
	case model.DataTransferStatusPlanned, model.DataTransferStatusRunning:
		return true
	default:
		httpx.WriteError(w, http.StatusConflict, "data transfer is not active")
		return false
	}
}

func (s *Server) loadAuthorizedDataWorkspace(w http.ResponseWriter, r *http.Request, principal model.Principal) (model.DataWorkspace, bool) {
	workspace, err := s.store.GetDataWorkspace(r.PathValue("workspace_id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return model.DataWorkspace{}, false
	}
	if !principalAllowsDataWorkspace(principal, workspace) {
		httpx.WriteError(w, http.StatusForbidden, "data workspace is not visible to this tenant")
		return model.DataWorkspace{}, false
	}
	return workspace, true
}

func (s *Server) loadAuthorizedDataTransfer(w http.ResponseWriter, r *http.Request, principal model.Principal) (model.DataTransfer, model.DataWorkspace, bool) {
	transfer, err := s.store.GetDataTransfer(r.PathValue("transfer_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return model.DataTransfer{}, model.DataWorkspace{}, false
	}
	if !principal.IsPlatformAdmin() && transfer.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "data transfer is not visible to this tenant")
		return model.DataTransfer{}, model.DataWorkspace{}, false
	}
	workspace, err := s.store.GetDataWorkspace(transfer.WorkspaceID, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return model.DataTransfer{}, model.DataWorkspace{}, false
	}
	if !principalAllowsDataWorkspace(principal, workspace) {
		httpx.WriteError(w, http.StatusForbidden, "data workspace is not visible to this tenant")
		return model.DataTransfer{}, model.DataWorkspace{}, false
	}
	return transfer, workspace, true
}

func principalAllowsDataWorkspace(principal model.Principal, workspace model.DataWorkspace) bool {
	if principal.IsPlatformAdmin() {
		return true
	}
	if strings.TrimSpace(workspace.TenantID) != "" && workspace.TenantID != principal.TenantID {
		return false
	}
	return principal.AllowsProject(workspace.ProjectID)
}

func filterDataManifestAssets(manifest model.DataManifest, assets []string) model.DataManifest {
	if len(assets) == 0 {
		return manifest
	}
	allowed := map[string]struct{}{}
	for _, asset := range assets {
		asset = strings.TrimSpace(asset)
		if asset != "" {
			allowed[asset] = struct{}{}
		}
	}
	filtered := manifest
	filtered.Entries = nil
	for _, entry := range manifest.Entries {
		if _, ok := allowed[entry.AssetName]; ok {
			filtered.Entries = append(filtered.Entries, entry)
		}
	}
	return model.NormalizeDataManifest(filtered)
}

func readInt64Query(r *http.Request, name string, fallback int64) (int64, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return value, nil
}

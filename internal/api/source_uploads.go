package api

import (
	"net/http"
	"sort"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleGetSourceUpload(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)

	upload, err := s.store.GetSourceUpload(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principalAllowsSourceUpload(principal, upload.TenantID) {
		httpx.WriteError(w, http.StatusForbidden, "cannot view source upload for another tenant")
		return
	}

	inspection, err := s.buildSourceUploadInspection(upload)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(principal, "source_upload.read", "source_upload", upload.ID, upload.TenantID, map[string]string{
		"filename": upload.Filename,
		"sha256":   upload.SHA256,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"source_upload": inspection})
}

func principalAllowsSourceUpload(principal model.Principal, tenantID string) bool {
	if principal.IsPlatformAdmin() {
		return true
	}
	tenantID = strings.TrimSpace(tenantID)
	principalTenantID := strings.TrimSpace(principal.TenantID)
	return tenantID != "" && principalTenantID != "" && tenantID == principalTenantID
}

func (s *Server) buildSourceUploadInspection(upload model.SourceUpload) (model.SourceUploadInspection, error) {
	ops, err := s.store.ListOperations(upload.TenantID, false)
	if err != nil {
		return model.SourceUploadInspection{}, err
	}

	appNames := make(map[string]string)
	references := make([]model.SourceUploadReference, 0)
	for _, op := range ops {
		source := op.DesiredSource
		if source == nil || strings.TrimSpace(source.UploadID) != strings.TrimSpace(upload.ID) {
			continue
		}

		appID := strings.TrimSpace(op.AppID)
		appName := ""
		if appID != "" {
			if cached, ok := appNames[appID]; ok {
				appName = cached
			} else if app, err := s.store.GetApp(appID); err == nil {
				appName = strings.TrimSpace(app.Name)
				appNames[appID] = appName
			} else if err != nil && err != store.ErrNotFound {
				return model.SourceUploadInspection{}, err
			}
		}

		references = append(references, model.SourceUploadReference{
			OperationID:      strings.TrimSpace(op.ID),
			OperationType:    strings.TrimSpace(op.Type),
			OperationStatus:  strings.TrimSpace(op.Status),
			AppID:            appID,
			AppName:          appName,
			BuildStrategy:    strings.TrimSpace(source.BuildStrategy),
			SourceDir:        strings.TrimSpace(source.SourceDir),
			DockerfilePath:   strings.TrimSpace(source.DockerfilePath),
			BuildContextDir:  strings.TrimSpace(source.BuildContextDir),
			ResolvedImageRef: strings.TrimSpace(source.ResolvedImageRef),
			CreatedAt:        op.CreatedAt,
			UpdatedAt:        op.UpdatedAt,
		})
	}
	sort.Slice(references, func(i, j int) bool {
		if !references[i].CreatedAt.Equal(references[j].CreatedAt) {
			return references[i].CreatedAt.After(references[j].CreatedAt)
		}
		return references[i].OperationID < references[j].OperationID
	})

	return model.SourceUploadInspection{
		Upload:     upload,
		References: references,
	}, nil
}

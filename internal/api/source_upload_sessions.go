package api

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) authorizedSourceSession(w http.ResponseWriter, r *http.Request) (model.SourceUploadSession, bool) {
	v, err := s.store.GetSourceUploadSession(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return v, false
	}
	if !sourceSessionAllowed(mustPrincipal(r), v) {
		httpx.WriteError(w, http.StatusNotFound, "source upload session not found")
		return v, false
	}
	return v, true
}
func sourceSessionAllowed(p model.Principal, v model.SourceUploadSession) bool {
	return p.IsPlatformAdmin() || (p.TenantID == v.TenantID && p.ActorType == v.ActorType && p.ActorID == v.ActorID && (p.ProjectID == "" || p.ProjectID == v.ProjectID))
}
func (s *Server) handleCreateSourceUploadSession(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() && !p.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	var req struct {
		RequestID string `json:"request_id"`
		TenantID  string `json:"tenant_id"`
		Filename  string `json:"filename"`
		SizeBytes int64  `json:"size_bytes"`
		SHA256    string `json:"sha256"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, 400, err.Error())
		return
	}
	tenant, ok := s.resolveTenantID(p, req.TenantID)
	if !ok {
		httpx.WriteError(w, 403, "cannot upload for another tenant")
		return
	}
	if err := s.store.CleanupSourceUploadChunks(); err != nil {
		s.writeStoreError(w, err)
		return
	}
	v, err := s.store.CreateSourceUploadSession(model.SourceUploadSession{RequestID: req.RequestID, TenantID: tenant, ProjectID: p.ProjectID, ActorType: p.ActorType, ActorID: p.ActorID, Filename: req.Filename, SizeBytes: req.SizeBytes, SHA256: req.SHA256})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, 200, map[string]any{"session": v})
}
func (s *Server) handleGetSourceUploadSession(w http.ResponseWriter, r *http.Request) {
	v, ok := s.authorizedSourceSession(w, r)
	if ok {
		httpx.WriteJSON(w, 200, map[string]any{"session": v})
	}
}
func (s *Server) handleGetSourceUploadRequest(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	tenant, ok := s.resolveTenantID(p, r.URL.Query().Get("tenant_id"))
	if !ok {
		httpx.WriteError(w, 403, "cannot inspect another tenant request")
		return
	}
	v, err := s.store.GetSourceUploadRequest(tenant, r.PathValue("request_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !sourceSessionAllowed(p, v) {
		httpx.WriteError(w, 404, "source upload request not found")
		return
	}
	httpx.WriteJSON(w, 200, map[string]any{"session": v})
}
func (s *Server) handlePutSourceUploadChunk(w http.ResponseWriter, r *http.Request) {
	v, ok := s.authorizedSourceSession(w, r)
	if !ok {
		return
	}
	finish, ok := s.beginSourceUploadRequest(w, r)
	if !ok {
		return
	}
	defer finish()
	index, err := strconv.Atoi(r.PathValue("index"))
	if err != nil {
		httpx.WriteError(w, 400, "invalid chunk index")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 6<<20)
	var req struct {
		SHA256 string `json:"sha256"`
		Data   []byte `json:"data"`
	}
	if err = httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, 400, "invalid chunk body; maximum 4 MiB decoded")
		return
	}
	v, err = s.store.PutSourceUploadChunk(v.ID, index, req.SHA256, req.Data)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, 200, map[string]any{"session": v})
}
func (s *Server) handleCompleteSourceUploadSession(w http.ResponseWriter, r *http.Request) {
	v, ok := s.authorizedSourceSession(w, r)
	if !ok {
		return
	}
	finish, ok := s.beginSourceUploadRequest(w, r)
	if !ok {
		return
	}
	defer finish()
	v, err := s.store.CompleteSourceUploadSession(v.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	_ = s.store.CleanupSourceUploadChunks()
	httpx.WriteJSON(w, 200, map[string]any{"session": v})
}
func (s *Server) handleSubmitSourceUploadSession(w http.ResponseWriter, r *http.Request) {
	v, ok := s.authorizedSourceSession(w, r)
	if !ok {
		return
	}
	finish, ok := s.beginSourceUploadRequest(w, r)
	if !ok {
		return
	}
	defer finish()
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() && !p.HasScope("app.deploy") {
		httpx.WriteError(w, 403, "missing app.deploy scope")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
	var req importUploadRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, 400, err.Error())
		return
	}
	if req.DryRun {
		httpx.WriteError(w, 400, "session submit executes once; use deploy inspect for inspection")
		return
	}
	if req.TenantID != "" && req.TenantID != v.TenantID {
		httpx.WriteError(w, 409, "session tenant differs from import target")
		return
	}
	req.TenantID = v.TenantID
	if req.AppID != "" {
		app, err := s.store.GetApp(req.AppID)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if app.TenantID != v.TenantID || !principalAllowsApp(p, app) {
			httpx.WriteError(w, 403, "session cannot deploy this app")
			return
		}
	}
	canonical, err := json.Marshal(req)
	if err != nil {
		httpx.WriteError(w, 400, "invalid import intent")
		return
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(canonical))
	// Loading and archive validation precede reservation: a read failure has no
	// import effects and does not consume the request's single execution.
	upload, archive, err := s.store.GetSourceUploadArchive(v.UploadID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if upload.TenantID != v.TenantID || upload.SHA256 != v.SHA256 {
		httpx.WriteError(w, 409, "session archive mismatch")
		return
	}
	v, started, err := s.store.BeginSourceUploadSubmission(v.ID, hash)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !started {
		httpx.WriteJSON(w, 200, map[string]any{"session": v, "replayed": true})
		return
	}
	req.sourceSessionID = v.ID
	record := httptest.NewRecorder()
	header := &multipart.FileHeader{Filename: upload.Filename, Size: upload.SizeBytes, Header: textproto.MIMEHeader{"Content-Type": []string{upload.ContentType}}}
	s.importUploadDecoded(record, r, req, header, archive, &upload)
	v, err = s.store.FinishSourceUploadSubmission(v.ID, record.Code)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	result := map[string]any{"session": v}
	if record.Code >= 200 && record.Code < 300 {
		result["result"] = json.RawMessage(record.Body.Bytes())
	} else {
		var problem any
		if json.Unmarshal(record.Body.Bytes(), &problem) == nil {
			result["error"] = problem
		}
	}
	httpx.WriteJSON(w, 200, result)
}

func (s *Server) createImportSourceUpload(tenantID, filename, contentType string, archive []byte, existing *model.SourceUpload) (model.SourceUpload, error) {
	if existing == nil {
		return s.store.CreateSourceUpload(tenantID, filename, contentType, archive)
	}
	if strings.TrimSpace(tenantID) != existing.TenantID {
		return model.SourceUpload{}, store.ErrConflict
	}
	return *existing, nil
}

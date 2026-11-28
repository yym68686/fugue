package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fugue/internal/appdbimport"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"

	"github.com/gorilla/websocket"
)

const (
	maxAppDatabaseImportRequestFieldBytes = 64 << 10
	maxAppDatabaseImportDumpBytes         = appdbimport.MaxDumpBytes
	maxAppDatabaseImportRequestBytes      = 130 << 20
	appDatabaseTunnelBufferBytes          = 32 * 1024
)

type appDatabaseImportRequestError struct {
	status  int
	message string
}

func (e *appDatabaseImportRequestError) Error() string {
	return e.message
}

func newAppDatabaseImportRequestError(status int, message string) error {
	return &appDatabaseImportRequestError{status: status, message: message}
}

func appDatabaseImportErrorStatus(err error) int {
	var requestErr *appDatabaseImportRequestError
	if errors.As(err, &requestErr) {
		return requestErr.status
	}
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusInternalServerError
}

func writeAppDatabaseImportError(w http.ResponseWriter, err error) {
	status := appDatabaseImportErrorStatus(err)
	message := err.Error()
	var maxBytesErr *http.MaxBytesError
	if status == http.StatusRequestEntityTooLarge && errors.As(err, &maxBytesErr) {
		message = fmt.Sprintf("database import request exceeds %d bytes", maxAppDatabaseImportRequestBytes)
	}
	httpx.WriteError(w, status, message)
}

func (s *Server) writeAppDatabaseImportStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrManagedPostgresDatabaseImportConflict) {
		httpx.WriteError(w, http.StatusConflict, store.ManagedPostgresDatabaseImportConflictMessage)
		return
	}
	if errors.Is(err, store.ErrAppDatabaseImportOutcomeUnknown) {
		httpx.WriteError(w, http.StatusConflict, store.AppDatabaseImportAmbiguousMessage)
		return
	}
	s.writeStoreError(w, err)
}

func (s *Server) handleGetAppDatabaseImport(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.read") && !principal.HasScope("app.write") && !principal.HasScope("app.migrate") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.read or app.migrate scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	jobs, err := s.store.ListAppDatabaseImportJobs(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var latest *model.AppDatabaseImportJob
	if len(jobs) > 0 {
		job := jobs[0]
		latest = &job
	}
	httpx.WriteJSON(w, http.StatusOK, model.AppDatabaseImportResponse{
		App: sanitizeAppForAPI(app),
		Job: latest,
	})
}

func (s *Server) handleImportAppDatabase(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.migrate") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.migrate scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	if store.OwnedManagedPostgresSpec(app) == nil {
		httpx.WriteError(w, http.StatusBadRequest, "managed postgres is not configured for this app")
		return
	}
	// Avoid reading and persisting a potentially large dump when the database is
	// already known to be unavailable. CreateAppDatabaseImportJob repeats this
	// validation atomically with the job write to close the suspend race.
	if err := s.store.ValidateAppDatabaseImportRunnable(app.ID); err != nil {
		s.writeAppDatabaseImportStoreError(w, err)
		return
	}

	req, dumpFilename, dumpContentType, dumpBytes, err := decodeAppDatabaseImportMultipart(w, r)
	if err != nil {
		writeAppDatabaseImportError(w, err)
		return
	}
	req.Label = strings.TrimSpace(req.Label)
	req.Format = normalizeAppDatabaseImportRequestFormat(req.Format)
	if req.Format == "" {
		httpx.WriteError(w, http.StatusBadRequest, "format must be auto, sql, or custom")
		return
	}

	upload, err := s.store.CreateSourceUpload(app.TenantID, dumpFilename, dumpContentType, dumpBytes)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	job, err := s.createAppDatabaseImportJobWithNewUpload(upload, model.AppDatabaseImportJob{
		AppID:                app.ID,
		TenantID:             app.TenantID,
		SourceUploadID:       upload.ID,
		SourceUploadFilename: upload.Filename,
		SourceUploadSHA256:   upload.SHA256,
		Label:                req.Label,
		Format:               req.Format,
		Clean:                req.Clean,
		Status:               model.OperationStatusPending,
		RequestedByType:      principal.ActorType,
		RequestedByID:        principal.ActorID,
	})
	if err != nil {
		s.writeAppDatabaseImportStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.database.import", "app", app.ID, app.TenantID, map[string]string{
		"job_id":    job.ID,
		"upload_id": upload.ID,
		"sha256":    upload.SHA256,
		"format":    req.Format,
	})
	httpx.WriteJSON(w, http.StatusAccepted, model.AppDatabaseImportResponse{
		App: sanitizeAppForAPI(app),
		Job: &job,
	})
}

func (s *Server) createAppDatabaseImportJobWithNewUpload(upload model.SourceUpload, candidate model.AppDatabaseImportJob) (model.AppDatabaseImportJob, error) {
	job, err := s.store.CreateAppDatabaseImportJob(candidate)
	if err == nil {
		return job, nil
	}
	// The upload has not been published to the caller yet. Discard requires the
	// exact ID, tenant, and one-time download capability and independently refuses
	// any durable reference, which keeps ambiguous commit failures safe.
	if cleanupErr := s.store.DiscardNewSourceUpload(upload); cleanupErr != nil && s.log != nil {
		s.log.Printf("discard unreferenced database import source upload %s failed: %v", upload.ID, cleanupErr)
	}
	return model.AppDatabaseImportJob{}, err
}

func (s *Server) handleRetryAppDatabaseImport(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.migrate") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.migrate scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	if err := s.store.ValidateAppDatabaseImportRunnable(app.ID); err != nil {
		s.writeAppDatabaseImportStoreError(w, err)
		return
	}
	var req model.AppDatabaseImportRetryRequest
	if r.Body != nil {
		if err := httpx.DecodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	jobs, err := s.store.ListAppDatabaseImportJobs(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var source model.AppDatabaseImportJob
	switch strings.TrimSpace(req.JobID) {
	case "":
		for _, job := range jobs {
			if job.Status == model.OperationStatusFailed {
				source = job
				break
			}
		}
	default:
		source, err = s.store.GetAppDatabaseImportJob(app.ID, req.JobID)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
	}
	if strings.TrimSpace(source.ID) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "no failed database import job is available to retry")
		return
	}
	if source.Status != model.OperationStatusFailed {
		httpx.WriteError(w, http.StatusConflict, "only failed database import jobs can be retried")
		return
	}
	job, err := s.store.CreateAppDatabaseImportJob(model.AppDatabaseImportJob{
		AppID:                app.ID,
		TenantID:             app.TenantID,
		SourceUploadID:       source.SourceUploadID,
		SourceUploadFilename: source.SourceUploadFilename,
		SourceUploadSHA256:   source.SourceUploadSHA256,
		Label:                source.Label,
		Format:               source.Format,
		Clean:                source.Clean,
		Status:               model.OperationStatusPending,
		RetryCount:           source.RetryCount + 1,
		RetryOfJobID:         source.ID,
		RequestedByType:      principal.ActorType,
		RequestedByID:        principal.ActorID,
	})
	if err != nil {
		s.writeAppDatabaseImportStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.database.import.retry", "app", app.ID, app.TenantID, map[string]string{
		"job_id":        job.ID,
		"retry_of_job":  source.ID,
		"source_upload": source.SourceUploadID,
	})
	httpx.WriteJSON(w, http.StatusAccepted, model.AppDatabaseImportResponse{
		App: sanitizeAppForAPI(app),
		Job: &job,
	})
}

func (s *Server) handleListAppDatabaseAccessGrants(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.read") && !principal.HasScope("app.write") && !principal.HasScope("app.migrate") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.read or app.migrate scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	grants, err := s.store.ListAppDatabaseAccessGrants(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.AppDatabaseAccessResponse{
		App:    sanitizeAppForAPI(app),
		Grants: grants,
	})
}

func (s *Server) handleCreateAppDatabaseAccessGrant(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.migrate") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.migrate scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	if store.OwnedManagedPostgresSpec(app) == nil {
		httpx.WriteError(w, http.StatusBadRequest, "managed postgres is not configured for this app")
		return
	}
	var req model.AppDatabaseAccessGrantCreateRequest
	if r.Body != nil {
		if err := httpx.DecodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	mode := normalizeAppDatabaseAccessMode(req.Mode)
	if mode == "" {
		httpx.WriteError(w, http.StatusBadRequest, "mode must be read-write")
		return
	}
	var expiresAt *time.Time
	if req.ExpiresInMinutes > 0 {
		value := time.Now().UTC().Add(time.Duration(req.ExpiresInMinutes) * time.Minute)
		expiresAt = &value
	}
	grant, secret, err := s.store.CreateAppDatabaseAccessGrant(model.AppDatabaseAccessGrant{
		AppID:           app.ID,
		TenantID:        app.TenantID,
		Label:           strings.TrimSpace(req.Label),
		Mode:            mode,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		ExpiresAt:       expiresAt,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.database.access.create", "app", app.ID, app.TenantID, map[string]string{
		"grant_id": grant.ID,
		"mode":     grant.Mode,
		"expires":  formatOptionalTime(grant.ExpiresAt),
	})
	httpx.WriteJSON(w, http.StatusCreated, model.AppDatabaseAccessGrantCreateResponse{
		Grant:  grant,
		Secret: secret,
	})
}

func (s *Server) handleRevokeAppDatabaseAccessGrant(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.migrate") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.migrate scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	grantID := strings.TrimSpace(r.PathValue("grant_id"))
	removed, err := s.store.RevokeAppDatabaseAccessGrant(app.ID, grantID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.database.access.revoke", "app", app.ID, app.TenantID, map[string]string{
		"grant_id": grantID,
		"removed":  strconv.FormatBool(removed),
	})
	httpx.WriteJSON(w, http.StatusOK, model.AppDatabaseAccessRevokeResponse{Removed: removed})
}

func (s *Server) handleAppDatabaseAccessTunnel(w http.ResponseWriter, r *http.Request) {
	appID := strings.TrimSpace(r.PathValue("id"))
	grantID := strings.TrimSpace(r.PathValue("grant_id"))
	secret := strings.TrimSpace(r.URL.Query().Get("token"))
	grant, err := s.store.AuthenticateAppDatabaseAccessGrant(appID, grantID, secret)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if grant.Mode != model.AppDatabaseAccessModeReadWrite {
		httpx.WriteError(w, http.StatusForbidden, "database tunnel grant is not read-write")
		return
	}
	tenant, err := s.store.GetTenant(grant.TenantID)
	if err != nil || tenant.Status == model.TenantStatusAccessHeld {
		httpx.WriteError(w, http.StatusForbidden, "database access unavailable")
		return
	}
	app, err := s.store.GetApp(appID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	connection, err := authorizedDatabaseConnection(app)
	if err != nil {
		httpx.WriteError(w, http.StatusForbidden, err.Error())
		return
	}
	s.appendAudit(model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   "database-access:" + grant.ID,
		TenantID:  app.TenantID,
	}, "app.database.access.tunnel", "app", app.ID, app.TenantID, map[string]string{
		"grant_id": grant.ID,
		"mode":     grant.Mode,
	})
	target := net.JoinHostPort(connection.Host, defaultString(connection.Port, "5432"))
	dialer := s.dialAppDatabaseTunnel
	if dialer == nil {
		var netDialer net.Dialer
		dialer = netDialer.DialContext
	}
	dbConn, err := dialer(r.Context(), "tcp", target)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, fmt.Sprintf("dial app database: %v", err))
		return
	}
	defer dbConn.Close()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	errCh := make(chan error, 2)
	go relayTCPToWebSocket(ws, dbConn, errCh)
	go relayWebSocketToTCP(ws, dbConn, errCh)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-errCh:
			return
		case <-r.Context().Done():
			return
		case <-ticker.C:
			current, err := s.store.GetTenant(grant.TenantID)
			if err != nil || current.Status == model.TenantStatusAccessHeld {
				return
			}
			if _, err := s.store.AuthenticateAppDatabaseAccessGrant(appID, grantID, secret); err != nil {
				return
			}
		}
	}
}

func (s *Server) StartBackgroundAppDatabaseImports(ctx context.Context) {
	appdbimport.Run(ctx, s.store.APIDatabaseImportExecution(), s.appDatabaseImportRunner, s.appDatabaseImportLog)
}

func (s *Server) appDatabaseImportLog(format string, args ...any) {
	if s.log != nil {
		s.log.Printf(format, args...)
	}
}

func decodeAppDatabaseImportMultipart(w http.ResponseWriter, r *http.Request) (model.AppDatabaseImportRequest, string, string, []byte, error) {
	var req model.AppDatabaseImportRequest
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	mediaType, parameters, err := mime.ParseMediaType(contentType)
	if err != nil {
		if strings.EqualFold(strings.TrimSpace(strings.Split(contentType, ";")[0]), "multipart/form-data") {
			return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "multipart Content-Type is malformed")
		}
		return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusUnsupportedMediaType, "Content-Type must be multipart/form-data")
	}
	if !strings.EqualFold(mediaType, "multipart/form-data") {
		return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusUnsupportedMediaType, "Content-Type must be multipart/form-data")
	}
	boundary := strings.TrimSpace(parameters["boundary"])
	if boundary == "" {
		return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "multipart boundary is required")
	}
	if r.ContentLength > maxAppDatabaseImportRequestBytes {
		return req, "", "", nil, newAppDatabaseImportRequestError(
			http.StatusRequestEntityTooLarge,
			fmt.Sprintf("database import request exceeds %d bytes", maxAppDatabaseImportRequestBytes),
		)
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxAppDatabaseImportRequestBytes)
	reader := multipart.NewReader(r.Body, boundary)
	var (
		dumpContentType string
		dumpFilename    string
		dumpPath        string
		dumpSize        int64
		requestSeen     bool
		dumpSeen        bool
	)
	defer func() {
		if dumpPath != "" {
			_ = os.Remove(dumpPath)
		}
	}()

	for {
		part, nextErr := reader.NextPart()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(nextErr, &maxBytesErr) {
				return req, "", "", nil, nextErr
			}
			return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, fmt.Sprintf("parse multipart body: %v", nextErr))
		}

		field := part.FormName()
		dispositionType, disposition, dispositionErr := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
		_, hasFilename := disposition["filename"]
		if dispositionErr != nil || !strings.EqualFold(dispositionType, "form-data") || field == "" {
			_ = part.Close()
			return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "multipart part Content-Disposition must be form-data with a name")
		}

		switch field {
		case "request":
			if requestSeen {
				_ = part.Close()
				return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "multipart field request must be sent exactly once")
			}
			if hasFilename {
				_ = part.Close()
				return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "multipart field request must be a JSON field, not a file")
			}
			requestSeen = true
			rawRequest, readErr := io.ReadAll(io.LimitReader(part, maxAppDatabaseImportRequestFieldBytes+1))
			_ = part.Close()
			if readErr != nil {
				var maxBytesErr *http.MaxBytesError
				if errors.As(readErr, &maxBytesErr) {
					return req, "", "", nil, readErr
				}
				return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, fmt.Sprintf("read multipart field request: %v", readErr))
			}
			if len(rawRequest) > maxAppDatabaseImportRequestFieldBytes {
				return req, "", "", nil, newAppDatabaseImportRequestError(
					http.StatusRequestEntityTooLarge,
					fmt.Sprintf("multipart field request exceeds %d bytes", maxAppDatabaseImportRequestFieldBytes),
				)
			}
			if len(bytes.TrimSpace(rawRequest)) == 0 {
				return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "multipart field request is required")
			}
			decoder := json.NewDecoder(bytes.NewReader(rawRequest))
			decoder.DisallowUnknownFields()
			if decodeErr := decoder.Decode(&req); decodeErr != nil {
				return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, fmt.Sprintf("decode request: %v", decodeErr))
			}
			if decodeErr := decoder.Decode(&struct{}{}); !errors.Is(decodeErr, io.EOF) {
				return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "request must contain a single JSON document")
			}
		case "dump":
			if dumpSeen {
				_ = part.Close()
				return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "multipart file dump must be sent exactly once")
			}
			if !hasFilename {
				_ = part.Close()
				return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "multipart field dump must be a file")
			}
			dumpSeen = true
			dumpFilename = appDatabaseImportDumpFilename(part.FileName())
			dumpContentType = strings.TrimSpace(part.Header.Get("Content-Type"))
			if dumpContentType == "" {
				dumpContentType = "application/octet-stream"
			}
			temp, createErr := os.CreateTemp("", "fugue-database-import-*")
			if createErr != nil {
				_ = part.Close()
				return req, "", "", nil, fmt.Errorf("create database import temporary file: %w", createErr)
			}
			dumpPath = temp.Name()
			dumpSize, err = copyAppDatabaseImportDump(temp, part, maxAppDatabaseImportDumpBytes)
			closeErr := temp.Close()
			_ = part.Close()
			if err != nil {
				var maxBytesErr *http.MaxBytesError
				if errors.As(err, &maxBytesErr) {
					return req, "", "", nil, err
				}
				var pathErr *os.PathError
				if !errors.As(err, &pathErr) {
					return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, fmt.Sprintf("read multipart file dump: %v", err))
				}
				return req, "", "", nil, fmt.Errorf("write database import temporary file: %w", err)
			}
			if closeErr != nil {
				return req, "", "", nil, fmt.Errorf("close database import temporary file: %w", closeErr)
			}
		default:
			_ = part.Close()
			if hasFilename {
				return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, fmt.Sprintf("unsupported multipart file field %q", field))
			}
			return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, fmt.Sprintf("unsupported multipart field %q", field))
		}
	}

	if !requestSeen {
		return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "multipart field request must be sent exactly once")
	}
	if !dumpSeen {
		return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "multipart file dump must be sent exactly once")
	}
	if dumpSize == 0 {
		return req, "", "", nil, newAppDatabaseImportRequestError(http.StatusBadRequest, "dump file is empty")
	}
	dumpBytes, err := os.ReadFile(dumpPath)
	if err != nil {
		return req, "", "", nil, fmt.Errorf("read database import temporary file: %w", err)
	}
	return req, dumpFilename, dumpContentType, dumpBytes, nil
}

func copyAppDatabaseImportDump(destination io.Writer, source io.Reader, maxBytes int64) (int64, error) {
	if destination == nil || source == nil || maxBytes < 0 {
		return 0, errors.New("invalid database import dump boundary")
	}
	written, err := io.Copy(destination, io.LimitReader(source, maxBytes+1))
	if err != nil {
		return written, err
	}
	if written > maxBytes {
		return written, newAppDatabaseImportRequestError(
			http.StatusRequestEntityTooLarge,
			fmt.Sprintf("dump file exceeds %d bytes", maxBytes),
		)
	}
	return written, nil
}

func appDatabaseImportDumpFilename(raw string) string {
	name := strings.TrimSpace(filepath.Base(raw))
	if name == "." || name == "/" || name == "" {
		return "database.dump"
	}
	return name
}

func normalizeAppDatabaseImportRequestFormat(raw string) string {
	return appdbimport.NormalizeFormat(raw)
}

func normalizeAppDatabaseAccessMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppDatabaseAccessModeReadWrite:
		return model.AppDatabaseAccessModeReadWrite
	default:
		return ""
	}
}

func relayTCPToWebSocket(ws *websocket.Conn, conn net.Conn, errCh chan<- error) {
	buffer := make([]byte, appDatabaseTunnelBufferBytes)
	for {
		n, err := conn.Read(buffer)
		if n > 0 {
			_ = ws.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if writeErr := ws.WriteMessage(websocket.BinaryMessage, buffer[:n]); writeErr != nil {
				errCh <- writeErr
				return
			}
		}
		if err != nil {
			errCh <- err
			return
		}
	}
}

func relayWebSocketToTCP(ws *websocket.Conn, conn net.Conn, errCh chan<- error) {
	for {
		messageType, reader, err := ws.NextReader()
		if err != nil {
			errCh <- err
			return
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		if _, err := io.Copy(conn, reader); err != nil {
			errCh <- err
			return
		}
	}
}

package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"

	"github.com/gorilla/websocket"
)

const (
	maxAppDatabaseImportRequestFieldBytes = 64 << 10
	maxAppDatabaseImportDumpBytes         = 128 << 20
	maxAppDatabaseImportRequestBytes      = 130 << 20
	maxAppDatabaseImportExpandedBytes     = 256 << 20
	appDatabaseImportPollInterval         = 5 * time.Second
	appDatabaseImportBatchSize            = 5
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
	app, err := s.store.GetApp(appID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	spec := cloneAppSpec(app.Spec)
	envDetails := mergedAppEnvDetails(app, spec)
	connection, err := resolveAppDatabaseConnection(app, spec, envDetails.Env)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
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
	<-errCh
}

func (s *Server) StartBackgroundAppDatabaseImports(ctx context.Context) {
	ticker := time.NewTicker(appDatabaseImportPollInterval)
	defer ticker.Stop()
	for {
		s.processPendingAppDatabaseImportJobs(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) processPendingAppDatabaseImportJobs(ctx context.Context) {
	jobs, err := s.store.ListPendingAppDatabaseImportJobs(appDatabaseImportBatchSize)
	if err != nil {
		if s.log != nil {
			s.log.Printf("list pending database import jobs failed: %v", err)
		}
		return
	}
	for _, pending := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
		}
		job, err := s.store.ClaimAppDatabaseImportJob(pending.ID)
		if err != nil {
			if !errors.Is(err, store.ErrConflict) && !errors.Is(err, store.ErrNotFound) && s.log != nil {
				s.log.Printf("claim database import job %s failed: %v", pending.ID, err)
			}
			continue
		}
		if _, err := s.store.AppendAppDatabaseImportJobLog(job.ID, "starting database import"); err != nil && s.log != nil {
			s.log.Printf("append database import job log %s failed: %v", job.ID, err)
		}
		runner := s.appDatabaseImportRunner
		if runner == nil {
			runner = s.runAppDatabaseImportJob
		}
		result, runErr := runner(ctx, job)
		if runErr != nil {
			if _, err := s.store.AppendAppDatabaseImportJobLog(job.ID, runErr.Error()); err != nil && s.log != nil {
				s.log.Printf("append failed database import job log %s failed: %v", job.ID, err)
			}
			if _, err := s.store.CompleteAppDatabaseImportJob(job.ID, model.OperationStatusFailed, "", runErr.Error()); err != nil && s.log != nil {
				s.log.Printf("complete failed database import job %s failed: %v", job.ID, err)
			}
			continue
		}
		if _, err := s.store.AppendAppDatabaseImportJobLog(job.ID, "database import completed"); err != nil && s.log != nil {
			s.log.Printf("append completed database import job log %s failed: %v", job.ID, err)
		}
		if _, err := s.store.CompleteAppDatabaseImportJob(job.ID, model.OperationStatusCompleted, result, ""); err != nil && s.log != nil {
			s.log.Printf("complete database import job %s failed: %v", job.ID, err)
		}
	}
}

func (s *Server) runAppDatabaseImportJob(ctx context.Context, job model.AppDatabaseImportJob) (string, error) {
	// Claim applies the same check under the app serialization lock. Keep this
	// final guard immediately before resolving credentials/opening PostgreSQL so
	// alternate runners cannot bypass the lifecycle invariant.
	if err := s.store.ValidateAppDatabaseImportRunnable(job.AppID); err != nil {
		return "", err
	}
	app, err := s.store.GetApp(job.AppID)
	if err != nil {
		return "", err
	}
	if store.OwnedManagedPostgresSpec(app) == nil {
		return "", fmt.Errorf("managed postgres is not configured for this app")
	}
	spec := cloneAppSpec(app.Spec)
	envDetails := mergedAppEnvDetails(app, spec)
	connection, err := resolveAppDatabaseConnection(app, spec, envDetails.Env)
	if err != nil {
		return "", err
	}
	upload, dumpBytes, err := s.store.GetSourceUploadArchive(job.SourceUploadID)
	if err != nil {
		return "", err
	}
	if len(dumpBytes) == 0 {
		return "", fmt.Errorf("database import dump is empty")
	}
	if len(dumpBytes) > maxAppDatabaseImportDumpBytes {
		return "", fmt.Errorf("database import dump exceeds %d bytes", maxAppDatabaseImportDumpBytes)
	}
	data, err := maybeGunzipDatabaseDump(dumpBytes, upload.Filename)
	if err != nil {
		return "", err
	}
	format := detectDatabaseImportFormat(job.Format, data, upload.Filename)
	if format == "" {
		return "", fmt.Errorf("could not detect database import format")
	}
	if _, err := s.store.AppendAppDatabaseImportJobLog(job.ID, fmt.Sprintf("running %s import into %s/%s", format, connection.Host, connection.Database)); err != nil && s.log != nil {
		s.log.Printf("append database import job log %s failed: %v", job.ID, err)
	}
	output, err := runPostgresImportCommand(ctx, connection, format, job.Clean, data)
	if err != nil {
		return "", err
	}
	message := fmt.Sprintf("imported %d bytes into database %s", len(data), connection.Database)
	if strings.TrimSpace(output) != "" {
		message += ": " + trimCommandOutput(output)
	}
	return message, nil
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
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppDatabaseImportFormatAuto:
		return model.AppDatabaseImportFormatAuto
	case model.AppDatabaseImportFormatSQL:
		return model.AppDatabaseImportFormatSQL
	case model.AppDatabaseImportFormatCustom:
		return model.AppDatabaseImportFormatCustom
	default:
		return ""
	}
}

func normalizeAppDatabaseAccessMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppDatabaseAccessModeReadWrite:
		return model.AppDatabaseAccessModeReadWrite
	default:
		return ""
	}
}

func maybeGunzipDatabaseDump(data []byte, filename string) ([]byte, error) {
	return maybeGunzipDatabaseDumpWithLimit(data, filename, maxAppDatabaseImportExpandedBytes)
}

func maybeGunzipDatabaseDumpWithLimit(data []byte, filename string, maxExpandedBytes int64) ([]byte, error) {
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		if !strings.HasSuffix(strings.ToLower(strings.TrimSpace(filename)), ".gz") {
			return data, nil
		}
	}
	if maxExpandedBytes < 0 {
		return nil, errors.New("invalid gzip database import expansion boundary")
	}
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("open gzip dump: %w", err)
	}
	defer reader.Close()
	out, err := io.ReadAll(io.LimitReader(reader, maxExpandedBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read gzip dump: %w", err)
	}
	if int64(len(out)) > maxExpandedBytes {
		return nil, fmt.Errorf("gzip database import expands beyond %d bytes", maxExpandedBytes)
	}
	return out, nil
}

func detectDatabaseImportFormat(requested string, data []byte, filename string) string {
	requested = normalizeAppDatabaseImportRequestFormat(requested)
	if requested != model.AppDatabaseImportFormatAuto {
		return requested
	}
	if bytes.HasPrefix(data, []byte("PGDMP")) {
		return model.AppDatabaseImportFormatCustom
	}
	lowerName := strings.ToLower(strings.TrimSpace(filename))
	switch {
	case strings.HasSuffix(lowerName, ".dump"), strings.HasSuffix(lowerName, ".backup"), strings.HasSuffix(lowerName, ".custom"):
		return model.AppDatabaseImportFormatCustom
	default:
		return model.AppDatabaseImportFormatSQL
	}
}

func runPostgresImportCommand(ctx context.Context, connection appDatabaseConnection, format string, clean bool, data []byte) (string, error) {
	pgEnv, commonArgs, err := postgresCommandEnvAndArgs(connection)
	if err != nil {
		return "", err
	}
	tempFile, err := os.CreateTemp("", "fugue-db-import-*")
	if err != nil {
		return "", fmt.Errorf("create temporary database import file: %w", err)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)
	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		return "", fmt.Errorf("write temporary database import file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return "", fmt.Errorf("close temporary database import file: %w", err)
	}

	var name string
	var args []string
	switch format {
	case model.AppDatabaseImportFormatCustom:
		name = "pg_restore"
		args = append([]string{"--no-owner", "--no-privileges"}, commonArgs...)
		if clean {
			args = append(args, "--clean", "--if-exists")
		}
		args = append(args, tempPath)
	case model.AppDatabaseImportFormatSQL:
		name = "psql"
		args = append([]string{"--set", "ON_ERROR_STOP=on"}, commonArgs...)
		if clean {
			args = append(args, "--command", sqlCleanPublicSchema(connection.User))
		}
		args = append(args, "--file", tempPath)
	default:
		return "", fmt.Errorf("unsupported database import format %q", format)
	}

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), pgEnv...)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Run(); err != nil {
		return output.String(), fmt.Errorf("%s failed: %w: %s", name, err, trimCommandOutput(output.String()))
	}
	return output.String(), nil
}

func postgresCommandEnvAndArgs(connection appDatabaseConnection) ([]string, []string, error) {
	parsed, err := url.Parse(connection.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("parse database dsn: %w", err)
	}
	host := firstNonEmptyString(connection.Host, parsed.Hostname())
	port := firstNonEmptyString(connection.Port, parsed.Port(), "5432")
	database := firstNonEmptyString(connection.Database, strings.TrimPrefix(parsed.Path, "/"))
	user := firstNonEmptyString(connection.User, parsed.User.Username())
	if host == "" || database == "" || user == "" {
		return nil, nil, fmt.Errorf("database connection is missing host, database, or user")
	}
	password, _ := parsed.User.Password()
	env := []string{
		"PGHOST=" + host,
		"PGPORT=" + port,
		"PGDATABASE=" + database,
		"PGUSER=" + user,
		"PGSSLMODE=" + firstNonEmptyString(parsed.Query().Get("sslmode"), "disable"),
	}
	if password != "" {
		env = append(env, "PGPASSWORD="+password)
	}
	args := []string{
		"--host", host,
		"--port", port,
		"--username", user,
		"--dbname", database,
	}
	return env, args, nil
}

func sqlCleanPublicSchema(owner string) string {
	quotedOwner := quotePostgresIdentifier(strings.TrimSpace(owner))
	if quotedOwner == "" {
		quotedOwner = "CURRENT_USER"
	}
	return "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO " + quotedOwner + "; GRANT ALL ON SCHEMA public TO public;"
}

func quotePostgresIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func trimCommandOutput(output string) string {
	output = strings.TrimSpace(output)
	if len(output) <= 2000 {
		return output
	}
	return output[:2000] + "...[truncated]"
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

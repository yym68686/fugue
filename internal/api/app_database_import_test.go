package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestImportAppDatabaseCreatesPendingJob(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	})
	body, contentType := newAppDatabaseImportMultipartBody(t, model.AppDatabaseImportRequest{
		Label:  "legacy-vps",
		Format: model.AppDatabaseImportFormatSQL,
		Clean:  true,
	}, "dump.sql", []byte("select 1;"))

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/"+app.ID+"/database/import", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var response model.AppDatabaseImportResponse
	mustDecodeJSON(t, recorder, &response)
	if response.Job == nil || response.Job.Status != model.OperationStatusPending || response.Job.Format != model.AppDatabaseImportFormatSQL {
		t.Fatalf("unexpected import response: %+v", response)
	}
	if !response.Job.Clean || response.Job.Label != "legacy-vps" {
		t.Fatalf("expected clean import label, got %+v", response.Job)
	}
	upload, dumpBytes, err := stateStore.GetSourceUploadArchive(response.Job.SourceUploadID)
	if err != nil {
		t.Fatalf("get source upload archive: %v", err)
	}
	if upload.Filename != "dump.sql" || string(dumpBytes) != "select 1;" {
		t.Fatalf("unexpected stored dump upload=%+v data=%q", upload, string(dumpBytes))
	}
}

func TestImportAppDatabaseReturnsStableConflictBeforeReadingDumpWhenManagedPostgresUnavailable(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		prepare func(*testing.T, *store.Store, model.App, model.BackingService)
	}{
		{
			name: "suspending",
			prepare: func(t *testing.T, stateStore *store.Store, app model.App, service model.BackingService) {
				t.Helper()
				if _, err := stateStore.CreateOperation(model.Operation{
					TenantID:  app.TenantID,
					Type:      model.OperationTypeDatabaseSuspend,
					AppID:     app.ID,
					ServiceID: service.ID,
				}); err != nil {
					t.Fatalf("create active suspend: %v", err)
				}
			},
		},
		{
			name: "suspended",
			prepare: func(t *testing.T, stateStore *store.Store, app model.App, service model.BackingService) {
				t.Helper()
				completeManagedPostgresLifecycleForTest(t, stateStore, app, service, model.OperationTypeDatabaseSuspend)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
			testCase.prepare(t, stateStore, app, service)
			// An invalid body proves the lifecycle preflight runs before multipart
			// parsing or dump persistence.
			req := httptest.NewRequest(http.MethodPost, "/v1/apps/"+app.ID+"/database/import", strings.NewReader("sensitive-dump-marker"))
			req.Header.Set("Authorization", "Bearer "+apiKey)
			req.Header.Set("Content-Type", "application/octet-stream")
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, req)

			if recorder.Code != http.StatusConflict {
				t.Fatalf("status = %d body=%s, want 409", recorder.Code, recorder.Body.String())
			}
			if !strings.Contains(recorder.Body.String(), store.ManagedPostgresDatabaseImportConflictMessage) {
				t.Fatalf("conflict body = %s, want stable message %q", recorder.Body.String(), store.ManagedPostgresDatabaseImportConflictMessage)
			}
			if strings.Contains(recorder.Body.String(), "sensitive-dump-marker") || strings.Contains(recorder.Body.String(), "lifecycle-secret") {
				t.Fatalf("conflict leaked dump or database secret: %s", recorder.Body.String())
			}
			jobs, err := stateStore.ListAppDatabaseImportJobs(app.ID)
			if err != nil {
				t.Fatalf("list database import jobs: %v", err)
			}
			if len(jobs) != 0 {
				t.Fatalf("blocked request created jobs: %+v", jobs)
			}
		})
	}
}

func TestRetryAppDatabaseImportReturnsStableConflictWhenManagedPostgresSuspended(t *testing.T) {
	stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
	upload, err := stateStore.CreateSourceUpload(app.TenantID, "dump.sql", "application/sql", []byte("select 1;"))
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}
	job, err := stateStore.CreateAppDatabaseImportJob(model.AppDatabaseImportJob{
		AppID:                app.ID,
		TenantID:             app.TenantID,
		SourceUploadID:       upload.ID,
		SourceUploadFilename: upload.Filename,
		SourceUploadSHA256:   upload.SHA256,
		Format:               model.AppDatabaseImportFormatSQL,
		Status:               model.OperationStatusPending,
	})
	if err != nil {
		t.Fatalf("create source database import job: %v", err)
	}
	if _, err := stateStore.ClaimAppDatabaseImportJob(job.ID); err != nil {
		t.Fatalf("claim source database import job: %v", err)
	}
	if _, err := stateStore.CompleteAppDatabaseImportJob(job.ID, model.OperationStatusFailed, "", "test failure"); err != nil {
		t.Fatalf("fail source database import job: %v", err)
	}
	completeManagedPostgresLifecycleForTest(t, stateStore, app, service, model.OperationTypeDatabaseSuspend)

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/database/import/retry", apiKey, map[string]any{"job_id": job.ID})
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), store.ManagedPostgresDatabaseImportConflictMessage) {
		t.Fatalf("retry status=%d body=%s, want stable 409", recorder.Code, recorder.Body.String())
	}
	jobs, err := stateStore.ListAppDatabaseImportJobs(app.ID)
	if err != nil {
		t.Fatalf("list database import jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("blocked retry created another job: %+v", jobs)
	}
}

func TestRunAppDatabaseImportJobDefendsAgainstSuspendedManagedPostgres(t *testing.T) {
	stateStore, server, _, app, service := managedPostgresLifecycleFixture(t, 0)
	completeManagedPostgresLifecycleForTest(t, stateStore, app, service, model.OperationTypeDatabaseSuspend)

	_, err := server.runAppDatabaseImportJob(context.Background(), model.AppDatabaseImportJob{
		ID:             "dbimport_defense",
		AppID:          app.ID,
		SourceUploadID: "upload_must_not_be_read",
	})
	if !errors.Is(err, store.ErrManagedPostgresDatabaseImportConflict) {
		t.Fatalf("runAppDatabaseImportJob error = %v, want managed postgres import conflict", err)
	}
}

func TestCreateAppDatabaseImportJobRaceDiscardsUnpublishedUpload(t *testing.T) {
	stateStore, server, _, app, service := managedPostgresLifecycleFixture(t, 0)
	if _, err := stateStore.CreateOperation(model.Operation{
		TenantID:  app.TenantID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     app.ID,
		ServiceID: service.ID,
	}); err != nil {
		t.Fatalf("create suspend that wins post-preflight race: %v", err)
	}
	upload, err := stateStore.CreateSourceUpload(app.TenantID, "race.sql", "application/sql", []byte("sensitive-race-dump"))
	if err != nil {
		t.Fatalf("create unpublished source upload: %v", err)
	}

	_, err = server.createAppDatabaseImportJobWithNewUpload(upload, model.AppDatabaseImportJob{
		AppID:                app.ID,
		TenantID:             app.TenantID,
		SourceUploadID:       upload.ID,
		SourceUploadFilename: upload.Filename,
		SourceUploadSHA256:   upload.SHA256,
		Format:               model.AppDatabaseImportFormatSQL,
		Status:               model.OperationStatusPending,
	})
	if !errors.Is(err, store.ErrManagedPostgresDatabaseImportConflict) {
		t.Fatalf("create job race error = %v, want managed postgres import conflict", err)
	}
	if _, _, err := stateStore.GetSourceUploadArchive(upload.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unpublished race upload was not discarded: %v", err)
	}
	jobs, err := stateStore.ListAppDatabaseImportJobs(app.ID)
	if err != nil {
		t.Fatalf("list database import jobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("failed create race published a job: %+v", jobs)
	}
}

func TestBackingServiceLifecycleBusyConflictsReturnDirectionSpecificMessages(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name        string
		err         error
		wantMessage string
		wantAudit   string
	}{
		{
			name:        "backup in progress blocks suspend",
			err:         store.ErrManagedPostgresBackupInProgressConflict,
			wantMessage: store.ManagedPostgresBackupInProgressConflictMessage,
			wantAudit:   "rejected_backup_in_progress",
		},
		{
			name:        "import in progress blocks suspend",
			err:         store.ErrManagedPostgresImportInProgressConflict,
			wantMessage: store.ManagedPostgresImportInProgressConflictMessage,
			wantAudit:   "rejected_import_in_progress",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			(&Server{}).writeBackingServiceLifecycleStoreError(recorder, testCase.err)
			if recorder.Code != http.StatusConflict {
				t.Fatalf("status = %d body=%s, want 409", recorder.Code, recorder.Body.String())
			}
			var response struct {
				Error string `json:"error"`
			}
			mustDecodeJSON(t, recorder, &response)
			if response.Error != testCase.wantMessage {
				t.Fatalf("error = %q, want %q", response.Error, testCase.wantMessage)
			}
			if got := lifecycleStoreErrorResult(testCase.err); got != testCase.wantAudit {
				t.Fatalf("audit result = %q, want %q", got, testCase.wantAudit)
			}
		})
	}
}

func TestImportAppDatabaseRejectsInvalidMediaTypeAndMultipartInventory(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/database-import:latest",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "import_target",
			User:     "importer",
			Password: "test-only-password",
		},
	})
	target := "/v1/apps/" + app.ID + "/database/import"

	wrongMedia := httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{}`))
	wrongMedia.Header.Set("Authorization", "Bearer "+apiKey)
	wrongMedia.Header.Set("Content-Type", "application/json")
	wrongMediaRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(wrongMediaRecorder, wrongMedia)
	if wrongMediaRecorder.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("wrong media status = %d body=%s, want 415", wrongMediaRecorder.Code, wrongMediaRecorder.Body.String())
	}

	malformedMedia := httptest.NewRequest(http.MethodPost, target, strings.NewReader("body"))
	malformedMedia.Header.Set("Authorization", "Bearer "+apiKey)
	malformedMedia.Header.Set("Content-Type", "multipart/form-data; boundary")
	malformedMediaRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(malformedMediaRecorder, malformedMedia)
	if malformedMediaRecorder.Code != http.StatusBadRequest {
		t.Fatalf("malformed multipart media status = %d body=%s, want 400", malformedMediaRecorder.Code, malformedMediaRecorder.Body.String())
	}

	completeBody, completeContentType := newAppDatabaseImportMultipartBody(t, model.AppDatabaseImportRequest{}, "dump.sql", []byte("select 1;"))
	truncatedBytes := completeBody.Bytes()[:completeBody.Len()-8]
	truncated := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(truncatedBytes))
	truncated.Header.Set("Authorization", "Bearer "+apiKey)
	truncated.Header.Set("Content-Type", completeContentType)
	truncatedRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(truncatedRecorder, truncated)
	if truncatedRecorder.Code != http.StatusBadRequest {
		t.Fatalf("truncated multipart status = %d body=%s, want 400", truncatedRecorder.Code, truncatedRecorder.Body.String())
	}

	cases := []struct {
		name  string
		build func(*multipart.Writer)
	}{
		{
			name: "missing request",
			build: func(writer *multipart.Writer) {
				writeAppDatabaseImportDumpPart(t, writer, "dump.sql", []byte("select 1;"))
			},
		},
		{
			name: "duplicate request",
			build: func(writer *multipart.Writer) {
				writeAppDatabaseImportRequestPart(t, writer, `{}`)
				writeAppDatabaseImportRequestPart(t, writer, `{}`)
				writeAppDatabaseImportDumpPart(t, writer, "dump.sql", []byte("select 1;"))
			},
		},
		{
			name: "missing dump",
			build: func(writer *multipart.Writer) {
				writeAppDatabaseImportRequestPart(t, writer, `{}`)
			},
		},
		{
			name: "duplicate dump",
			build: func(writer *multipart.Writer) {
				writeAppDatabaseImportRequestPart(t, writer, `{}`)
				writeAppDatabaseImportDumpPart(t, writer, "one.sql", []byte("select 1;"))
				writeAppDatabaseImportDumpPart(t, writer, "two.sql", []byte("select 2;"))
			},
		},
		{
			name: "unknown scalar field",
			build: func(writer *multipart.Writer) {
				writeAppDatabaseImportRequestPart(t, writer, `{}`)
				writeAppDatabaseImportDumpPart(t, writer, "dump.sql", []byte("select 1;"))
				if err := writer.WriteField("unexpected", "value"); err != nil {
					t.Fatalf("write unexpected field: %v", err)
				}
			},
		},
		{
			name: "unknown file field",
			build: func(writer *multipart.Writer) {
				writeAppDatabaseImportRequestPart(t, writer, `{}`)
				writeAppDatabaseImportDumpPart(t, writer, "dump.sql", []byte("select 1;"))
				part, err := writer.CreateFormFile("unexpected", "other.bin")
				if err != nil {
					t.Fatalf("create unexpected file: %v", err)
				}
				_, _ = part.Write([]byte("other"))
			},
		},
		{
			name: "request sent as file",
			build: func(writer *multipart.Writer) {
				part, err := writer.CreateFormFile("request", "request.json")
				if err != nil {
					t.Fatalf("create request file: %v", err)
				}
				_, _ = part.Write([]byte(`{}`))
				writeAppDatabaseImportDumpPart(t, writer, "dump.sql", []byte("select 1;"))
			},
		},
		{
			name: "dump sent as scalar",
			build: func(writer *multipart.Writer) {
				writeAppDatabaseImportRequestPart(t, writer, `{}`)
				if err := writer.WriteField("dump", "select 1;"); err != nil {
					t.Fatalf("write scalar dump: %v", err)
				}
			},
		},
		{
			name: "unknown request property",
			build: func(writer *multipart.Writer) {
				writeAppDatabaseImportRequestPart(t, writer, `{"unknown":true}`)
				writeAppDatabaseImportDumpPart(t, writer, "dump.sql", []byte("select 1;"))
			},
		},
		{
			name: "multiple JSON documents",
			build: func(writer *multipart.Writer) {
				writeAppDatabaseImportRequestPart(t, writer, `{} {}`)
				writeAppDatabaseImportDumpPart(t, writer, "dump.sql", []byte("select 1;"))
			},
		},
		{
			name: "empty dump",
			build: func(writer *multipart.Writer) {
				writeAppDatabaseImportRequestPart(t, writer, `{}`)
				writeAppDatabaseImportDumpPart(t, writer, "dump.sql", nil)
			},
		},
		{
			name: "inline request disposition",
			build: func(writer *multipart.Writer) {
				header := make(textproto.MIMEHeader)
				header.Set("Content-Disposition", `inline; name="request"`)
				part, err := writer.CreatePart(header)
				if err != nil {
					t.Fatalf("create inline request part: %v", err)
				}
				_, _ = part.Write([]byte(`{}`))
				writeAppDatabaseImportDumpPart(t, writer, "dump.sql", []byte("select 1;"))
			},
		},
		{
			name: "attachment dump disposition",
			build: func(writer *multipart.Writer) {
				writeAppDatabaseImportRequestPart(t, writer, `{}`)
				header := make(textproto.MIMEHeader)
				header.Set("Content-Disposition", `attachment; name="dump"; filename="dump.sql"`)
				part, err := writer.CreatePart(header)
				if err != nil {
					t.Fatalf("create attachment dump part: %v", err)
				}
				_, _ = part.Write([]byte("select 1;"))
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			body, contentType := newCustomAppDatabaseImportMultipartBody(t, testCase.build)
			request := httptest.NewRequest(http.MethodPost, target, body)
			request.Header.Set("Authorization", "Bearer "+apiKey)
			request.Header.Set("Content-Type", contentType)
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body=%s, want 400", recorder.Code, recorder.Body.String())
			}
		})
	}

	jobs, err := stateStore.ListAppDatabaseImportJobs(app.ID)
	if err != nil {
		t.Fatalf("list import jobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("invalid requests created import jobs: %+v", jobs)
	}
}

func TestImportAppDatabaseEnforcesRequestAndEnvelopeLimits(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/database-import-limits:latest",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "import_limits",
			User:     "importer",
			Password: "test-only-password",
		},
	})
	target := "/v1/apps/" + app.ID + "/database/import"

	body, contentType := newAppDatabaseImportMultipartBody(t, model.AppDatabaseImportRequest{}, "dump.sql", []byte("select 1;"))
	preflight := httptest.NewRequest(http.MethodPost, target, body)
	preflight.Header.Set("Authorization", "Bearer "+apiKey)
	preflight.Header.Set("Content-Type", contentType)
	preflight.ContentLength = maxAppDatabaseImportRequestBytes + 1
	preflightRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(preflightRecorder, preflight)
	if preflightRecorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("envelope preflight status = %d body=%s, want 413", preflightRecorder.Code, preflightRecorder.Body.String())
	}

	oversizedRequest := `{"label":"` + strings.Repeat("x", maxAppDatabaseImportRequestFieldBytes) + `"}`
	requestBody, requestContentType := newCustomAppDatabaseImportMultipartBody(t, func(writer *multipart.Writer) {
		writeAppDatabaseImportRequestPart(t, writer, oversizedRequest)
		writeAppDatabaseImportDumpPart(t, writer, "dump.sql", []byte("select 1;"))
	})
	request := httptest.NewRequest(http.MethodPost, target, requestBody)
	request.Header.Set("Authorization", "Bearer "+apiKey)
	request.Header.Set("Content-Type", requestContentType)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("request field limit status = %d body=%s, want 413", recorder.Code, recorder.Body.String())
	}
}

func TestDatabaseImportDumpAndGzipHelpersEnforceHardLimits(t *testing.T) {
	t.Parallel()

	var destination bytes.Buffer
	written, err := copyAppDatabaseImportDump(&destination, strings.NewReader("abcdef"), 5)
	if written != 6 || appDatabaseImportErrorStatus(err) != http.StatusRequestEntityTooLarge {
		t.Fatalf("bounded dump copy written=%d err=%v status=%d", written, err, appDatabaseImportErrorStatus(err))
	}

	var compressed bytes.Buffer
	zipWriter := gzip.NewWriter(&compressed)
	if _, err := zipWriter.Write([]byte("0123456789")); err != nil {
		t.Fatalf("write gzip fixture: %v", err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatalf("close gzip fixture: %v", err)
	}
	if _, err := maybeGunzipDatabaseDumpWithLimit(compressed.Bytes(), "dump.sql.gz", 5); err == nil || !strings.Contains(err.Error(), "expands beyond 5 bytes") {
		t.Fatalf("gzip expansion error = %v", err)
	}
	expanded, err := maybeGunzipDatabaseDumpWithLimit(compressed.Bytes(), "dump.sql.gz", 10)
	if err != nil || string(expanded) != "0123456789" {
		t.Fatalf("bounded gzip expansion = %q err=%v", expanded, err)
	}
}

func TestAppDatabaseAccessGrantCanBeCreatedAndRevoked(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	})

	create := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/database/access", apiKey, map[string]any{
		"label":              "legacy-vps",
		"expires_in_minutes": 30,
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, create.Code, create.Body.String())
	}
	var createResponse model.AppDatabaseAccessGrantCreateResponse
	mustDecodeJSON(t, create, &createResponse)
	if createResponse.Secret == "" || createResponse.Grant.ID == "" || createResponse.Grant.TokenHash != "" {
		t.Fatalf("unexpected create response: %+v", createResponse)
	}
	if _, err := stateStore.AuthenticateAppDatabaseAccessGrant(app.ID, createResponse.Grant.ID, createResponse.Secret); err != nil {
		t.Fatalf("authenticate access grant: %v", err)
	}

	revoke := performJSONRequest(t, server, http.MethodDelete, "/v1/apps/"+app.ID+"/database/access/"+createResponse.Grant.ID, apiKey, nil)
	if revoke.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, revoke.Code, revoke.Body.String())
	}
	if _, err := stateStore.AuthenticateAppDatabaseAccessGrant(app.ID, createResponse.Grant.ID, createResponse.Secret); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected revoked grant conflict, got %v", err)
	}
}

func newAppDatabaseImportMultipartBody(t *testing.T, req model.AppDatabaseImportRequest, dumpName string, dumpBytes []byte) (*bytes.Buffer, string) {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	requestJSON, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if err := writer.WriteField("request", string(requestJSON)); err != nil {
		t.Fatalf("write request field: %v", err)
	}
	part, err := writer.CreateFormFile("dump", dumpName)
	if err != nil {
		t.Fatalf("create dump part: %v", err)
	}
	if _, err := part.Write(dumpBytes); err != nil {
		t.Fatalf("write dump part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	return &body, writer.FormDataContentType()
}

func newCustomAppDatabaseImportMultipartBody(t *testing.T, build func(*multipart.Writer)) (*bytes.Buffer, string) {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	build(writer)
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	return &body, writer.FormDataContentType()
}

func writeAppDatabaseImportRequestPart(t *testing.T, writer *multipart.Writer, raw string) {
	t.Helper()
	if err := writer.WriteField("request", raw); err != nil {
		t.Fatalf("write request field: %v", err)
	}
}

func writeAppDatabaseImportDumpPart(t *testing.T, writer *multipart.Writer, name string, data []byte) {
	t.Helper()
	part, err := writer.CreateFormFile("dump", name)
	if err != nil {
		t.Fatalf("create dump part: %v", err)
	}
	if _, err := part.Write(data); err != nil {
		t.Fatalf("write dump part: %v", err)
	}
}

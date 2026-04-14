package api

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestImportUploadAppQueuesPendingImportWithPersistentStorage(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Upload Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "uploader", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain: "apps.example.com",
	})

	archiveBytes := mustTarGz(t, map[string]string{
		"index.html": "<h1>demo</h1>\n",
	})
	startupCommand := "npm run preview"
	body, contentType := newImportUploadMultipartBody(t, importUploadRequest{
		Name:           "demo-app",
		BuildStrategy:  model.AppBuildStrategyStaticSite,
		StartupCommand: &startupCommand,
		PersistentStorage: &model.AppPersistentStorageSpec{
			Mounts: []model.AppPersistentStorageMount{
				{
					Kind: "directory",
					Path: "/var/lib/data",
				},
				{
					Kind:        "file",
					Path:        "/srv/config.json",
					SeedContent: "{\"demo\":true}",
				},
			},
		},
	}, "demo-app.tgz", archiveBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App       model.App       `json:"app"`
		Operation model.Operation `json:"operation"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.App.ID == "" {
		t.Fatal("expected app id in response")
	}
	if response.Operation.ID == "" {
		t.Fatal("expected operation id in response")
	}
	if response.Operation.Type != model.OperationTypeImport {
		t.Fatalf("expected import operation, got %q", response.Operation.Type)
	}

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.DesiredSource == nil {
		t.Fatal("expected desired source on queued operation")
	}
	if op.DesiredSource.Type != model.AppSourceTypeUpload {
		t.Fatalf("expected upload source type, got %q", op.DesiredSource.Type)
	}
	if op.DesiredSource.UploadID == "" {
		t.Fatal("expected upload id on queued source")
	}

	upload, archiveData, err := s.GetSourceUploadArchive(op.DesiredSource.UploadID)
	if err != nil {
		t.Fatalf("get source upload archive: %v", err)
	}
	if upload.TenantID != tenant.ID {
		t.Fatalf("expected upload tenant %q, got %q", tenant.ID, upload.TenantID)
	}
	if len(archiveData) == 0 {
		t.Fatal("expected stored archive bytes")
	}

	app, err := s.GetApp(response.App.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if len(app.Spec.Command) != 3 || app.Spec.Command[0] != "sh" || app.Spec.Command[1] != "-lc" || app.Spec.Command[2] != startupCommand {
		t.Fatalf("expected app command to wrap startup command, got %#v", app.Spec.Command)
	}
	if app.Spec.PersistentStorage == nil || len(app.Spec.PersistentStorage.Mounts) != 2 {
		t.Fatalf("expected app persistent storage mounts, got %+v", app.Spec.PersistentStorage)
	}
	if got := app.Spec.PersistentStorage.Mounts[0].Mode; got != 0o755 {
		t.Fatalf("expected directory mount mode 0755, got %o", got)
	}
	if got := app.Spec.PersistentStorage.Mounts[1].Mode; got != 0o644 {
		t.Fatalf("expected file mount mode 0644, got %o", got)
	}
	if op.DesiredSpec == nil {
		t.Fatal("expected desired spec on queued operation")
	}
	if len(op.DesiredSpec.Command) != 3 || op.DesiredSpec.Command[0] != "sh" || op.DesiredSpec.Command[1] != "-lc" || op.DesiredSpec.Command[2] != startupCommand {
		t.Fatalf("expected desired spec command to wrap startup command, got %#v", op.DesiredSpec.Command)
	}
	if op.DesiredSpec.PersistentStorage == nil || len(op.DesiredSpec.PersistentStorage.Mounts) != 2 {
		t.Fatalf("expected desired spec persistent storage mounts, got %+v", op.DesiredSpec.PersistentStorage)
	}
}

func TestImportUploadAppDerivesNameFromArchiveWhenRequestNameBlank(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Upload Derive Name Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "uploader", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain: "apps.example.com",
	})

	archiveBytes := mustTarGz(t, map[string]string{
		"index.html": "<h1>demo</h1>\n",
	})
	body, contentType := newImportUploadMultipartBody(t, importUploadRequest{
		BuildStrategy: model.AppBuildStrategyStaticSite,
	}, "demo-main.tgz", archiveBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App       model.App       `json:"app"`
		Operation model.Operation `json:"operation"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.App.Name != "demo-main" {
		t.Fatalf("expected derived app name demo-main, got %q", response.App.Name)
	}
	if response.Operation.ID == "" {
		t.Fatal("expected operation id in response")
	}
}

func TestImportUploadAppAcceptsZipArchive(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Upload Zip Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "uploader", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain: "apps.example.com",
	})

	archiveBytes := mustZip(t, map[string]string{
		"demo-main/index.html": "<h1>zip upload</h1>\n",
	})
	body, contentType := newImportUploadMultipartBody(t, importUploadRequest{
		BuildStrategy: model.AppBuildStrategyStaticSite,
	}, "demo-main.zip", archiveBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App       model.App       `json:"app"`
		Operation model.Operation `json:"operation"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.App.Name != "demo-main" {
		t.Fatalf("expected derived app name demo-main, got %q", response.App.Name)
	}
	if response.Operation.ID == "" {
		t.Fatal("expected operation id in response")
	}

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.DesiredSource == nil {
		t.Fatal("expected desired source on queued operation")
	}
	if op.DesiredSource.UploadID == "" {
		t.Fatal("expected upload id on queued source")
	}

	upload, archiveData, err := s.GetSourceUploadArchive(op.DesiredSource.UploadID)
	if err != nil {
		t.Fatalf("get source upload archive: %v", err)
	}
	if upload.Filename != "demo-main.zip" {
		t.Fatalf("expected stored upload filename demo-main.zip, got %q", upload.Filename)
	}
	if len(archiveData) == 0 {
		t.Fatal("expected stored archive bytes")
	}
}

func TestImportUploadBackgroundModeSkipsRouteAndServicePort(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Upload Background Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "uploader", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain: "apps.example.com",
	})

	archiveBytes := mustTarGz(t, map[string]string{
		"Dockerfile": "FROM nginx:alpine\n",
	})
	body, contentType := newImportUploadMultipartBody(t, importUploadRequest{
		Name:           "demo-app",
		BuildStrategy:  model.AppBuildStrategyDockerfile,
		NetworkMode:    model.AppNetworkModeBackground,
		ServicePort:    8080,
		DockerfilePath: "Dockerfile",
	}, "demo-app.tgz", archiveBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App       model.App       `json:"app"`
		Operation model.Operation `json:"operation"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	app, err := s.GetApp(response.App.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Spec.NetworkMode != model.AppNetworkModeBackground {
		t.Fatalf("expected app network mode %q, got %q", model.AppNetworkModeBackground, app.Spec.NetworkMode)
	}
	if len(app.Spec.Ports) != 0 {
		t.Fatalf("expected background app to clear service ports, got %v", app.Spec.Ports)
	}
	if app.Route != nil && app.Route.Hostname != "" {
		t.Fatalf("expected background app route to stay empty, got %+v", app.Route)
	}

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.DesiredSpec == nil {
		t.Fatal("expected desired spec on queued operation")
	}
	if op.DesiredSpec.NetworkMode != model.AppNetworkModeBackground {
		t.Fatalf("expected desired spec network mode %q, got %q", model.AppNetworkModeBackground, op.DesiredSpec.NetworkMode)
	}
	if len(op.DesiredSpec.Ports) != 0 {
		t.Fatalf("expected background desired spec to clear service ports, got %v", op.DesiredSpec.Ports)
	}
}

func TestImportUploadInternalModeSkipsRouteButExposesInternalService(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Upload Internal Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "uploader", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	archiveBytes := mustTarGz(t, map[string]string{
		"Dockerfile": "FROM nginx:alpine\n",
	})
	body, contentType := newImportUploadMultipartBody(t, importUploadRequest{
		Name:           "demo-app",
		BuildStrategy:  model.AppBuildStrategyDockerfile,
		NetworkMode:    model.AppNetworkModeInternal,
		ServicePort:    7777,
		DockerfilePath: "Dockerfile",
	}, "demo-app.tgz", archiveBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App       model.App       `json:"app"`
		Operation model.Operation `json:"operation"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if response.App.InternalService == nil || response.App.InternalService.Port != 7777 {
		t.Fatalf("expected internal service metadata with port 7777, got %+v", response.App.InternalService)
	}
	if response.App.Route != nil && response.App.Route.Hostname != "" {
		t.Fatalf("expected response app route to stay empty, got %+v", response.App.Route)
	}

	app, err := s.GetApp(response.App.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Spec.NetworkMode != model.AppNetworkModeInternal {
		t.Fatalf("expected app network mode %q, got %q", model.AppNetworkModeInternal, app.Spec.NetworkMode)
	}
	if len(app.Spec.Ports) != 1 || app.Spec.Ports[0] != 7777 {
		t.Fatalf("expected internal app to preserve service port 7777, got %v", app.Spec.Ports)
	}
	if app.Route != nil && app.Route.Hostname != "" {
		t.Fatalf("expected internal app route to stay empty, got %+v", app.Route)
	}

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.DesiredSpec == nil {
		t.Fatal("expected desired spec on queued operation")
	}
	if op.DesiredSpec.NetworkMode != model.AppNetworkModeInternal {
		t.Fatalf("expected desired spec network mode %q, got %q", model.AppNetworkModeInternal, op.DesiredSpec.NetworkMode)
	}
	if len(op.DesiredSpec.Ports) != 1 || op.DesiredSpec.Ports[0] != 7777 {
		t.Fatalf("expected internal desired spec to preserve service port 7777, got %v", op.DesiredSpec.Ports)
	}
}

func TestImportUploadAppImportsComposeTopology(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Upload Compose Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "uploader", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain: "apps.example.com",
	})

	archiveBytes := mustTarGz(t, map[string]string{
		"docker-compose.yml": `
services:
  web:
    build:
      context: ./web
      dockerfile: Dockerfile
    ports:
      - "3000:3000"
    depends_on:
      - db
      - worker
    environment:
      DATABASE_URL: postgresql://demo:secret@db:5432/demo
      WORKER_URL: http://worker:8080
  worker:
    image: ghcr.io/example/worker:latest
    environment:
      DATABASE_URL: postgresql://demo:secret@db:5432/demo
  db:
    image: postgres:17-alpine
    environment:
      POSTGRES_DB: demo
      POSTGRES_USER: demo
      POSTGRES_PASSWORD: secret
`,
		"web/Dockerfile": "FROM node:22-alpine\nEXPOSE 3000\n",
	})
	body, contentType := newImportUploadMultipartBody(t, importUploadRequest{
		Name: "demo-stack",
	}, "demo-stack.tgz", archiveBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App        model.App         `json:"app"`
		Operation  model.Operation   `json:"operation"`
		Apps       []model.App       `json:"apps"`
		Operations []model.Operation `json:"operations"`
		Compose    struct {
			ComposePath    string `json:"compose_path"`
			PrimaryService string `json:"primary_service"`
			Services       []struct {
				Service        string `json:"service"`
				BuildStrategy  string `json:"build_strategy"`
				AppID          string `json:"app_id"`
				OperationID    string `json:"operation_id"`
				ComposeService string `json:"compose_service"`
			} `json:"services"`
		} `json:"compose_stack"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Compose.ComposePath != "docker-compose.yml" {
		t.Fatalf("expected compose path docker-compose.yml, got %q", response.Compose.ComposePath)
	}
	if response.Compose.PrimaryService != "web" {
		t.Fatalf("expected primary service web, got %q", response.Compose.PrimaryService)
	}
	if len(response.Apps) != 2 {
		t.Fatalf("expected 2 apps, got %d", len(response.Apps))
	}
	if len(response.Operations) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(response.Operations))
	}
	if len(response.Compose.Services) != 2 {
		t.Fatalf("expected 2 compose services, got %d", len(response.Compose.Services))
	}

	appsByService := map[string]model.App{}
	for _, app := range response.Apps {
		if app.Source == nil {
			t.Fatalf("expected app %s to preserve source metadata", app.Name)
		}
		appsByService[app.Source.ComposeService] = app
	}

	webApp, ok := appsByService["web"]
	if !ok {
		t.Fatal("expected web app in compose response")
	}
	if webApp.Source.Type != model.AppSourceTypeUpload {
		t.Fatalf("expected web source type %q, got %q", model.AppSourceTypeUpload, webApp.Source.Type)
	}
	if webApp.Source.UploadID == "" {
		t.Fatal("expected upload-backed web source to keep upload id")
	}
	if webApp.Source.DockerfilePath != "web/Dockerfile" {
		t.Fatalf("expected dockerfile path web/Dockerfile, got %q", webApp.Source.DockerfilePath)
	}
	if webApp.Source.BuildContextDir != "web" {
		t.Fatalf("expected build context web, got %q", webApp.Source.BuildContextDir)
	}
	if stringsContain(webApp.Source.ComposeDependsOn, "db") {
		t.Fatalf("expected managed postgres backing to be removed from dependencies, got %v", webApp.Source.ComposeDependsOn)
	}
	if webApp.Route == nil || webApp.Route.ServicePort != 3000 {
		t.Fatalf("expected web route service port 3000, got %+v", webApp.Route)
	}
	if got := webApp.Spec.Env["WORKER_URL"]; got != "http://"+runtime.ComposeServiceAliasName(webApp.ProjectID, "worker")+":8080" {
		t.Fatalf("expected worker URL env to target compose alias, got %+v", webApp.Spec.Env)
	}
	if got := webApp.Spec.Env["DATABASE_URL"]; got == "" {
		t.Fatalf("expected rewritten database url, got %+v", webApp.Spec.Env)
	}

	workerApp, ok := appsByService["worker"]
	if !ok {
		t.Fatal("expected worker app in compose response")
	}
	if workerApp.Source.Type != model.AppSourceTypeDockerImage {
		t.Fatalf("expected worker source type %q, got %q", model.AppSourceTypeDockerImage, workerApp.Source.Type)
	}
	if workerApp.Source.ImageRef != "ghcr.io/example/worker:latest" {
		t.Fatalf("expected worker image ref ghcr.io/example/worker:latest, got %q", workerApp.Source.ImageRef)
	}

	storedApps, err := s.ListApps(tenant.ID, false)
	if err != nil {
		t.Fatalf("list apps: %v", err)
	}
	if len(storedApps) != 2 {
		t.Fatalf("expected 2 stored apps, got %d", len(storedApps))
	}
}

func TestImportUploadAppRejectsStartupCommandForTopologyImport(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Upload Compose Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "uploader", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain: "apps.example.com",
	})

	archiveBytes := mustTarGz(t, map[string]string{
		"docker-compose.yml": `
services:
  web:
    image: ghcr.io/example/web:latest
    ports:
      - "3000:3000"
`,
	})
	startupCommand := "npm run start"
	body, contentType := newImportUploadMultipartBody(t, importUploadRequest{
		Name:           "demo-stack",
		StartupCommand: &startupCommand,
	}, "demo-stack.tgz", archiveBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error != "startup_command is only supported for single-app imports" {
		t.Fatalf("expected startup command topology error, got %q", response.Error)
	}
}

func TestImportUploadAppRejectsPersistentStorageForTopologyImport(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Upload Compose Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "uploader", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain: "apps.example.com",
	})

	archiveBytes := mustTarGz(t, map[string]string{
		"docker-compose.yml": `
services:
  web:
    image: ghcr.io/example/web:latest
    ports:
      - "3000:3000"
`,
	})
	body, contentType := newImportUploadMultipartBody(t, importUploadRequest{
		Name: "demo-stack",
		PersistentStorage: &model.AppPersistentStorageSpec{
			Mounts: []model.AppPersistentStorageMount{
				{
					Kind: "directory",
					Path: "/var/lib/data",
				},
			},
		},
	}, "demo-stack.tgz", archiveBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error != "persistent_storage is only supported for single-app imports" {
		t.Fatalf("expected persistent storage topology error, got %q", response.Error)
	}
}

func TestGetSourceUploadArchiveRequiresDownloadToken(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Download Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	archiveBytes := mustTarGz(t, map[string]string{
		"index.html": "<h1>download</h1>\n",
	})
	upload, err := s.CreateSourceUpload(tenant.ID, "site.tgz", "application/gzip", archiveBytes)
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	missingTokenReq := httptest.NewRequest(http.MethodGet, "/v1/source-uploads/"+upload.ID+"/archive", nil)
	missingTokenRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingTokenRecorder, missingTokenReq)
	if missingTokenRecorder.Code != http.StatusBadRequest {
		t.Fatalf("expected missing token status %d, got %d body=%s", http.StatusBadRequest, missingTokenRecorder.Code, missingTokenRecorder.Body.String())
	}

	invalidTokenReq := httptest.NewRequest(http.MethodGet, "/v1/source-uploads/"+upload.ID+"/archive?download_token=wrong", nil)
	invalidTokenRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(invalidTokenRecorder, invalidTokenReq)
	if invalidTokenRecorder.Code != http.StatusNotFound {
		t.Fatalf("expected invalid token status %d, got %d body=%s", http.StatusNotFound, invalidTokenRecorder.Code, invalidTokenRecorder.Body.String())
	}

	validTokenReq := httptest.NewRequest(http.MethodGet, "/v1/source-uploads/"+upload.ID+"/archive", nil)
	validTokenQuery := url.Values{}
	validTokenQuery.Set("download_token", upload.DownloadToken)
	validTokenReq.URL.RawQuery = validTokenQuery.Encode()
	validTokenRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(validTokenRecorder, validTokenReq)
	if validTokenRecorder.Code != http.StatusOK {
		t.Fatalf("expected valid token status %d, got %d body=%s", http.StatusOK, validTokenRecorder.Code, validTokenRecorder.Body.String())
	}
	if got := validTokenRecorder.Body.Bytes(); !bytes.Equal(got, archiveBytes) {
		t.Fatalf("unexpected archive response body length=%d want=%d", len(got), len(archiveBytes))
	}
}

func newImportUploadMultipartBody(t *testing.T, req importUploadRequest, archiveName string, archiveBytes []byte) (*bytes.Buffer, string) {
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
	part, err := writer.CreateFormFile("archive", archiveName)
	if err != nil {
		t.Fatalf("create archive part: %v", err)
	}
	if _, err := part.Write(archiveBytes); err != nil {
		t.Fatalf("write archive part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	return &body, writer.FormDataContentType()
}

func mustTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	for name, content := range files {
		header := &tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(content)),
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatalf("write tar header: %v", err)
		}
		if _, err := tarWriter.Write([]byte(content)); err != nil {
			t.Fatalf("write tar content: %v", err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	return buffer.Bytes()
}

func mustZip(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for name, content := range files {
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatalf("create zip entry: %v", err)
		}
		if _, err := entry.Write([]byte(content)); err != nil {
			t.Fatalf("write zip content: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	return buffer.Bytes()
}

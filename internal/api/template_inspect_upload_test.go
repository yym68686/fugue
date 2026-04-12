package api

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/store"
)

func TestInspectUploadTemplateReturnsComposeInspection(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Upload Inspect Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "template-reader", []string{"app.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ImportWorkDir: t.TempDir(),
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
    volumes:
      - ./config.yaml:/workspace/config.yaml
`,
		"web/Dockerfile": "FROM scratch\nEXPOSE 3000\n",
	})
	body, contentType := newImportUploadMultipartBody(t, importUploadRequest{
		Name: "demo-stack",
	}, "demo-stack.tgz", archiveBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/templates/inspect-upload", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Upload struct {
			ArchiveFilename string `json:"archive_filename"`
			DefaultAppName  string `json:"default_app_name"`
			SourceKind      string `json:"source_kind"`
			SourcePath      string `json:"source_path"`
		} `json:"upload"`
		ComposeStack *struct {
			ComposePath    string `json:"compose_path"`
			PrimaryService string `json:"primary_service"`
			Services       []struct {
				Service                    string `json:"service"`
				BuildStrategy              string `json:"build_strategy"`
				InternalPort               int    `json:"internal_port"`
				PersistentStorageSeedFiles []struct {
					Path        string `json:"path"`
					Mode        int32  `json:"mode"`
					SeedContent string `json:"seed_content"`
				} `json:"persistent_storage_seed_files"`
				Published bool `json:"published"`
			} `json:"services"`
		} `json:"compose_stack"`
		FugueManifest any `json:"fugue_manifest"`
	}
	mustDecodeJSON(t, recorder, &response)

	if response.Upload.ArchiveFilename != "demo-stack.tgz" {
		t.Fatalf("expected archive filename demo-stack.tgz, got %+v", response.Upload)
	}
	if response.Upload.DefaultAppName != "demo-stack" || response.Upload.SourceKind != "compose" || response.Upload.SourcePath != "docker-compose.yml" {
		t.Fatalf("unexpected upload metadata: %+v", response.Upload)
	}
	if response.FugueManifest != nil {
		t.Fatalf("expected fugue_manifest to be absent, got %+v", response.FugueManifest)
	}
	if response.ComposeStack == nil {
		t.Fatal("expected compose_stack inspection")
	}
	if response.ComposeStack.ComposePath != "docker-compose.yml" || response.ComposeStack.PrimaryService != "web" {
		t.Fatalf("unexpected compose stack metadata: %+v", response.ComposeStack)
	}
	if len(response.ComposeStack.Services) != 1 {
		t.Fatalf("expected one service, got %+v", response.ComposeStack.Services)
	}
	service := response.ComposeStack.Services[0]
	if service.Service != "web" || service.BuildStrategy != "dockerfile" || service.InternalPort != 3000 || !service.Published {
		t.Fatalf("unexpected compose service: %+v", service)
	}
	if len(service.PersistentStorageSeedFiles) != 1 {
		t.Fatalf("expected one editable persistent storage file, got %+v", service.PersistentStorageSeedFiles)
	}
	seedFile := service.PersistentStorageSeedFiles[0]
	if seedFile.Path != "/workspace/config.yaml" || seedFile.Mode != 0o644 || seedFile.SeedContent != "" {
		t.Fatalf("unexpected persistent storage seed file: %+v", seedFile)
	}
}

func TestInspectUploadTemplateReturnsComposeInspectionFromZipArchive(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Upload Inspect Zip Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "template-reader", []string{"app.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ImportWorkDir: t.TempDir(),
	})

	archiveBytes := mustZip(t, map[string]string{
		"demo-main/docker-compose.yml": `
services:
  web:
    build:
      context: ./web
      dockerfile: Dockerfile
    ports:
      - "3000:3000"
`,
		"demo-main/web/Dockerfile":      "FROM scratch\nEXPOSE 3000\n",
		"__MACOSX/._docker-compose.yml": "ignored metadata\n",
	})
	body, contentType := newImportUploadMultipartBody(t, importUploadRequest{}, "demo-main.zip", archiveBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/templates/inspect-upload", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Upload struct {
			ArchiveFilename string `json:"archive_filename"`
			DefaultAppName  string `json:"default_app_name"`
			SourceKind      string `json:"source_kind"`
			SourcePath      string `json:"source_path"`
		} `json:"upload"`
		ComposeStack *struct {
			ComposePath    string `json:"compose_path"`
			PrimaryService string `json:"primary_service"`
		} `json:"compose_stack"`
	}
	mustDecodeJSON(t, recorder, &response)

	if response.Upload.ArchiveFilename != "demo-main.zip" {
		t.Fatalf("expected archive filename demo-main.zip, got %+v", response.Upload)
	}
	if response.Upload.DefaultAppName != "demo-main" || response.Upload.SourceKind != "compose" || response.Upload.SourcePath != "docker-compose.yml" {
		t.Fatalf("unexpected upload metadata: %+v", response.Upload)
	}
	if response.ComposeStack == nil {
		t.Fatal("expected compose_stack inspection")
	}
	if response.ComposeStack.ComposePath != "docker-compose.yml" || response.ComposeStack.PrimaryService != "web" {
		t.Fatalf("unexpected compose stack metadata: %+v", response.ComposeStack)
	}
}

package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func raiseManagedTestCap(t *testing.T, s *store.Store, tenantID string) {
	t.Helper()

	if _, err := s.UpdateTenantBilling(tenantID, model.BillingResourceSpec{
		CPUMilliCores:    2000,
		MemoryMebibytes:  4096,
		StorageGibibytes: 30,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}
}

func TestAppReadRedactsSecretFilesButDedicatedConfigEndpointsReturnValues(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Env: map[string]string{
			"OPENAI_API_KEY": "sk-demo",
			"LOG_LEVEL":      "debug",
		},
		Files: []model.AppFile{
			{
				Path:    "/home/api.yaml",
				Content: "providers: []",
				Secret:  true,
				Mode:    0o600,
			},
			{
				Path:    "/srv/banner.txt",
				Content: "hello",
				Mode:    0o644,
			},
		},
	})
	_ = s

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID, apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var appResponse struct {
		App model.App `json:"app"`
	}
	mustDecodeJSON(t, recorder, &appResponse)
	if got := appResponse.App.Spec.Files[0].Content; got != "" {
		t.Fatalf("expected secret file to be redacted, got %q", got)
	}
	if got := appResponse.App.Spec.Files[1].Content; got != "hello" {
		t.Fatalf("expected non-secret file content to remain visible, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var listResponse struct {
		Apps []model.App `json:"apps"`
	}
	mustDecodeJSON(t, recorder, &listResponse)
	if len(listResponse.Apps) != 1 {
		t.Fatalf("expected 1 app, got %d", len(listResponse.Apps))
	}
	if got := listResponse.Apps[0].Spec.Files[0].Content; got != "" {
		t.Fatalf("expected secret file to be redacted in list response, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/env", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var envResponse struct {
		Env map[string]string `json:"env"`
	}
	mustDecodeJSON(t, recorder, &envResponse)
	if got := envResponse.Env["OPENAI_API_KEY"]; got != "sk-demo" {
		t.Fatalf("expected env value to be visible, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/files", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var filesResponse struct {
		Files []model.AppFile `json:"files"`
	}
	mustDecodeJSON(t, recorder, &filesResponse)
	if len(filesResponse.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(filesResponse.Files))
	}
	if got := filesResponse.Files[0].Content; got != "providers: []" {
		t.Fatalf("expected dedicated files endpoint to return file content, got %q", got)
	}
}

func TestPatchAppEnvAndRestartCreateDeployOperations(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Env: map[string]string{
			"OLD": "1",
		},
	})

	recorder := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/env", apiKey, map[string]any{
		"set": map[string]string{
			"OLD": "3",
			"NEW": "2",
		},
		"delete": []string{"MISSING"},
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var patchResponse struct {
		Env       map[string]string `json:"env"`
		Operation model.Operation   `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if patchResponse.Operation.DesiredSpec == nil {
		t.Fatal("expected desired spec in deploy operation")
	}
	if got := patchResponse.Operation.DesiredSpec.Env["NEW"]; got != "2" {
		t.Fatalf("expected NEW env in desired spec, got %q", got)
	}
	if got := patchResponse.Env["OLD"]; got != "3" {
		t.Fatalf("expected OLD env to be updated, got %q", got)
	}

	completeNextManagedOperation(t, s)

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/env", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var envResponse struct {
		Env map[string]string `json:"env"`
	}
	mustDecodeJSON(t, recorder, &envResponse)
	if got := envResponse.Env["OLD"]; got != "3" {
		t.Fatalf("expected persisted OLD env=3, got %q", got)
	}
	if got := envResponse.Env["NEW"]; got != "2" {
		t.Fatalf("expected persisted NEW env=2, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/restart", apiKey, map[string]any{})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var restartResponse struct {
		RestartToken string          `json:"restart_token"`
		Operation    model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &restartResponse)
	if restartResponse.RestartToken == "" {
		t.Fatal("expected restart token")
	}
	if restartResponse.Operation.DesiredSpec == nil || restartResponse.Operation.DesiredSpec.RestartToken == "" {
		t.Fatal("expected restart token in desired spec")
	}

	completeNextManagedOperation(t, s)

	updatedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after restart: %v", err)
	}
	if updatedApp.Spec.RestartToken == "" {
		t.Fatal("expected restart token to persist on app spec")
	}
}

func TestPatchAppEnvAndRestartRecoverFailedImportedAppDesiredState(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app, recoveredImage, recoveredSource := setupFailedImportedAppRecoveryServer(t)

	recorder := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/env", apiKey, map[string]any{
		"set": map[string]string{
			"FIXED": "1",
		},
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var patchResponse struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if patchResponse.Operation.DesiredSpec == nil {
		t.Fatal("expected desired spec in recovered deploy operation")
	}
	if got := patchResponse.Operation.DesiredSpec.Image; got != recoveredImage {
		t.Fatalf("expected recovered image %q, got %q", recoveredImage, got)
	}
	if got := patchResponse.Operation.DesiredSpec.Env["BROKEN"]; got != "1" {
		t.Fatalf("expected recovered env BROKEN=1, got %q", got)
	}
	if got := patchResponse.Operation.DesiredSpec.Env["FIXED"]; got != "1" {
		t.Fatalf("expected patched env FIXED=1, got %q", got)
	}
	if patchResponse.Operation.DesiredSource == nil {
		t.Fatal("expected desired source in recovered deploy operation")
	}
	if got := patchResponse.Operation.DesiredSource.ResolvedImageRef; got != recoveredSource.ResolvedImageRef {
		t.Fatalf("expected recovered resolved image ref %q, got %q", recoveredSource.ResolvedImageRef, got)
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/restart", apiKey, map[string]any{})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var restartResponse struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &restartResponse)
	if restartResponse.Operation.DesiredSpec == nil {
		t.Fatal("expected desired spec on restart operation")
	}
	if got := restartResponse.Operation.DesiredSpec.Image; got != recoveredImage {
		t.Fatalf("expected restart image %q, got %q", recoveredImage, got)
	}
	if got := restartResponse.Operation.DesiredSpec.Env["FIXED"]; got != "1" {
		t.Fatalf("expected restart to preserve recovered env FIXED=1, got %q", got)
	}
	if restartResponse.Operation.DesiredSpec.RestartToken == "" {
		t.Fatal("expected restart token in desired spec")
	}
	if restartResponse.Operation.DesiredSource == nil {
		t.Fatal("expected desired source on restart operation")
	}
	if got := restartResponse.Operation.DesiredSource.ResolvedImageRef; got != recoveredSource.ResolvedImageRef {
		t.Fatalf("expected restart resolved image ref %q, got %q", recoveredSource.ResolvedImageRef, got)
	}
}

func TestGetAppEnvRecoversFailedImportedAppDesiredState(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app, _, _ := setupFailedImportedAppRecoveryServer(t)

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/env", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var envResponse struct {
		Env map[string]string `json:"env"`
	}
	mustDecodeJSON(t, recorder, &envResponse)
	if got := envResponse.Env["BASE"]; got != "1" {
		t.Fatalf("expected recovered env BASE=1, got %q", got)
	}
	if got := envResponse.Env["BROKEN"]; got != "1" {
		t.Fatalf("expected recovered env BROKEN=1, got %q", got)
	}
}

func TestRestartAppRejectsBlankDeployableImage(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Blank Image Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "ghcr.io/example/demo:latest",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/restart", apiKey, map[string]any{})
	if recorder.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusConflict, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "app has no deployable image") {
		t.Fatalf("expected conflict to mention missing deployable image, got body=%s", recorder.Body.String())
	}

	ops, err := s.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected no operations to be queued, got %d", len(ops))
	}
}

func TestPatchAppImageMirrorLimitUpdatesAppWithoutDeployOperation(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID, apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var getResponse struct {
		App model.App `json:"app"`
	}
	mustDecodeJSON(t, recorder, &getResponse)
	if got := getResponse.App.Spec.ImageMirrorLimit; got != model.DefaultAppImageMirrorLimit {
		t.Fatalf("expected default image mirror limit %d, got %d", model.DefaultAppImageMirrorLimit, got)
	}

	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID, apiKey, map[string]any{
		"image_mirror_limit": 3,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var patchResponse struct {
		AlreadyCurrent bool      `json:"already_current"`
		App            model.App `json:"app"`
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if patchResponse.AlreadyCurrent {
		t.Fatal("expected patch response to report a change")
	}
	if got := patchResponse.App.Spec.ImageMirrorLimit; got != 3 {
		t.Fatalf("expected patched image mirror limit 3, got %d", got)
	}

	ops, err := s.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected no deploy operations for image mirror limit patch, got %d", len(ops))
	}

	updatedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get updated app: %v", err)
	}
	if got := updatedApp.Spec.ImageMirrorLimit; got != 3 {
		t.Fatalf("expected stored image mirror limit 3, got %d", got)
	}

	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID, apiKey, map[string]any{
		"image_mirror_limit": 3,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if !patchResponse.AlreadyCurrent {
		t.Fatal("expected repeated patch response to report already_current")
	}
}

func TestPatchAppStartupCommandQueuesDeployOperation(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})

	startupCommand := "npm run start"
	recorder := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID, apiKey, map[string]any{
		"startup_command": startupCommand,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var patchResponse struct {
		AlreadyCurrent bool            `json:"already_current"`
		App            model.App       `json:"app"`
		Operation      model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if patchResponse.AlreadyCurrent {
		t.Fatal("expected startup command patch to report a change")
	}
	if patchResponse.Operation.ID == "" {
		t.Fatal("expected deploy operation in patch response")
	}
	if len(patchResponse.App.Spec.Command) != 0 {
		t.Fatalf("expected response app to remain unchanged until deploy completes, got %#v", patchResponse.App.Spec.Command)
	}

	op, err := s.GetOperation(patchResponse.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.Type != model.OperationTypeDeploy {
		t.Fatalf("expected deploy operation, got %q", op.Type)
	}
	if op.DesiredSpec == nil {
		t.Fatal("expected desired spec on deploy operation")
	}
	if len(op.DesiredSpec.Command) != 3 || op.DesiredSpec.Command[0] != "sh" || op.DesiredSpec.Command[1] != "-lc" || op.DesiredSpec.Command[2] != startupCommand {
		t.Fatalf("expected desired spec command to wrap startup command, got %#v", op.DesiredSpec.Command)
	}

	completeNextManagedOperation(t, s)

	updatedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get updated app: %v", err)
	}
	if len(updatedApp.Spec.Command) != 3 || updatedApp.Spec.Command[0] != "sh" || updatedApp.Spec.Command[1] != "-lc" || updatedApp.Spec.Command[2] != startupCommand {
		t.Fatalf("expected stored command to wrap startup command, got %#v", updatedApp.Spec.Command)
	}

	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID, apiKey, map[string]any{
		"startup_command": startupCommand,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if !patchResponse.AlreadyCurrent {
		t.Fatal("expected repeated startup command patch to report already_current")
	}

	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID, apiKey, map[string]any{
		"startup_command": "",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if patchResponse.AlreadyCurrent {
		t.Fatal("expected clearing startup command to report a change")
	}
	if patchResponse.Operation.ID == "" {
		t.Fatal("expected deploy operation when clearing startup command")
	}

	op, err = s.GetOperation(patchResponse.Operation.ID)
	if err != nil {
		t.Fatalf("get clear operation: %v", err)
	}
	if op.DesiredSpec == nil {
		t.Fatal("expected desired spec when clearing startup command")
	}
	if len(op.DesiredSpec.Command) != 0 {
		t.Fatalf("expected cleared startup command to remove command, got %#v", op.DesiredSpec.Command)
	}

	completeNextManagedOperation(t, s)

	updatedApp, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get cleared app: %v", err)
	}
	if len(updatedApp.Spec.Command) != 0 {
		t.Fatalf("expected cleared startup command to persist, got %#v", updatedApp.Spec.Command)
	}
}

func TestPatchAppPersistentStorageQueuesCombinedDeployOperation(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})

	startupCommand := "npm run start"
	requestBody := map[string]any{
		"startup_command": startupCommand,
		"persistent_storage": map[string]any{
			"mounts": []map[string]any{
				{
					"kind": "directory",
					"path": "/var/lib/data",
				},
				{
					"kind":         "file",
					"path":         "/srv/config.json",
					"seed_content": "{\"enabled\":true}",
				},
			},
		},
	}

	recorder := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID, apiKey, requestBody)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var patchResponse struct {
		AlreadyCurrent bool            `json:"already_current"`
		App            model.App       `json:"app"`
		Operation      model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if patchResponse.AlreadyCurrent {
		t.Fatal("expected persistent storage patch to report a change")
	}
	if patchResponse.Operation.ID == "" {
		t.Fatal("expected deploy operation for persistent storage patch")
	}
	if patchResponse.App.Spec.PersistentStorage != nil {
		t.Fatalf("expected response app persistent storage to stay unchanged until deploy completes, got %+v", patchResponse.App.Spec.PersistentStorage)
	}
	if len(patchResponse.App.Spec.Command) != 0 {
		t.Fatalf("expected response app command to stay unchanged until deploy completes, got %#v", patchResponse.App.Spec.Command)
	}

	op, err := s.GetOperation(patchResponse.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.Type != model.OperationTypeDeploy {
		t.Fatalf("expected deploy operation, got %q", op.Type)
	}
	if op.DesiredSpec == nil {
		t.Fatal("expected desired spec on persistent storage patch operation")
	}
	if len(op.DesiredSpec.Command) != 3 || op.DesiredSpec.Command[0] != "sh" || op.DesiredSpec.Command[1] != "-lc" || op.DesiredSpec.Command[2] != startupCommand {
		t.Fatalf("expected desired spec command to wrap startup command, got %#v", op.DesiredSpec.Command)
	}
	if op.DesiredSpec.PersistentStorage == nil || len(op.DesiredSpec.PersistentStorage.Mounts) != 2 {
		t.Fatalf("expected desired persistent storage mounts, got %+v", op.DesiredSpec.PersistentStorage)
	}
	if got := op.DesiredSpec.PersistentStorage.Mounts[0].Mode; got != 0o755 {
		t.Fatalf("expected default directory mount mode 0755, got %o", got)
	}
	if got := op.DesiredSpec.PersistentStorage.Mounts[1].Mode; got != 0o644 {
		t.Fatalf("expected default file mount mode 0644, got %o", got)
	}
	if got := op.DesiredSpec.PersistentStorage.Mounts[1].SeedContent; got != "{\"enabled\":true}" {
		t.Fatalf("expected desired file seed content to persist, got %q", got)
	}

	ops, err := s.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected a single combined deploy operation, got %d", len(ops))
	}

	completeNextManagedOperation(t, s)

	updatedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get updated app: %v", err)
	}
	if len(updatedApp.Spec.Command) != 3 || updatedApp.Spec.Command[0] != "sh" || updatedApp.Spec.Command[1] != "-lc" || updatedApp.Spec.Command[2] != startupCommand {
		t.Fatalf("expected stored command to wrap startup command, got %#v", updatedApp.Spec.Command)
	}
	if updatedApp.Spec.PersistentStorage == nil || len(updatedApp.Spec.PersistentStorage.Mounts) != 2 {
		t.Fatalf("expected stored persistent storage mounts, got %+v", updatedApp.Spec.PersistentStorage)
	}

	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID, apiKey, requestBody)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if !patchResponse.AlreadyCurrent {
		t.Fatal("expected repeated persistent storage patch to report already_current")
	}

	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID, apiKey, map[string]any{
		"persistent_storage": map[string]any{
			"mounts": []map[string]any{},
		},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if patchResponse.AlreadyCurrent {
		t.Fatal("expected clearing persistent storage to report a change")
	}
	if patchResponse.Operation.ID == "" {
		t.Fatal("expected deploy operation when clearing persistent storage")
	}

	op, err = s.GetOperation(patchResponse.Operation.ID)
	if err != nil {
		t.Fatalf("get clear operation: %v", err)
	}
	if op.DesiredSpec == nil {
		t.Fatal("expected desired spec on clear persistent storage operation")
	}
	if op.DesiredSpec.PersistentStorage != nil {
		t.Fatalf("expected persistent storage to be cleared in desired spec, got %+v", op.DesiredSpec.PersistentStorage)
	}
	if len(op.DesiredSpec.Command) != 3 || op.DesiredSpec.Command[0] != "sh" || op.DesiredSpec.Command[1] != "-lc" || op.DesiredSpec.Command[2] != startupCommand {
		t.Fatalf("expected startup command to remain unchanged when clearing persistent storage, got %#v", op.DesiredSpec.Command)
	}

	completeNextManagedOperation(t, s)

	updatedApp, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get cleared app: %v", err)
	}
	if updatedApp.Spec.PersistentStorage != nil {
		t.Fatalf("expected persistent storage to be cleared, got %+v", updatedApp.Spec.PersistentStorage)
	}
}

func TestGetAppEnvMergesBindingEnvAndAppEnvOverrides(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Env: map[string]string{
			"DB_HOST":   "override-db.internal",
			"LOG_LEVEL": "debug",
		},
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "root",
			Password: "secret",
		},
	})

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/env", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Env map[string]string `json:"env"`
	}
	mustDecodeJSON(t, recorder, &response)
	if got := response.Env["DB_TYPE"]; got != "postgres" {
		t.Fatalf("expected DB_TYPE=postgres, got %q", got)
	}
	if got := response.Env["DB_HOST"]; got != "override-db.internal" {
		t.Fatalf("expected app env to override DB_HOST, got %q", got)
	}
	if got := response.Env["DB_USER"]; got != "root" {
		t.Fatalf("expected DB_USER=root from binding env, got %q", got)
	}
	if got := response.Env["DB_NAME"]; got != "demo" {
		t.Fatalf("expected DB_NAME=demo from binding env, got %q", got)
	}
	if got := response.Env["LOG_LEVEL"]; got != "debug" {
		t.Fatalf("expected LOG_LEVEL=debug, got %q", got)
	}
}

func TestGetImportedAppEnvMergesBindingEnvWithoutRecovery(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Imported Config Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateImportedApp(
		tenant.ID,
		project.ID,
		"demo",
		"",
		model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_managed_shared",
			Env: map[string]string{
				"DB_HOST":   "override-db.internal",
				"LOG_LEVEL": "debug",
			},
			Postgres: &model.AppPostgresSpec{
				Database: "demo",
				User:     "root",
				Password: "secret",
			},
		},
		model.AppSource{
			Type:     model.AppSourceTypeDockerImage,
			ImageRef: "ghcr.io/example/demo:latest",
		},
		model.AppRoute{},
	)
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/env", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Env map[string]string `json:"env"`
	}
	mustDecodeJSON(t, recorder, &response)
	if got := response.Env["DB_TYPE"]; got != "postgres" {
		t.Fatalf("expected DB_TYPE=postgres, got %q", got)
	}
	if got := response.Env["DB_HOST"]; got != "override-db.internal" {
		t.Fatalf("expected app env to override DB_HOST, got %q", got)
	}
	if got := response.Env["DB_USER"]; got != "root" {
		t.Fatalf("expected DB_USER=root from binding env, got %q", got)
	}
	if got := response.Env["DB_NAME"]; got != "demo" {
		t.Fatalf("expected DB_NAME=demo from binding env, got %q", got)
	}
	if got := response.Env["LOG_LEVEL"]; got != "debug" {
		t.Fatalf("expected LOG_LEVEL=debug, got %q", got)
	}
}

func TestUpsertAndDeleteAppFilesCreateDeployOperations(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Files: []model.AppFile{
			{
				Path:    "/home/api.yaml",
				Content: "providers: []",
				Secret:  true,
				Mode:    0o600,
			},
		},
	})

	recorder := performJSONRequest(t, server, http.MethodPut, "/v1/apps/"+app.ID+"/files", apiKey, map[string]any{
		"files": []map[string]any{
			{
				"path":    "/home/api.yaml",
				"content": "providers:\n  - gemini",
				"secret":  true,
				"mode":    0o600,
			},
			{
				"path":    "/srv/banner.txt",
				"content": "hello",
				"mode":    0o644,
			},
		},
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var upsertResponse struct {
		Files     []model.AppFile `json:"files"`
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &upsertResponse)
	if len(upsertResponse.Files) != 2 {
		t.Fatalf("expected 2 files after upsert, got %d", len(upsertResponse.Files))
	}
	if upsertResponse.Operation.DesiredSpec == nil || len(upsertResponse.Operation.DesiredSpec.Files) != 2 {
		t.Fatal("expected desired spec with 2 files")
	}

	completeNextManagedOperation(t, s)

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/files", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var filesResponse struct {
		Files []model.AppFile `json:"files"`
	}
	mustDecodeJSON(t, recorder, &filesResponse)
	if len(filesResponse.Files) != 2 {
		t.Fatalf("expected 2 files after deploy, got %d", len(filesResponse.Files))
	}
	if got := filesResponse.Files[0].Content; got != "providers:\n  - gemini" {
		t.Fatalf("expected updated file content, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodDelete, "/v1/apps/"+app.ID+"/files?path=/srv/banner.txt", apiKey, nil)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	completeNextManagedOperation(t, s)

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/files", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &filesResponse)
	if len(filesResponse.Files) != 1 {
		t.Fatalf("expected 1 file after delete, got %d", len(filesResponse.Files))
	}
	if filesResponse.Files[0].Path != "/home/api.yaml" {
		t.Fatalf("unexpected remaining file path %q", filesResponse.Files[0].Path)
	}
}

func TestDeleteAppIsIdempotentAndHiddenFromAppListWhileDeleting(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})

	recorder := performJSONRequest(t, server, http.MethodDelete, "/v1/apps/"+app.ID, apiKey, nil)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var firstDelete struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &firstDelete)
	if firstDelete.Operation.ID == "" {
		t.Fatal("expected delete operation id")
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var listResponse struct {
		Apps []model.App `json:"apps"`
	}
	mustDecodeJSON(t, recorder, &listResponse)
	if len(listResponse.Apps) != 0 {
		t.Fatalf("expected deleting app to be hidden from app list, got %+v", listResponse.Apps)
	}

	recorder = performJSONRequest(t, server, http.MethodDelete, "/v1/apps/"+app.ID, apiKey, nil)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var secondDelete struct {
		AlreadyDeleting bool            `json:"already_deleting"`
		Operation       model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &secondDelete)
	if !secondDelete.AlreadyDeleting {
		t.Fatal("expected already_deleting=true")
	}
	if secondDelete.Operation.ID != firstDelete.Operation.ID {
		t.Fatalf("expected repeated delete to return same operation %q, got %q", firstDelete.Operation.ID, secondDelete.Operation.ID)
	}

	ops, err := s.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected exactly one delete operation, got %d", len(ops))
	}
}

func TestForceDeletePurgesFailedImportedApp(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app, _, _ := setupFailedImportedAppRecoveryServer(t)

	recorder := performJSONRequest(
		t,
		server,
		http.MethodDelete,
		"/v1/apps/"+app.ID+"?force=true",
		apiKey,
		nil,
	)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Deleted bool `json:"deleted"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.Deleted {
		t.Fatal("expected deleted=true")
	}

	if _, err := s.GetApp(app.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected app to be purged, got %v", err)
	}
}

func TestForceDeleteFailsActiveDeployAndQueuesDelete(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})

	nextSpec := app.Spec
	nextSpec.Image = "ghcr.io/example/demo:next"
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:    app.TenantID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &nextSpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim deploy operation: %v", err)
	} else if !found {
		t.Fatal("expected deploy operation to be claimable")
	}

	recorder := performJSONRequest(
		t,
		server,
		http.MethodDelete,
		"/v1/apps/"+app.ID+"?force=true",
		apiKey,
		nil,
	)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Deleted   bool            `json:"deleted"`
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Deleted {
		t.Fatal("expected delete to be queued, not immediate")
	}
	if response.Operation.Type != model.OperationTypeDelete {
		t.Fatalf("expected delete operation response, got %+v", response.Operation)
	}

	failedDeploy, err := s.GetOperation(deployOp.ID)
	if err != nil {
		t.Fatalf("get failed deploy operation: %v", err)
	}
	if failedDeploy.Status != model.OperationStatusFailed {
		t.Fatalf("expected failed deploy status, got %q", failedDeploy.Status)
	}

	storedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get deleting app: %v", err)
	}
	if got := storedApp.Status.Phase; got != "deleting" {
		t.Fatalf("expected deleting phase, got %q", got)
	}

	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/deploy.yaml", "done"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected complete failed deploy to conflict, got %v", err)
	}
}

func setupAppConfigTestServer(t *testing.T, spec model.AppSpec) (*store.Store, *Server, string, model.App) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Config Test Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", spec)
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	return s, server, apiKey, app
}

func setupFailedImportedAppRecoveryServer(t *testing.T) (*store.Store, *Server, string, model.App, string, model.AppSource) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Recovered Import Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Env: map[string]string{
			"BASE": "1",
		},
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "ghcr.io/example/demo:1.2.3",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	importSpec := app.Spec
	importSource := *app.Source
	importOp, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeImport,
		AppID:         app.ID,
		DesiredSpec:   &importSpec,
		DesiredSource: &importSource,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim import operation: %v", err)
	} else if !found {
		t.Fatal("expected import operation")
	}

	recoveredSpec := app.Spec
	recoveredSpec.Image = "registry.pull.example/fugue-apps/demo:image-abc123"
	recoveredSpec.Env = map[string]string{
		"BASE":   "1",
		"BROKEN": "1",
	}
	recoveredSource := *app.Source
	recoveredSource.ResolvedImageRef = "registry.push.example/fugue-apps/demo:image-abc123"
	if _, err := s.CompleteManagedOperationWithResult(importOp.ID, "/tmp/import.yaml", "imported", &recoveredSpec, &recoveredSource); err != nil {
		t.Fatalf("complete import operation: %v", err)
	}

	failedDeploySpec := recoveredSpec
	failedDeployOp, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &failedDeploySpec,
		DesiredSource: &recoveredSource,
	})
	if err != nil {
		t.Fatalf("create failed deploy operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim failed deploy operation: %v", err)
	} else if !found {
		t.Fatal("expected failed deploy operation")
	}
	if _, err := s.FailOperation(failedDeployOp.ID, "runtime exited with code 3"); err != nil {
		t.Fatalf("fail deploy operation: %v", err)
	}

	storedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get failed app: %v", err)
	}
	if storedApp.Spec.Image != "" {
		t.Fatalf("expected stored app image to remain empty after failed deploy, got %q", storedApp.Spec.Image)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	return s, server, apiKey, storedApp, recoveredSpec.Image, recoveredSource
}

func performJSONRequest(t *testing.T, server *Server, method, target, apiKey string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
	}

	req := httptest.NewRequest(method, target, bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	return recorder
}

func mustDecodeJSON(t *testing.T, recorder *httptest.ResponseRecorder, dst any) {
	t.Helper()
	if err := json.Unmarshal(recorder.Body.Bytes(), dst); err != nil {
		t.Fatalf("decode response: %v body=%s", err, recorder.Body.String())
	}
}

func completeNextManagedOperation(t *testing.T, s *store.Store) {
	t.Helper()
	op, found, err := s.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim next operation: %v", err)
	}
	if !found {
		t.Fatal("expected pending operation")
	}
	if _, err := s.CompleteManagedOperation(op.ID, "/tmp/app.yaml", "done"); err != nil {
		t.Fatalf("complete managed operation: %v", err)
	}
}

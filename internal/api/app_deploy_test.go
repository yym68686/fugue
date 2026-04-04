package api

import (
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestDeployAppWorkspacePatchPreservesCurrentSpec(t *testing.T) {
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
		},
	})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/deploy", apiKey, map[string]any{
		"workspace": map[string]any{
			"mount_path": "/workspace",
		},
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Operation.ID == "" {
		t.Fatal("expected deploy response to include operation id")
	}

	ops, err := s.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 queued operation, got %d", len(ops))
	}
	if ops[0].DesiredSpec == nil {
		t.Fatal("expected desired spec on deploy operation")
	}
	if ops[0].DesiredSpec.Workspace == nil || ops[0].DesiredSpec.Workspace.MountPath != "/workspace" {
		t.Fatalf("expected workspace mount path /workspace, got %+v", ops[0].DesiredSpec.Workspace)
	}
	if got := ops[0].DesiredSpec.Env["OPENAI_API_KEY"]; got != "sk-demo" {
		t.Fatalf("expected env to be preserved, got %q", got)
	}
	if got := ops[0].DesiredSpec.Image; got != "ghcr.io/example/demo:latest" {
		t.Fatalf("expected image to be preserved, got %q", got)
	}
	if len(ops[0].DesiredSpec.Files) != 1 {
		t.Fatalf("expected 1 desired file, got %d", len(ops[0].DesiredSpec.Files))
	}
	if got := ops[0].DesiredSpec.Files[0].Content; got != "providers: []" {
		t.Fatalf("expected secret file content to be preserved in desired spec, got %q", got)
	}
}

func TestDeployAppWorkspacePatchRecoversFailedImportedAppDesiredState(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app, recoveredImage, recoveredSource := setupFailedImportedAppRecoveryServer(t)

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/deploy", apiKey, map[string]any{
		"workspace": map[string]any{
			"mount_path": "/workspace",
		},
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Operation.DesiredSpec == nil {
		t.Fatal("expected desired spec on deploy response")
	}
	if got := response.Operation.DesiredSpec.Image; got != recoveredImage {
		t.Fatalf("expected recovered image %q, got %q", recoveredImage, got)
	}
	if response.Operation.DesiredSpec.Workspace == nil || response.Operation.DesiredSpec.Workspace.MountPath != "/workspace" {
		t.Fatalf("expected workspace mount path /workspace, got %+v", response.Operation.DesiredSpec.Workspace)
	}
	if response.Operation.DesiredSource == nil {
		t.Fatal("expected desired source on deploy response")
	}
	if got := response.Operation.DesiredSource.ResolvedImageRef; got != recoveredSource.ResolvedImageRef {
		t.Fatalf("expected resolved image ref %q, got %q", recoveredSource.ResolvedImageRef, got)
	}

	ops, err := s.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 3 {
		t.Fatalf("expected import, failed deploy, and recovered deploy operations, got %d", len(ops))
	}
}

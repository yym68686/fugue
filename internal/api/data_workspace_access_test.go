package api

import (
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestDataWorkspaceTenantAccessGrantExposesGlobalWorkspace(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	owner, err := stateStore.CreateTenant("Owner Tenant")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	grantee, err := stateStore.CreateTenant("Grantee Tenant")
	if err != nil {
		t.Fatalf("create grantee tenant: %v", err)
	}
	_, adminKey, err := stateStore.CreateAPIKey(owner.ID, "platform-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create admin key: %v", err)
	}
	_, readerKey, err := stateStore.CreateAPIKey(grantee.ID, "reader", []string{"data.read", "data.write"})
	if err != nil {
		t.Fatalf("create reader key: %v", err)
	}
	_, secondReaderKey, err := stateStore.CreateAPIKey(grantee.ID, "second-reader", []string{"data.read"})
	if err != nil {
		t.Fatalf("create second reader key: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{
		Name: "global weights",
		Assets: []model.DataAsset{{
			Name:            "weights",
			Path:            "./weights.pt",
			MaterializePath: "./weights.pt",
			Mode:            model.DataAssetModeReadMostly,
			Required:        true,
		}},
	})
	if err != nil {
		t.Fatalf("create global workspace: %v", err)
	}
	if _, err := stateStore.CreateDataSnapshot(model.DataSnapshot{
		WorkspaceID: workspace.ID,
		Version:     "v1",
		Manifest: model.DataManifest{
			Entries: []model.DataManifestEntry{},
		},
	}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/data/workspaces/"+workspace.ID, readerKey, nil)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected shared workspace to be hidden before grant, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/data/workspaces/"+workspace.ID+"/access", adminKey, map[string]any{
		"tenant_id": grantee.ID,
		"role":      "reader",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected access grant, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	for _, secret := range []string{readerKey, secondReaderKey} {
		recorder = performJSONRequest(t, server, http.MethodGet, "/v1/data/workspaces/"+workspace.ID, secret, nil)
		if recorder.Code != http.StatusOK {
			t.Fatalf("expected tenant key to read shared workspace, got %d body=%s", recorder.Code, recorder.Body.String())
		}
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/data/workspaces/"+workspace.ID+"/snapshots", readerKey, map[string]any{
		"version": "reader-write-blocked",
		"manifest": map[string]any{
			"entries": []any{},
		},
	})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected reader role to block writes, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/data/workspaces/"+workspace.ID+"/transfers/plan-download", readerKey, map[string]any{
		"version": "v1",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected reader role to plan download, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var plan struct {
		Transfer model.DataTransfer `json:"transfer"`
	}
	mustDecodeJSON(t, recorder, &plan)
	if plan.Transfer.TenantID != grantee.ID {
		t.Fatalf("expected shared download transfer to belong to grantee tenant %q, got %q", grantee.ID, plan.Transfer.TenantID)
	}
}

func TestDataWorkspaceAPIKeyAccessGrantIsTokenScoped(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	owner, err := stateStore.CreateTenant("Owner Tenant")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	grantee, err := stateStore.CreateTenant("Grantee Tenant")
	if err != nil {
		t.Fatalf("create grantee tenant: %v", err)
	}
	_, adminKey, err := stateStore.CreateAPIKey(owner.ID, "platform-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create admin key: %v", err)
	}
	targetKey, targetSecret, err := stateStore.CreateAPIKey(grantee.ID, "target-token", []string{"data.read"})
	if err != nil {
		t.Fatalf("create target key: %v", err)
	}
	_, otherSecret, err := stateStore.CreateAPIKey(grantee.ID, "other-token", []string{"data.read"})
	if err != nil {
		t.Fatalf("create other key: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{Name: "token scoped weights"})
	if err != nil {
		t.Fatalf("create global workspace: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/data/workspaces/"+workspace.ID+"/access", adminKey, map[string]any{
		"api_key_id": targetKey.ID,
		"role":       "reader",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected api-key access grant, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/data/workspaces/"+workspace.ID, targetSecret, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected target token to read shared workspace, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/data/workspaces/"+workspace.ID, otherSecret, nil)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected other token in same tenant to remain unauthorized, got %d body=%s", recorder.Code, recorder.Body.String())
	}
}

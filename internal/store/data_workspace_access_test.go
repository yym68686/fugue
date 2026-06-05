package store

import (
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestDataWorkspaceAccessRoleForTenantAndAPIKeyGrants(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Grantee")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	apiKey, _, err := s.CreateAPIKey(tenant.ID, "target", []string{"data.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	tenantWorkspace, err := s.CreateDataWorkspace(model.DataWorkspace{Name: "tenant shared"})
	if err != nil {
		t.Fatalf("create tenant workspace: %v", err)
	}
	apiKeyWorkspace, err := s.CreateDataWorkspace(model.DataWorkspace{Name: "token shared"})
	if err != nil {
		t.Fatalf("create token workspace: %v", err)
	}

	if _, err := s.GrantDataWorkspaceAccess(tenantWorkspace.ID, "", true, model.DataWorkspaceAccessSubjectTenant, tenant.ID, model.DataWorkspaceAccessRoleReader, "test"); err != nil {
		t.Fatalf("grant tenant access: %v", err)
	}
	grants, err := s.ListDataWorkspaceAccessGrants(tenantWorkspace.ID)
	if err != nil {
		t.Fatalf("list tenant grants: %v", err)
	}
	if len(grants) != 1 {
		t.Fatalf("expected one tenant grant, got %+v", grants)
	}
	role, ok, err := s.DataWorkspaceAccessRole(tenantWorkspace.ID, tenant.ID, model.ActorTypeAPIKey, apiKey.ID, false)
	if err != nil {
		t.Fatalf("read tenant role: %v", err)
	}
	if !ok || role != model.DataWorkspaceAccessRoleReader {
		t.Fatalf("expected tenant reader role, got role=%q ok=%t grants=%+v tenant=%q actor_type=%q actor_id=%q", role, ok, grants, tenant.ID, model.ActorTypeAPIKey, apiKey.ID)
	}

	if _, err := s.GrantDataWorkspaceAccess(apiKeyWorkspace.ID, "", true, model.DataWorkspaceAccessSubjectAPIKey, apiKey.ID, model.DataWorkspaceAccessRoleWriter, "test"); err != nil {
		t.Fatalf("grant api key access: %v", err)
	}
	role, ok, err = s.DataWorkspaceAccessRole(apiKeyWorkspace.ID, tenant.ID, model.ActorTypeAPIKey, apiKey.ID, false)
	if err != nil {
		t.Fatalf("read api key role: %v", err)
	}
	if !ok || role != model.DataWorkspaceAccessRoleWriter {
		t.Fatalf("expected api key writer role, got role=%q ok=%t", role, ok)
	}
}

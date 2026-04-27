package api

import (
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestDeleteRuntimeRemovesOwnedOfflineRuntime(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Delete Runtime Owner")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, ownerKey, err := s.CreateAPIKey(tenant.ID, "runtime-owner", []string{"runtime.write"})
	if err != nil {
		t.Fatalf("create owner api key: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "offline-node", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	if _, err := s.DetachRuntimeOwnership(runtimeObj.ID); err != nil {
		t.Fatalf("detach runtime ownership: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodDelete, "/v1/runtimes/"+runtimeObj.ID, ownerKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Deleted bool          `json:"deleted"`
		Runtime model.Runtime `json:"runtime"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.Deleted {
		t.Fatal("expected runtime delete response to be marked deleted")
	}
	if response.Runtime.ID != runtimeObj.ID {
		t.Fatalf("expected deleted runtime %q, got %q", runtimeObj.ID, response.Runtime.ID)
	}
	if _, err := s.GetRuntime(runtimeObj.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected runtime to be deleted, got %v", err)
	}

	events, err := s.ListAuditEvents(tenant.ID, false, 0)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one audit event, got %+v", events)
	}
	if events[0].Action != "runtime.delete" {
		t.Fatalf("expected runtime.delete audit event, got %q", events[0].Action)
	}
	if events[0].TargetID != runtimeObj.ID {
		t.Fatalf("expected delete audit target %q, got %q", runtimeObj.ID, events[0].TargetID)
	}
}

func TestDeleteRuntimeRequiresOwner(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Delete Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	other, err := s.CreateTenant("Delete Other")
	if err != nil {
		t.Fatalf("create other tenant: %v", err)
	}
	_, otherKey, err := s.CreateAPIKey(other.ID, "other-runtime-writer", []string{"runtime.write"})
	if err != nil {
		t.Fatalf("create other api key: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(owner.ID, "owner-offline-node", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	if _, err := s.DetachRuntimeOwnership(runtimeObj.ID); err != nil {
		t.Fatalf("detach runtime ownership: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodDelete, "/v1/runtimes/"+runtimeObj.ID, otherKey, nil)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "only runtime owner can delete this server") {
		t.Fatalf("expected owner-only error, got %s", recorder.Body.String())
	}
}

func TestDeleteRuntimeReturnsConflictDetailsForReferencedOfflineRuntime(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Delete Runtime Blocked Owner")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "blocked-runtime", "blocked runtime project")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:   500,
		MemoryMebibytes: 1024,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}
	_, ownerKey, err := s.CreateAPIKey(tenant.ID, "runtime-owner", []string{"runtime.write"})
	if err != nil {
		t.Fatalf("create owner api key: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "blocked-offline-node", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	if _, err := s.CreateApp(tenant.ID, project.ID, "blocked-app", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
	}); err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := s.DetachRuntimeOwnership(runtimeObj.ID); err != nil {
		t.Fatalf("detach runtime ownership: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodDelete, "/v1/runtimes/"+runtimeObj.ID, ownerKey, nil)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusConflict, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "apps, services, or active operations") {
		t.Fatalf("expected detailed conflict error, got %s", recorder.Body.String())
	}
}

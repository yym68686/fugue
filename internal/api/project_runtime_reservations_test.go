package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestProjectRuntimeReservationRequiresRuntimeReserveScope(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Runtime Reservation API Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := stateStore.CreateRuntime(tenant.ID, "tenant-vps", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	_, projectWriter, err := stateStore.CreateAPIKey(tenant.ID, "project-writer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create project writer key: %v", err)
	}
	_, runtimeReserver, err := stateStore.CreateAPIKey(tenant.ID, "runtime-reserver", []string{"runtime.reserve"})
	if err != nil {
		t.Fatalf("create runtime reserver key: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})

	forbiddenReq := httptest.NewRequest(http.MethodPost, "/v1/projects/"+project.ID+"/runtime-reservations", strings.NewReader(`{"runtime_id":"`+runtimeObj.ID+`"}`))
	forbiddenReq.Header.Set("Authorization", "Bearer "+projectWriter)
	forbiddenReq.Header.Set("Content-Type", "application/json")
	forbiddenRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(forbiddenRecorder, forbiddenReq)
	if forbiddenRecorder.Code != http.StatusForbidden {
		t.Fatalf("expected forbidden without runtime.reserve, got %d body=%s", forbiddenRecorder.Code, forbiddenRecorder.Body.String())
	}

	reserveReq := httptest.NewRequest(http.MethodPost, "/v1/projects/"+project.ID+"/runtime-reservations", strings.NewReader(`{"runtime_id":"`+runtimeObj.ID+`"}`))
	reserveReq.Header.Set("Authorization", "Bearer "+runtimeReserver)
	reserveReq.Header.Set("Content-Type", "application/json")
	reserveRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(reserveRecorder, reserveReq)
	if reserveRecorder.Code != http.StatusCreated {
		t.Fatalf("expected created reservation, got %d body=%s", reserveRecorder.Code, reserveRecorder.Body.String())
	}
	var reserveResponse struct {
		RuntimeReservation model.ProjectRuntimeReservation `json:"runtime_reservation"`
	}
	if err := json.Unmarshal(reserveRecorder.Body.Bytes(), &reserveResponse); err != nil {
		t.Fatalf("decode reserve response: %v", err)
	}
	if reserveResponse.RuntimeReservation.RuntimeID != runtimeObj.ID || reserveResponse.RuntimeReservation.ProjectID != project.ID {
		t.Fatalf("unexpected reserve response: %+v", reserveResponse.RuntimeReservation)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/v1/projects/"+project.ID+"/runtime-reservations", nil)
	listReq.Header.Set("Authorization", "Bearer "+projectWriter)
	listRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(listRecorder, listReq)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("expected reservation list, got %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	var listResponse struct {
		RuntimeReservations []model.ProjectRuntimeReservation `json:"runtime_reservations"`
	}
	if err := json.Unmarshal(listRecorder.Body.Bytes(), &listResponse); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listResponse.RuntimeReservations) != 1 || listResponse.RuntimeReservations[0].RuntimeID != runtimeObj.ID {
		t.Fatalf("unexpected reservation list: %+v", listResponse.RuntimeReservations)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/v1/projects/"+project.ID+"/runtime-reservations/"+runtimeObj.ID, nil)
	deleteReq.Header.Set("Authorization", "Bearer "+runtimeReserver)
	deleteRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(deleteRecorder, deleteReq)
	if deleteRecorder.Code != http.StatusOK {
		t.Fatalf("expected deleted reservation, got %d body=%s", deleteRecorder.Code, deleteRecorder.Body.String())
	}
}

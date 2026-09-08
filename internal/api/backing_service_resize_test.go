package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestResizeBackingServiceQueuesSecretSafeInPlaceOperationWithoutPersistingTargetEarly(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
	target := model.ResourceSpec{
		CPUMilliCores:        1000,
		MemoryMebibytes:      1536,
		CPULimitMilliCores:   1500,
		MemoryLimitMebibytes: 2048,
	}
	response := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/resize", apiKey, map[string]any{
		"runtime_resources": target,
	})
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, response.Code, response.Body.String())
	}
	assertResponseOmitsSecrets(t, response.Body.String(), "lifecycle-secret")

	var payload struct {
		BackingService model.BackingService `json:"backing_service"`
		Operation      model.Operation      `json:"operation"`
	}
	mustDecodeJSON(t, response, &payload)
	if payload.Operation.Type != model.OperationTypeDatabaseResize ||
		payload.Operation.AppID != app.ID || payload.Operation.ServiceID != service.ID {
		t.Fatalf("unexpected resize operation: %+v", payload.Operation)
	}
	if payload.Operation.DesiredSpec == nil || payload.Operation.DesiredSpec.Postgres == nil ||
		payload.Operation.DesiredSpec.Postgres.RuntimeResources == nil ||
		*payload.Operation.DesiredSpec.Postgres.RuntimeResources != target {
		t.Fatalf("unexpected resize target: %+v", payload.Operation.DesiredSpec)
	}
	if payload.Operation.DesiredSpec.Postgres.Password != apiRedactedSecretValue {
		t.Fatalf("resize operation leaked postgres password: %+v", payload.Operation.DesiredSpec.Postgres)
	}
	if payload.BackingService.Spec.Postgres == nil || payload.BackingService.Spec.Postgres.Password != apiRedactedSecretValue {
		t.Fatalf("resize response leaked backing service password: %+v", payload.BackingService.Spec.Postgres)
	}

	persisted, err := stateStore.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("reload backing service: %v", err)
	}
	if persisted.Spec.Postgres == nil || persisted.Spec.Postgres.RuntimeResources != nil {
		t.Fatalf("runtime target must not persist before controller verification: %+v", persisted.Spec.Postgres)
	}
	if persisted.Spec.Postgres.Resources == nil || persisted.Spec.Postgres.Resources.CPUMilliCores != 750 ||
		persisted.Spec.Postgres.Resources.MemoryMebibytes != 1024 {
		t.Fatalf("resize request changed postgres bootstrap resources: %+v", persisted.Spec.Postgres.Resources)
	}
	assertBackingServiceLifecycleAudit(t, stateStore, app.TenantID, "backing_service.resize", "accepted")
}

func TestResizeBackingServiceRetryReusesExactOperationAndRejectsChangedTarget(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
	target := model.ResourceSpec{
		CPUMilliCores:        1000,
		MemoryMebibytes:      1536,
		CPULimitMilliCores:   1500,
		MemoryLimitMebibytes: 2048,
	}
	request := map[string]any{"runtime_resources": target}
	first := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/resize", apiKey, request)
	if first.Code != http.StatusAccepted {
		t.Fatalf("expected first status %d, got %d body=%s", http.StatusAccepted, first.Code, first.Body.String())
	}
	var firstPayload struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, first, &firstPayload)

	retry := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/resize", apiKey, request)
	if retry.Code != http.StatusAccepted {
		t.Fatalf("expected retry status %d, got %d body=%s", http.StatusAccepted, retry.Code, retry.Body.String())
	}
	var retryPayload struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, retry, &retryPayload)
	if retryPayload.Operation.ID == "" || retryPayload.Operation.ID != firstPayload.Operation.ID {
		t.Fatalf("expected exact retry to reuse %s, got %s", firstPayload.Operation.ID, retryPayload.Operation.ID)
	}
	assertBackingServiceLifecycleAudit(t, stateStore, app.TenantID, "backing_service.resize", "reused_existing")

	changed := target
	changed.CPUMilliCores++
	conflict := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/resize", apiKey, map[string]any{
		"runtime_resources": changed,
	})
	if conflict.Code != http.StatusConflict {
		t.Fatalf("expected changed target status %d, got %d body=%s", http.StatusConflict, conflict.Code, conflict.Body.String())
	}

	operations, err := stateStore.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	resizeCount := 0
	for _, operation := range operations {
		if operation.Type == model.OperationTypeDatabaseResize {
			resizeCount++
			if operation.ID != firstPayload.Operation.ID {
				t.Fatalf("unexpected second resize operation: %+v", operation)
			}
		}
	}
	if resizeCount != 1 {
		t.Fatalf("expected one resize operation, got %d operations=%+v", resizeCount, operations)
	}
}

func TestResizeBackingServiceRequiresWriteScope(t *testing.T) {
	t.Parallel()

	stateStore, server, _, app, service := managedPostgresLifecycleFixture(t, 0)
	_, readOnlyKey, err := stateStore.CreateAPIKey(app.TenantID, "database-reader", []string{"app.read"})
	if err != nil {
		t.Fatalf("create read-only api key: %v", err)
	}
	response := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/resize", readOnlyKey, map[string]any{
		"runtime_resources": model.ResourceSpec{
			CPUMilliCores:        1000,
			MemoryMebibytes:      1536,
			CPULimitMilliCores:   1500,
			MemoryLimitMebibytes: 2048,
		},
	})
	if response.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusForbidden, response.Code, response.Body.String())
	}
	operations, err := stateStore.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	for _, operation := range operations {
		if operation.Type == model.OperationTypeDatabaseResize {
			t.Fatalf("read-only credential queued resize operation: %+v", operation)
		}
	}
}

func TestResizeBackingServiceRejectsIncompleteOrInvertedResourceEnvelope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		resources map[string]any
	}{
		{
			name: "missing cpu limit",
			resources: map[string]any{
				"cpu_millicores": 1000, "memory_mebibytes": 1536, "memory_limit_mebibytes": 2048,
			},
		},
		{
			name: "cpu limit below request",
			resources: map[string]any{
				"cpu_millicores": 1000, "memory_mebibytes": 1536,
				"cpu_limit_millicores": 999, "memory_limit_mebibytes": 2048,
			},
		},
		{
			name: "memory limit below request",
			resources: map[string]any{
				"cpu_millicores": 1000, "memory_mebibytes": 1536,
				"cpu_limit_millicores": 1500, "memory_limit_mebibytes": 1535,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
			response := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/resize", apiKey, map[string]any{
				"runtime_resources": test.resources,
			})
			if response.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, response.Code, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), "invalid input") {
				t.Fatalf("expected stable invalid-input error, got %s", response.Body.String())
			}
			operations, err := stateStore.ListOperationsByApp(app.TenantID, false, app.ID)
			if err != nil {
				t.Fatalf("list operations: %v", err)
			}
			for _, operation := range operations {
				if operation.Type == model.OperationTypeDatabaseResize {
					t.Fatalf("invalid request queued resize operation: %+v", operation)
				}
			}
			assertBackingServiceLifecycleAudit(t, stateStore, app.TenantID, "backing_service.resize", "invalid_resource_envelope")
		})
	}
}

func TestResizeBackingServiceReportsExactDatabaseMutationInterlocks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "backup",
			err:     store.ErrManagedPostgresBackupInProgressConflict,
			message: store.ManagedPostgresBackupInProgressConflictMessage,
		},
		{
			name:    "import",
			err:     store.ErrManagedPostgresImportInProgressConflict,
			message: store.ManagedPostgresImportInProgressConflictMessage,
		},
		{
			name:    "restore",
			err:     store.ErrManagedPostgresRestoreInProgressConflict,
			message: store.ManagedPostgresRestoreInProgressConflictMessage,
		},
	}
	server := &Server{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			server.writeBackingServiceResizeStoreError(recorder, test.err)
			if recorder.Code != http.StatusConflict {
				t.Fatalf("expected status %d, got %d body=%s", http.StatusConflict, recorder.Code, recorder.Body.String())
			}
			if !strings.Contains(recorder.Body.String(), test.message) {
				t.Fatalf("expected exact interlock message %q, got %s", test.message, recorder.Body.String())
			}
		})
	}
}

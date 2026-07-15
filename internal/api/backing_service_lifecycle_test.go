package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestSuspendBackingServiceQueuesOperationWithoutPersistingDesiredStateEarly(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
	response := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/suspend", apiKey, nil)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, response.Code, response.Body.String())
	}
	assertResponseOmitsSecrets(t, response.Body.String(), "lifecycle-secret")

	var payload struct {
		BackingService model.BackingService `json:"backing_service"`
		Operation      model.Operation      `json:"operation"`
		AlreadyCurrent bool                 `json:"already_current"`
	}
	mustDecodeJSON(t, response, &payload)
	if payload.AlreadyCurrent {
		t.Fatal("expected a newly queued suspend operation")
	}
	if payload.Operation.Type != model.OperationTypeDatabaseSuspend || payload.Operation.AppID != app.ID || payload.Operation.ServiceID != service.ID {
		t.Fatalf("unexpected suspend operation: %+v", payload.Operation)
	}
	if payload.Operation.DesiredSpec == nil || payload.Operation.DesiredSpec.Postgres == nil || !payload.Operation.DesiredSpec.Postgres.Suspended {
		t.Fatalf("expected suspended desired postgres spec, got %+v", payload.Operation.DesiredSpec)
	}
	if payload.Operation.DesiredSpec.Postgres.Password != apiRedactedSecretValue {
		t.Fatalf("lifecycle operation leaked postgres password: %+v", payload.Operation.DesiredSpec.Postgres)
	}
	if payload.BackingService.Spec.Postgres == nil || !payload.BackingService.Spec.Postgres.Suspended {
		t.Fatalf("accepted response must project desired suspended=true, got %+v", payload.BackingService.Spec.Postgres)
	}
	if payload.BackingService.Spec.Postgres.Password != apiRedactedSecretValue {
		t.Fatalf("lifecycle service leaked postgres password: %+v", payload.BackingService.Spec.Postgres)
	}
	if payload.BackingService.RuntimeStatus == nil || payload.BackingService.RuntimeStatus.Phase != model.ManagedPostgresRuntimePhaseSuspending {
		t.Fatalf("accepted response must project suspending phase, got %+v", payload.BackingService.RuntimeStatus)
	}

	persisted, err := stateStore.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("get persisted backing service: %v", err)
	}
	if persisted.Spec.Postgres == nil || persisted.Spec.Postgres.Suspended {
		t.Fatalf("desired state must not persist before controller verification, got %+v", persisted.Spec.Postgres)
	}
	assertBackingServiceLifecycleAudit(t, stateStore, app.TenantID, "backing_service.suspend", "accepted")
}

func TestResumeBackingServiceQueuesOperationAndProjectsResumingState(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
	suspend, err := stateStore.CreateOperation(model.Operation{
		TenantID:  app.TenantID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     app.ID,
		ServiceID: service.ID,
	})
	if err != nil {
		t.Fatalf("queue setup suspend operation: %v", err)
	}
	if _, err := stateStore.CompleteManagedOperation(suspend.ID, "", "suspended for resume test"); err != nil {
		t.Fatalf("complete setup suspend operation: %v", err)
	}
	service, err = stateStore.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("get suspended backing service: %v", err)
	}

	response := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/resume", apiKey, nil)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, response.Code, response.Body.String())
	}
	var payload struct {
		BackingService model.BackingService `json:"backing_service"`
		Operation      model.Operation      `json:"operation"`
	}
	mustDecodeJSON(t, response, &payload)
	if payload.Operation.Type != model.OperationTypeDatabaseResume || payload.Operation.AppID != app.ID {
		t.Fatalf("unexpected resume operation: %+v", payload.Operation)
	}
	if payload.BackingService.Spec.Postgres == nil || payload.BackingService.Spec.Postgres.Suspended {
		t.Fatalf("accepted response must project desired suspended=false, got %+v", payload.BackingService.Spec.Postgres)
	}
	if payload.BackingService.RuntimeStatus == nil || payload.BackingService.RuntimeStatus.Phase != model.ManagedPostgresRuntimePhaseResuming {
		t.Fatalf("accepted response must project resuming phase, got %+v", payload.BackingService.RuntimeStatus)
	}
	persisted, err := stateStore.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("get persisted backing service: %v", err)
	}
	if persisted.Spec.Postgres == nil || !persisted.Spec.Postgres.Suspended {
		t.Fatalf("resume must not persist before controller verification, got %+v", persisted.Spec.Postgres)
	}
}

func TestSuspendBackingServiceRejectsRunningOwnerWithoutBinding(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 1)
	for _, binding := range app.Bindings {
		if binding.ServiceID != service.ID {
			continue
		}
		if _, err := stateStore.UnbindBackingService(binding.ID); err != nil {
			t.Fatalf("remove owner binding: %v", err)
		}
	}
	app, err := stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after unbind: %v", err)
	}
	if appHasServiceBinding(app, service.ID) {
		t.Fatal("test setup still has a service binding")
	}

	response := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/suspend", apiKey, nil)
	if response.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusConflict, response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "stop every app") {
		t.Fatalf("expected actionable running-app rejection, got %s", response.Body.String())
	}
	operations, err := stateStore.ListOperations(app.TenantID, false)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(operations) != 0 {
		t.Fatalf("unsafe suspend must not queue an operation, got %+v", operations)
	}
	assertBackingServiceLifecycleAudit(t, stateStore, app.TenantID, "backing_service.suspend", "rejected_bound_app_running")
}

func TestSuspendBackingServiceFailsClosedWithoutFreshRuntimeProof(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
	server.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
		return nil, errors.New("kubernetes API unavailable")
	}
	response := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/suspend", apiKey, nil)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusServiceUnavailable, response.Code, response.Body.String())
	}
	assertNoLifecycleOperation(t, stateStore, app.TenantID)
	assertBackingServiceLifecycleAudit(t, stateStore, app.TenantID, "backing_service.suspend", "observation_unavailable")
}

func TestSuspendBackingServiceFailsClosedWhenManagedAppStatusIsMissing(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
	installManagedAppInventoryServer(t, server, nil)
	response := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/suspend", apiKey, nil)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusServiceUnavailable, response.Code, response.Body.String())
	}
	assertNoLifecycleOperation(t, stateStore, app.TenantID)
}

func TestBackingServiceLifecycleRetryReusesSameOperationAndOppositeConflicts(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
	first := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/suspend", apiKey, nil)
	if first.Code != http.StatusAccepted {
		t.Fatalf("expected first suspend status %d, got %d body=%s", http.StatusAccepted, first.Code, first.Body.String())
	}
	var firstPayload struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, first, &firstPayload)
	if firstPayload.Operation.ID == "" {
		t.Fatal("expected first lifecycle operation id")
	}
	auditsBefore := countAuditAction(t, stateStore, app.TenantID, "backing_service.suspend")
	server.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
		return nil, errors.New("runtime inventory unavailable during network retry")
	}

	retry := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/suspend", apiKey, nil)
	if retry.Code != http.StatusAccepted {
		t.Fatalf("expected retry suspend status %d, got %d body=%s", http.StatusAccepted, retry.Code, retry.Body.String())
	}
	var retryPayload struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, retry, &retryPayload)
	if retryPayload.Operation.ID != firstPayload.Operation.ID {
		t.Fatalf("expected retry to reuse operation %s, got %s", firstPayload.Operation.ID, retryPayload.Operation.ID)
	}
	operations, err := stateStore.ListOperations(app.TenantID, false)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	activeLifecycleCount := 0
	for _, operation := range operations {
		if operation.ServiceID == service.ID && operation.Type == model.OperationTypeDatabaseSuspend &&
			(operation.Status == model.OperationStatusPending || operation.Status == model.OperationStatusRunning || operation.Status == model.OperationStatusWaitingAgent) {
			activeLifecycleCount++
		}
	}
	if activeLifecycleCount != 1 {
		t.Fatalf("expected one active lifecycle operation, got %d from %+v", activeLifecycleCount, operations)
	}
	if auditsAfter := countAuditAction(t, stateStore, app.TenantID, "backing_service.suspend"); auditsAfter != auditsBefore {
		t.Fatalf("idempotent retry duplicated accepted audit: before=%d after=%d", auditsBefore, auditsAfter)
	}

	opposite := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/resume", apiKey, nil)
	if opposite.Code != http.StatusConflict {
		t.Fatalf("expected opposite lifecycle status %d, got %d body=%s", http.StatusConflict, opposite.Code, opposite.Body.String())
	}
}

func TestBackingServiceLifecycleConvergenceMatchesControllerReadiness(t *testing.T) {
	t.Parallel()

	service := model.BackingService{
		Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{Instances: 3}},
		RuntimeStatus: &model.BackingServiceRuntimeStatus{
			Phase:            model.ManagedPostgresRuntimePhaseActive,
			ReadyInstances:   1,
			DesiredInstances: 3,
		},
	}
	if !backingServiceLifecycleConverged(service, false) {
		t.Fatal("primary-ready active service must be considered converged while replicas recover")
	}
	service.Spec.Postgres.Suspended = true
	service.RuntimeStatus.Phase = model.ManagedPostgresRuntimePhaseSuspended
	service.RuntimeStatus.ReadyInstances = 0
	if !backingServiceLifecycleConverged(service, true) {
		t.Fatal("hibernated service with zero ready instances must be considered converged")
	}
}

func TestBackingServiceGetOverlaysObservedRuntimeStatus(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
	cacheManagedPostgresStatus(server, app.ID, runtime.ManagedBackingServiceStatus{
		ServiceID:        service.ID,
		Phase:            model.ManagedPostgresRuntimePhaseSuspended,
		Message:          "hibernated",
		ReadyInstances:   0,
		DesiredInstances: 1,
	})
	server.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
		return nil, errors.New("runtime status refresh unavailable")
	}
	response := performJSONRequest(t, server, http.MethodGet, "/v1/backing-services/"+service.ID, apiKey, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, response.Code, response.Body.String())
	}
	var payload struct {
		BackingService model.BackingService `json:"backing_service"`
	}
	mustDecodeJSON(t, response, &payload)
	if payload.BackingService.RuntimeStatus == nil || payload.BackingService.RuntimeStatus.Phase != model.ManagedPostgresRuntimePhaseSuspended {
		t.Fatalf("expected observed suspended runtime status, got %+v", payload.BackingService.RuntimeStatus)
	}
	persisted, err := server.store.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("get persisted backing service: %v", err)
	}
	if persisted.RuntimeStatus != nil {
		t.Fatalf("observed runtime status must not be persisted, got %+v", persisted.RuntimeStatus)
	}
}

func TestBackingServiceLifecycleRefreshesStaleTransitionToTerminal(t *testing.T) {
	for _, test := range []struct {
		name              string
		suspended         bool
		transitionalPhase string
		terminalPhase     string
		terminalReady     int
		endpoint          string
	}{
		{
			name:              "suspend",
			suspended:         true,
			transitionalPhase: model.ManagedPostgresRuntimePhaseSuspending,
			terminalPhase:     model.ManagedPostgresRuntimePhaseSuspended,
			terminalReady:     0,
			endpoint:          "/suspend",
		},
		{
			name:              "resume",
			suspended:         false,
			transitionalPhase: model.ManagedPostgresRuntimePhaseResuming,
			terminalPhase:     model.ManagedPostgresRuntimePhaseActive,
			terminalReady:     1,
			endpoint:          "/resume",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stateStore, server, apiKey, app, service := managedPostgresLifecycleFixture(t, 0)
			completeManagedPostgresLifecycleForTest(t, stateStore, app, service, model.OperationTypeDatabaseSuspend)
			if !test.suspended {
				completeManagedPostgresLifecycleForTest(t, stateStore, app, service, model.OperationTypeDatabaseResume)
			}
			service, err := stateStore.GetBackingService(service.ID)
			if err != nil {
				t.Fatalf("get terminal desired service: %v", err)
			}
			cacheManagedPostgresStatus(server, app.ID, runtime.ManagedBackingServiceStatus{
				ServiceID:        service.ID,
				Phase:            test.transitionalPhase,
				ReadyInstances:   0,
				DesiredInstances: 1,
			})
			installManagedAppInventoryServer(t, server, []runtime.ManagedAppObject{{
				APIVersion: runtime.ManagedAppAPIVersion,
				Kind:       runtime.ManagedAppKind,
				Spec: runtime.ManagedAppSpec{
					AppID:     app.ID,
					TenantID:  app.TenantID,
					ProjectID: app.ProjectID,
					Name:      app.Name,
				},
				Status: runtime.ManagedAppStatus{
					Phase:           runtime.ManagedAppPhaseDisabled,
					DesiredReplicas: 0,
					ReadyReplicas:   0,
					BackingServices: []runtime.ManagedBackingServiceStatus{{
						ServiceID:        service.ID,
						Phase:            test.terminalPhase,
						ReadyInstances:   test.terminalReady,
						DesiredInstances: 1,
					}},
				},
			}})

			getResponse := performJSONRequest(t, server, http.MethodGet, "/v1/backing-services/"+service.ID, apiKey, nil)
			if getResponse.Code != http.StatusOK {
				t.Fatalf("expected terminal GET status %d, got %d body=%s", http.StatusOK, getResponse.Code, getResponse.Body.String())
			}
			var getPayload struct {
				BackingService model.BackingService `json:"backing_service"`
			}
			mustDecodeJSON(t, getResponse, &getPayload)
			if getPayload.BackingService.RuntimeStatus == nil || getPayload.BackingService.RuntimeStatus.Phase != test.terminalPhase {
				t.Fatalf("expected fresh terminal phase %q, got %+v", test.terminalPhase, getPayload.BackingService.RuntimeStatus)
			}

			before, err := stateStore.ListOperations(app.TenantID, false)
			if err != nil {
				t.Fatalf("list operations before repeat: %v", err)
			}
			repeat := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+test.endpoint, apiKey, nil)
			if repeat.Code != http.StatusOK {
				t.Fatalf("expected idempotent repeat status %d, got %d body=%s", http.StatusOK, repeat.Code, repeat.Body.String())
			}
			var repeatPayload struct {
				AlreadyCurrent bool `json:"already_current"`
			}
			mustDecodeJSON(t, repeat, &repeatPayload)
			if !repeatPayload.AlreadyCurrent {
				t.Fatal("expected already_current=true after fresh terminal observation")
			}
			after, err := stateStore.ListOperations(app.TenantID, false)
			if err != nil {
				t.Fatalf("list operations after repeat: %v", err)
			}
			if len(after) != len(before) {
				t.Fatalf("terminal repeat queued a duplicate operation: before=%d after=%d", len(before), len(after))
			}
		})
	}
}

func TestBackingServiceLifecycleEnforcesProjectScopedPrincipal(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Project Scoped Postgres")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)
	projectA, err := stateStore.CreateProject(tenant.ID, "project-a", "")
	if err != nil {
		t.Fatalf("create project A: %v", err)
	}
	projectB, err := stateStore.CreateProject(tenant.ID, "project-b", "")
	if err != nil {
		t.Fatalf("create project B: %v", err)
	}
	createDisabled := func(project model.Project, name string) (model.App, model.BackingService) {
		app, err := stateStore.CreateApp(tenant.ID, project.ID, name, "", model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: model.DefaultManagedRuntimeID,
			Postgres: &model.AppPostgresSpec{
				Database:    name,
				User:        name,
				Password:    name + "-secret",
				StorageSize: "1Gi",
				Resources: &model.ResourceSpec{
					CPUMilliCores:   500,
					MemoryMebibytes: 512,
				},
			},
		})
		if err != nil {
			t.Fatalf("create app %s: %v", name, err)
		}
		zero := 0
		scale, err := stateStore.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeScale, AppID: app.ID, DesiredReplicas: &zero})
		if err != nil {
			t.Fatalf("queue disable for %s: %v", name, err)
		}
		if _, err := stateStore.CompleteManagedOperation(scale.ID, "", "disabled"); err != nil {
			t.Fatalf("complete disable for %s: %v", name, err)
		}
		app, err = stateStore.GetApp(app.ID)
		if err != nil {
			t.Fatalf("get disabled app %s: %v", name, err)
		}
		return app, app.BackingServices[0]
	}
	appA, serviceA := createDisabled(projectA, "postgres-a")
	_, serviceB := createDisabled(projectB, "postgres-b")
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	principal := model.Principal{
		ActorType: model.ActorTypeWorkload,
		ActorID:   "project-scoped-test",
		TenantID:  tenant.ID,
		ProjectID: projectA.ID,
		AppID:     appA.ID,
		Scopes:    map[string]struct{}{"app.write": {}},
	}

	otherRequest := httptest.NewRequest(http.MethodPost, "/v1/backing-services/"+serviceB.ID+"/suspend", nil)
	otherRequest.SetPathValue("id", serviceB.ID)
	otherRecorder := httptest.NewRecorder()
	if _, allowed := server.loadAuthorizedBackingService(otherRecorder, otherRequest, principal); allowed {
		t.Fatal("project-scoped principal unexpectedly authorized for another project")
	}
	if otherRecorder.Code != http.StatusForbidden {
		t.Fatalf("expected cross-project authorization status %d, got %d body=%s", http.StatusForbidden, otherRecorder.Code, otherRecorder.Body.String())
	}

	sameRequest := httptest.NewRequest(http.MethodPost, "/v1/backing-services/"+serviceA.ID+"/suspend", nil)
	sameRequest.SetPathValue("id", serviceA.ID)
	sameRecorder := httptest.NewRecorder()
	loaded, allowed := server.loadAuthorizedBackingService(sameRecorder, sameRequest, principal)
	if !allowed || loaded.ID != serviceA.ID {
		t.Fatalf("same-project backing service should be authorized, allowed=%v service=%+v body=%s", allowed, loaded, sameRecorder.Body.String())
	}
	filtered := filterBackingServicesForPrincipal(principal, []model.BackingService{serviceA, serviceB})
	if len(filtered) != 1 || filtered[0].ID != serviceA.ID {
		t.Fatalf("expected project-scoped list filter to retain only service A, got %+v", filtered)
	}
}

func completeManagedPostgresLifecycleForTest(t *testing.T, stateStore *store.Store, app model.App, service model.BackingService, operationType string) {
	t.Helper()
	operation, err := stateStore.CreateOperation(model.Operation{
		TenantID:  app.TenantID,
		Type:      operationType,
		AppID:     app.ID,
		ServiceID: service.ID,
	})
	if err != nil {
		t.Fatalf("queue %s setup operation: %v", operationType, err)
	}
	if _, err := stateStore.CompleteManagedOperation(operation.ID, "", "terminal lifecycle test setup"); err != nil {
		t.Fatalf("complete %s setup operation: %v", operationType, err)
	}
}

func managedPostgresLifecycleFixture(t *testing.T, replicas int) (*store.Store, *Server, string, model.App, model.BackingService) {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Lifecycle Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)
	project, err := stateStore.CreateProject(tenant.ID, "database", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "postgres-owner", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database:    "demo",
			User:        "demo",
			Password:    "lifecycle-secret",
			StorageSize: "1Gi",
			Resources: &model.ResourceSpec{
				CPUMilliCores:   750,
				MemoryMebibytes: 1024,
			},
		},
	})
	if err != nil {
		t.Fatalf("create app with managed postgres: %v", err)
	}
	if len(app.BackingServices) != 1 {
		t.Fatalf("expected one owned backing service, got %+v", app.BackingServices)
	}
	if replicas == 0 {
		zero := 0
		op, err := stateStore.CreateOperation(model.Operation{
			TenantID:        tenant.ID,
			Type:            model.OperationTypeScale,
			AppID:           app.ID,
			DesiredReplicas: &zero,
		})
		if err != nil {
			t.Fatalf("queue app disable: %v", err)
		}
		if _, err := stateStore.CompleteManagedOperation(op.ID, "", "disabled for database lifecycle test"); err != nil {
			t.Fatalf("complete app disable: %v", err)
		}
		app, err = stateStore.GetApp(app.ID)
		if err != nil {
			t.Fatalf("get disabled app: %v", err)
		}
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "database-operator", []string{"app.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	phase := runtime.ManagedAppPhaseReady
	if replicas == 0 {
		phase = runtime.ManagedAppPhaseDisabled
	}
	installManagedAppInventoryServer(t, server, []runtime.ManagedAppObject{{
		APIVersion: runtime.ManagedAppAPIVersion,
		Kind:       runtime.ManagedAppKind,
		Metadata: runtime.ManagedAppMeta{
			Name:      runtime.ManagedAppResourceName(app),
			Namespace: runtime.NamespaceForTenant(app.TenantID),
		},
		Spec: runtime.ManagedAppSpec{AppID: app.ID, TenantID: app.TenantID, ProjectID: app.ProjectID, Name: app.Name},
		Status: runtime.ManagedAppStatus{
			Phase:           phase,
			DesiredReplicas: replicas,
			ReadyReplicas:   replicas,
			BackingServices: []runtime.ManagedBackingServiceStatus{{
				ServiceID:        app.BackingServices[0].ID,
				Phase:            model.ManagedPostgresRuntimePhaseActive,
				ReadyInstances:   1,
				DesiredInstances: 1,
			}},
		},
	}})
	return stateStore, server, apiKey, app, app.BackingServices[0]
}

func installManagedAppInventoryServer(t *testing.T, server *Server, items []runtime.ManagedAppObject) {
	t.Helper()
	kubeAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"items": items}); err != nil {
			t.Errorf("encode managed app inventory: %v", err)
		}
	}))
	t.Cleanup(kubeAPI.Close)
	server.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
		return &managedAppStatusClient{client: kubeAPI.Client(), baseURL: kubeAPI.URL, bearerToken: "test"}, nil
	}
}

func cacheManagedPostgresStatus(server *Server, appID string, status runtime.ManagedBackingServiceStatus) {
	now := time.Now()
	server.managedAppStatusCache.setList(managedAppStatusListCacheEntry{
		items: map[string]runtime.ManagedAppObject{
			appID: {
				Spec: runtime.ManagedAppSpec{AppID: appID},
				Status: runtime.ManagedAppStatus{
					BackingServices: []runtime.ManagedBackingServiceStatus{status},
				},
			},
		},
		ok:          true,
		refreshedAt: now,
		expiresAt:   now.Add(time.Minute),
	})
}

func assertBackingServiceLifecycleAudit(t *testing.T, stateStore *store.Store, tenantID, action, result string) {
	t.Helper()
	events, err := stateStore.ListAuditEvents(tenantID, false, 100)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	for _, event := range events {
		if event.Action == action && event.Metadata["result"] == result {
			return
		}
	}
	t.Fatalf("expected audit action=%q result=%q, got %+v", action, result, events)
}

func countAuditAction(t *testing.T, stateStore *store.Store, tenantID, action string) int {
	t.Helper()
	events, err := stateStore.ListAuditEvents(tenantID, false, 100)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	count := 0
	for _, event := range events {
		if event.Action == action {
			count++
		}
	}
	return count
}

func assertNoLifecycleOperation(t *testing.T, stateStore *store.Store, tenantID string) {
	t.Helper()
	operations, err := stateStore.ListOperations(tenantID, false)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	for _, operation := range operations {
		if operation.Type == model.OperationTypeDatabaseSuspend || operation.Type == model.OperationTypeDatabaseResume {
			t.Fatalf("unexpected lifecycle operation: %+v", operation)
		}
	}
}

func TestManagedPostgresOrphanListAndAdoptAreAdminOnlyAndSecretSafe(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Orphan Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)
	project, err := stateStore.CreateProject(tenant.ID, "retained", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, tenantKey, err := stateStore.CreateAPIKey(tenant.ID, "tenant-operator", []string{"app.write"})
	if err != nil {
		t.Fatalf("create tenant key: %v", err)
	}

	const bootstrapKey = "orphan-bootstrap-test-key"
	const databasePassword = "orphan-database-password-must-not-leak"
	const databaseURL = "postgres://orphan:orphan-dsn-secret@database:5432/orphan"
	const repositoryToken = "orphan-repository-token-must-not-leak"
	managed := retainedManagedPostgresOrphan(tenant.ID, project.ID, databasePassword, databaseURL, repositoryToken)
	managed.Spec.BackingServices[0].OwnerAppID = ""
	storageMode := "ready"
	kubeAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		exactManagedAppPath := "/apis/" + runtime.ManagedAppAPIGroup + "/v1alpha1/namespaces/" + managed.Metadata.Namespace + "/" + runtime.ManagedAppPlural + "/" + managed.Metadata.Name
		if r.URL.Path == exactManagedAppPath {
			if err := json.NewEncoder(w).Encode(managed); err != nil {
				t.Errorf("encode exact managed app: %v", err)
			}
			return
		}
		if r.URL.Path == "/apis/"+runtime.ManagedAppAPIGroup+"/v1alpha1/"+runtime.ManagedAppPlural {
			if err := json.NewEncoder(w).Encode(map[string]any{"items": []runtime.ManagedAppObject{managed}}); err != nil {
				t.Errorf("encode managed app list: %v", err)
			}
			return
		}
		deployments := runtime.ManagedBackingServiceDeployments(runtime.AppFromManagedApp(managed), managed.Spec.Scheduling)
		if len(deployments) != 1 {
			t.Errorf("expected one managed postgres deployment, got %+v", deployments)
			http.Error(w, "invalid test deployment", http.StatusInternalServerError)
			return
		}
		clusterName := deployments[0].ResourceName
		if strings.Contains(r.URL.Path, "/deployments/") {
			if storageMode == "forbidden" {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			replicas := 0
			if storageMode == "deployment-nonzero" {
				replicas = 1
			}
			deletionTimestamp := ""
			if storageMode == "deployment-deleting" {
				deletionTimestamp = time.Now().UTC().Format(time.RFC3339)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name":              runtime.RuntimeAppResourceName(runtime.AppFromManagedApp(managed)),
					"uid":               "app-deployment-orphan-api-test-uid",
					"generation":        9,
					"deletionTimestamp": deletionTimestamp,
					"ownerReferences":   []map[string]any{{"uid": managed.Metadata.UID}},
				},
				"spec": map[string]any{"replicas": replicas},
				"status": map[string]any{
					"observedGeneration":  9,
					"replicas":            replicas,
					"updatedReplicas":     replicas,
					"readyReplicas":       replicas,
					"availableReplicas":   replicas,
					"unavailableReplicas": 0,
				},
			})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/pods") {
			items := []map[string]any{}
			if storageMode == "terminating-pod" {
				items = append(items, map[string]any{"metadata": map[string]any{
					"name":              "terminating-orphan-app-pod",
					"deletionTimestamp": time.Now().UTC().Format(time.RFC3339),
					"labels":            map[string]string{runtime.FugueLabelAppID: managed.Spec.AppID},
				}})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
			return
		}
		if strings.Contains(r.URL.Path, "/clusters/") {
			switch storageMode {
			case "forbidden":
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			case "missing":
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			deletionTimestamp := ""
			if storageMode == "deleting" {
				deletionTimestamp = time.Now().UTC().Format(time.RFC3339)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
				"name":              clusterName,
				"uid":               "cnpg-cluster-orphan-api-test-uid",
				"deletionTimestamp": deletionTimestamp,
				"ownerReferences":   []map[string]any{{"uid": managed.Metadata.UID}},
			}})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims") {
			items := []map[string]any{}
			if storageMode != "zero-pvc" {
				deletionTimestamp := ""
				if storageMode == "deleting-pvc" {
					deletionTimestamp = time.Now().UTC().Format(time.RFC3339)
				}
				items = append(items, map[string]any{
					"metadata": map[string]any{
						"name":              clusterName + "-1",
						"deletionTimestamp": deletionTimestamp,
						"labels":            map[string]string{"cnpg.io/cluster": clusterName},
					},
					"status": map[string]any{"phase": "Bound"},
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	t.Cleanup(kubeAPI.Close)

	server := NewServer(stateStore, auth.New(stateStore, bootstrapKey), nil, ServerConfig{})
	server.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
		return &managedAppStatusClient{client: kubeAPI.Client(), baseURL: kubeAPI.URL, bearerToken: "test"}, nil
	}
	cacheManagedAppObject(server, managed)

	forbidden := performJSONRequest(t, server, http.MethodGet, "/v1/backing-services/orphans", tenantKey, nil)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("expected tenant orphan list status %d, got %d body=%s", http.StatusForbidden, forbidden.Code, forbidden.Body.String())
	}
	list := performJSONRequest(t, server, http.MethodGet, "/v1/backing-services/orphans", bootstrapKey, nil)
	if list.Code != http.StatusOK {
		t.Fatalf("expected orphan list status %d, got %d body=%s", http.StatusOK, list.Code, list.Body.String())
	}
	assertResponseOmitsSecrets(t, list.Body.String(), databasePassword, databaseURL, repositoryToken)
	var listPayload struct {
		Orphans []managedPostgresOrphanSummary `json:"orphans"`
	}
	mustDecodeJSON(t, list, &listPayload)
	if len(listPayload.Orphans) != 1 || listPayload.Orphans[0].AppID != managed.Spec.AppID || len(listPayload.Orphans[0].BackingServices) != 1 {
		t.Fatalf("unexpected orphan summary: %+v", listPayload.Orphans)
	}
	for _, test := range []struct {
		mode       string
		statusCode int
	}{
		{mode: "missing", statusCode: http.StatusConflict},
		{mode: "deleting", statusCode: http.StatusConflict},
		{mode: "zero-pvc", statusCode: http.StatusConflict},
		{mode: "deleting-pvc", statusCode: http.StatusConflict},
		{mode: "deployment-nonzero", statusCode: http.StatusConflict},
		{mode: "deployment-deleting", statusCode: http.StatusConflict},
		{mode: "terminating-pod", statusCode: http.StatusConflict},
		{mode: "forbidden", statusCode: http.StatusServiceUnavailable},
	} {
		storageMode = test.mode
		blocked := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/orphans/"+managed.Spec.AppID+"/adopt", bootstrapKey, nil)
		if blocked.Code != test.statusCode {
			t.Fatalf("expected storage mode %s status %d, got %d body=%s", test.mode, test.statusCode, blocked.Code, blocked.Body.String())
		}
		if _, err := stateStore.GetApp(managed.Spec.AppID); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("storage mode %s must not create an app, got %v", test.mode, err)
		}
	}
	storageMode = "ready"

	orphanMessage := managed.Status.Message
	managed.Status.Message = "managed app disabled without orphan evidence"
	notOrphan := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/orphans/"+managed.Spec.AppID+"/adopt", bootstrapKey, nil)
	if notOrphan.Code != http.StatusConflict {
		t.Fatalf("expected missing non-orphan status %d, got %d body=%s", http.StatusConflict, notOrphan.Code, notOrphan.Body.String())
	}
	if _, err := stateStore.GetApp(managed.Spec.AppID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("non-orphan snapshot must not create an app, got %v", err)
	}
	managed.Status.Message = orphanMessage

	adopt := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/orphans/"+managed.Spec.AppID+"/adopt", bootstrapKey, nil)
	if adopt.Code != http.StatusCreated {
		t.Fatalf("expected orphan adopt status %d, got %d body=%s", http.StatusCreated, adopt.Code, adopt.Body.String())
	}
	assertResponseOmitsSecrets(t, adopt.Body.String(), databasePassword, databaseURL, repositoryToken)
	var adoptPayload struct {
		App             model.App              `json:"app"`
		BackingServices []model.BackingService `json:"backing_services"`
		AlreadyAdopted  bool                   `json:"already_adopted"`
	}
	mustDecodeJSON(t, adopt, &adoptPayload)
	if adoptPayload.AlreadyAdopted || adoptPayload.App.Spec.Replicas != 0 || adoptPayload.App.Status.CurrentReplicas != 0 {
		t.Fatalf("adopted app must remain disabled: %+v", adoptPayload.App)
	}
	if len(adoptPayload.BackingServices) != 1 || adoptPayload.BackingServices[0].Spec.Postgres == nil || adoptPayload.BackingServices[0].Spec.Postgres.Password != apiRedactedSecretValue {
		t.Fatalf("expected one redacted adopted service, got %+v", adoptPayload.BackingServices)
	}
	if adoptPayload.BackingServices[0].OwnerAppID != managed.Spec.AppID {
		t.Fatalf("expected exclusive bound service ownership to transfer to restored app, got %+v", adoptPayload.BackingServices[0])
	}

	managed.Status.Message = "managed app disabled after ownership was restored"
	managed.Metadata.ResourceVersion = "4243"
	again := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/orphans/"+managed.Spec.AppID+"/adopt", bootstrapKey, nil)
	if again.Code != http.StatusOK {
		t.Fatalf("expected idempotent adopt status %d, got %d body=%s", http.StatusOK, again.Code, again.Body.String())
	}
	var againPayload struct {
		AlreadyAdopted bool `json:"already_adopted"`
	}
	mustDecodeJSON(t, again, &againPayload)
	if !againPayload.AlreadyAdopted {
		t.Fatal("expected already_adopted=true on repeat adoption")
	}
	managed.Spec.BackingServices[0].Spec.Postgres.Password = "changed-retained-password-must-not-leak"
	managed.Metadata.Generation++
	managed.Metadata.ResourceVersion = "4244"
	changed := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/orphans/"+managed.Spec.AppID+"/adopt", bootstrapKey, nil)
	if changed.Code != http.StatusConflict {
		t.Fatalf("expected changed retained resource status %d, got %d body=%s", http.StatusConflict, changed.Code, changed.Body.String())
	}
	assertResponseOmitsSecrets(t, changed.Body.String(), "changed-retained-password-must-not-leak")
	persisted, err := stateStore.GetBackingService("service_orphan_api_test")
	if err != nil {
		t.Fatalf("get service after conflicting retry: %v", err)
	}
	if persisted.Spec.Postgres == nil || persisted.Spec.Postgres.Password != databasePassword {
		t.Fatalf("conflicting retry mutated retained service: %+v", persisted.Spec.Postgres)
	}
	events, err := stateStore.ListAuditEvents("", true, 100)
	if err != nil {
		t.Fatalf("list adoption audit events: %v", err)
	}
	var foundVersionedAudit bool
	for _, event := range events {
		if event.Action == "backing_service.orphan.adopt" &&
			event.TargetID == managed.Spec.AppID &&
			event.Metadata["managed_app_uid"] == managed.Metadata.UID &&
			event.Metadata["managed_app_resource_version"] == managed.Metadata.ResourceVersion {
			foundVersionedAudit = true
			break
		}
	}
	if !foundVersionedAudit {
		t.Fatalf("expected adoption audit to identify exact managed app revision, got %+v", events)
	}
}

func TestManagedAppInventoryRejectsDuplicateAppID(t *testing.T) {
	t.Parallel()

	managed := runtime.ManagedAppObject{Spec: runtime.ManagedAppSpec{AppID: "app_duplicate"}}
	kubeAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []runtime.ManagedAppObject{managed, managed}})
	}))
	t.Cleanup(kubeAPI.Close)
	client := &managedAppStatusClient{client: kubeAPI.Client(), baseURL: kubeAPI.URL, bearerToken: "test"}
	if _, err := client.listManagedAppsByAppID(context.Background()); err == nil || !strings.Contains(err.Error(), "duplicate appID") {
		t.Fatalf("expected duplicate appID inventory rejection, got %v", err)
	}
}

func TestManagedPostgresOrphanCandidateRequiresCanonicalResourceIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*runtime.ManagedAppObject)
	}{
		{name: "wrong namespace", mutate: func(managed *runtime.ManagedAppObject) { managed.Metadata.Namespace = "unexpected-namespace" }},
		{name: "missing uid", mutate: func(managed *runtime.ManagedAppObject) { managed.Metadata.UID = "" }},
		{name: "missing resource version", mutate: func(managed *runtime.ManagedAppObject) { managed.Metadata.ResourceVersion = "" }},
		{name: "deleting", mutate: func(managed *runtime.ManagedAppObject) {
			managed.Metadata.DeletionTimestamp = time.Now().UTC().Format(time.RFC3339)
		}},
		{name: "stale orphan generation", mutate: func(managed *runtime.ManagedAppObject) { managed.Metadata.Generation++ }},
		{name: "workload zero unverified", mutate: func(managed *runtime.ManagedAppObject) { managed.Status.Conditions[0].Status = "False" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			managed := retainedManagedPostgresOrphan("tenant_identity", "project_identity", "password", "dsn", "token")
			test.mutate(&managed)
			if _, _, eligible := managedPostgresOrphanCandidate(managed); eligible {
				t.Fatalf("unsafe managed app must not be adoptable: %+v", managed.Metadata)
			}
		})
	}
}

func TestManagedPostgresOrphanCandidateAcceptsExclusiveBoundServiceWithoutOwnerID(t *testing.T) {
	t.Parallel()

	managed := retainedManagedPostgresOrphan("tenant_bound", "project_bound", "password", "dsn", "token")
	managed.Spec.BackingServices[0].OwnerAppID = ""
	if _, _, eligible := managedPostgresOrphanCandidate(managed); !eligible {
		t.Fatal("exclusive managed postgres binding must remain adoptable when legacy owner_app_id is empty")
	}
}

func TestManagedPostgresOrphanCandidateRejectsSnapshotThatStorePreflightWouldReject(t *testing.T) {
	t.Parallel()

	managed := retainedManagedPostgresOrphan("tenant_preflight", "project_preflight", "password", "dsn", "token")
	managed.Spec.BackingServices[0].Spec.Postgres.Password = ""
	if _, _, eligible := managedPostgresOrphanCandidate(managed); eligible {
		t.Fatal("orphan missing retained database password must not be listed as adoptable")
	}
}

func retainedManagedPostgresOrphan(tenantID, projectID, password, databaseURL, repositoryToken string) runtime.ManagedAppObject {
	appID := "app_orphan_api_test"
	now := time.Now().UTC()
	return runtime.ManagedAppObject{
		APIVersion: runtime.ManagedAppAPIVersion,
		Kind:       runtime.ManagedAppKind,
		Metadata: runtime.ManagedAppMeta{
			Name:            "app-orphan-api-test",
			Namespace:       runtime.NamespaceForTenant(tenantID),
			UID:             "managed-app-orphan-api-test-uid",
			ResourceVersion: "4242",
			Generation:      7,
		},
		Spec: runtime.ManagedAppSpec{
			AppID:     appID,
			TenantID:  tenantID,
			ProjectID: projectID,
			Name:      "orphan-api-test",
			Source:    &model.AppSource{RepoURL: "https://github.com/example/private", RepoAuthToken: repositoryToken},
			AppSpec: model.AppSpec{
				Image:     "ghcr.io/example/demo:latest",
				Ports:     []int{8080},
				Replicas:  3,
				RuntimeID: "runtime_managed_shared",
			},
			Bindings: []model.ServiceBinding{{
				ID:        "binding_orphan_api_test",
				TenantID:  tenantID,
				AppID:     appID,
				ServiceID: "service_orphan_api_test",
				Alias:     "postgres",
				Env:       map[string]string{"DATABASE_URL": databaseURL, "DB_PASSWORD": password},
				CreatedAt: now,
				UpdatedAt: now,
			}},
			BackingServices: []model.BackingService{{
				ID:          "service_orphan_api_test",
				TenantID:    tenantID,
				ProjectID:   projectID,
				OwnerAppID:  appID,
				Name:        "orphan-postgres",
				Type:        model.BackingServiceTypePostgres,
				Provisioner: model.BackingServiceProvisionerManaged,
				Status:      model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
					RuntimeID:   "runtime_managed_shared",
					Database:    "orphan",
					User:        "orphan",
					Password:    password,
					ServiceName: "orphan-postgres",
					StorageSize: "20Gi",
					Instances:   1,
				}},
				CreatedAt: now,
				UpdatedAt: now,
			}},
		},
		Status: runtime.ManagedAppStatus{
			Phase:              runtime.ManagedAppPhaseDisabled,
			Message:            "orphaned managed app: app not found in store; disabled workload and retained storage for audit",
			ObservedGeneration: 7,
			Conditions: []runtime.ManagedAppCondition{{
				Type:   "OrphanWorkloadZero",
				Status: "True",
				Reason: "Verified",
			}},
		},
	}
}

func cacheManagedAppObject(server *Server, managed runtime.ManagedAppObject) {
	now := time.Now()
	server.managedAppStatusCache.setList(managedAppStatusListCacheEntry{
		items:       map[string]runtime.ManagedAppObject{managed.Spec.AppID: managed},
		ok:          true,
		refreshedAt: now,
		expiresAt:   now.Add(time.Minute),
	})
}

func assertResponseOmitsSecrets(t *testing.T, body string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if secret != "" && strings.Contains(body, secret) {
			t.Fatalf("response leaked secret %q: %s", secret, body)
		}
	}
}

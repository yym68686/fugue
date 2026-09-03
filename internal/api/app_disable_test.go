package api

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestDisableAppQueuesScaleWhenDurableZeroHasPhysicalReplica(t *testing.T) {
	server, apiKey, app := setupDisableAppTestServer(t)
	cacheDisableAppObservation(t, server, app, 1, 1, time.Now().UTC(), "cluster-test")

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/disable", apiKey, nil)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected scale-to-zero operation status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Operation       model.Operation `json:"operation"`
		AlreadyDisabled bool            `json:"already_disabled"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.AlreadyDisabled {
		t.Fatal("disable must not claim idempotent success while a physical replica remains")
	}
	if response.Operation.Type != model.OperationTypeScale || response.Operation.DesiredReplicas == nil || *response.Operation.DesiredReplicas != 0 {
		t.Fatalf("expected scale-to-zero operation, got %+v", response.Operation)
	}
}

func TestDisableAppReturnsAlreadyDisabledOnlyForFreshConfirmedZero(t *testing.T) {
	tests := []struct {
		name         string
		physical     int
		physicalWant int
		observedAt   time.Time
		clusterID    string
		wantAccepted bool
	}{
		{
			name:         "fresh zero",
			physical:     0,
			physicalWant: 0,
			observedAt:   time.Now().UTC(),
			clusterID:    "cluster-test",
			wantAccepted: false,
		},
		{
			name:         "stale zero",
			physical:     0,
			physicalWant: 0,
			observedAt:   time.Now().UTC().Add(-2 * defaultAppObservedStatusMaxAge),
			clusterID:    "cluster-test",
			wantAccepted: true,
		},
		{
			name:         "missing cluster identity",
			physical:     0,
			physicalWant: 0,
			observedAt:   time.Now().UTC(),
			clusterID:    "",
			wantAccepted: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, apiKey, app := setupDisableAppTestServer(t)
			cacheDisableAppObservation(t, server, app, test.physical, test.physicalWant, test.observedAt, test.clusterID)

			recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/disable", apiKey, nil)
			if test.wantAccepted {
				if recorder.Code != http.StatusAccepted {
					t.Fatalf("expected scale-to-zero operation status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
				}
				return
			}
			if recorder.Code != http.StatusOK {
				t.Fatalf("expected already-disabled status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
			}
			var response struct {
				AlreadyDisabled bool `json:"already_disabled"`
			}
			mustDecodeJSON(t, recorder, &response)
			if !response.AlreadyDisabled {
				t.Fatal("expected already_disabled response for fresh confirmed zero")
			}
		})
	}
}

func setupDisableAppTestServer(t *testing.T) (*Server, string, model.App) {
	t.Helper()

	stateStore := storeForDisableAppTest(t)
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	tenant, err := stateStore.CreateTenant("Disable Test Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	zero := 0
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeScale,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "disable-test",
		AppID:           app.ID,
		DesiredReplicas: &zero,
	})
	if err != nil {
		t.Fatalf("create setup scale operation: %v", err)
	}
	if _, err := stateStore.CompleteManagedOperation(op.ID, "", "disabled for test"); err != nil {
		t.Fatalf("complete setup scale operation: %v", err)
	}
	app, err = stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload disabled app: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(app.TenantID, "disabler", []string{"app.scale"})
	if err != nil {
		t.Fatalf("create disable api key: %v", err)
	}
	return server, apiKey, app
}

func storeForDisableAppTest(t *testing.T) *store.Store {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	return stateStore
}

func cacheDisableAppObservation(t *testing.T, server *Server, app model.App, physical, physicalDesired int, observedAt time.Time, clusterID string) {
	t.Helper()
	namespacePresent := true
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("build managed app observation: %v", err)
	}
	server.managedAppStatusCache.mu.Lock()
	defer server.managedAppStatusCache.mu.Unlock()
	server.managedAppStatusCache.byApp[managedAppStatusCacheKey(app)] = managedAppStatusCacheEntry{
		managed:     managed,
		found:       true,
		ok:          true,
		clusterID:   clusterID,
		refreshedAt: observedAt,
		expiresAt:   time.Now().UTC().Add(time.Minute),
		evidence: managedAppRuntimeEvidence{
			appObservationKey:       managedAppRuntimeEvidenceObservationKey(app),
			namespacePresent:        &namespacePresent,
			physicalReplicas:        intPointerForDisableAppTest(physical),
			physicalDesiredReplicas: intPointerForDisableAppTest(physicalDesired),
			evidenceSources:         []string{runtime.AppObservationSourceKubernetesAPI},
		},
	}
}

func intPointerForDisableAppTest(value int) *int { return &value }

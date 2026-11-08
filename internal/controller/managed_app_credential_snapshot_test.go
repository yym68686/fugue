package controller

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

// Exercise the full reconciliation entrypoint: the selection helper alone does
// not prove that its recovered backing-service snapshot reaches credential apply.
func TestReconcileManagedAppRecoveryUsesPersistedCredentialIdentity(t *testing.T) {
	s := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("snapshot recovery")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{CPUMilliCores: 4000, MemoryMebibytes: 8192, StorageGibibytes: 20}); err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "recovery", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "database-owner", "", model.AppSpec{
		Image: "example.invalid/app:ready", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1,
		Postgres: &model.AppPostgresSpec{Database: "demo", User: "demo", Password: "fixture", ServiceName: "demo-database", StorageSize: "1Gi"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !managedAppBaselineNeedsRecovery(app) || len(app.BackingServices) != 1 {
		t.Fatal("fixture must retain a runtime snapshot with an incomplete stored build baseline")
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatal(err)
	}
	service := app.BackingServices[0]
	if _, err := s.AssignManagedPostgresCredentialSecret(service, "persisted-credential"); err != nil {
		t.Fatal(err)
	}
	stop := errors.New("stop after credential identity reconciliation")
	readCluster := false
	client := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/status") {
			return okJSONResponse(`{}`), nil
		}
		if readCluster {
			return nil, stop
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/clusters/") {
			readCluster = true
		}
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected write before credential reconciliation: %s %s", r.Method, r.URL.Path)
		}
		return notFoundJSONResponse(), nil
	})}}
	err = (&Service{Store: s}).reconcileManagedAppObject(context.Background(), client, managed)
	if !readCluster || err == nil || !strings.Contains(err.Error(), stop.Error()) {
		t.Fatalf("persisted credential should pass reconciliation before the later transport failure, got %v", err)
	}
	if managed.Spec.BackingServices[0].Spec.Postgres.CredentialSecretName != "" {
		t.Fatal("recovery mutated the caller's old snapshot")
	}
	current, err := s.GetBackingService(service.ID)
	if err != nil || current.Spec.Postgres.CredentialSecretName != "persisted-credential" {
		t.Fatal("recovery changed the persisted credential identity", err)
	}
}

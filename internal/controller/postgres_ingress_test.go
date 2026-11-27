package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"

	networkingv1 "k8s.io/api/networking/v1"
)

func livePostgresFixture(t *testing.T) (*store.Store, model.App) {
	t.Helper()
	state := newStoreTestDatabase(t, "")
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := state.CreateTenant("ingress fixture")
	if err != nil {
		t.Fatal(err)
	}
	_, err = state.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{CPUMilliCores: 4000, MemoryMebibytes: 8192, StorageGibibytes: 40})
	if err != nil {
		t.Fatal(err)
	}
	project, err := state.CreateProject(tenant.ID, "fixture", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := state.CreateApp(tenant.ID, project.ID, "fixture", "", model.AppSpec{Image: "example.invalid/fixture:1", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Postgres: &model.AppPostgresSpec{ServiceName: "current", Database: "demo", User: "demo", Password: "fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	app, err = state.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	return state, app
}

func TestLivePostgresIngressIncludesRetainedGenerations(t *testing.T) {
	state, app := livePostgresFixture(t)
	db := app.BackingServices[0]
	ns := runtime.NamespaceForTenant(app.TenantID)
	labels := map[string]string{runtime.FugueLabelTenantID: app.TenantID, runtime.FugueLabelOwnerAppID: db.OwnerAppID, runtime.FugueLabelProjectID: app.ProjectID, runtime.FugueLabelBackingServiceID: db.ID}
	names := []string{db.Spec.Postgres.ServiceName, "retained", "incident"}
	policies := map[string]bool{}
	handler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/apis/postgresql.cnpg.io/v1/clusters":
			items := []map[string]any{}
			for _, name := range names {
				items = append(items, map[string]any{"metadata": map[string]any{"name": name, "namespace": ns, "labels": labels}})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
		case strings.HasSuffix(r.URL.Path, "/networkpolicies") && r.Method == "GET":
			_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"incident-policy"},"spec":{"podSelector":{"matchLabels":{"cnpg.io/cluster":"incident"}},"policyTypes":["Ingress"]}}]}`))
		case strings.HasSuffix(r.URL.Path, "/networkpolicies") && r.Method == "POST":
			var policy networkingv1.NetworkPolicy
			if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
				t.Error(err)
				return
			}
			name := policy.Spec.PodSelector.MatchLabels["cnpg.io/cluster"]
			policies[name] = false
			for _, peer := range policy.Spec.Ingress[0].From {
				if peer.PodSelector != nil && peer.PodSelector.MatchLabels[runtime.FugueLabelAppID] == app.ID {
					policies[name] = true
				}
			}
			w.WriteHeader(http.StatusCreated)
		default:
			http.NotFound(w, r)
		}
	}))
	defer handler.Close()
	s := &Service{Store: state, Config: config.ControllerConfig{ControlPlaneNamespace: "fugue-system"}}
	if err := s.reconcileLivePostgres(context.Background(), &kubeClient{client: handler.Client(), baseURL: handler.URL}); err != nil {
		t.Fatal(err)
	}
	retained, created := policies["retained"]
	if len(policies) != 2 || !policies[names[0]] || !created || retained {
		t.Fatalf("unexpected application grants: %v", policies)
	}
}

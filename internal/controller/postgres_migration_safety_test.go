package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

func TestPostgresNodeMatchesAnyPlacement(t *testing.T) {
	t.Parallel()
	placements := []runtimepkg.SchedulingConstraints{
		{NodeSelector: map[string]string{"hostname": "source"}},
		{NodeSelector: map[string]string{"runtime": "destination", "tenant": "owner"}},
	}
	for _, test := range []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"source", map[string]string{"hostname": "source"}, true},
		{"target", map[string]string{"runtime": "destination", "tenant": "owner"}, true},
		{"wrong tenant", map[string]string{"runtime": "destination", "tenant": "other"}, false},
		{"stale pin", map[string]string{"hostname": "actual-primary"}, false},
		{"unknown node", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := postgresNodeMatchesAnyPlacement(test.labels, placements); got != test.want {
				t.Fatalf("matches=%t, want %t", got, test.want)
			}
		})
	}
	if postgresNodeMatchesAnyPlacement(nil, []runtimepkg.SchedulingConstraints{{}}) {
		t.Fatal("an unconstrained placement cannot prove source protection")
	}
}

func TestPostgresMigrationPreflightAndRollbackRequireLiveService(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		drift    bool
		endpoint bool
		sqlErr   error
		want     string
	}{
		{name: "healthy", endpoint: true},
		{name: "durable source pin drift", drift: true, endpoint: true, want: "would exclude existing instance"},
		{name: "missing rw endpoint", want: "not serving verified read-write SQL"},
		{name: "sql shutting down", endpoint: true, sqlErr: errors.New("database shutting down"), want: "not serving verified read-write SQL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := state.CreateTenant("Migration Tenant")
			if err != nil {
				t.Fatal(err)
			}
			source, _, err := state.CreateRuntime(tenant.ID, "source", model.RuntimeTypeManagedOwned, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			target, _, err := state.CreateRuntime(tenant.ID, "target", model.RuntimeTypeManagedOwned, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			app := model.App{TenantID: tenant.ID, Spec: model.AppSpec{RuntimeID: source.ID}}
			namespace := runtimepkg.NamespaceForTenant(tenant.ID)
			postgres := model.AppPostgresSpec{ServiceName: "sample-postgres", RuntimeID: source.ID, Instances: 1}
			var writes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					http.Error(w, "preflight cannot write", http.StatusForbidden)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				pod := map[string]any{"metadata": map[string]any{"name": "sample-postgres-1"}, "spec": map[string]any{"nodeName": "actual-source"}, "status": map[string]any{"phase": "Running", "podIP": "10.42.1.2", "conditions": []map[string]any{{"type": "Ready", "status": "True"}}}}
				switch r.URL.Path {
				case cloudNativePGClusterAPIPath(namespace, postgres.ServiceName):
					_ = json.NewEncoder(w).Encode(map[string]any{"spec": map[string]any{"instances": 1}, "status": map[string]any{"instances": 1, "readyInstances": 1, "currentPrimary": "sample-postgres-1", "targetPrimary": "sample-postgres-1"}})
				case "/api/v1/namespaces/" + namespace + "/pods/sample-postgres-1":
					_ = json.NewEncoder(w).Encode(pod)
				case "/api/v1/namespaces/" + namespace + "/pods":
					_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{pod}})
				case "/api/v1/nodes/actual-source":
					runtimeID := source.ID
					if test.drift {
						runtimeID = "runtime_actual_elsewhere"
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "actual-source", "labels": map[string]string{runtimepkg.RuntimeIDLabelKey: runtimeID, runtimepkg.TenantIDLabelKey: tenant.ID}}})
				case "/api/v1/namespaces/" + namespace + "/endpoints/sample-postgres-rw":
					addresses := []map[string]string{}
					if test.endpoint {
						addresses = append(addresses, map[string]string{"ip": "10.42.1.2"})
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"subsets": []map[string]any{{"addresses": addresses}}})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := &kubeClient{client: server.Client(), baseURL: server.URL, namespace: namespace}
			timeout := 2 * time.Second
			if !test.endpoint || test.sqlErr != nil {
				timeout = 100 * time.Millisecond
			}
			svc := &Service{Store: state, Config: config.ControllerConfig{ManagedAppRolloutTimeout: timeout}, Logger: log.New(io.Discard, "", 0), newKubeClient: func(string) (*kubeClient, error) { return client, nil }, managedPostgresPrimarySQLProbe: func(context.Context, string, string, model.AppPostgresSpec) error { return test.sqlErr }}
			staged := databaseSwitchoverStagePostgresSpec(&postgres, source.ID, target.ID)
			err = svc.validateManagedPostgresSwitchoverStage(context.Background(), client, app, namespace, postgres.ServiceName, staged, postgres)
			if test.want == "" && err != nil || test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("preflight error=%v, want %q", err, test.want)
			}
			if writes.Load() != 0 {
				t.Fatal("preflight mutated the source")
			}
			localized := databaseLocalizeStagePostgresSpec(&postgres, source.ID, target.ID, "destination")
			err = svc.validateManagedPostgresStage(context.Background(), client, app, namespace, postgres.ServiceName, &localized, postgres, true)
			if (err == nil) != (test.endpoint && test.sqlErr == nil) {
				t.Fatalf("localization preflight=%v", err)
			}
			if err == nil && (localized.Instances < 2 || !localized.PrimaryPlacementPendingRebalance || localized.SynchronousReplicas != 0) {
				t.Fatalf("localization did not independently prepare held destination: %+v", localized)
			}
			// A manifest apply cannot make a missing endpoint or failed SQL probe
			// count as a recovered service.
			err = svc.verifyManagedPostgresRollback(context.Background(), model.Operation{}, app, &postgres)
			if (err == nil) != (test.endpoint && test.sqlErr == nil) {
				t.Fatalf("rollback verification=%v, endpoint=%t sqlError=%v", err, test.endpoint, test.sqlErr)
			}
		})
	}
}

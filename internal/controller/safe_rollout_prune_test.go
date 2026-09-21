package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"fugue/internal/model"
)

func TestFailedAndStartingReleaseResourcesSurviveOrdinaryPruning(t *testing.T) {
	for _, status := range []string{model.AppReleaseStatusCreating, model.AppReleaseStatusFailed} {
		t.Run(status, func(t *testing.T) {
			stateStore, app, _, _ := newSafeRolloutTestState(t)
			release, err := stateStore.CreateAppRelease(model.AppRelease{TenantID: app.TenantID, AppID: app.ID, Role: model.AppReleaseRoleCandidate, Status: status, DeploymentName: "candidate-revision", ServiceName: "candidate-revision"})
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var deleted []string
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodDelete {
					mu.Lock()
					deleted = append(deleted, r.URL.Path)
					mu.Unlock()
					_, _ = w.Write([]byte(`{}`))
					return
				}
				items := []any{}
				if strings.HasSuffix(r.URL.Path, "/deployments") || strings.HasSuffix(r.URL.Path, "/services") {
					items = append(items, map[string]any{"metadata": map[string]any{"name": "candidate-revision"}})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
			}))
			defer kube.Close()
			client := &kubeClient{client: kube.Client(), baseURL: kube.URL}
			svc := &Service{Store: stateStore}
			if err := svc.pruneManagedAppStaleObjects(context.Background(), client, "tenant-test", app, nil); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			count := len(deleted)
			mu.Unlock()
			if count != 0 {
				t.Fatal("unretired release was deleted before traffic withdrawal", deleted)
			}
			// Only the existing explicit retirement lifecycle releases these
			// identities to ordinary pruning; a failed health check is insufficient.
			release.Role, release.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
			if _, err = stateStore.UpdateAppRelease(release); err != nil {
				t.Fatal(err)
			}
			if err := svc.pruneManagedAppStaleObjects(context.Background(), client, "tenant-test", app, nil); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(deleted) != 2 {
				t.Fatalf("retired workload not released for cleanup: %v", deleted)
			}
		})
	}
}

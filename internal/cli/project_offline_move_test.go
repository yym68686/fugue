package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestProjectOfflineMovePreflightsThenWaitsForDatabaseAndCopiesStoppedApp(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(fmt.Sprintf("blocked-%v", blocked), func(t *testing.T) {
			app := model.App{ID: "app_worker", TenantID: "tenant_test", ProjectID: "project_test", Name: "worker", Spec: model.AppSpec{RuntimeID: "source", Replicas: 0, PersistentStorage: &model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeDedicatedPVC, StorageClassName: "source-local", StorageSize: "10Gi"}}}
			service := model.BackingService{ID: "service_db", TenantID: app.TenantID, ProjectID: app.ProjectID, Name: "database", Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "source"}}}
			var calls []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/backing-services/service_db/recover":
					var body map[string]any
					_ = json.NewDecoder(r.Body).Decode(&body)
					if body["storage_class_name"] != "target-network" {
						t.Error("missing explicit destination")
					}
					if body["dry_run"] == true {
						calls = append(calls, "service-preflight")
						_, _ = w.Write([]byte(`{"dry_run":true}`))
						return
					}
					calls = append(calls, "service-apply")
					_, _ = w.Write([]byte(`{"operation":{"id":"op_db","status":"pending"}}`))
				case "/v1/operations/op_db":
					calls = append(calls, "service-wait")
					service.Spec.Postgres.RuntimeID = "target"
					_, _ = w.Write([]byte(`{"operation":{"id":"op_db","status":"completed"}}`))
				case "/v1/backing-services/service_db":
					_ = json.NewEncoder(w).Encode(map[string]any{"backing_service": service})
				case "/v1/apps/app_worker":
					_ = json.NewEncoder(w).Encode(map[string]any{"app": app})
				case "/v1/apps/app_worker/migrate":
					var body map[string]any
					_ = json.NewDecoder(r.Body).Decode(&body)
					if body["offline_storage_class_name"] != "target-network" {
						t.Error("offline volume destination missing")
					}
					if body["dry_run"] == true {
						calls = append(calls, "app-preflight")
						_ = json.NewEncoder(w).Encode(map[string]any{"impact": model.AppMoveImpact{AppID: app.ID, Pass: !blocked, Blockers: func() []string {
							if blocked {
								return []string{"image missing"}
							}
							return nil
						}()}})
						return
					}
					calls = append(calls, "app-apply")
					if service.Spec.Postgres.RuntimeID != "target" {
						t.Error("app copied before database converged")
					}
					_, _ = w.Write([]byte(`{"operation":{"id":"op_app","status":"pending"}}`))
				case "/v1/operations/op_app":
					calls = append(calls, "app-wait")
					app.Spec.RuntimeID = "target"
					app.Status.CurrentRuntimeID = "target"
					app.Spec.PersistentStorage.Mode = model.AppPersistentStorageModeMovableRWO
					_, _ = w.Write([]byte(`{"operation":{"id":"op_app","status":"completed"}}`))
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			var out, errout bytes.Buffer
			cli := &CLI{stdout: &out, stderr: &errout, root: rootOptions{JSONOutput: true}}
			client := &Client{baseURL: server.URL, httpClient: server.Client()}
			opts := projectMoveCommandOptions{Wait: true, RecoverOffline: true, StorageClass: "target-network"}
			project := model.Project{ID: app.ProjectID, TenantID: app.TenantID, Name: "demo"}
			err := cli.moveProjectWithOfflineRecovery(client, project, []model.App{app}, []model.BackingService{service}, "target", opts)
			if blocked {
				if err == nil {
					t.Fatal("blocked preflight was accepted")
				}
				if !reflect.DeepEqual(calls, []string{"service-preflight", "app-preflight"}) {
					t.Fatalf("mutation after failed preflight: %v", calls)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"service-preflight", "app-preflight", "service-apply", "service-wait", "app-preflight", "app-apply", "app-wait"}
			if !reflect.DeepEqual(calls, want) {
				t.Fatalf("got calls=%v want=%v", calls, want)
			}
			calls = nil
			if err := cli.moveProjectWithOfflineRecovery(client, project, []model.App{app}, []model.BackingService{service}, "target", opts); err != nil {
				t.Fatal(err)
			}
			if len(calls) != 0 {
				t.Fatalf("successful rerun performed operations: %v", calls)
			}
		})
	}
}

func TestOfflineMoveResumesColdEndpointAfterTargetWasPersisted(t *testing.T) {
	service := model.BackingService{ID: "service_db", Name: "database", Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "target", ServiceName: "restored-db", EndpointServiceName: "original-db"}}}
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/backing-services/service_db/recover":
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["dry_run"] == true {
				calls = append(calls, "preflight")
				w.Write([]byte(`{"dry_run":true}`))
				return
			}
			calls = append(calls, "resume")
			w.Write([]byte(`{"operation":{"id":"op_resume","status":"pending"}}`))
		case "/v1/operations/op_resume":
			calls = append(calls, "verify")
			w.Write([]byte(`{"operation":{"id":"op_resume","status":"completed"}}`))
		case "/v1/backing-services/service_db":
			json.NewEncoder(w).Encode(map[string]any{"backing_service": service})
		default:
			t.Errorf("unexpected %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	var out, errout bytes.Buffer
	c := &CLI{stdout: &out, stderr: &errout, root: rootOptions{JSONOutput: true}}
	client := &Client{baseURL: server.URL, httpClient: server.Client()}
	if err := c.moveProjectWithOfflineRecovery(client, model.Project{ID: "project_test", Name: "demo"}, nil, []model.BackingService{service}, "target", projectMoveCommandOptions{Wait: true, RecoverOffline: true, StorageClass: "cloneable"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"preflight", "resume", "verify"}) {
		t.Fatalf("skipped incomplete cutover: %v", calls)
	}
}

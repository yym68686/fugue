package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestHistoricalWorkloadMigrationRequiresSourceAndExactExistingResources(t *testing.T) {
	for _, scenario := range []string{"previous", "failed", "missing_source", "wrong_spec", "new_uid", "new_generation", "recreated_after_operation", "old_resource", "wrong_owner", "wrong_selector", "wrong_image", "old_agent", "missing_deployment", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			st, _, app, _ := newSafeRolloutTestState(t)
			rt, _, err := st.CreateRuntime(app.TenantID, "history", model.RuntimeTypeManagedShared, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			app.Spec.RuntimeID = rt.ID
			s := &Service{Store: st, Config: config.ControllerConfig{KubectlApply: true}, Renderer: runtime.Renderer{StrictDrain: runtime.DefaultStrictDrainConfig()}}
			op, err := st.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec, ExecutionMode: model.ExecutionModeManaged})
			if err != nil {
				t.Fatal(err)
			}
			if _, ok, err := st.TryClaimPendingOperation(op.ID); err != nil || !ok {
				t.Fatal("claim", err)
			}
			role, status, action := model.AppReleaseRolePrevious, model.AppReleaseStatusDraining, "app.release.promote"
			if scenario == "failed" {
				role, status, action = model.AppReleaseRoleCandidate, model.AppReleaseStatusFailed, "app.release.abort.auto"
			}
			r, err := st.CreateAppRelease(model.AppRelease{TenantID: app.TenantID, AppID: app.ID, Role: role, Status: status, SpecSnapshot: &app.Spec, RuntimeID: rt.ID, ResolvedImageRef: app.Spec.Image, DeploymentName: runtime.RuntimeAppResourceName(app), ServiceName: runtime.RuntimeAppServiceName(app)})
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "missing_source" {
				s.appendSafeRolloutAuditEvent(app, action, r.ID, map[string]string{"operation_id": op.ID})
			}
			if scenario == "failed" {
				_, err = st.FailOperation(op.ID, "gate failed")
			} else {
				_, err = st.CompleteManagedOperation(op.ID, "", "done")
			}
			if err != nil {
				t.Fatal(err)
			}
			op, err = st.GetOperation(op.ID)
			if err != nil {
				t.Fatal(err)
			}
			r, err = st.GetAppRelease(app.TenantID, false, r.ID)
			if err != nil {
				t.Fatal(err)
			}
			prepared := s.Renderer.PrepareApp(app)
			objects := s.Renderer.BuildManagedAppRevisionChildObjects(prepared, runtime.SchedulingForRuntime(rt), nil, nil, safeRolloutCandidateRevision(r.ID))
			live := map[string]map[string]any{}
			for _, o := range objects {
				if o["kind"] != "Deployment" && o["kind"] != "Service" {
					continue
				}
				obj := cloneKubeMap(o)
				m := objectMapField(obj, "metadata")
				m["uid"], m["generation"], m["resourceVersion"] = strings.ToLower(o["kind"].(string))+"-uid", float64(1), "1"
				m["creationTimestamp"] = op.CreatedAt.Truncate(time.Second).Format(time.RFC3339)
				live[o["kind"].(string)] = obj
			}
			dep, service := live["Deployment"], live["Service"]
			switch scenario {
			case "wrong_spec":
				copy := *r.SpecSnapshot
				copy.Command = []string{"changed"}
				r.SpecSnapshot = &copy
				r, err = st.UpdateAppRelease(r)
				if err != nil {
					t.Fatal(err)
				}
			case "recreated_after_operation":
				objectMapField(service, "metadata")["creationTimestamp"] = op.CompletedAt.Add(time.Hour).Format(time.RFC3339)
			case "old_resource":
				objectMapField(dep, "metadata")["creationTimestamp"] = op.CreatedAt.Add(-time.Hour).Format(time.RFC3339)
			case "wrong_owner":
				objectMapField(objectMapField(dep, "metadata"), "labels")[runtime.FugueLabelTenantID] = "foreign"
			case "wrong_selector":
				objectMapField(objectMapField(service, "spec"), "selector")[runtime.FugueLabelAppReleaseID] = "other"
			case "wrong_image":
				mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "containers"))[0]["image"] = "registry.example/other:v2"
			case "old_agent":
				mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "initContainers"))[0]["image"] = "registry.example/legacy-agent:v1"
			}
			reads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet {
					t.Error("migration mutated Kubernetes")
					http.Error(w, "mutation", 400)
					return
				}
				reads++
				if reads == 4 {
					if scenario == "new_uid" {
						objectMapField(dep, "metadata")["uid"] = "replacement"
					}
					if scenario == "new_generation" {
						objectMapField(dep, "metadata")["generation"] = float64(2)
					}
				}
				if strings.Contains(req.URL.Path, "/deployments/") && scenario != "missing_deployment" {
					_ = json.NewEncoder(w).Encode(dep)
				} else if strings.Contains(req.URL.Path, "/services/") {
					_ = json.NewEncoder(w).Encode(service)
				} else {
					http.NotFound(w, req)
				}
			}))
			defer server.Close()
			s.newKubeClient = func(string) (*kubeClient, error) {
				return &kubeClient{baseURL: server.URL, client: server.Client()}, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}
			bound, err := s.migrateSafeRolloutWorkload(ctx, app, r)
			if scenario != "previous" && scenario != "failed" {
				if err == nil {
					t.Fatal("unsafe migration accepted")
				}
				kept, _ := st.GetAppRelease(app.TenantID, false, r.ID)
				if kept.RevisionWorkload != nil {
					t.Fatal("rejected migration wrote binding")
				}
				return
			}
			if err != nil || bound.RevisionWorkload == nil || bound.RevisionWorkload.OperationID != op.ID || bound.RevisionWorkload.DeploymentUID != "deployment-uid" {
				t.Fatal("migration failed", err)
			}
			copy := bound
			copy.RevisionWorkload, copy.UpdatedAt = nil, r.UpdatedAt
			if !reflect.DeepEqual(copy, r) {
				t.Fatal("migration changed serving or lifecycle fields")
			}
			if repeated, err := s.migrateSafeRolloutWorkload(ctx, app, bound); err != nil || !reflect.DeepEqual(repeated, bound) {
				t.Fatal("bound migration is not idempotent", err)
			}
		})
	}
}

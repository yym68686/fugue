package controller

import (
	"context"
	"encoding/json"
	"fmt"
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
	for _, scenario := range []string{"previous", "failed", "missing_source", "wrong_spec", "new_uid", "new_generation", "recreated_after_operation", "old_resource", "wrong_owner", "wrong_selector", "wrong_image", "old_agent", "agent_changed", "agent_config_changed", "missing_deployment", "canceled", "missing_labels", "label_retry", "label_wrong_owner", "label_wrong_image", "label_replaced_after_preflight"} {
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
			labelScenario := strings.HasPrefix(scenario, "label_") || scenario == "missing_labels"
			var rs, pod map[string]any
			if labelScenario {
				delete(objectMapValue(nestedObjectValue(dep, "spec", "template", "metadata", "labels")), runtime.FugueLabelAppWorkload)
				delete(objectMapValue(nestedObjectValue(service, "spec", "selector")), runtime.FugueLabelAppWorkload)
				dep["status"] = map[string]any{"observedGeneration": float64(1), "readyReplicas": float64(0)}
				rs = map[string]any{"metadata": cloneKubeMap(objectMapField(dep, "metadata")), "spec": map[string]any{"template": cloneKubeMap(objectMapValue(nestedObjectValue(dep, "spec", "template")))}}
				rm := objectMapField(rs, "metadata")
				rm["name"], rm["uid"] = "revision-rs", "rs-uid"
				rm["ownerReferences"] = []any{map[string]any{"kind": "Deployment", "name": objectStringField(objectMapField(dep, "metadata"), "name"), "uid": "deployment-uid", "controller": true}}
				pod = map[string]any{"metadata": cloneKubeMap(rm), "status": map[string]any{}}
				pm := objectMapField(pod, "metadata")
				pm["name"], pm["uid"] = "revision-pod", "pod-uid"
				pm["ownerReferences"] = []any{map[string]any{"kind": "ReplicaSet", "name": "revision-rs", "uid": "rs-uid", "controller": true}}
				if scenario == "label_wrong_owner" {
					objectMapField(objectMapField(service, "metadata"), "labels")[runtime.FugueLabelTenantID] = "foreign"
				}
				if scenario == "label_wrong_image" {
					mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "containers"))[0]["image"] = "registry.example/other:v3"
				}
			}
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
			case "old_agent", "agent_changed", "agent_config_changed":
				mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "initContainers"))[0]["image"] = "registry.example/legacy-agent:v1"
				if scenario == "agent_config_changed" {
					mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "initContainers"))[0]["args"] = []string{"unexpected"}
				}
			}
			reads := 0
			writes := 0
			failedPatch := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if labelScenario {
					reads++
					if scenario == "label_replaced_after_preflight" && reads == 4 {
						objectMapField(dep, "metadata")["uid"] = "replacement"
					}
					var obj map[string]any
					switch {
					case strings.Contains(req.URL.Path, "/deployments/"):
						obj = dep
					case strings.Contains(req.URL.Path, "/services/"):
						obj = service
					case strings.HasSuffix(req.URL.Path, "/replicasets"):
						_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{rs}})
						return
					case strings.Contains(req.URL.Path, "/replicasets/"):
						obj = rs
					case strings.HasSuffix(req.URL.Path, "/pods"):
						_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{pod}})
						return
					case strings.Contains(req.URL.Path, "/pods/"):
						obj = pod
					}
					if obj == nil {
						http.NotFound(w, req)
						return
					}
					if req.Method == http.MethodPatch {
						var patch map[string]any
						if err := json.NewDecoder(req.Body).Decode(&patch); err != nil {
							t.Error(err)
						}
						for _, key := range []string{"uid", "resourceVersion"} {
							if objectMapField(patch, "metadata")[key] != objectMapField(obj, "metadata")[key] {
								t.Error("unguarded migration", key)
							}
						}
						if scenario == "label_retry" && !failedPatch && strings.Contains(req.URL.Path, "/pods/") {
							failedPatch = true
							http.Error(w, "interrupted", 503)
							return
						}
						mergeWorkloadTestPatch(obj, patch)
						writes++
						objectMapField(obj, "metadata")["resourceVersion"] = fmt.Sprint(writes + 1)
						if strings.Contains(req.URL.Path, "/deployments/") {
							objectMapField(dep, "metadata")["generation"] = float64(writes + 1)
							objectMapField(dep, "status")["observedGeneration"] = float64(writes + 1)
						}
					} else if req.Method != http.MethodGet {
						t.Error("unexpected mutation", req.Method)
					}
					_ = json.NewEncoder(w).Encode(obj)
					return
				}
				if req.Method != http.MethodGet {
					t.Error("migration mutated Kubernetes")
					http.Error(w, "mutation", 400)
					return
				}
				reads++
				if reads == 6 {
					if scenario == "agent_changed" {
						mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "initContainers"))[0]["image"] = "registry.example/legacy-agent:v2"
					}
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
			if scenario == "label_retry" {
				if err == nil {
					t.Fatal("expected interrupted migration")
				}
				bound, err = s.migrateSafeRolloutWorkload(ctx, app, r)
			}
			if scenario != "previous" && scenario != "failed" && scenario != "old_agent" && scenario != "missing_labels" && scenario != "label_retry" {
				if err == nil {
					t.Fatal("unsafe migration accepted")
				}
				kept, _ := st.GetAppRelease(app.TenantID, false, r.ID)
				if kept.RevisionWorkload != nil {
					t.Fatal("rejected migration wrote binding")
				}
				if writes != 0 {
					t.Fatal("rejected source mutated workload")
				}
				return
			}
			if err != nil || bound.RevisionWorkload == nil || bound.RevisionWorkload.OperationID != op.ID || bound.RevisionWorkload.DeploymentUID != "deployment-uid" {
				t.Fatal("migration failed", err)
			}
			if labelScenario {
				name := objectStringField(objectMapField(dep, "metadata"), "name")
				if objectMapField(pod, "metadata")["uid"] != "pod-uid" || objectStringMapValue(objectMapField(pod, "metadata")["labels"])[runtime.FugueLabelAppWorkload] != name || objectStringMapValue(nestedObjectValue(service, "spec", "selector"))[runtime.FugueLabelAppWorkload] != name || objectMapField(dep, "spec")["paused"] != false {
					t.Fatal("label migration lost identities or pause state")
				}
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

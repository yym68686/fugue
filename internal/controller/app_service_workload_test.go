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

	"fugue/internal/runtime"
)

func TestAppServiceWorkloadMigrationOrdersAndRecoversWrites(t *testing.T) {
	for _, paused := range []bool{false, true} {
		for _, failPodPatch := range []bool{false, true} {
			t.Run(fmt.Sprintf("paused=%t/retry=%t", paused, failPodPatch), func(t *testing.T) {
				labels := map[string]string{runtime.FugueLabelAppID: "app_test", runtime.FugueLabelTenantID: "tenant_test", runtime.FugueLabelManagedBy: "fugue"}
				meta := func(name, uid string) map[string]any {
					return map[string]any{"name": name, "uid": uid, "resourceVersion": "1", "generation": 1, "labels": labels}
				}
				owner := func(kind, name, uid string) []map[string]any {
					return []map[string]any{{"apiVersion": "apps/v1", "kind": kind, "name": name, "uid": uid, "controller": true}}
				}
				deployment := map[string]any{"metadata": meta("workload", "deployment-uid"), "spec": map[string]any{"paused": paused, "replicas": 1, "selector": map[string]any{"matchLabels": labels}, "template": map[string]any{"metadata": map[string]any{"labels": labels}}}, "status": map[string]any{"observedGeneration": 1, "readyReplicas": 1}}
				rs := map[string]any{"metadata": meta("revision", "rs-uid"), "spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"labels": labels}}}}
				objectMapField(rs, "metadata")["ownerReferences"] = owner("Deployment", "workload", "deployment-uid")
				pod := map[string]any{"metadata": meta("pod", "pod-uid"), "status": map[string]any{"conditions": []map[string]any{{"type": "Ready", "status": "True"}}}}
				objectMapField(pod, "metadata")["ownerReferences"] = owner("ReplicaSet", "revision", "rs-uid")
				foreign := map[string]any{"metadata": meta("candidate-pod", "candidate-pod-uid")}
				objectMapField(foreign, "metadata")["ownerReferences"] = owner("ReplicaSet", "revision", "replaced-rs-uid")
				// JSON cloning prevents test maps from sharing label references.
				deployment, rs, pod, foreign = cloneKubeMap(deployment), cloneKubeMap(rs), cloneKubeMap(pod), cloneKubeMap(foreign)
				selectorBefore := cloneKubeMap(objectMapField(objectMapField(deployment, "spec"), "selector"))
				serviceApplied, failed := false, false
				writes := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					var object map[string]any
					switch {
					case strings.HasSuffix(req.URL.Path, "/deployments/workload"):
						object = deployment
					case strings.HasSuffix(req.URL.Path, "/replicasets/revision"):
						object = rs
					case strings.HasSuffix(req.URL.Path, "/pods/pod"):
						object = pod
					case strings.HasSuffix(req.URL.Path, "/replicasets"):
						_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{rs}})
						return
					case strings.HasSuffix(req.URL.Path, "/pods"):
						_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{pod, foreign}})
						return
					case strings.HasSuffix(req.URL.Path, "/services/service"):
						if req.Method == http.MethodGet {
							http.NotFound(w, req)
							return
						}
						if objectStringMapValue(objectMapField(pod, "metadata")["labels"])[runtime.FugueLabelAppWorkload] != "workload" {
							t.Error("Service applied before existing Pod was labeled")
						}
						serviceApplied = true
						_, _ = w.Write([]byte(`{}`))
						return
					default:
						t.Errorf("unexpected request: %s %s", req.Method, req.URL)
						http.NotFound(w, req)
						return
					}
					if req.Method == http.MethodPatch {
						var patch map[string]any
						_ = json.NewDecoder(req.Body).Decode(&patch)
						for _, key := range []string{"uid", "resourceVersion"} {
							if objectMapField(patch, "metadata")[key] != objectMapField(object, "metadata")[key] {
								t.Error("unguarded object patch", key)
							}
						}
						if failPodPatch && !failed && strings.HasSuffix(req.URL.Path, "/pods/pod") {
							failed = true
							http.Error(w, "interrupted write", http.StatusServiceUnavailable)
							return
						}
						mergeWorkloadTestPatch(object, patch)
						writes++
						objectMapField(object, "metadata")["resourceVersion"] = fmt.Sprint(writes + 1)
						if strings.HasSuffix(req.URL.Path, "/deployments/workload") {
							objectMapField(object, "metadata")["generation"] = float64(writes + 1)
							objectMapField(object, "status")["observedGeneration"] = float64(writes + 1)
						}
					}
					_ = json.NewEncoder(w).Encode(object)
				}))
				defer server.Close()
				service := map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "service", "namespace": "tenant"}, "spec": map[string]any{"selector": map[string]string{runtime.FugueLabelAppID: "app_test", runtime.FugueLabelTenantID: "tenant_test", runtime.FugueLabelAppWorkload: "workload"}}}
				client := &kubeClient{client: server.Client(), baseURL: server.URL}
				err := client.applyObjects(context.Background(), []map[string]any{service})
				if failPodPatch {
					if err == nil || serviceApplied || objectStringMapValue(objectMapField(deployment, "metadata")["annotations"])[appWorkloadMigrationAnnotation] == "" {
						t.Fatal("failed migration did not retain resumable state", err)
					}
					client = &kubeClient{client: server.Client(), baseURL: server.URL}
					err = client.applyObjects(context.Background(), []map[string]any{service})
				}
				if err != nil || !serviceApplied {
					t.Fatal("migration did not complete", err)
				}
				if !reflect.DeepEqual(selectorBefore, objectMapField(objectMapField(deployment, "spec"), "selector")) || objectMapField(deployment, "spec")["paused"] != paused {
					t.Fatal("changed immutable selector or original pause state")
				}
				if objectStringMapValue(objectMapField(foreign, "metadata")["labels"])[runtime.FugueLabelAppWorkload] != "" {
					t.Fatal("labeled a Pod owned by another ReplicaSet UID")
				}
				if objectStringMapValue(nestedObjectValue(rs, "spec", "template", "metadata", "labels"))[runtime.FugueLabelAppWorkload] != "workload" {
					t.Fatal("ReplicaSet cannot produce correctly labeled replacements")
				}
				beforeWrites := writes
				if err := client.prepareAppServiceWorkloads(context.Background(), []map[string]any{service}); err != nil || writes != beforeWrites {
					t.Fatal("completed migration was not idempotent", err)
				}
			})
		}
	}
}

func mergeWorkloadTestPatch(target, patch map[string]any) {
	for key, value := range patch {
		if value == nil {
			delete(target, key)
		} else if nested, ok := value.(map[string]any); ok {
			child, ok := target[key].(map[string]any)
			if !ok {
				child = map[string]any{}
				target[key] = child
			}
			mergeWorkloadTestPatch(child, nested)
		} else {
			target[key] = value
		}
	}
}

func TestAppServiceWorkloadRejectsUnsafeIdentityBeforeMutation(t *testing.T) {
	for _, reason := range []string{"missing_uid", "wrong_owner", "wrong_selector", "wrong_workload", "unknown_migration"} {
		t.Run(reason, func(t *testing.T) {
			labels := map[string]string{runtime.FugueLabelAppID: "app_test", runtime.FugueLabelTenantID: "tenant_test", runtime.FugueLabelManagedBy: "fugue"}
			deployment := map[string]any{"metadata": map[string]any{"uid": "deployment-uid", "resourceVersion": "1", "labels": labels}, "spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"labels": labels}}}}
			deployment = cloneKubeMap(deployment)
			meta := objectMapField(deployment, "metadata")
			template := objectMapValue(nestedObjectValue(deployment, "spec", "template", "metadata"))
			switch reason {
			case "missing_uid":
				delete(meta, "uid")
			case "wrong_owner":
				objectMapField(meta, "labels")[runtime.FugueLabelTenantID] = "other"
			case "wrong_selector":
				objectMapField(template, "labels")[runtime.FugueLabelAppID] = "other"
			case "wrong_workload":
				objectMapField(template, "labels")[runtime.FugueLabelAppWorkload] = "candidate"
			case "unknown_migration":
				meta["annotations"] = map[string]string{appWorkloadMigrationAnnotation: "unknown"}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/deployments/workload") {
					t.Errorf("unsafe identity caused further action: %s %s", r.Method, r.URL)
				}
				_ = json.NewEncoder(w).Encode(deployment)
			}))
			defer server.Close()
			client := &kubeClient{client: server.Client(), baseURL: server.URL}
			if err := client.prepareAppServiceWorkload(context.Background(), "tenant", "workload", map[string]string{runtime.FugueLabelAppID: "app_test", runtime.FugueLabelTenantID: "tenant_test", runtime.FugueLabelAppWorkload: "workload"}); err == nil {
				t.Fatal("unsafe identity accepted")
			}
		})
	}
}

func TestAppWorkloadPatchRetriesOnlyStatusConflicts(t *testing.T) {
	for _, changedSpec := range []bool{false, true} {
		t.Run(fmt.Sprint(changedSpec), func(t *testing.T) {
			original := map[string]any{"metadata": map[string]any{"uid": "uid", "resourceVersion": "1", "labels": map[string]any{"owner": "app"}}, "spec": map[string]any{"paused": true}}
			fresh := cloneKubeMap(original)
			objectMapField(fresh, "metadata")["resourceVersion"] = "2"
			if changedSpec {
				objectMapField(fresh, "spec")["paused"] = false
			}
			patches := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					_ = json.NewEncoder(w).Encode(fresh)
					return
				}
				patches++
				if patches == 1 {
					http.Error(w, "status changed", http.StatusConflict)
					return
				}
				var patch map[string]any
				_ = json.NewDecoder(r.Body).Decode(&patch)
				if objectMapField(patch, "metadata")["resourceVersion"] != "2" {
					t.Error("retry did not use fresh version")
				}
				_, _ = w.Write([]byte(`{}`))
			}))
			defer server.Close()
			client := &kubeClient{client: server.Client(), baseURL: server.URL}
			err := client.patchAppWorkloadObject(context.Background(), "/deployment", original, map[string]any{"spec": map[string]any{"paused": false}})
			if changedSpec && (err == nil || patches != 1) || !changedSpec && (err != nil || patches != 2) {
				t.Fatalf("retry state patches=%d error=%v", patches, err)
			}
		})
	}
}

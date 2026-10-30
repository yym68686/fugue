package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestAppCPUResizeIncludesVerifiedRevisionsAndRecovers(t *testing.T) {
	for _, scenario := range []string{"ready", "not-ready", "unstable", "pending", "pending-scheduled", "pending-running", "lost-response", "retired", "replacement", "restart-compensation", "foreign-owner", "policy-blocked", "actual-missing"} {
		t.Run(scenario, func(t *testing.T) {
			st := store.New(t.TempDir() + "/state.json")
			if err := st.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, _ := st.CreateTenant("Resize Tenant")
			project, _ := st.CreateProject(tenant.ID, "services", "")
			app, err := st.CreateApp(tenant.ID, project.ID, "service", "", model.AppSpec{RuntimeID: "runtime_managed_shared", Image: "registry.example/service:one", Replicas: 1,
				Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 128, MemoryLimitMebibytes: 256}})
			if err != nil {
				t.Fatal(err)
			}
			if err := st.SyncManagedAppObservedStatus(app.ID, "ready", 1, "serving", nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			app, _ = st.GetApp(app.ID)
			desired := app.Spec
			desired.Resources = model.CloneResourceSpec(app.Spec.Resources)
			desired.Resources.CPUMilliCores = 75
			op, err := st.CreateAppCPUResizeOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeAppCPUResize, DesiredSpec: &desired}, app.Spec)
			if err != nil {
				t.Fatal(err)
			}
			op, claimed, err := st.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed {
				t.Fatal(err)
			}
			serving := runtime.RuntimeAppResourceName(app)
			revision := serving + "-revision"
			controller := true
			pods := map[string]kubeResizePod{}
			for _, workload := range []string{serving, revision} {
				pod := appResizeTestPod(t)
				pod.Metadata.Name, pod.Metadata.UID = workload+"-pod", workload+"-pod-uid"
				pod.Metadata.Labels = map[string]string{runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue, runtime.FugueLabelTenantID: tenant.ID, runtime.FugueLabelAppID: app.ID, runtime.FugueLabelAppWorkload: workload}
				pod.Metadata.OwnerReferences = []kubeResizeOwnerReference{{Kind: "ReplicaSet", Name: workload + "-rs", UID: workload + "-rs-uid", Controller: &controller}}
				if workload == revision {
					// Legacy candidates can lack a workload label; ownership is decisive.
					delete(pod.Metadata.Labels, runtime.FugueLabelAppWorkload)
					if scenario == "not-ready" || scenario == "unstable" {
						pod.Status.Conditions[0].Status = "False"
						pod.Status.ContainerStatuses[0].Ready = false
						if scenario == "unstable" {
							pod.Status.ContainerStatuses[0].RestartCount = 1
						}
					}
					if strings.HasPrefix(scenario, "pending") {
						pod.Status.Phase, pod.Spec.NodeName = "Pending", ""
						pod.Status.Conditions = nil
						pod.Status.ContainerStatuses = nil
					}
					if scenario == "policy-blocked" {
						pod.Spec.Containers[0].ResizePolicy = []kubeResizePolicy{{ResourceName: "cpu", RestartPolicy: "RestartContainer"}}
					}
					if scenario == "actual-missing" {
						pod.Status.ContainerStatuses[0].Resources = nil
					}
				}
				pods[pod.Metadata.Name] = pod
			}
			deployment := func(name string) map[string]any {
				labels := map[string]string{runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue, runtime.FugueLabelTenantID: tenant.ID, runtime.FugueLabelAppID: app.ID, runtime.FugueLabelAppWorkload: name}
				uid := name + "-uid"
				if scenario == "foreign-owner" && name == revision {
					uid = "different-deployment-uid"
				}
				return map[string]any{"metadata": map[string]any{"name": name, "uid": uid, "generation": 1}, "spec": map[string]any{"replicas": 1, "template": map[string]any{"metadata": map[string]any{"labels": labels}}}, "status": map[string]any{"observedGeneration": 1}}
			}
			patches := map[string]int{}
			lostResponse := scenario == "lost-response" || scenario == "retired" || scenario == "replacement" || scenario == "restart-compensation"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1":
					json.NewEncoder(w).Encode(map[string]any{"groupVersion": "v1", "resources": []map[string]any{{"name": "pods/resize", "kind": "Pod", "namespaced": true, "verbs": []string{"patch"}}}})
				case r.Method == http.MethodPost && r.URL.Path == kubeSelfSubjectAccessReviewPath:
					var review map[string]any
					json.NewDecoder(r.Body).Decode(&review)
					review["status"] = map[string]any{"allowed": true}
					json.NewEncoder(w).Encode(review)
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/services/"):
					json.NewEncoder(w).Encode(map[string]any{"spec": map[string]any{"selector": map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: tenant.ID, runtime.FugueLabelAppWorkload: serving}}})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/deployments"):
					json.NewEncoder(w).Encode(map[string]any{"items": []any{deployment(serving), deployment(revision)}})
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/deployments/"):
					json.NewEncoder(w).Encode(deployment(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]))
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/replicasets/"):
					name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
					owner := strings.TrimSuffix(name, "-rs")
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": name + "-uid", "ownerReferences": []map[string]any{{"kind": "Deployment", "name": owner, "uid": owner + "-uid", "controller": true}}}})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pods"):
					items := []kubeResizePod{}
					for _, pod := range pods {
						items = append(items, pod)
					}
					json.NewEncoder(w).Encode(map[string]any{"items": items})
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/pods/"):
					pod, found := pods[r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]]
					if !found {
						http.NotFound(w, r)
						return
					}
					json.NewEncoder(w).Encode(pod)
				case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/resize"):
					parts := strings.Split(r.URL.Path, "/")
					name := parts[len(parts)-2]
					pod := pods[name]
					var patch kubeResizePodPatch
					if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
						t.Fatal(err)
					}
					journal, recorded, err := (&Service{Store: st}).loadAppCPUResizeJournal(op)
					wantPods := 2
					if scenario == "foreign-owner" || scenario == "policy-blocked" || scenario == "actual-missing" || scenario == "unstable" {
						wantPods = 1
					}
					if err != nil || !recorded || len(journal.Pods) != wantPods {
						t.Errorf("mutation preceded complete serving/revision journal: %+v %v", journal, err)
					}
					resources := patch.Spec.Containers[0].Resources
					if patch.Metadata.ResourceVersion != pod.Metadata.ResourceVersion || resources.Requests["memory"] != "128Mi" || resources.Requests["ephemeral-storage"] != "1Gi" || resources.Limits["memory"] != "256Mi" {
						t.Error("resize changed the resource envelope or omitted CAS")
					}
					patches[name]++
					pod.Spec.Containers[0].Resources = resources
					pod.Metadata.ResourceVersion += "1"
					pod.Metadata.Generation++
					pod.Status.ObservedGeneration = pod.Metadata.Generation
					if name == revision+"-pod" && scenario == "pending-scheduled" {
						pod.Spec.NodeName = "worker-a"
					}
					if name == revision+"-pod" && scenario == "pending-running" {
						started := appResizeTestPod(t)
						pod.Spec.NodeName = started.Spec.NodeName
						pod.Status.Phase = "Running"
						pod.Status.Conditions = started.Status.Conditions
						pod.Status.ContainerStatuses = started.Status.ContainerStatuses
					}
					if len(pod.Status.ContainerStatuses) > 0 {
						pod.Status.ContainerStatuses[0].Resources = &resources
						pod.Status.ContainerStatuses[0].AllocatedResources["cpu"] = resources.Requests["cpu"]
					}
					pods[name] = pod
					if lostResponse && name == revision+"-pod" && patches[name] == 1 {
						http.Error(w, "lost accepted response", http.StatusBadGateway)
						return
					}
					json.NewEncoder(w).Encode(pod)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			svc := &Service{Store: st, Config: config.ControllerConfig{KubectlApply: true, AppCPURequestResizeEnabled: true}, newKubeClient: func(string) (*kubeClient, error) {
				return &kubeClient{baseURL: server.URL, client: server.Client()}, nil
			}}
			err = svc.executeAppCPUResize(context.Background(), op, app)
			if lostResponse {
				if !errors.Is(err, errAppCPUResizeUnsettled) {
					t.Fatal("accepted candidate PATCH lost its interlock", err)
				}
				if scenario == "retired" {
					delete(pods, revision+"-pod")
				}
				if scenario == "replacement" {
					pod := pods[revision+"-pod"]
					pod.Metadata.UID = "replacement-pod-uid"
					resources := cloneKubeResourceRequirements(pod.Spec.Containers[0].Resources)
					resources.Requests["cpu"] = "100m"
					pod.Spec.Containers[0].Resources = resources
					pod.Status.ContainerStatuses[0].Resources = &resources
					pod.Status.ContainerStatuses[0].AllocatedResources = map[string]string{"cpu": "100m"}
					pods[revision+"-pod"] = pod
				}
				if scenario == "restart-compensation" {
					pod := pods[revision+"-pod"]
					pod.Status.ContainerStatuses[0].RestartCount++
					pods[revision+"-pod"] = pod
				}
				err = svc.executeAppCPUResize(context.Background(), op, app)
			}
			if scenario == "restart-compensation" {
				for attempt := 0; attempt < 3 && !errors.Is(err, errAppCPUResizeAborted); attempt++ {
					err = svc.executeAppCPUResize(context.Background(), op, app)
				}
				if !errors.Is(err, errAppCPUResizeAborted) {
					t.Fatal("revision drift did not compensate the whole cohort", err)
				}
				for _, pod := range pods {
					if pod.Spec.Containers[0].Resources.Requests["cpu"] != "100m" {
						t.Fatal("surviving Pod not restored", pod.Metadata.Name)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if patches[serving+"-pod"] != 1 {
				t.Fatal("serving patch repeated", patches)
			}
			want := 1
			if scenario == "foreign-owner" || scenario == "policy-blocked" || scenario == "actual-missing" || scenario == "unstable" {
				want = 0
			}
			if patches[revision+"-pod"] != want {
				t.Fatal("revision was skipped, replayed or not owned", patches)
			}
			actual, _ := st.GetApp(app.ID)
			if actual.Spec.Resources.CPUMilliCores != 75 || actual.Status.Phase != app.Status.Phase {
				t.Fatal("resize did not preserve and commit app state")
			}
			if scenario == "replacement" {
				client, _ := svc.kubeClient()
				queued, err := svc.reconcileAppCPUResizeTarget(t.Context(), client, actual)
				if err != nil || !queued || patches[revision+"-pod"] != 1 {
					t.Fatal("replacement revision did not queue a fresh guarded resize", queued, patches, err)
				}
			}
			if scenario == "policy-blocked" || scenario == "actual-missing" || scenario == "unstable" {
				journal, _, _ := svc.loadAppCPUResizeJournal(op)
				if len(journal.Deferred) != 1 || journal.Deferred[0].Reason == "" {
					t.Fatal("unsafe revision was not explained in the journal", journal.Deferred)
				}
			}
		})
	}
}

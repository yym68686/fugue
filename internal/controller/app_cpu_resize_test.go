package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func appResizeTestPod(t *testing.T) kubeResizePod {
	t.Helper()
	var pod kubeResizePod
	err := json.Unmarshal([]byte(`{"metadata":{"name":"service-rs-pod","uid":"pod-one","resourceVersion":"1","generation":1},"spec":{"nodeName":"worker-a","containers":[{"name":"service","resources":{"requests":{"cpu":"100m","memory":"128Mi","ephemeral-storage":"1Gi"},"limits":{"memory":"256Mi"}}}]},"status":{"phase":"Running","observedGeneration":1,"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"service","ready":true,"restartCount":0,"allocatedResources":{"cpu":"100m"},"resources":{"requests":{"cpu":"100m","memory":"128Mi"},"limits":{"memory":"256Mi"}},"state":{"running":{"startedAt":"2026-01-01T00:00:00Z"}}}]}}`), &pod)
	if err != nil {
		t.Fatal(err)
	}
	return pod
}

func TestAppCPUResizeSafetyAndConvergence(t *testing.T) {
	pod := appResizeTestPod(t)
	ob, err := observeAppCPUResizePod(pod, "service")
	if err != nil {
		t.Fatal(err)
	}
	if err := assessAppCPURequestResize(ob, 75, false); err != nil {
		t.Fatal(err)
	}
	baseline := appCPUResizePodBaseline{Name: ob.PodName, UID: ob.PodUID, Node: ob.NodeName, Restarts: ob.RestartCount, StartedAt: ob.ContainerStartedAt, Resources: ob.DesiredResources}
	if appCPURequestResizeConverged(ob, 75) {
		t.Fatal("old actual resources counted as resized")
	}
	for _, change := range []string{"uid", "restart", "started", "node", "deleting"} {
		changed := ob
		switch change {
		case "uid":
			changed.PodUID = "other"
		case "restart":
			changed.RestartCount++
		case "started":
			changed.ContainerStartedAt = "later"
		case "node":
			changed.NodeName = "other"
		case "deleting":
			changed.DeletionTimestamp = "now"
		}
		if validateAppCPUResizeIdentity(changed, baseline) == nil {
			t.Fatal("identity drift accepted", change)
		}
	}
	target := cloneKubeResourceRequirements(ob.DesiredResources)
	target.Requests["cpu"] = "75m"
	if !appCPUResizeEnvelopeMatches(target, ob.DesiredResources, 75) {
		t.Fatal("CPU-only change rejected")
	}
	target.Requests["ephemeral-storage"] = "2Gi"
	if appCPUResizeEnvelopeMatches(target, ob.DesiredResources, 75) {
		t.Fatal("extra resource silently changed")
	}
	ob.ResizePolicy = []kubeResizePolicy{{ResourceName: "cpu", RestartPolicy: "RestartContainer"}}
	if assessAppCPURequestResize(ob, 75, false) == nil {
		t.Fatal("restart policy accepted")
	}
	ob.ResizePolicy = nil
	ob.DesiredResources.Limits["cpu"] = "100m"
	ob.DesiredResources.Limits["memory"] = "128Mi"
	ob.Containers[0].Resources = ob.DesiredResources
	if assessAppCPURequestResize(ob, 75, false) == nil {
		t.Fatal("QoS class transition accepted")
	}
}

func TestAppCPUResizeExecutorRecoversAcceptedPatchWithoutReplayingOrRedeploying(t *testing.T) {
	for _, scenario := range []string{"normal", "lost-response", "restart-compensation", "deadline-compensation"} {
		t.Run(scenario, func(t *testing.T) {
			lostResponse := scenario != "normal"
			s := store.New(t.TempDir() + "/state.json")
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := s.CreateTenant("Resize Tenant")
			if err != nil {
				t.Fatal(err)
			}
			project, err := s.CreateProject(tenant.ID, "services", "")
			if err != nil {
				t.Fatal(err)
			}
			app, err := s.CreateApp(tenant.ID, project.ID, "service", "", model.AppSpec{Image: "registry.example/service:one", RuntimeID: "runtime_managed_shared", Replicas: 1, Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 128, MemoryLimitMebibytes: 256}})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.SyncManagedAppObservedStatus(app.ID, "ready", 1, "serving", nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			app, err = s.GetApp(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			desired := app.Spec
			desired.Resources = model.CloneResourceSpec(app.Spec.Resources)
			desired.Resources.CPUMilliCores = 75
			op, err := s.CreateAppCPUResizeOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeAppCPUResize, DesiredSpec: &desired}, app.Spec)
			if err != nil {
				t.Fatal(err)
			}
			op, claimed, err := s.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed {
				t.Fatal(err)
			}
			workload := runtime.RuntimeAppResourceName(app)
			pod := appResizeTestPod(t)
			pod.Metadata.Labels = map[string]string{runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue, runtime.FugueLabelTenantID: tenant.ID, runtime.FugueLabelAppID: app.ID, runtime.FugueLabelAppWorkload: workload}
			controller := true
			pod.Metadata.OwnerReferences = []kubeResizeOwnerReference{{Kind: "ReplicaSet", Name: "service-rs", UID: "rs-one", Controller: &controller}}
			patches := 0
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
					json.NewEncoder(w).Encode(map[string]any{"spec": map[string]any{"selector": pod.Metadata.Labels}})
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/deployments/"):
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": workload, "uid": "deployment-one", "generation": 1}, "spec": map[string]any{"replicas": 1}, "status": map[string]any{"observedGeneration": 1, "readyReplicas": 1}})
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/replicasets/"):
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "rs-one", "ownerReferences": []map[string]any{{"kind": "Deployment", "name": workload, "uid": "deployment-one", "controller": true}}}})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pods"):
					json.NewEncoder(w).Encode(map[string]any{"items": []kubeResizePod{pod}})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pods/"+pod.Metadata.Name):
					json.NewEncoder(w).Encode(pod)
				case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/resize"):
					patches++
					evidence, err := s.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: tenant.ID, OperationID: op.ID, Types: []string{appCPUResizePreflight}})
					if err != nil || len(evidence) < 1 {
						t.Error("PATCH without durable preflight journal")
					}
					var patch kubeResizePodPatch
					if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
						t.Error(err)
					}
					resources := patch.Spec.Containers[0].Resources
					expectedCPU := "75m"
					if patches > 1 {
						expectedCPU = "100m"
					}
					if resources.Requests["cpu"] != expectedCPU || resources.Requests["ephemeral-storage"] != "1Gi" || resources.Limits["memory"] != "256Mi" || patch.Metadata.ResourceVersion != pod.Metadata.ResourceVersion {
						t.Error("unsafe resize payload", patch)
					}
					pod.Spec.Containers[0].Resources = resources
					pod.Metadata.ResourceVersion += "1"
					pod.Metadata.Generation++
					pod.Status.ObservedGeneration = pod.Metadata.Generation
					pod.Status.ContainerStatuses[0].Resources = &resources
					pod.Status.ContainerStatuses[0].AllocatedResources = map[string]string{"cpu": expectedCPU}
					if lostResponse && patches == 1 {
						http.Error(w, "response lost after acceptance", http.StatusBadGateway)
						return
					}
					json.NewEncoder(w).Encode(pod)
				default:
					t.Errorf("unexpected Kubernetes mutation/read %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			svc := &Service{Store: s, Config: config.ControllerConfig{KubectlApply: true, AppCPURequestResizeEnabled: true}, newKubeClient: func(string) (*kubeClient, error) {
				return &kubeClient{baseURL: server.URL, client: server.Client(), bearerToken: "test"}, nil
			}}
			err = svc.executeAppCPUResize(context.Background(), op, app)
			if lostResponse {
				if !errors.Is(err, errAppCPUResizeUnsettled) {
					t.Fatal("ambiguous PATCH lost interlock", err)
				}
				current, _ := s.GetOperation(op.ID)
				if current.Status != model.OperationStatusRunning {
					t.Fatal("unfinished mutation finalized")
				}
				if scenario == "restart-compensation" {
					pod.Status.ContainerStatuses[0].RestartCount++
				}
				if scenario == "deadline-compensation" {
					journal, _, err := svc.loadAppCPUResizeJournal(op)
					if err != nil {
						t.Fatal(err)
					}
					journal.CreatedAt = time.Now().Add(-time.Hour)
					if err := svc.recordAppCPUResizeEvidence(op, app, appCPUResizePreflight, "expired preflight", journal); err != nil {
						t.Fatal(err)
					}
				}
				err = svc.executeAppCPUResize(context.Background(), op, app)
			}
			if strings.HasSuffix(scenario, "compensation") {
				if !errors.Is(err, errAppCPUResizeUnsettled) {
					t.Fatal("compensation finalized before actual resource verification", err)
				}
				err = svc.executeAppCPUResize(context.Background(), op, app)
				if !errors.Is(err, errAppCPUResizeAborted) || patches != 2 {
					t.Fatal("interrupted resize not safely compensated", patches, err)
				}
				if _, err := s.FailManagedOperationGuarded(op, err.Error()); err != nil {
					t.Fatal(err)
				}
				actual, _ := s.GetApp(app.ID)
				if actual.Spec.Resources.CPUMilliCores != 100 || !reflect.DeepEqual(actual.Status, app.Status) {
					t.Fatal("compensation changed serving configuration/status")
				}
				if _, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &actual.Spec}); err != nil {
					t.Fatal("compensated resize blocked configuration recovery", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if patches != 1 {
				t.Fatal("mutation replayed", patches)
			}
			current, _ := s.GetOperation(op.ID)
			if current.Status != model.OperationStatusCompleted {
				t.Fatal(current.Status)
			}
			actual, _ := s.GetApp(app.ID)
			if actual.Spec.Resources.CPUMilliCores != 75 || !reflect.DeepEqual(actual.Status, app.Status) {
				t.Fatal("resize lost serving state")
			}
		})
	}
}

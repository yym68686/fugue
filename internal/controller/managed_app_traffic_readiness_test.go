package controller

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/releaseproof"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func trafficReadinessFixture(t *testing.T, stored ...model.App) (model.App, map[string]any) {
	t.Helper()
	app := model.App{ID: "app-a", TenantID: "tenant-a", Name: "sample", Spec: model.AppSpec{Image: "registry.example/sample@sha256:" + strings.Repeat("a", 64), RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Ports: []int{8080}}}
	if len(stored) > 0 {
		app = stored[0]
	}
	app = (runtime.Renderer{}).PrepareApp(app)
	ns := runtime.NamespaceForTenant(app.TenantID)
	dn, sn := runtime.RuntimeAppResourceName(app), runtime.RuntimeAppServiceName(app)
	var dep releaseproof.Deployment
	var svc releaseproof.Service
	for _, object := range (runtime.Renderer{}).BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil) {
		data, _ := json.Marshal(object)
		if object["kind"] == "Deployment" {
			if err := json.Unmarshal(data, &dep); err != nil {
				t.Fatal(err)
			}
		}
		if object["kind"] == "Service" {
			if err := json.Unmarshal(data, &svc); err != nil {
				t.Fatal(err)
			}
		}
	}
	dep.Metadata.UID, dep.Metadata.ResourceVersion, dep.Metadata.Generation = "deployment-uid", "dep-v1", 3
	dep.Spec.Replicas = new(int)
	*dep.Spec.Replicas = 1
	dep.Status.ObservedGeneration, dep.Status.Replicas, dep.Status.UpdatedReplicas, dep.Status.ReadyReplicas, dep.Status.AvailableReplicas = 3, 1, 1, 1, 1
	svc.Metadata.UID, svc.Metadata.ResourceVersion = "service-uid", "svc-v1"
	yes := true
	var rs releaseproof.ReplicaSet
	rs.Metadata.Name, rs.Metadata.Namespace, rs.Metadata.UID = "sample-rs", ns, "rs-uid"
	rs.Metadata.Labels = maps.Clone(dep.Spec.Template.Metadata.Labels)
	rs.Metadata.OwnerReferences = []releaseproof.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: dn, UID: dep.Metadata.UID, Controller: &yes}}
	rs.Spec.Template.Metadata.Labels = maps.Clone(dep.Spec.Template.Metadata.Labels)
	rs.Spec.Template.Metadata.Annotations = dep.Spec.Template.Metadata.Annotations
	var pod releaseproof.Pod
	pod.Metadata.Name, pod.Metadata.Namespace, pod.Metadata.UID = "sample-pod", ns, "pod-uid"
	pod.Metadata.Labels = maps.Clone(dep.Spec.Template.Metadata.Labels)
	pod.Metadata.Annotations = dep.Spec.Template.Metadata.Annotations
	pod.Metadata.OwnerReferences = []releaseproof.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Metadata.Name, UID: rs.Metadata.UID, Controller: &yes}}
	pod.Spec.Containers = dep.Spec.Template.Spec.Containers
	pod.Status.Phase, pod.Status.PodIP = "Running", "192.0.2.10"
	pod.Status.Conditions = []struct {
		Type   string `json:"type"`
		Status string `json:"status"`
	}{{Type: "Ready", Status: "True"}}
	var slice releaseproof.EndpointSlice
	slice.Metadata.Name, slice.Metadata.Namespace, slice.Metadata.UID = "sample-slice", ns, "slice-uid"
	slice.Metadata.Labels = map[string]string{"kubernetes.io/service-name": sn}
	slice.Metadata.OwnerReferences = []releaseproof.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: sn, UID: svc.Metadata.UID, Controller: &yes}}
	ep := releaseproof.Endpoint{Addresses: []string{pod.Status.PodIP}}
	ep.TargetRef.Kind, ep.TargetRef.Name, ep.TargetRef.Namespace, ep.TargetRef.UID = "Pod", pod.Metadata.Name, ns, pod.Metadata.UID
	ep.Conditions.Ready = &yes
	slice.Endpoints = []releaseproof.Endpoint{ep}
	var namespace releaseproof.MetadataEvidence
	namespace.Metadata.Name, namespace.Metadata.UID = ns, "ns-uid"
	return app, map[string]any{
		"/api/v1/namespaces/" + ns:                                            namespace,
		deploymentAPIPath(ns, dn):                                             dep,
		"/api/v1/namespaces/" + ns + "/services/" + sn:                        svc,
		"/api/v1/namespaces/" + ns + "/pods/" + pod.Metadata.Name:             pod,
		"/apis/apps/v1/namespaces/" + ns + "/replicasets/" + rs.Metadata.Name: rs,
		"/apis/discovery.k8s.io/v1/namespaces/" + ns + "/endpointslices":      map[string]any{"items": []releaseproof.EndpointSlice{slice}},
	}
}

func TestOrdinaryRolloutWaitsForTrafficProofBeforeCompleting(t *testing.T) {
	for _, scenario := range []string{"endpoint becomes ready", "endpoint observation remains denied"} {
		t.Run(scenario, func(t *testing.T) {
			st := store.New(t.TempDir() + "/state.json")
			if err := st.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := st.CreateTenant("Sample tenant")
			if err != nil {
				t.Fatal(err)
			}
			project, err := st.CreateProject(tenant.ID, "Sample project", "")
			if err != nil {
				t.Fatal(err)
			}
			image := "registry.example/sample@sha256:" + strings.Repeat("a", 64)
			stored, err := st.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "sample", "", model.AppSpec{Image: image, RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Ports: []int{8080}}, model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: image})
			if err != nil {
				t.Fatal(err)
			}
			app, objects := trafficReadinessFixture(t, stored)
			ns := runtime.NamespaceForTenant(app.TenantID)
			dp := deploymentAPIPath(ns, runtime.RuntimeAppResourceName(app))
			proofDep := objects[dp].(releaseproof.Deployment)
			dep, found := (&Service{Renderer: runtime.Renderer{}}).expectedManagedAppDeployment(app, runtime.SchedulingConstraints{})
			if !found {
				t.Fatal("expected rendered deployment")
			}
			dep.Metadata.UID, dep.Metadata.ResourceVersion = proofDep.Metadata.UID, proofDep.Metadata.ResourceVersion
			managedAppLiveGuardMarkReady(&dep, 1)
			// Each observer decodes only its own evidence fields. Serve a complete
			// Kubernetes object, not either observer's deliberately narrow DTO.
			data, _ := json.Marshal(dep)
			var deploymentObject map[string]any
			if err := json.Unmarshal(data, &deploymentObject); err != nil {
				t.Fatal(err)
			}
			deploymentObject["metadata"].(map[string]any)["labels"] = proofDep.Metadata.Labels
			objects[dp] = deploymentObject
			pod := readyTemplatePod("sample-pod", dep, kubeResourceRequirements{})
			managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
			if err != nil {
				t.Fatal(err)
			}
			managed.Metadata.Generation, managed.Status.ObservedGeneration = 1, 1
			managed.Status.LastAppliedSpecHash = runtime.ManagedAppSpecHash(managed.Spec)
			objects[managedAppAPIPath(ns, runtime.ManagedAppResourceName(app))] = managed
			objects["/api/v1/namespaces/"+ns+"/pods"] = kubePodList{Items: []kubePod{pod}}
			var endpoints atomic.Int32
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/status") {
					if endpoints.Load() < 2 {
						t.Error("published completion before independent traffic proof")
					}
					_, _ = w.Write([]byte(`{}`))
					return
				}
				if r.Method != http.MethodGet {
					t.Error("readiness mutated workload")
					http.Error(w, "not read-only", 405)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/endpointslices") {
					if endpoints.Add(1) == 1 || scenario == "endpoint observation remains denied" {
						http.Error(w, "pending independent observation", 403)
						return
					}
				}
				data, ok := objects[r.URL.Path]
				if !ok || r.URL.Query().Get("watch") != "" {
					http.NotFound(w, r)
					return
				}
				_ = json.NewEncoder(w).Encode(data)
			}))
			defer kube.Close()
			timeout := 4 * time.Second
			if scenario == "endpoint observation remains denied" {
				timeout = 150 * time.Millisecond
			}
			client := &kubeClient{client: kube.Client(), baseURL: kube.URL}
			s := &Service{Store: st, Renderer: runtime.Renderer{}, Config: config.ControllerConfig{ManagedAppRolloutTimeout: timeout}, newKubeClient: func(string) (*kubeClient, error) { return client, nil }}
			err = s.waitForManagedAppRolloutWithScheduling(context.Background(), app, "", runtime.SchedulingConstraints{})
			if scenario == "endpoint becomes ready" {
				if err != nil || endpoints.Load() < 2 {
					t.Fatalf("completed without full traffic proof: reads=%d err=%v", endpoints.Load(), err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "traffic readiness observation") {
				t.Fatalf("denied evidence completed deployment: reads=%d err=%v", endpoints.Load(), err)
			}
		})
	}
}

func TestOrdinaryRolloutTrafficGateUsesExactCompilerReadiness(t *testing.T) {
	for _, scenario := range []string{"ready", "old cohort", "old generation", "old endpoint", "wrong image", "foreign service", "foreign rs", "wrong address", "unready endpoint", "missing owner", "endpoint denied", "deployment changed", "service replaced", "incomplete inventory"} {
		t.Run(scenario, func(t *testing.T) {
			app, objects := trafficReadinessFixture(t)
			ns := runtime.NamespaceForTenant(app.TenantID)
			dn, sn := runtime.RuntimeAppResourceName(app), runtime.RuntimeAppServiceName(app)
			dp, sp := deploymentAPIPath(ns, dn), "/api/v1/namespaces/"+ns+"/services/"+sn
			pp, rp := "/api/v1/namespaces/"+ns+"/pods/sample-pod", "/apis/apps/v1/namespaces/"+ns+"/replicasets/sample-rs"
			epPath := "/apis/discovery.k8s.io/v1/namespaces/" + ns + "/endpointslices"
			dep, svc, pod, rs := objects[dp].(releaseproof.Deployment), objects[sp].(releaseproof.Service), objects[pp].(releaseproof.Pod), objects[rp].(releaseproof.ReplicaSet)
			slices := objects[epPath].(map[string]any)["items"].([]releaseproof.EndpointSlice)
			switch scenario {
			case "old cohort":
				dep.Status.Replicas = 2
			case "old generation":
				dep.Status.ObservedGeneration = 2
			case "old endpoint":
				pod.Metadata.Annotations.ReleaseKey = "previous"
			case "wrong image":
				pod.Spec.Containers[0].Image = "registry.example/other:old"
			case "foreign service":
				svc.Spec.Selector[runtime.FugueLabelAppWorkload] = "another-workload"
			case "foreign rs":
				rs.Metadata.OwnerReferences[0].UID = "another-deployment"
			case "wrong address":
				slices[0].Endpoints[0].Addresses = []string{"192.0.2.99"}
			case "unready endpoint":
				no := false
				slices[0].Endpoints[0].Conditions.Ready = &no
			case "missing owner":
				pod.Metadata.OwnerReferences = nil
			case "incomplete inventory":
				objects[epPath].(map[string]any)["metadata"] = map[string]string{"continue": "more"}
			}
			objects[dp], objects[sp], objects[pp], objects[rp] = dep, svc, pod, rs
			reads := map[string]int{}
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads[r.URL.Path]++
				if scenario == "endpoint denied" && r.URL.Path == epPath {
					http.Error(w, "denied", 403)
					return
				}
				data, found := objects[r.URL.Path]
				if !found {
					http.NotFound(w, r)
					return
				}
				if r.URL.Path == epPath && r.URL.Query().Get("labelSelector") != "kubernetes.io/service-name="+sn {
					t.Error("endpoint query not scoped to Service")
				}
				if scenario == "deployment changed" && r.URL.Path == dp && reads[dp] > 1 {
					dep.Metadata.ResourceVersion = "dep-v2"
					data = dep
				}
				if scenario == "service replaced" && r.URL.Path == sp && reads[sp] > 1 {
					svc.Metadata.UID = "replacement"
					data = svc
				}
				_ = json.NewEncoder(w).Encode(data)
			}))
			defer kube.Close()
			ready, reason, err := (&Service{}).managedAppTrafficReady(context.Background(), &kubeClient{client: kube.Client(), baseURL: kube.URL}, app)
			if ready != (scenario == "ready") {
				t.Fatalf("ready=%v reason=%s err=%v", ready, reason, err)
			}
			if scenario == "ready" && err != nil {
				t.Fatal(err)
			}
			if !ready && reason == "" && err == nil {
				t.Fatal("missing failing evidence")
			}
			if scenario == "ready" && (reads[dp] != 2 || reads[sp] != 2) {
				t.Fatal("target not pinned across capture", reads)
			}
		})
	}
}

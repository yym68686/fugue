package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestImageMigrationAcceptsOnlyCompletedServingColdTarget(t *testing.T) {
	const (
		namespace = "fg-tenant-example"
		serviceID = "service_fixture"
		source    = "postgres-old"
		target    = "postgres-restored"
		targetUID = "target-uid"
		systemID  = "123456"
		endpoint  = "postgres-stable"
		podName   = "postgres-restored-1"
		podUID    = "pod-uid"
		podIP     = "10.42.0.7"
	)
	marker := coldStateName(serviceID)
	cases := map[string]string{
		"completed-target": "", "completed-retained-source": "",
		"incomplete-phase": "does not prove", "missing-record": "does not prove",
		"wrong-target-uid": "does not prove", "source-cluster": "does not prove",
		"unfenced-source": "not the exact fenced Cluster", "changed-source-uid": "not the exact fenced Cluster",
		"wrong-service-selector": "does not exclusively select", "wrong-endpoint-pod": "does not exclusively serve",
		"wrong-endpoint-ip": "address is not on", "unready-endpoint": "does not exclusively serve",
		"stale-endpoint-slice": "does not belong to", "missing-endpoints": "has no Ready target",
		"forbidden-endpoints": "cannot verify the cold migration stable EndpointSlices",
	}
	for scenario, wantError := range cases {
		t.Run(scenario, func(t *testing.T) {
			state := coldPostgresState{
				Version: 1, ServiceID: serviceID, SourceName: source, SourceUID: "source-uid",
				TargetName: target, TargetUID: targetUID, Endpoint: endpoint, SystemID: systemID, Phase: "complete",
			}
			clusterName, clusterUID := target, targetUID
			if scenario == "incomplete-phase" {
				state.Phase = "copying"
			}
			if scenario == "wrong-target-uid" {
				clusterUID = "replacement-uid"
			}
			if scenario == "source-cluster" {
				clusterName, clusterUID = source, state.SourceUID
			}
			rawState, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			svc := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: endpoint, Namespace: namespace, UID: "service-uid"}, Spec: corev1.ServiceSpec{Selector: map[string]string{
				cloudNativePGClusterLabel: state.TargetName, cloudNativePGInstanceRoleLabel: "primary",
			}}}
			if scenario == "wrong-service-selector" {
				svc.Spec.Selector[cloudNativePGClusterLabel] = source
			}
			ready, serving, terminating := true, true, false
			slice := discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Name: "postgres-stable-abc", Namespace: namespace, Labels: map[string]string{discoveryv1.LabelServiceName: endpoint},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: endpoint, UID: svc.UID}},
			}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{podIP}, Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &serving, Terminating: &terminating}, TargetRef: &corev1.ObjectReference{
				APIVersion: "v1", Kind: "Pod", Namespace: namespace, Name: podName, UID: podUID,
			}}}}
			slices := discoveryv1.EndpointSliceList{Items: []discoveryv1.EndpointSlice{slice}}
			switch scenario {
			case "wrong-endpoint-pod":
				slice.Endpoints[0].TargetRef.UID = "foreign-pod-uid"
			case "wrong-endpoint-ip":
				slice.Endpoints[0].Addresses[0] = "10.42.0.99"
			case "unready-endpoint":
				ready = false
			case "stale-endpoint-slice":
				slice.OwnerReferences[0].UID = "prior-service-uid"
			case "missing-endpoints":
				slices.Items = nil
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected mutation: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case coldStatePath(namespace, serviceID):
					if scenario == "missing-record" {
						http.NotFound(w, r)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "migration-uid", "resourceVersion": "7"}, "data": map[string]string{"state.json": string(rawState)}})
				case cloudNativePGClusterAPIPath(namespace, source):
					annotations := map[string]string{coldMigrationAnnotation: marker, "cnpg.io/fencedInstances": `["*"]`}
					uid := state.SourceUID
					switch scenario {
					case "completed-retained-source":
					case "unfenced-source":
						delete(annotations, "cnpg.io/fencedInstances")
					case "changed-source-uid":
						uid = "other-source-uid"
					default:
						http.NotFound(w, r)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": source, "namespace": namespace, "uid": uid, "annotations": annotations}})
				case "/api/v1/namespaces/" + namespace + "/services/" + endpoint:
					_ = json.NewEncoder(w).Encode(svc)
				case "/apis/discovery.k8s.io/v1/namespaces/" + namespace + "/endpointslices":
					if r.URL.Query().Get("labelSelector") != discoveryv1.LabelServiceName+"="+endpoint {
						t.Errorf("unexpected EndpointSlice selector: %s", r.URL.RawQuery)
					}
					if scenario == "forbidden-endpoints" {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					_ = json.NewEncoder(w).Encode(slices)
				default:
					t.Errorf("unexpected Kubernetes request: %s", r.URL.String())
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := &kubeClient{baseURL: server.URL, client: server.Client(), namespace: namespace}
			cluster := kubeCloudNativePGCluster{}
			cluster.Metadata.Name = clusterName
			cluster.Metadata.UID = clusterUID
			cluster.Metadata.Labels = map[string]string{runtime.FugueLabelBackingServiceID: serviceID}
			cluster.Metadata.Annotations = map[string]string{coldMigrationAnnotation: marker}
			cluster.Status.SystemID = systemID
			pod, err := json.Marshal(corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace, UID: types.UID(podUID)}, Status: corev1.PodStatus{PodIP: podIP}})
			if err != nil {
				t.Fatal(err)
			}
			postgres := model.AppPostgresSpec{ServiceName: target, EndpointServiceName: endpoint}
			err = client.verifyImageMigrationColdTarget(context.Background(), namespace, serviceID, postgres, cluster, pod)
			if wantError == "" {
				if err != nil {
					t.Fatalf("completed cold target should be accepted: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), wantError) {
				t.Fatalf("want rejection containing %q, got %v", wantError, err)
			}
		})
	}
}

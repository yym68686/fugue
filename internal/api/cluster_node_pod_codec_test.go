package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
)

func inventoryPodFixture() *corev1.PodList {
	always := corev1.ContainerRestartPolicyAlways
	containers := []corev1.Container{{
		Name: "workload",
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("1.375"),
			corev1.ResourceMemory:           resource.MustParse("1.5Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("5e8"),
		}},
	}}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "app-a", Labels: map[string]string{
			"app.kubernetes.io/managed-by": "fugue", "app.kubernetes.io/name": "app-a",
		}},
		Spec: corev1.PodSpec{NodeName: "node-a", Containers: containers, InitContainers: []corev1.Container{{
			Name: "sidecar", RestartPolicy: &always,
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("25m")}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	done := *pod.DeepCopy()
	done.Name, done.Status.Phase = "app-done", corev1.PodSucceeded
	pending := *pod.DeepCopy()
	pending.Name, pending.Spec.NodeName, pending.Status.Phase = "app-pending", "", corev1.PodPending
	return &corev1.PodList{TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"}, Items: []corev1.Pod{pod, done, pending}}
}

func inventoryPodEncodings(t testing.TB, pods *corev1.PodList) ([]byte, []byte) {
	t.Helper()
	text, err := json.Marshal(pods)
	if err != nil {
		t.Fatal(err)
	}
	var binary bytes.Buffer
	if err := clusterCoreProtobuf.Encode(pods, &binary); err != nil {
		t.Fatal(err)
	}
	return text, binary.Bytes()
}

func TestClusterPodInventoryNegotiationPreservesInventory(t *testing.T) {
	const negotiation = kuberuntime.ContentTypeProtobuf + "," + kuberuntime.ContentTypeJSON
	text, binary := inventoryPodEncodings(t, inventoryPodFixture())
	var want map[string][]clusterNodePod
	for _, scenario := range []string{"json", "protobuf", "not-acceptable", "forbidden", "not-found", "corrupt", "server-error"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				expectAccept := negotiation
				if call > 1 {
					expectAccept = kuberuntime.ContentTypeJSON
				}
				if r.Method != http.MethodGet || r.URL.RequestURI() != clusterPodInventoryPath || r.Header.Get("Authorization") != "Bearer fixture" || r.Header.Get("Accept") != expectAccept {
					t.Errorf("request did not preserve current LIST and negotiation")
				}
				switch scenario {
				case "protobuf", "corrupt":
					w.Header().Set("Content-Type", kuberuntime.ContentTypeProtobuf)
					if scenario == "corrupt" {
						w.Write(binary[:len(binary)-1])
					} else {
						w.Write(binary)
					}
					return
				case "not-acceptable":
					if call == 1 {
						w.WriteHeader(http.StatusNotAcceptable)
						return
					}
				case "forbidden":
					w.WriteHeader(http.StatusForbidden)
					return
				case "not-found":
					w.WriteHeader(http.StatusNotFound)
					return
				case "server-error":
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.Write(text)
			}))
			defer server.Close()
			client := clusterNodeClient{baseURL: server.URL, bearerToken: "fixture", client: server.Client()}
			got, available, err := client.listActivePodsByNode(context.Background())
			expectedCalls := int32(1)
			if scenario == "not-acceptable" {
				expectedCalls = 2
			}
			if calls.Load() != expectedCalls {
				t.Fatalf("calls=%d want %d", calls.Load(), expectedCalls)
			}
			switch scenario {
			case "forbidden", "not-found":
				if got != nil || available || err != nil {
					t.Fatalf("did not preserve label-scoped fallback: %v %v %v", got, available, err)
				}
			case "corrupt", "server-error":
				if got != nil || available || err == nil {
					t.Fatalf("failed inventory was accepted: %v %v %v", got, available, err)
				}
			default:
				if err != nil || !available || len(got) != 1 || len(got["node-a"]) != 1 {
					t.Fatalf("incomplete inventory: %v %v %v", got, available, err)
				}
				if scenario == "json" {
					want = got
				}
				if !reflect.DeepEqual(got, want) || clusterNodePodRequests(got["node-a"]).cpuMilliCores != 1400 ||
					len(managedPodsFromActiveSnapshot(got)["node-a"]) != 1 {
					t.Fatalf("transport changed resource or ownership projection: got %#v want %#v", got, want)
				}
			}
		})
	}
}

func TestClusterPodInventoryRejectsIncompleteOrWrongType(t *testing.T) {
	pods := inventoryPodFixture()
	pods.Continue = "next-page"
	text, binary := inventoryPodEncodings(t, pods)
	var out clusterNodePodList
	for media, raw := range map[string][]byte{kuberuntime.ContentTypeProtobuf: binary, kuberuntime.ContentTypeJSON: text} {
		if err := decodeClusterPodInventory(raw, media, &out); err == nil {
			t.Fatalf("%s accepted incomplete inventory", media)
		}
	}
	pods.Continue = ""
	pods.Kind = "ServiceList"
	_, binary = inventoryPodEncodings(t, pods)
	if err := decodeClusterPodInventory(binary, kuberuntime.ContentTypeProtobuf, &out); err == nil {
		t.Fatal("protobuf accepted a different list kind")
	}
}

func BenchmarkClusterPodInventoryDecode(b *testing.B) {
	list := inventoryPodFixture()
	pod := list.Items[0]
	pod.Annotations = map[string]string{"fixture": strings.Repeat(`{"env":{"text":"synthetic config"},"enabled":true}`, 550)}
	list.Items = make([]corev1.Pod, 350)
	for i := range list.Items {
		list.Items[i] = pod
	}
	text, binary := inventoryPodEncodings(b, list)
	for _, fixture := range []struct {
		name, contentType string
		wire              []byte
	}{{"json", kuberuntime.ContentTypeJSON, text}, {"protobuf", kuberuntime.ContentTypeProtobuf, binary}} {
		b.Run(fixture.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(fixture.wire)), "wire-B")
			for i := 0; i < b.N; i++ {
				var out clusterNodePodList
				if err := decodeClusterPodInventory(fixture.wire, fixture.contentType, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

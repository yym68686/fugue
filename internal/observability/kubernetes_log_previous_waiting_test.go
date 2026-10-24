package observability

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// The previous process can have unread failure logs while its replacement is
// waiting. Reading those logs must not depend on the replacement starting.
func TestKubernetesLogCollectionReadsPreviousWhileReplacementWaiting(t *testing.T) {
	for _, init := range []bool{false, true} {
		name := "regular"
		if init {
			name = "init"
		}
		t.Run(name, func(t *testing.T) {
			ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
			pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "retrying", Namespace: "ns", UID: "pod-uid"}}
			container := corev1.Container{Name: "app"}
			status := corev1.ContainerStatus{
				Name: "app", RestartCount: 2,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ContainerID: "containerd://previous", ExitCode: 1, FinishedAt: metav1.NewTime(ts.Add(time.Second)),
				}},
			}
			if init {
				pod.Spec.InitContainers = []corev1.Container{container}
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{status}
			} else {
				pod.Spec.Containers = []corev1.Container{container}
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{status}
			}
			var previousReads, currentReads atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/pods" {
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{pod}})
					return
				}
				if r.URL.Query().Get("previous") != "true" {
					currentReads.Add(1)
					http.Error(w, "replacement has not started", http.StatusBadRequest)
					return
				}
				previousReads.Add(1)
				fmt.Fprintf(w, "%s failure before restart\n", ts.Format(time.RFC3339Nano))
			}))
			defer server.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			p := NewPipeline(Config{Enabled: true, QueueSize: 8, KubernetesLogMaxLinesPerCycle: 8, KubernetesLogPollInterval: time.Second}, nil)
			c := newKubernetesLogCollectorWithClient(p, client)
			c.collectOnce(t.Context())
			if previousReads.Load() != 1 || currentReads.Load() != 0 || p.kubernetesLogLines.Load() != 1 {
				t.Fatalf("previous failure log was not collected independently: previous=%d current=%d lines=%d", previousReads.Load(), currentReads.Load(), p.kubernetesLogLines.Load())
			}
			if event := mustReadQueuedEvent(t, p); event.Message != "failure before restart" {
				t.Fatalf("unexpected previous log: %q", event.Message)
			}
			// A completed previous process is not repeatedly ingested while the
			// replacement remains waiting and has no current log stream.
			c.collectOnce(t.Context())
			if previousReads.Load() != 1 || currentReads.Load() != 0 || p.kubernetesLogLines.Load() != 1 {
				t.Fatal("completed previous process was replayed or waiting replacement was read")
			}
		})
	}
}

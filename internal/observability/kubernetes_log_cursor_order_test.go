package observability

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCursorBoundaryContainsOnlyItsTimestamp(t *testing.T) {
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	high := at.Add(300 * time.Nanosecond)
	low := at.Add(276 * time.Nanosecond)
	next := at.Add(time.Second)
	var calls, retainedReads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/proxy/logs/") {
			retainedReads.Add(1)
			http.Error(w, "unexpected recovery", http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "%s later-written-first\n", high.Format(time.RFC3339Nano))
		fmt.Fprintf(w, "%s earlier-written-last\n", low.Format(time.RFC3339Nano))
		if calls.Add(1) > 1 {
			fmt.Fprintf(w, "%s next\n%s next\n", next.Format(time.RFC3339Nano), next.Format(time.RFC3339Nano))
		}
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(Config{Enabled: true, QueueSize: 32, MemoryLimitBytes: 1 << 20, KubernetesLogTailLines: 10, KubernetesLogMaxLinesPerCycle: 10, KubernetesLogPollInterval: time.Second}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod-a", UID: types.UID("uid-a")},
		Spec:       corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "app"}}},
		Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "app", ContainerID: "containerd://container-a"}}},
	}, container: "app"}
	poll := func() {
		var budget atomic.Int64
		budget.Store(10)
		c.collectCursorTarget(context.Background(), target, &budget)
	}
	poll()
	poll()
	poll()
	got := rotationMessages(t, p)
	if !reflect.DeepEqual(got, []string{"later-written-first", "earlier-written-last", "next", "next"}) {
		t.Fatalf("changed source record multiplicity: %v", got)
	}
	if retainedReads.Load() != 0 || p.kubernetesLogCursorGaps.Load() != 0 || p.kubernetesLogRotationReplays.Load() != 0 {
		t.Fatalf("unordered records caused spurious recovery: retained=%d gaps=%d replays=%d", retainedReads.Load(), p.kubernetesLogCursorGaps.Load(), p.kubernetesLogRotationReplays.Load())
	}
}

package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func runningLogPod(name string) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec:   corev1.PodSpec{NodeName: "node", Containers: []corev1.Container{{Name: "app"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "app", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
}

func coverageCollector(t *testing.T, cfg Config, handler http.HandlerFunc) (*kubernetesLogCollector, *Pipeline) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL, QPS: 100000, Burst: 100000})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(cfg, nil)
	return newKubernetesLogCollectorWithClient(p, client), p
}

func TestUnstartedPodsDoNotConsumeBudgetAndEmptyDiscoveryResetsCoverage(t *testing.T) {
	pods := make([]corev1.Pod, 1200)
	for i := range pods {
		pods[i] = runningLogPod(fmt.Sprintf("a-pending-%04d", i))
		pods[i].Status = corev1.PodStatus{Phase: corev1.PodPending}
		if i%2 == 0 {
			pods[i].Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}}
		}
	}
	pods = append(pods, runningLogPod("z-running"))
	reads := 0
	c, p := coverageCollector(t, Config{Enabled: true, KubernetesLogMaxPods: 1}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(corev1.PodList{Items: pods})
			return
		}
		reads++
		if !strings.Contains(r.URL.Path, "/z-running/") {
			t.Errorf("read unstarted pod: %s", r.URL.Path)
		}
	})
	c.collectOnce(t.Context())
	if reads != 1 || p.kubernetesLogPods.Load() != 1201 || p.kubernetesLogUnstartedPods.Load() != 1200 || p.kubernetesLogEligibleTargets.Load() != 1 || p.kubernetesLogErrors.Load() != 0 {
		t.Fatalf("incorrect coverage: %s", p.PrometheusMetrics())
	}
	for _, sample := range []string{"log_pods 1201\n", "log_unstarted_pods 1200\n", "log_eligible_pods 1\n", "log_selected_targets 1\n", "log_pod_budget 1\n", "log_discovery_complete 1\n"} {
		if !strings.Contains(p.PrometheusMetrics(), sample) {
			t.Errorf("missing coverage metric %q", sample)
		}
	}
	pods = nil
	c.collectOnce(t.Context())
	if p.kubernetesLogPods.Load() != 0 || p.kubernetesLogEligibleTargets.Load() != 0 || p.kubernetesLogSelectedPods.Load() != 0 || p.kubernetesLogReads.Load() != 0 || p.kubernetesLogDeferredTargets.Load() != 0 {
		t.Fatal("empty inventory retained stale coverage")
	}
}

func TestPodBudgetRotatesAcrossFullPaginatedInventoryAndChurn(t *testing.T) {
	pods := make([]corev1.Pod, 1200)
	for i := range pods {
		pods[i] = runningLogPod(fmt.Sprintf("pod-%04d", i))
	}
	var mu sync.Mutex
	reads := map[string]int{}
	pages := 0
	c, p := coverageCollector(t, Config{Enabled: true, KubernetesLogMaxPods: 500, KubernetesLogPollInterval: 10 * time.Second}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			pages++
			if r.URL.Query().Get("limit") != "500" {
				t.Error("discovery must be paginated")
			}
			start := 0
			fmt.Sscanf(r.URL.Query().Get("continue"), "%d", &start)
			end := min(start+500, len(pods))
			list := corev1.PodList{Items: pods[start:end]}
			if end < len(pods) {
				list.Continue = fmt.Sprint(end)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(list)
			return
		}
		mu.Lock()
		reads[r.URL.Path]++
		mu.Unlock()
	})
	now := time.Now().UTC()
	c.clock = func() time.Time { return now }
	c.collectOnce(t.Context())
	if len(reads) != 500 || p.kubernetesLogDeferredTargets.Load() != 700 {
		t.Fatalf("incorrect first cycle reads=%d deferred=%d", len(reads), p.kubernetesLogDeferredTargets.Load())
	}
	// Remove the next source and insert earlier names. A numeric offset would
	// move backwards; stable identity must continue at the old successor.
	pods = append(append([]corev1.Pod{runningLogPod("a-new")}, pods[:500]...), pods[501:]...)
	now = now.Add(10 * time.Minute)
	c.collectOnce(t.Context())
	if reads["/api/v1/namespaces/ns/pods/pod-1000/log"] != 1 {
		t.Fatal("churn moved the resume point backwards")
	}
	key := logTargetKey(kubernetesLogTarget{pod: runningLogPod("pod-1000"), container: "app"})
	if !c.cursors[key].Time.Equal(now.Add(-15 * time.Minute)) {
		t.Fatal("deferred first turn reset the original lookback")
	}
	c.collectOnce(t.Context())
	if len(reads) != 1200 || pages != 9 || p.kubernetesLogErrors.Load() != 0 {
		t.Fatalf("full pool starved: read=%d pages=%d", len(reads), pages)
	}
	if p.kubernetesLogMaxWaitMillis.Load() <= 0 {
		t.Fatal("unread source wait was hidden")
	}
}

func TestPendingInitAndEphemeralCurrentAndPreviousSourcesRemainEligible(t *testing.T) {
	pod := runningLogPod("pending-init")
	pod.Status.Phase = corev1.PodPending
	pod.Status.ContainerStatuses = nil
	pod.Spec.InitContainers = []corev1.Container{{Name: "init"}}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "init", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
	pod.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug"}}}
	pod.Status.EphemeralContainerStatuses = []corev1.ContainerStatus{{Name: "debug", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}}, LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: "old-debug"}}}}
	c, _ := coverageCollector(t, Config{Enabled: true}, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{pod}})
	})
	targets, _, ok := c.kubernetesLogTargets(t.Context(), "")
	if !ok || len(targets) != 2 {
		t.Fatalf("lost Pending init/ephemeral history: %+v", targets)
	}
	if targets[0].container != "debug" || !targets[0].previous || targets[1].container != "init" || targets[1].previous {
		t.Fatal("incorrect current/previous sources")
	}
}

func TestCycleDeadlineBoundsSweepWithoutNodeFaultOrCursorLoss(t *testing.T) {
	pods := make([]corev1.Pod, 20)
	for i := range pods {
		pods[i] = runningLogPod(fmt.Sprintf("pod-%02d", i))
	}
	c, p := coverageCollector(t, Config{Enabled: true, KubernetesLogPollInterval: 100 * time.Millisecond}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(corev1.PodList{Items: pods})
			return
		}
		<-r.Context().Done()
	})
	start := time.Now()
	c.collectOnce(t.Context())
	if time.Since(start) > time.Second {
		t.Fatal("cycle deadline did not bound the whole sweep")
	}
	if p.kubernetesLogErrors.Load() != 0 || p.kubernetesLogBackoffNodes.Load() != 0 {
		t.Fatal("cycle expiration classified as node failure")
	}
	if p.kubernetesLogDeferredTargets.Load() < 12 {
		t.Fatal("deadline deferral was hidden")
	}
	for _, cur := range c.cursors {
		if cur.Complete || !cur.DrainedThrough.IsZero() {
			t.Fatal("canceled stream declared drained")
		}
	}
	// A canceled caller must not make discovery appear complete.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c.collectOnce(ctx)
	if p.kubernetesLogDiscoveryComplete.Load() != 0 {
		t.Fatal("failed discovery reported complete")
	}
}

func TestPodBudgetIncludesAllContainersAndCompletedSourcesStopConsumingTurns(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	pod := runningLogPod("a")
	pod.Spec.InitContainers = []corev1.Container{{Name: "init"}}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "init", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(now)}}}}
	pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{ContainerID: "previous", FinishedAt: metav1.NewTime(now)}
	var mu sync.Mutex
	reads := map[string]int{}
	c, p := coverageCollector(t, Config{Enabled: true, KubernetesLogMaxPods: 1}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{pod, runningLogPod("b")}})
			return
		}
		mu.Lock()
		reads[r.URL.Path+"?"+r.URL.RawQuery]++
		mu.Unlock()
	})
	c.collectOnce(t.Context())
	if len(reads) != 3 || p.kubernetesLogSelectedPods.Load() != 1 || p.kubernetesLogDeferredTargets.Load() != 1 {
		t.Fatalf("container sources incorrectly spent distinct pod budget: reads=%d", len(reads))
	}
	c.collectOnce(t.Context())
	if len(reads) != 4 || p.kubernetesLogEligibleTargets.Load() != 2 || p.kubernetesLogReads.Load() != 1 {
		t.Fatal("completed processes consumed turns or next pod starved")
	}
}

func TestSlowSourcesCannotKeepHealthySuccessorFromItsNextTurn(t *testing.T) {
	pods := make([]corev1.Pod, 8)
	for i := range pods {
		pods[i] = runningLogPod(fmt.Sprintf("a-slow-%02d", i))
	}
	pods = append(pods, runningLogPod("z-healthy"))
	c, p := coverageCollector(t, Config{Enabled: true, KubernetesLogPollInterval: 100 * time.Millisecond}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(corev1.PodList{Items: pods})
			return
		}
		if strings.Contains(r.URL.Path, "/z-healthy/") {
			fmt.Fprintf(w, "%s healthy\n", time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano))
			return
		}
		<-r.Context().Done()
	})
	c.collectOnce(t.Context())
	c.collectOnce(t.Context())
	if p.kubernetesLogLines.Load() != 1 || p.kubernetesLogErrors.Load() != 0 {
		t.Fatal("slow sources starved the healthy successor or manufactured a node fault")
	}
}

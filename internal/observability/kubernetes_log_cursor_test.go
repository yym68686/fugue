package observability

import (
	"context"
	"encoding/json"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCatchupUsesOnlyUnusedCycleBudgetAfterEverySourceGetsATurn(t *testing.T) {
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	var quietReads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			pods := []corev1.Pod{}
			for _, name := range []string{"busy", "quiet"} {
				pods = append(pods, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}}})
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(corev1.PodList{Items: pods})
			return
		}
		if strings.Contains(r.URL.Path, "/quiet/") {
			quietReads.Add(1)
			fmt.Fprintf(w, "%s quiet\n", ts.Format(time.RFC3339Nano))
			return
		}
		for i := 0; i < 6; i++ {
			fmt.Fprintf(w, "%s line-%d\n", ts.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano), i)
		}
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	p := NewPipeline(Config{Enabled: true, QueueSize: 32, BatchSize: 2, KubernetesLogMaxLinesPerCycle: 6, KubernetesLogTailLines: 2, KubernetesLogPollInterval: time.Second}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	c.collectOnce(t.Context())
	if p.kubernetesLogLines.Load() != 6 || quietReads.Load() != 1 || p.dropped.Load() != 0 || p.kubernetesLogDeferredTargets.Load() != 0 {
		t.Fatalf("catchup broke budgets/fairness: lines=%d quiet=%d dropped=%d deferred=%d", p.kubernetesLogLines.Load(), quietReads.Load(), p.dropped.Load(), p.kubernetesLogDeferredTargets.Load())
	}
	if got := p.sourceObservationCycle; got.CatchupReads != 2 || got.RemainingBudget != 0 || got.Visited != 2 {
		t.Fatalf("incorrect cycle evidence: %+v", got)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var remaining atomic.Int64
	remaining.Store(10)
	if c.catchupCursorTargets(ctx, nil, &remaining) != 0 || remaining.Load() != 10 {
		t.Fatal("catchup ignored cancellation")
	}
}

func TestCollectionResumesAtFirstUnscheduledSourceAndReturnsUnusedBudget(t *testing.T) {
	var p *Pipeline
	ts := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	opened := make(chan struct{}, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			pods := []corev1.Pod{}
			for _, name := range []string{"a", "b", "c"} {
				pods = append(pods, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}}})
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(corev1.PodList{Items: pods})
			return
		}
		opened <- struct{}{}
		if strings.Contains(r.URL.Path, "/pods/a/") {
			return
		}
		fmt.Fprintf(w, "%s one\n", ts)
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	p = NewPipeline(Config{Enabled: true, QueueSize: 32, KubernetesLogMaxLinesPerCycle: 1, KubernetesLogPollInterval: 5 * time.Second}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	c.collectOnce(t.Context())
	if len(opened) != 2 || p.kubernetesLogLines.Load() != 1 || c.targetOffset != 2 {
		t.Fatalf("in-flight blocked source did not get next turn: requests=%d lines=%d offset=%d", len(opened), p.kubernetesLogLines.Load(), c.targetOffset)
	}
}

func TestCollectionReservesBudgetForSlowerInFlightSource(t *testing.T) {
	var p *Pipeline
	ts := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"PodList","items":[{"metadata":{"name":"slow","namespace":"ns"},"spec":{"containers":[{"name":"app"}]}},{"metadata":{"name":"fast","namespace":"ns"},"spec":{"containers":[{"name":"app"}]}}]}`)
			return
		}
		if strings.Contains(r.URL.Path, "/slow/") {
			deadline := time.Now().Add(time.Second)
			for p.kubernetesLogLines.Load() < 2 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
		}
		for i := 0; i < 10; i++ {
			fmt.Fprintf(w, "%s line-%d\n", ts, i)
		}
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	p = NewPipeline(Config{Enabled: true, QueueSize: 32, KubernetesLogMaxLinesPerCycle: 4, KubernetesLogTailLines: 10}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	c.collectOnce(t.Context())
	v, _ := p.DiagnosticSources(t.Context())
	rows := v.(map[string]any)["sources"].([]logSourceObservation)
	if len(rows) != 2 || rows[0].Lines != 2 || rows[1].Lines != 2 || p.kubernetesLogLines.Load() != 4 || p.dropped.Load() != 0 {
		t.Fatalf("fast source consumed slower source budget: %+v", rows)
	}
}

func TestCursorReadsFrontOfBacklogAndEqualTimestampOccurrences(t *testing.T) {
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	line := ts.Format(time.RFC3339Nano) + " repeated\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("tailLines") || !r.URL.Query().Has("sinceTime") {
			t.Error("expected inclusive cursor, not newest tail", r.URL)
		}
		fmt.Fprint(w, strings.Repeat(line, 5))
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(Config{Enabled: true, QueueSize: 32, MemoryLimitBytes: 1 << 20, KubernetesLogTailLines: 2, KubernetesLogMaxLinesPerCycle: 2, KubernetesLogPollInterval: time.Second}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}}, container: "app"}
	for i := 0; i < 4; i++ {
		var b atomic.Int64
		b.Store(2)
		c.collectCursorTarget(t.Context(), target, &b)
	}
	if got := p.Snapshot().KubernetesLogLines; got != 5 {
		t.Fatalf("lost or duplicated equal-time lines: %d", got)
	}
}
func TestCursorDoesNotAdvanceOverQueueRejection(t *testing.T) {
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s one\n%s two\n", ts.Format(time.RFC3339Nano), ts.Add(time.Second).Format(time.RFC3339Nano))
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	p := NewPipeline(Config{Enabled: true, QueueSize: 1, MemoryLimitBytes: 1 << 20, KubernetesLogTailLines: 10, KubernetesLogPollInterval: time.Second}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}}, container: "app"}
	var budget atomic.Int64
	budget.Store(10)
	c.collectCursorTarget(t.Context(), target, &budget)
	if !c.cursors[logTargetKey(target)].Time.Equal(ts) {
		t.Fatal("cursor skipped rejected record")
	}
}

func TestCursorOpenFailurePreservesStartAndRecoveryEvidence(t *testing.T) {
	var unavailable atomic.Bool
	unavailable.Store(true)
	var requestedSince []string
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedSince = append(requestedSince, r.URL.Query().Get("sinceTime"))
		if unavailable.Load() {
			http.Error(w, "source unavailable", http.StatusBadGateway)
			return
		}
		fmt.Fprintf(w, "%s recovered\n", ts.Format(time.RFC3339Nano))
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(Config{Enabled: true, QueueSize: 32, KubernetesLogPollInterval: time.Second}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "sample", Name: "pod"}}, container: "app"}
	key := logTargetKey(target)
	var budget atomic.Int64
	budget.Store(10)
	c.collectCursorTarget(t.Context(), target, &budget)
	cur, exists := c.cursors[key]
	if !exists || cur.Time.IsZero() || !cur.PendingAt.Equal(cur.Time) || cur.NextAttempt.IsZero() || budget.Load() != 10 {
		t.Fatalf("failed first read lost its coverage boundary: exists=%v cursor=%+v budget=%d", exists, cur, budget.Load())
	}
	if c.catchupCursorTargets(t.Context(), []kubernetesLogTarget{target}, &budget) != 0 {
		t.Fatal("failed source consumed catch-up requests")
	}
	cur.NextAttempt = time.Time{}
	c.cursors[key] = cur
	unavailable.Store(false)
	c.collectCursorTarget(t.Context(), target, &budget)
	if len(requestedSince) != 2 || requestedSince[0] != requestedSince[1] || !c.cursors[key].PendingAt.IsZero() || p.kubernetesLogLines.Load() != 1 || p.kubernetesLogErrors.Load() != 1 {
		t.Fatalf("recovery skipped/replayed data or hid failure: since=%v cursor=%+v lines=%d errors=%d", requestedSince, c.cursors[key], p.kubernetesLogLines.Load(), p.kubernetesLogErrors.Load())
	}
}

func TestCursorOpenFailurePreservesExistingProgressAndPendingBoundary(t *testing.T) {
	for _, code := range []int{http.StatusBadGateway, http.StatusNotFound} {
		for _, pending := range []bool{false, true} {
			t.Run(fmt.Sprintf("status-%d-pending-%t", code, pending), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if code == http.StatusNotFound {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(code)
						json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Failure", Code: 404, Reason: metav1.StatusReasonNotFound, Message: `pods "pod" not found`})
						return
					}
					http.Error(w, http.StatusText(code), code)
				}))
				defer server.Close()
				client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
				if err != nil {
					t.Fatal(err)
				}
				p := NewPipeline(Config{Enabled: true}, nil)
				c := newKubernetesLogCollectorWithClient(p, client)
				target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "sample", Name: "pod"}}, container: "app"}
				key := logTargetKey(target)
				start := time.Now().UTC().Add(-time.Hour)
				cur := kubernetesLogCursor{Time: start, DrainedThrough: start.Add(59 * time.Minute)}
				if pending {
					cur.PendingAt = start.Add(58 * time.Minute)
				}
				c.cursors = map[string]kubernetesLogCursor{key: cur}
				var budget atomic.Int64
				budget.Store(10)
				c.collectCursorTarget(t.Context(), target, &budget)
				got := c.cursors[key]
				wantPending := cur.PendingAt
				if !pending && code == http.StatusBadGateway {
					wantPending = cur.DrainedThrough
				}
				if !got.Time.Equal(cur.Time) || !got.DrainedThrough.Equal(cur.DrainedThrough) || !got.PendingAt.Equal(wantPending) || budget.Load() != 10 {
					t.Fatalf("failure changed read boundary: before=%+v after=%+v", cur, got)
				}
			})
		}
	}
}

func TestCursorDoesNotRepollFinishedContainersOrCountOldUnavailableLogs(t *testing.T) {
	var calls atomic.Int32
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprintf(w, "%s final message\n", ts.Format(time.RFC3339Nano))
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	p := NewPipeline(Config{Enabled: true, QueueSize: 32}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "app", ContainerID: "instance", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(ts)}}}}}}, container: "app"}
	var budget atomic.Int64
	budget.Store(20)
	c.collectCursorTarget(t.Context(), target, &budget)
	c.collectCursorTarget(t.Context(), target, &budget)
	if calls.Load() != 1 || p.Snapshot().KubernetesLogLines != 1 {
		t.Fatal("drained terminated container was reread")
	}
	target.pod.Status.ContainerStatuses[0].ContainerID = "old-instance"
	target.pod.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.NewTime(ts.Add(-time.Hour))
	c.collectCursorTarget(t.Context(), target, &budget)
	if calls.Load() != 1 {
		t.Fatal("queried terminated instance outside bootstrap window")
	}
}

func TestCursorUnavailableLogBodyIsCountedOnceAndBacksOff(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, "unable to retrieve container logs for containerd://expired")
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	p := NewPipeline(Config{Enabled: true}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}}, container: "app"}
	var budget atomic.Int64
	budget.Store(20)
	c.collectCursorTarget(t.Context(), target, &budget)
	c.collectCursorTarget(t.Context(), target, &budget)
	if calls.Load() != 1 || p.kubernetesLogCursorGaps.Load() != 1 || p.kubernetesLogLines.Load() != 0 {
		t.Fatal("unavailable body replayed or ingested")
	}
}

func TestCollectionBackpressureDefersReadsUntilQueueDrains(t *testing.T) {
	var logReads atomic.Int32
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"PodList","items":[{"metadata":{"namespace":"ns","name":"pod"},"spec":{"containers":[{"name":"app"}]}}]}`)
			return
		}
		logReads.Add(1)
		for i := 0; i < 4; i++ {
			fmt.Fprintf(w, "%s pending-%d\n", ts.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano), i)
		}
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	p := NewPipeline(Config{Enabled: true, QueueSize: 8, BatchSize: 2}, nil)
	for i := 0; i < 7; i++ {
		if !p.IngestLogLine(t.Context(), "test", "queued") {
			t.Fatal("fixture queue admission failed")
		}
	}
	c := newKubernetesLogCollectorWithClient(p, client)
	c.collectOnce(t.Context())
	if logReads.Load() != 0 || p.dropped.Load() != 0 {
		t.Fatal("full queue caused speculative reads/drops")
	}
	for len(p.queue) > 0 {
		q := <-p.queue
		p.ordinaryQueuedSlots.Add(-1)
		p.queueDepth.Add(-1)
		p.queuedBytes.Add(-int64(len(q.payload)))
	}
	c.collectOnce(t.Context())
	if logReads.Load() != 1 || p.kubernetesLogLines.Load() != 4 || p.dropped.Load() != 0 {
		t.Fatal("deferred source did not resume without loss")
	}
}

func TestBacklogUsesUnreadRecordTimeInsteadOfIdleCursor(t *testing.T) {
	var budget atomic.Int64
	budget.Store(1)
	ts := time.Now().UTC().Add(-time.Second).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A concurrent reader spends the shared budget after this source opens.
		budget.Store(0)
		fmt.Fprintf(w, "%s newly-arrived\n", ts.Format(time.RFC3339Nano))
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	p := NewPipeline(Config{Enabled: true}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "idle"}}, container: "app"}
	old := ts.Add(-time.Hour)
	c.cursors = map[string]kubernetesLogCursor{logTargetKey(target): {Time: old, Boundary: map[[32]byte]int{}}}
	c.collectCursorTarget(t.Context(), target, &budget)
	cur := c.cursors[logTargetKey(target)]
	if !cur.Time.Equal(old) || !cur.PendingAt.Equal(ts) {
		t.Fatalf("idle period reported as backlog or cursor advanced: %+v", cur)
	}
}

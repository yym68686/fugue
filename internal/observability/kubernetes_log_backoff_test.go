package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestOfflineNodeBackoffBoundsAllSourcesAndPreservesRecoveryCursors(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var unavailable atomic.Bool
	unavailable.Store(true)
	var offlineReads, healthyReads atomic.Int64
	var sinceMu sync.Mutex
	recoveredSince := map[string]string{}
	pods := []corev1.Pod{}
	for i := 0; i < 25; i++ {
		node, name := "offline", fmt.Sprintf("source-%02d", i)
		if i == 24 {
			node, name = "healthy", "healthy"
		}
		pods = append(pods, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}, Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "app"}}}})
		pods[len(pods)-1].Status = runningLogPod(name).Status
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(corev1.PodList{Items: pods})
			return
		}
		if strings.Contains(r.URL.Path, "/healthy/") {
			healthyReads.Add(1)
			return
		}
		offlineReads.Add(1)
		if unavailable.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Failure", Reason: metav1.StatusReasonInternalError, Code: 500, Message: "error dialing backend: proxy connection unavailable"})
			return
		}
		sinceMu.Lock()
		if _, exists := recoveredSince[r.URL.Path]; !exists {
			recoveredSince[r.URL.Path] = r.URL.Query().Get("sinceTime")
		}
		sinceMu.Unlock()
		fmt.Fprintf(w, "%s retained-record\n", now.Add(-time.Minute).Format(time.RFC3339Nano))
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL, QPS: 10000, Burst: 10000})
	p := NewPipeline(Config{Enabled: true, QueueSize: 1024, BatchSize: 32, KubernetesLogMaxLinesPerCycle: 512}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	c.clock = func() time.Time { return now }
	c.collectOnce(t.Context())
	first := offlineReads.Load()
	if first < 1 || first > 8 || healthyReads.Load() != 1 {
		t.Fatalf("unbounded initial burst or starved healthy node: offline=%d healthy=%d", first, healthyReads.Load())
	}
	cursors := map[string]time.Time{}
	for key, cur := range c.cursors {
		if !strings.Contains(key, "/healthy/") {
			cursors[key] = cur.Time
		}
	}
	for i := 0; i < 10; i++ {
		c.collectOnce(t.Context())
	}
	if offlineReads.Load() != first || healthyReads.Load() != 11 || p.kubernetesLogBackoffNodes.Load() != 1 {
		t.Fatalf("offline siblings bypassed node circuit: offline=%d healthy=%d", offlineReads.Load(), healthyReads.Load())
	}
	now = now.Add(6 * time.Minute)
	c.collectOnce(t.Context())
	if offlineReads.Load() != first+1 {
		t.Fatalf("half-open probe multiplied by pod count: %d", offlineReads.Load()-first)
	}
	for key, want := range cursors {
		if !c.cursors[key].Time.Equal(want) {
			t.Fatal("backoff advanced an unacknowledged cursor")
		}
	}
	unavailable.Store(false)
	now = now.Add(6 * time.Minute)
	c.collectOnce(t.Context())
	c.collectOnce(t.Context())
	if p.kubernetesLogBackoffNodes.Load() != 0 || len(recoveredSince) != 24 || p.kubernetesLogLines.Load() != 24 {
		t.Fatalf("recovery did not drain all preserved sources: reads=%d lines=%d", len(recoveredSince), p.kubernetesLogLines.Load())
	}
	for _, since := range recoveredSince {
		got, err := time.Parse(time.RFC3339Nano, since)
		if err != nil || !got.Equal(now.Add(-17*time.Minute)) {
			t.Fatalf("recovery skipped outage: %q", since)
		}
	}
}

func TestLogNodeProbeFencingAndSourceErrors(t *testing.T) {
	p := NewPipeline(Config{Enabled: true}, nil)
	c := newKubernetesLogCollectorWithClient(p, nil)
	now := time.Now().UTC().Truncate(time.Second)
	first, _, _ := c.beginLogNodeRead("node", now)
	late, _, _ := c.beginLogNodeRead("node", now)
	c.finishLogNodeRead(first, apierrors.NewServiceUnavailable("backend unavailable"), now, nil)
	c.finishLogNodeRead(late, nil, now, nil)
	if _, _, ok := c.beginLogNodeRead("node", now); ok {
		t.Fatal("late success reset failed node")
	}
	for n := 1; n <= 12; n++ {
		next := c.nodeRetries["node"].NextAttempt
		probe, _, ok := c.beginLogNodeRead("node", next)
		if !ok {
			t.Fatal("probe never admitted")
		}
		if _, _, ok := c.beginLogNodeRead("node", next); ok {
			t.Fatal("multiple half-open probes")
		}
		c.finishLogNodeRead(probe, apierrors.NewServiceUnavailable("backend unavailable"), next, nil)
		delay := c.nodeRetries["node"].NextAttempt.Sub(next)
		if delay <= 0 || delay > 5*time.Minute {
			t.Fatalf("unbounded retry delay: %s", delay)
		}
	}
	next := c.nodeRetries["node"].NextAttempt
	probe, _, _ := c.beginLogNodeRead("node", next)
	c.finishLogNodeRead(probe, apierrors.NewBadRequest("container has no log"), next, nil)
	if _, _, ok := c.beginLogNodeRead("node", next); !ok {
		t.Fatal("container error quarantined reachable node")
	}
	for _, err := range []error{apierrors.NewBadRequest("container"), apierrors.NewInternalError(fmt.Errorf("container log decoder failed")), apierrors.NewTooManyRequests("busy", 10), context.Canceled} {
		if logNodeUnavailable(err) {
			t.Fatalf("source or cancellation classified as node fault: %v", err)
		}
	}
}

func TestLogNodeBackoffRetainsOnlyTriggerForActiveEpoch(t *testing.T) {
	c := newKubernetesLogCollectorWithClient(NewPipeline(Config{Enabled: true}, nil), nil)
	now := time.Now().UTC()
	first, _, _ := c.beginLogNodeRead("node", now)
	late, _, _ := c.beginLogNodeRead("node", now)
	trigger := func(class string) func() logNodeBackoffObservation {
		return func() logNodeBackoffObservation { return logNodeBackoffObservation{ErrorClass: class} }
	}
	unused := func() logNodeBackoffObservation {
		t.Helper()
		t.Fatal("a stale, canceled or source-local attempt replaced the trigger")
		return logNodeBackoffObservation{}
	}
	c.finishLogNodeRead(first, apierrors.NewNotFound(corev1.Resource("pods"), "source"), now, unused)
	c.finishLogNodeRead(first, apierrors.NewServiceUnavailable("backend"), now, trigger("api_unavailable"))
	c.finishLogNodeRead(late, context.DeadlineExceeded, now, unused)
	skip, next, ok := c.beginLogNodeRead("node", now)
	if ok || skip.Backoff.Epoch != 1 || skip.Backoff.ErrorClass != "api_unavailable" || skip.Backoff.TriggeredAt != now || skip.Backoff.HalfOpenProbe {
		t.Fatalf("missing original cause: %+v", skip)
	}
	probe, _, _ := c.beginLogNodeRead("node", next)
	skip, _, ok = c.beginLogNodeRead("node", next)
	if ok || !skip.Backoff.HalfOpenProbe || skip.Backoff.Epoch != 1 {
		t.Fatalf("in-flight half-open probe misreported: %+v", skip)
	}
	c.finishLogNodeRead(probe, context.Canceled, next, unused)
	probe, _, _ = c.beginLogNodeRead("node", next)
	c.finishLogNodeRead(probe, context.DeadlineExceeded, next, trigger("deadline"))
	skip, nextProbe, ok := c.beginLogNodeRead("node", next)
	if ok || skip.Backoff.Epoch != 2 || skip.Backoff.ErrorClass != "deadline" || skip.Backoff.TriggeredAt != next || skip.Backoff.HalfOpenProbe {
		t.Fatalf("active epoch not updated: %+v", skip)
	}
	probe, _, _ = c.beginLogNodeRead("node", nextProbe)
	c.finishLogNodeRead(probe, nil, nextProbe, unused)
	recovered, _, ok := c.beginLogNodeRead("node", nextProbe)
	if !ok || recovered.Backoff.Epoch != 0 || c.nodeRetries["node"].Trigger.Epoch != 0 {
		t.Fatalf("recovered node kept a stale trigger: %+v", recovered)
	}
}

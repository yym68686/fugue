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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSourceObservationIsBoundedAndKeepsPendingSourcesFirst(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	for i := 0; i < 2100; i++ {
		p.observeLogSource(logSourceObservation{Identity: fmt.Sprintf("source-%04d", i), ObservedAt: time.Now().UTC(), Lines: 2, Outcome: "drained"})
	}
	p.observeLogSource(logSourceObservation{Identity: "pending", ObservedAt: time.Now().UTC(), PendingAt: time.Now().Add(-time.Minute), Outcome: "cycle_budget"})
	v, err := p.DiagnosticSources(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := v.(map[string]any)
	rows := result["sources"].([]logSourceObservation)
	if result["source_count"] != 2048 || result["evicted_sources"] != uint64(53) || result["truncated"] != true || len(rows) != 512 || rows[0].Identity != "pending" {
		t.Fatalf("unbounded snapshot: count=%v evicted=%v rows=%d", result["source_count"], result["evicted_sources"], len(rows))
	}
}

func TestSourceObservationCountersAndConcurrentProjection(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: time.Now(), Outcome: "open_error", Lines: 2})
				p.DiagnosticSources(context.Background())
			}
		}()
	}
	wg.Wait()
	v, err := p.DiagnosticSources(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	row := v.(map[string]any)["sources"].([]logSourceObservation)[0]
	if row.Attempts != 400 || row.Errors != 400 || row.TotalLines != 800 {
		t.Fatalf("lost observations: %+v", row)
	}
}

func TestCursorObservationCapturesBudgetReasonWithoutLogPayload(t *testing.T) {
	ts := time.Now().UTC().Add(-time.Minute)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s private-message\n%s later\n", ts.Format(time.RFC3339Nano), ts.Add(time.Second).Format(time.RFC3339Nano))
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(Config{Enabled: true, QueueSize: 32, KubernetesLogTailLines: 1}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}}, container: "app"}
	var b atomic.Int64
	b.Store(5)
	c.collectCursorTarget(t.Context(), target, &b)
	v, err := p.DiagnosticSources(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(v)
	if strings.Contains(string(raw), "private-message") {
		t.Fatal("log payload leaked through metadata observation")
	}
	rows := v.(map[string]any)["sources"].([]logSourceObservation)
	if len(rows) != 1 || rows[0].Outcome != "source_budget" || rows[0].Lines != 1 || rows[0].PendingAt.IsZero() {
		t.Fatalf("missing collection reason: %+v", rows)
	}
}

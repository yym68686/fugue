package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

func TestSourceRecoveryRetainsLastErrorEvidence(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	failedAt := time.Now().UTC().Add(-time.Minute)
	p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: failedAt, Outcome: "open_error", ErrorClass: "deadline", TotalMillis: 15000})
	p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: time.Now().UTC(), Outcome: "drained", Lines: 1})
	v, err := p.DiagnosticSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	row := v.(map[string]any)["sources"].([]logSourceObservation)[0]
	if row.Outcome != "drained" || row.ErrorClass != "" || row.Errors != 1 || !row.LastErrorAt.Equal(failedAt) || row.LastErrorStage != "open_error" || row.LastErrorClass != "deadline" || row.LastErrorMillis != 15000 {
		t.Fatalf("recovery erased or misrepresented previous failure: %+v", row)
	}
}

func TestSourceErrorSummarySurvivesDetailTruncation(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	for i := 0; i < 700; i++ {
		p.observeLogSource(logSourceObservation{Identity: fmt.Sprintf("source-%d", i), Node: "node-a", ObservedAt: time.Now(), Outcome: "open_error", ErrorClass: "deadline", TotalMillis: int64(i)})
	}
	p.observeLogSource(logSourceObservation{Identity: "other", Node: "node-b", ObservedAt: time.Now(), Outcome: "scan_error", ErrorClass: "connection", TotalMillis: 9})
	v, err := p.DiagnosticSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	result := v.(map[string]any)
	if result["truncated"] != true {
		t.Fatal("detail truncation was not reported")
	}
	summaries := result["error_summary"].([]sourceErrorSummary)
	if len(summaries) != 2 || summaries[0].Node != "node-a" || summaries[0].Sources != 700 || summaries[0].Errors != 700 || summaries[1].Node != "node-b" {
		t.Fatalf("error summary lost sources outside detail window: %+v", summaries)
	}
}

func TestSourceRecoveryRetainsGapAndHTTPStatusWithoutCountingBackoffAsRead(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	now := time.Now().UTC()
	lower, upper := now.Add(-time.Hour), now.Add(-time.Minute)
	p.observeLogSource(logSourceObservation{Identity: "source", Node: "node-a", ObservedAt: now, Outcome: "open_error", ErrorClass: "forbidden", HTTPStatus: 403,
		GapEvents: 1, GapReason: "retention_window", GapLowerBound: lower, GapUpperBound: upper})
	p.observeLogSource(logSourceObservation{Identity: "source", Node: "node-a", ObservedAt: now, Outcome: "node_backoff"})
	p.observeLogSource(logSourceObservation{Identity: "source", Node: "node-a", ObservedAt: now, Outcome: "drained"})
	value, _ := p.DiagnosticSources(t.Context())
	row := value.(map[string]any)["sources"].([]logSourceObservation)[0]
	if row.Attempts != 2 || row.Errors != 1 || row.HTTPStatus != 0 || row.LastHTTPStatus != 403 || row.GapEvents != 1 || row.GapReason != "retention_window" || !row.GapLowerBound.Equal(lower) || !row.GapUpperBound.Equal(upper) {
		t.Fatalf("recovery/backoff lost evidence or counted synthetic reads: %+v", row)
	}
	p.observeLogSource(logSourceObservation{Identity: "source", Node: "node-a", ObservedAt: now, Outcome: "rotation_gap", GapEvents: 1, GapReason: "rotation_boundary_missing", GapLowerBound: upper, GapUpperBound: now})
	row = p.sourceObservations["source"]
	if row.GapEvents != 2 || row.GapReason != "rotation_boundary_missing" || !row.GapLowerBound.Equal(upper) || !row.GapUpperBound.Equal(now) {
		t.Fatalf("new gap did not accumulate and replace latest bounds: %+v", row)
	}
}

func TestLogReadErrorMetadataIsTypedAndDoesNotDependOnBody(t *testing.T) {
	for _, tc := range []struct {
		err    error
		class  string
		status int
	}{
		{apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "synthetic"), "not_found", 404},
		{apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "synthetic", errors.New("private-response-body")), "forbidden", 403},
		{apierrors.NewTooManyRequests("private-response-body", 1), "rate_limited", 429},
		{context.DeadlineExceeded, "deadline", 0},
		{io.ErrUnexpectedEOF, "unexpected_eof", 0},
	} {
		err := fmt.Errorf("wrapped: %w", tc.err)
		if logReadErrorClass(err) != tc.class || logReadHTTPStatus(err) != tc.status {
			t.Fatalf("lost typed read failure: %s %d", logReadErrorClass(err), logReadHTTPStatus(err))
		}
	}
}

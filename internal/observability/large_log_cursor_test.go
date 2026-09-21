package observability

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestCursorReadsLargeLineIndependentlyOfHTTPBodyLimit(t *testing.T) {
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	line := `{"msg":"large structured record","detail":"` + strings.Repeat("x", 1500000) + `"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s %s\n%s following\n", ts.Format(time.RFC3339Nano), line, ts.Add(time.Second).Format(time.RFC3339Nano))
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "sample", Name: "sample"}}, container: "app"}
	for _, limit := range []int{2 << 20, 1 << 20} {
		p := NewPipeline(Config{Enabled: true, QueueSize: 32, MaxPayloadBytes: 1024, KubernetesLogMaxLineBytes: limit}, nil)
		c := newKubernetesLogCollectorWithClient(p, client)
		var budget atomic.Int64
		budget.Store(10)
		c.collectCursorTarget(t.Context(), target, &budget)
		v, _ := p.DiagnosticSources(t.Context())
		row := v.(map[string]any)["sources"].([]logSourceObservation)[0]
		if row.LineLimitBytes != limit {
			t.Fatal("missing source bound")
		}
		if limit > len(line) {
			if row.Outcome != "drained" || row.Lines != 2 || row.MaxLineBytes < len(line) || p.kubernetesLogErrors.Load() != 0 {
				t.Fatalf("large record failed: %+v", row)
			}
			c.collectCursorTarget(t.Context(), target, &budget)
			if p.kubernetesLogLines.Load() != 2 {
				t.Fatal("large record replayed")
			}
		} else if row.Outcome != "scan_error" || row.ErrorClass != "line_too_long" || row.Lines != 0 || row.PendingAt.IsZero() || !row.CursorBefore.Equal(row.CursorAfter) {
			t.Fatalf("oversize record skipped or hidden: %+v", row)
		}
	}
}

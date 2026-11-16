package observability

import (
	"encoding/json"
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

func TestInvalidLogRecordKeepsActualReadWithoutPayloadOrCursorChanges(t *testing.T) {
	for _, trial := range []struct{ line, class string }{
		{"unable to retrieve container logs for containerd://private-runtime", "runtime_log_unavailable_message"},
		{"private-record", "timestamp_missing"},
		{"private-time private-payload", "timestamp_invalid"},
		{"0001-01-01T00:00:00Z private-payload", "timestamp_zero"},
		{"", "timestamp_missing"},
	} {
		t.Run(trial.class+fmt.Sprint(len(trial.line)), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, trial.line)
			}))
			defer server.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			p := NewPipeline(Config{Enabled: true, QueueSize: 32}, nil)
			c := newKubernetesLogCollectorWithClient(p, client)
			target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}}, container: "app"}
			key := logTargetKey(target)
			before := time.Now().UTC().Add(-time.Minute)
			c.cursors = map[string]kubernetesLogCursor{key: {Time: before}}
			var budget atomic.Int64
			budget.Store(5)
			c.collectCursorTarget(t.Context(), target, &budget)
			value, err := p.DiagnosticSources(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			result := value.(map[string]any)
			row := result["sources"].([]logSourceObservation)[0]
			fact := row.InvalidRecord
			if fact == nil || fact.Class != trial.class || fact.RecordBytes != len(trial.line) || fact.ObservedAt.IsZero() || fact.Read.Phase != "pod_log" || !fact.Read.StreamOpened || !fact.Read.RequestWritten || !fact.Read.FirstResponseByte || fact.Read.BodyBytes == 0 {
				t.Fatalf("missing actual rejection/read milestones: %+v", fact)
			}
			if !c.cursors[key].Time.Equal(before) || !c.cursors[key].SourceUnavailable || row.GapEvents != 1 || budget.Load() != 5 || p.kubernetesLogLines.Load() != 0 || row.Errors != 0 {
				t.Fatal("diagnostics changed cursor, queue, gap or error behavior")
			}
			p.observeLogSource(logSourceObservation{Identity: key, Outcome: "drained"})
			fact.Class = "changed"
			history := result["gap_history"].(map[string]any)["receipts"].([]logGapObservation)
			if len(history) != 1 || history[0].InvalidRecord == nil || history[0].InvalidRecord.Class != trial.class {
				t.Fatal("gap lost rejection evidence or aliases the source projection")
			}
			history[0].InvalidRecord.Read.Phase = "changed"
			value, _ = p.DiagnosticSources(t.Context())
			history = value.(map[string]any)["gap_history"].(map[string]any)["receipts"].([]logGapObservation)
			if history[0].InvalidRecord.Read.Phase != "pod_log" {
				t.Fatal("returned history aliases retained diagnostic state")
			}
			raw, _ := json.Marshal(value)
			if strings.Contains(string(raw), "private-") || strings.Contains(string(raw), "containerd://") {
				t.Fatal("invalid record payload escaped into diagnostic state")
			}
		})
	}
}

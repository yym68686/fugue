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
	"k8s.io/apimachinery/pkg/types"
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

func TestSourceObservationProjectsSelectedInventoryLifecycle(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	now := time.Now().UTC()
	current := kubernetesLogTarget{pod: runningLogPod("current"), container: "app"}
	retired := kubernetesLogTarget{pod: runningLogPod("retired"), container: "app"}
	p.observeLogSource(logSourceObservation{Identity: logTargetKey(current), Node: "node-a", ObservedAt: now, Outcome: "drained"})
	p.observeLogSource(logSourceObservation{Identity: logTargetKey(retired), Node: "node-b", ObservedAt: now, Outcome: "open_error", ErrorClass: "not_found"})
	value, _ := p.DiagnosticSources(t.Context())
	if value.(map[string]any)["selected_inventory"] != nil || value.(map[string]any)["sources"].([]logSourceObservation)[0].SelectedInventoryState != "" {
		t.Fatal("invented membership before successful discovery")
	}
	c := newKubernetesLogCollectorWithClient(p, nil)
	c.clock = func() time.Time { return now }
	c.observeSelectedLogInventory([]kubernetesLogTarget{current})
	// A late read result preserves absent membership and its failure evidence.
	p.observeLogSource(logSourceObservation{Identity: logTargetKey(retired), ObservedAt: now, Outcome: "drained"})

	value, err := p.DiagnosticSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	result := value.(map[string]any)
	rows := result["sources"].([]logSourceObservation)
	states := map[string]string{}
	for _, row := range rows {
		states[row.Identity] = row.SelectedInventoryState
		if row.Identity == logTargetKey(retired) && (row.Errors != 1 || row.LastErrorClass != "not_found") {
			t.Fatal("inventory erased retained failure evidence")
		}
	}
	if states[logTargetKey(current)] != "present" || states[logTargetKey(retired)] != "absent" {
		t.Fatalf("inventory lifecycle missing: %+v", states)
	}
	inventory := result["selected_inventory"].(map[string]any)
	if inventory["targets"] != 1 || inventory["tracked_sources"] != 2 || inventory["tracked_present"] != 1 || inventory["tracked_absent"] != 1 || inventory["tracked_unknown"] != 0 || !inventory["observed_at"].(time.Time).Equal(now) {
		t.Fatalf("selected inventory summary inconsistent: %+v", inventory)
	}
}

func TestSelectedInventoryUsesPodAndContainerIdentityAndDoesNotGuessNewMembership(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	c := newKubernetesLogCollectorWithClient(p, nil)
	pod := runningLogPod("shared-name")
	pod.UID = types.UID("pod-current")
	pod.Status.ContainerStatuses[0].ContainerID = "containerd://current"
	pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{ContainerID: "containerd://previous"}
	current := kubernetesLogTarget{pod: pod, container: "app"}
	previous := kubernetesLogTarget{pod: pod, container: "app", previous: true}
	oldPod := current
	oldPod.pod = *pod.DeepCopy()
	oldPod.pod.UID = types.UID("pod-retired")
	oldContainer := current
	oldContainer.pod = *pod.DeepCopy()
	oldContainer.pod.Status.ContainerStatuses[0].ContainerID = "containerd://retired"
	for _, target := range []kubernetesLogTarget{oldPod, oldContainer, previous} {
		p.observeLogSource(logSourceObservation{Identity: logTargetKey(target), ObservedAt: time.Now()})
	}
	c.observeSelectedLogInventory([]kubernetesLogTarget{current, previous})
	p.observeLogSource(logSourceObservation{Identity: logTargetKey(current), ObservedAt: time.Now(), SelectedInventoryState: "present"})
	value, _ := p.DiagnosticSources(t.Context())
	states := map[string]string{}
	for _, row := range value.(map[string]any)["sources"].([]logSourceObservation) {
		states[row.Identity] = row.SelectedInventoryState
	}
	if states[logTargetKey(current)] != "unknown" || states[logTargetKey(previous)] != "present" || states[logTargetKey(oldPod)] != "absent" || states[logTargetKey(oldContainer)] != "absent" {
		t.Fatal("membership confused replacements, previous logs, or new observations", states)
	}
	c.observeSelectedLogInventory([]kubernetesLogTarget{current, previous})
	if p.sourceObservations[logTargetKey(current)].SelectedInventoryState != "present" {
		t.Fatal("next successful inventory did not resolve unknown membership")
	}
}

func TestSelectedInventorySummaryCoversBoundedRetentionNotOnlyDetailRows(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	c := newKubernetesLogCollectorWithClient(p, nil)
	targets := make([]kubernetesLogTarget, 2200)
	for i := range targets {
		targets[i] = kubernetesLogTarget{pod: runningLogPod(fmt.Sprintf("pod-%04d", i)), container: "app"}
		p.observeLogSource(logSourceObservation{Identity: logTargetKey(targets[i]), ObservedAt: time.Unix(int64(i), 0)})
	}
	c.observeSelectedLogInventory(targets[:2100])
	p.observeLogSource(logSourceObservation{Identity: "new-after-discovery", ObservedAt: time.Now()})
	value, _ := p.DiagnosticSources(t.Context())
	result := value.(map[string]any)
	inventory := result["selected_inventory"].(map[string]any)
	if len(p.sourceObservations) != 2048 || result["source_count"] != 2048 || len(result["sources"].([]logSourceObservation)) != 512 || result["truncated"] != true || inventory["targets"] != 2100 || inventory["tracked_sources"] != 2048 || inventory["tracked_present"] != 1947 || inventory["tracked_absent"] != 100 || inventory["tracked_unknown"] != 1 {
		t.Fatal("inventory retention or full-set summary is incorrect", inventory)
	}
}

func TestSelectedInventoryIgnoresFailedPartialDiscoveryAndRetainsCompletedSources(t *testing.T) {
	var phase atomic.Int32
	var unexpectedReads atomic.Int32
	pod := runningLogPod("selected")
	pod.Status.ContainerStatuses[0].ContainerID = "containerd://current"
	pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{ContainerID: "containerd://previous"}
	other := runningLogPod("not-selected")
	other.Namespace = "excluded"
	c, p := coverageCollector(t, Config{KubernetesLogNamespaces: []string{"ns"}, KubernetesLogLabelSelector: "app=example"}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/pods" {
			unexpectedReads.Add(1)
			http.Error(w, "unexpected log read", http.StatusInternalServerError)
			return
		}
		if r.URL.Query().Get("labelSelector") != "app=example" {
			t.Error("discovery changed the label selector")
		}
		w.Header().Set("Content-Type", "application/json")
		switch phase.Load() {
		case 0:
			json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{pod, other}})
		case 1:
			if r.URL.Query().Get("continue") == "" {
				json.NewEncoder(w).Encode(corev1.PodList{ListMeta: metav1.ListMeta{Continue: "page-two"}, Items: []corev1.Pod{other}})
			} else {
				http.Error(w, "inventory unavailable", http.StatusForbidden)
			}
		default:
			json.NewEncoder(w).Encode(corev1.PodList{})
		}
	})
	now := time.Now().UTC()
	c.clock = func() time.Time { return now }
	c.cursors = map[string]kubernetesLogCursor{}
	for _, previous := range []bool{false, true} {
		target := kubernetesLogTarget{pod: pod, container: "app", previous: previous}
		key := logTargetKey(target)
		p.observeLogSource(logSourceObservation{Identity: key, ObservedAt: now, Outcome: "drained"})
		c.cursors[key] = kubernetesLogCursor{Time: now, Visited: now, Complete: true}
	}
	c.collectOnce(t.Context())
	if p.sourceInventoryTargetCount != 2 || !p.sourceInventoryObservedAt.Equal(now) || p.kubernetesLogDiscoveryComplete.Load() != 1 {
		t.Fatal("successful discovery did not include completed current/previous sources")
	}
	phase.Store(1)
	now = now.Add(time.Second)
	c.collectOnce(t.Context())
	if !p.sourceInventoryObservedAt.Equal(now.Add(-time.Second)) || p.sourceInventoryTargetCount != 2 || p.kubernetesLogDiscoveryComplete.Load() != 0 {
		t.Fatal("partial failed discovery replaced the last successful inventory")
	}
	for _, row := range p.sourceObservations {
		if row.SelectedInventoryState != "present" || row.Attempts != 1 {
			t.Fatal("completed source membership or read counters changed", row)
		}
	}
	phase.Store(2)
	now = now.Add(time.Second)
	c.collectOnce(t.Context())
	if p.sourceInventoryTargetCount != 0 || !p.sourceInventoryObservedAt.Equal(now) || unexpectedReads.Load() != 0 {
		t.Fatal("empty successful inventory was ignored or annotation caused log reads")
	}
	for _, row := range p.sourceObservations {
		if row.SelectedInventoryState != "absent" || row.Attempts != 1 {
			t.Fatal("empty inventory fabricated delivery or modified read counters", row)
		}
	}
}

func TestSelectedInventoryConcurrentObservationAndProjection(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	c := newKubernetesLogCollectorWithClient(p, nil)
	target := kubernetesLogTarget{pod: runningLogPod("concurrent"), container: "app"}
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				switch i {
				case 0:
					c.observeSelectedLogInventory([]kubernetesLogTarget{target})
				case 1:
					p.observeLogSource(logSourceObservation{Identity: logTargetKey(target), ObservedAt: time.Now()})
				case 2:
					p.DiagnosticSources(t.Context())
				}
			}
		}()
	}
	wg.Wait()
	if p.sourceObservations[logTargetKey(target)].Attempts != 100 {
		t.Fatal("inventory annotation changed read accounting")
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

func TestSameGapWideningPreservesCountAndUpdatedCoverageBounds(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	now := time.Now().UTC()
	lower, firstUpper, secondUpper := now.Add(-time.Hour), now.Add(-time.Minute), now
	p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: now, Outcome: "queue_rejected",
		GapEvents: 1, GapReason: "rotation_boundary_missing", GapLowerBound: lower, GapUpperBound: firstUpper})
	p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: now, Outcome: "queue_rejected",
		GapReason: "rotation_boundary_missing", GapLowerBound: lower, GapUpperBound: secondUpper})
	p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: now, Outcome: "node_backoff"})
	row := p.sourceObservations["source"]
	if row.GapEvents != 1 || row.GapReason != "rotation_boundary_missing" || !row.GapLowerBound.Equal(lower) || !row.GapUpperBound.Equal(secondUpper) {
		t.Fatal("same-gap extension lost coverage or fabricated another event", row)
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
